import asyncio
import json
import structlog
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, OffsetAndMetadata
from dishka import AsyncContainer
from pydantic import ValidationError

from app.schemas.events import PaymentEvent
from app.services.event_handler import PaymentEventHandlerProtocol

logger = structlog.get_logger()


class AnalyticsConsumerRunner:
    def __init__(
        self,
        consumer: AIOKafkaConsumer,
        producer: AIOKafkaProducer,
        container: AsyncContainer,
        dlq_topic: str,
    ):
        self._consumer = consumer
        self._producer = producer
        self._container = container
        self._dlq_topic = dlq_topic
        self._stop_event = asyncio.Event()

    async def _send_to_dlq(
        self, raw_message: bytes, error_msg: str, original_offset: int
    ):
        try:
            headers = [
                ("error", error_msg.encode("utf-8")),
                ("original_offset", str(original_offset).encode("utf-8")),
            ]
            await self._producer.send_and_wait(
                topic=self._dlq_topic,
                value=raw_message,
                headers=headers,
            )
            logger.info(
                "message_sent_to_dlq",
                topic=self._dlq_topic,
                offset=original_offset,
            )
        except Exception as dlq_e:
            # если даже в DLQ не смогли отправить, можно уронить сервис или проигнорировать.
            # для надежности кидаем исключение, чтобы не потерять сообщение
            logger.error(
                "failed_to_send_to_dlq",
                error=str(dlq_e),
                original_error=error_msg,
            )
            raise

    async def run(self):
        logger.info("analytics_consumer_runner_started", dlq_topic=self._dlq_topic)

        try:
            while not self._stop_event.is_set():
                batches = await self._consumer.getmany(timeout_ms=1000, max_records=100)

                for tp, messages in batches.items():
                    if not messages:
                        continue

                    for msg in messages:
                        try:
                            event = PaymentEvent.model_validate_json(msg.value)

                            async with self._container() as request_container:
                                handler = await request_container.get(
                                    PaymentEventHandlerProtocol
                                )
                                await handler.handle(event)

                        except ValidationError as e:
                            logger.error(
                                "invalid_event_payload", error=str(e), offset=msg.offset
                            )
                            # отправляем в DLQ и продолжаем обработку
                            await self._send_to_dlq(
                                msg.value, f"ValidationError: {e}", msg.offset
                            )
                        except Exception as e:
                            # бизнес-ошибка или ошибка инфраструктуры
                            logger.exception(
                                "failed_to_process_message",
                                error=str(e),
                                offset=msg.offset,
                            )
                            await self._send_to_dlq(
                                msg.value, f"ProcessingError: {e}", msg.offset
                            )

                    last_offset = messages[-1].offset
                    await self._consumer.commit(
                        {tp: OffsetAndMetadata(last_offset + 1, "")}
                    )
                    logger.debug(
                        "partition_offsets_committed",
                        partition=tp.partition,
                        next_offset=last_offset + 1,
                    )

        except asyncio.CancelledError:
            logger.info("analytics_consumer_runner_cancelled")
        finally:
            logger.info("analytics_consumer_runner_stopped")

    async def stop(self):
        logger.info("analytics_consumer_runner_stopping")
        self._stop_event.set()
