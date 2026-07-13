"""
Relay единого outbox: публикует PENDING-записи в Kafka. Топик и ключ берутся
из самой записи (ADR-006). Гарантия ADR-002: событие уходит в шину тогда и
только тогда, когда бизнес-эффект закоммичен - dual write исключён.

Механика общая с payment_service/orchestrator_service.
"""

import asyncio

import structlog

from app.application.ports.outbox_publisher import (
    OutboxPublisherProtocol,
    OutboxScopeFactory,
)
from app.domain.outbox import OutboxStatus

logger = structlog.get_logger()

# сколько символов ошибки сохраняем в outbox.last_error
_LAST_ERROR_MAX_LEN = 1000


class OutboxRelayService:
    def __init__(
        self,
        publisher: OutboxPublisherProtocol,
        scope_factory: OutboxScopeFactory,
        max_publish_attempts: int = 5,
    ) -> None:
        self._publisher = publisher
        self._scope_factory = scope_factory
        self._max_publish_attempts = max_publish_attempts
        self._is_running = False

    async def run(self, poll_interval: float = 2.0, batch_size: int = 50) -> None:
        self._is_running = True
        logger.info("outbox_relay_started")

        while self._is_running:
            try:
                await self._process_batch(batch_size)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("outbox_relay_error")

            if self._is_running:
                await asyncio.sleep(poll_interval)

        logger.info("outbox_relay_stopped")

    async def _process_batch(self, batch_size: int) -> None:
        # новый scope на каждую пачку - свежая сессия и транзакция
        async with self._scope_factory() as scope:
            async with scope.uow:
                messages = await scope.outbox_repo.get_unpublished(limit=batch_size)
                if not messages:
                    return

                published = 0
                for message in messages:
                    try:
                        await self._publisher.publish(message)
                    except Exception as e:
                        # фиксируем неудачную попытку; после max_publish_attempts
                        # запись считается "ядовитой" и помечается FAILED, чтобы
                        # не блокировать очередь бесконечными ретраями
                        message.attempts += 1
                        message.last_error = str(e)[:_LAST_ERROR_MAX_LEN]

                        if message.attempts >= self._max_publish_attempts:
                            message.status = OutboxStatus.FAILED
                            logger.error(
                                "outbox_message_failed_permanently",
                                message_id=str(message.id),
                                type=message.type,
                                topic=message.topic,
                                attempts=message.attempts,
                                error=message.last_error,
                            )
                        else:
                            logger.warning(
                                "outbox_message_publish_failed",
                                message_id=str(message.id),
                                type=message.type,
                                topic=message.topic,
                                attempts=message.attempts,
                                error=message.last_error,
                            )

                        await scope.outbox_repo.update(message)
                        # прерываем батч: более новые сообщения не должны обгонять
                        # упавшее (порядок событий одного заказа - часть контракта)
                        break

                    message.status = OutboxStatus.SUCCESS
                    await scope.outbox_repo.update(message)
                    published += 1

                if published:
                    logger.info("outbox_relay_batch_processed", count=published)

    def stop(self) -> None:
        self._is_running = False
        logger.info("outbox_relay_stopping")
