"""
Консюмер финализации заказов: saga.completed / saga.cancelled / saga.failed
из orders.events -> финальный статус заказа + инвалидация кэша
(application/services/saga_events.py).

Инварианты подписчика выставлены и менять их нельзя: group_id (иначе offset'ы
не коммитятся и рестарт теряет события), auto_offset_reset="earliest",
ack_policy=NACK_ON_ERROR (at-least-once; дубли гасит processed_events).

Всё состояние собирается фабриками (create_app) - глобалей уровня модуля нет.
"""

import asyncio
from typing import Any

import structlog
from dishka_faststream import FromDishka, setup_dishka
from faststream import AckPolicy, FastStream
from faststream.kafka import KafkaBroker

from app.application.services.saga_events import SagaEventsHandlerService
from app.core.logging import setup_logging
from app.core.settings import Settings, get_settings
from app.domain.clock import utc_now
from app.infrastructure.di import create_container

logger = structlog.get_logger(__name__)


def build_dlq_envelope(
    original: dict[str, Any],
    source_topic: str,
    consumer_group: str,
    error_message: str,
) -> dict[str, Any]:
    """Конверт contracts/envelope/dlq-envelope.v1 (осознанная дупликация
    между сервисами - общих пакетов нет по легенде мультирепо)"""
    return {
        "original": original,
        "dlqMeta": {
            "sourceTopic": source_topic,
            "consumerGroup": consumer_group,
            "errorClass": "PoisonMessage",
            "errorMessage": error_message,
            "retryCount": 0,
            "redriveCount": 0,
            "failedAt": utc_now().isoformat(),
        },
    }


def create_broker(settings: Settings) -> KafkaBroker:
    return KafkaBroker(settings.KAFKA_BOOTSTRAP_SERVERS)


def register_subscribers(broker: KafkaBroker, settings: Settings) -> None:
    @broker.subscriber(
        settings.KAFKA_EVENTS_TOPIC,
        group_id=settings.KAFKA_CONSUMER_GROUP,
        auto_offset_reset="earliest",
        ack_policy=AckPolicy.NACK_ON_ERROR,
    )
    async def handle_saga_events(
        message: dict, handler: FromDishka[SagaEventsHandlerService]
    ) -> None:
        action = await handler.handle(message)
        if action == "poison":
            # контрактный брак финального события: в DLQ на ручной разбор
            envelope = build_dlq_envelope(
                message,
                settings.KAFKA_EVENTS_TOPIC,
                settings.KAFKA_CONSUMER_GROUP,
                "malformed saga finalization event",
            )
            await broker.publish(envelope, topic=settings.KAFKA_EVENTS_DLQ_TOPIC)
            logger.error(
                "poison_message_sent_to_dlq",
                dlq_topic=settings.KAFKA_EVENTS_DLQ_TOPIC,
            )


def create_app() -> FastStream:
    setup_logging()
    settings = get_settings()
    container = create_container(settings)

    broker = create_broker(settings)
    setup_dishka(container=container, broker=broker, auto_inject=True)
    register_subscribers(broker, settings)

    app = FastStream(broker)

    @app.after_shutdown
    async def close_container() -> None:
        await container.close()
        logger.info("order_saga_consumer_stopped")

    return app


async def main() -> None:
    await create_app().run()


if __name__ == "__main__":
    asyncio.run(main())
