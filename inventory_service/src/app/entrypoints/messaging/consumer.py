"""
Консюмер команд склада: inventory.commands -> InventoryService.

Здесь только транспорт и сборка: роутинг, валидация и политика ошибок живут
в router.py (тестируются без Kafka и БД).

Дедуп по commandId и запись события в outbox делаются ВНУТРИ транзакции
сервиса: консюмер их не делает - иначе бизнес-эффект и журнал/outbox
разъедутся при падении между ними (dual write).

Всё состояние собирается фабриками (create_app) - глобалей уровня модуля нет.
"""

import asyncio
from typing import Any

import structlog
from dishka_faststream import FromDishka, setup_dishka
from faststream import AckPolicy, FastStream
from faststream.kafka import KafkaBroker, KafkaMessage

from app.application.services.inventory_service import InventoryServiceProtocol
from app.core.logging import setup_logging
from app.core.settings import Settings, get_settings
from app.entrypoints.messaging.router import build_dlq_envelope, process_command_message
from app.infrastructure.di import create_container

logger = structlog.get_logger(__name__)


def create_broker(settings: Settings) -> KafkaBroker:
    return KafkaBroker(settings.KAFKA_BOOTSTRAP_SERVERS)


def register_subscribers(broker: KafkaBroker, settings: Settings) -> None:
    dlq_topic = settings.KAFKA_COMMANDS_DLQ_TOPIC

    # Инварианты подписчика (менять нельзя): group_id - без него offset'ы не
    # коммитятся и рестарт теряет команды; earliest - не пропускаем пришедшее
    # до старта; NACK_ON_ERROR - at-least-once, дубли гасит журнал по commandId
    @broker.subscriber(
        settings.KAFKA_COMMANDS_TOPIC,
        group_id=settings.KAFKA_CONSUMER_GROUP,
        auto_offset_reset="earliest",
        ack_policy=AckPolicy.NACK_ON_ERROR,
    )
    async def handle_commands(
        message: dict,
        service: FromDishka[InventoryServiceProtocol],
        kafka_message: KafkaMessage,
    ) -> None:
        raw = getattr(kafka_message, "raw_message", None)

        async def send_to_dlq(original: Any, error: Exception) -> None:
            # публикацию в DLQ не глотаем: упала - исключение всплывёт, сработает
            # NACK, команда придёт снова и снова попробует уехать в DLQ
            await broker.publish(
                build_dlq_envelope(
                    original=original,
                    source_topic=settings.KAFKA_COMMANDS_TOPIC,
                    consumer_group=settings.KAFKA_CONSUMER_GROUP,
                    error=error,
                    partition=getattr(raw, "partition", None),
                    offset=getattr(raw, "offset", None),
                ),
                topic=dlq_topic,
            )
            logger.error(
                "poison_command_sent_to_dlq",
                dlq_topic=dlq_topic,
                error_class=type(error).__name__,
                error=str(error),
            )

        await process_command_message(
            message=message,
            service=service,
            send_to_dlq=send_to_dlq,
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
        logger.info("inventory_consumer_stopped")

    return app


async def main() -> None:
    await create_app().run()


if __name__ == "__main__":
    asyncio.run(main())
