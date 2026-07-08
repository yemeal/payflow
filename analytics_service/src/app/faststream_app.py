import structlog
from faststream import FastStream, AckPolicy
from faststream.kafka import KafkaBroker
from dishka_faststream import setup_dishka, FromDishka, FastStreamProvider
from pydantic import ValidationError

from app.core.di.provider import (
    SettingsProvider,
    DatabaseProvider,
    RedisProvider,
    RepositoriesProvider,
    ServiceProvider,
    KafkaProvider,
)
from dishka import make_async_container
from app.core.settings import get_settings
from app.schemas.events import PaymentEvent
from app.services.event_handler import PaymentEventHandlerProtocol

logger = structlog.get_logger()
settings = get_settings()

broker = KafkaBroker(settings.KAFKA_BOOTSTRAP_SERVERS)
app = FastStream(broker)

# Создаем DI контейнер с добавлением FastStreamProvider
container = make_async_container(
    SettingsProvider(),
    DatabaseProvider(),
    RedisProvider(),
    RepositoriesProvider(),
    ServiceProvider(),
    KafkaProvider(),
    FastStreamProvider(),
)
setup_dishka(container=container, broker=broker, auto_inject=True)

DLQ_TOPIC = f"{settings.KAFKA_TOPIC}.dlq"


# group_id обязателен: без него offset'ы не коммитятся в Kafka,
# и рестарт сервиса теряет события, пришедшие во время даунтайма.
# NACK_ON_ERROR: ack после успешной обработки; единственный raise в хендлере —
# отказ публикации в DLQ, в этом случае сообщение корректно переигрывается.
@broker.subscriber(
    settings.KAFKA_TOPIC,
    group_id=settings.KAFKA_CONSUMER_GROUP,
    auto_offset_reset="earliest",
    ack_policy=AckPolicy.NACK_ON_ERROR,
)
async def handle_payment_event(
    raw_event: dict,
    handler: FromDishka[PaymentEventHandlerProtocol],
):
    try:
        event = PaymentEvent.model_validate(raw_event)
    except ValidationError as e:
        logger.error("invalid_event_payload", error=str(e))
        try:
            await broker.publish(
                message=raw_event,
                topic=DLQ_TOPIC,
                headers={"error": f"ValidationError: {e}"},
            )
            logger.info("invalid_event_payload_sent_to_dlq")
        except Exception as dlq_e:
            logger.error("failed_to_send_invalid_event_to_dlq", error=str(dlq_e))
        return

    logger.info(
        "processing_payment_event",
        event_id=str(event.metadata.event_id),
        event_type=event.metadata.event_type,
    )
    try:
        await handler.handle(event)
    except Exception as e:
        logger.exception(
            "payment_event_processing_failed",
            event_id=str(event.metadata.event_id),
            error=str(e),
        )
        try:
            await broker.publish(
                message=raw_event,
                topic=DLQ_TOPIC,
                headers={
                    "error": f"ProcessingError: {e}",
                    "event_id": str(event.metadata.event_id),
                },
            )
            logger.info(
                "failed_event_sent_to_dlq", event_id=str(event.metadata.event_id)
            )
        except Exception as dlq_e:
            logger.error(
                "failed_to_send_failed_event_to_dlq",
                error=str(dlq_e),
                original_error=str(e),
            )
            raise
