import structlog
from faststream import FastStream, AckPolicy
from faststream.kafka import KafkaBroker
from dishka_faststream import setup_dishka, FromDishka

from app.infrastructure.di import create_container
from app.core.logging import setup_logging
from app.core.settings import get_settings
from app.entrypoints.messaging.schemas.commands import (
    ProcessPaymentCommand,
    CommandMetadata,
)
from app.entrypoints.http.schemas.payments import PaymentCreate, PaymentResponse
from app.application.services.idempotency import IdempotencyService
from app.application.services.payment_service import PaymentServiceProtocol

setup_logging()
logger = structlog.get_logger(__name__)

settings = get_settings()

broker = KafkaBroker(settings.KAFKA_BOOTSTRAP_SERVERS)
app = FastStream(broker)

# Setup Dishka container
container = create_container()
setup_dishka(container=container, broker=broker, auto_inject=True)


class CommandRouter:
    """Роутер для маршрутизации команд по их типу"""

    def __init__(self):
        self._handlers = {}

    def register(self, command_type: str):
        def decorator(func):
            self._handlers[command_type] = func
            return func

        return decorator

    async def handle(
        self,
        command_type: str,
        msg: dict,
        payment_service: PaymentServiceProtocol,
        idempotency_service: IdempotencyService,
    ):
        handler = self._handlers.get(command_type)
        if not handler:
            logger.error("no_handler_found_for_command", command_type=command_type)
            return None
        return await handler(msg, payment_service, idempotency_service)


router = CommandRouter()


@router.register("payment.process")
async def handle_process_payment_command(
    msg: dict,
    payment_service: PaymentServiceProtocol,
    idempotency_service: IdempotencyService,
):
    # Валидируем через Pydantic
    command = ProcessPaymentCommand.model_validate(msg)
    idempotency_key = str(command.metadata.command_id)

    payload = PaymentCreate(
        amount=command.data.amount,
        currency=command.data.currency,
        customer_id=command.data.customer_id,
        description=command.data.description,
    )

    payload_dict = payload.model_dump(mode="json")
    db_lookup = payment_service.build_idempotency_db_lookup()

    # Применяем Two-Level Idempotency
    async with idempotency_service(idempotency_key, payload_dict, db_lookup) as guard:
        if guard.has_cached_result and guard.cached_status_code is not None:
            logger.info("payment_command_idempotent_hit", command_id=idempotency_key)
            return guard.cached_response

        created_payment = await payment_service.create(payload, idempotency_key)
        response = PaymentResponse.model_validate(created_payment).model_dump(
            mode="json"
        )

        guard.set_result(status_code=201, response=response)
        logger.info(
            "payment_command_processed_successfully",
            command_id=idempotency_key,
            payment_id=str(created_payment.id),
        )
        return response


# group_id обязателен: без него offset'ы не коммитятся в Kafka,
# и любой рестарт контейнера теряет команды, пришедшие во время даунтайма.
# NACK_ON_ERROR: ack только после успешной обработки (at-least-once),
# при исключении сообщение переигрывается; дубли гасятся Two-Level Idempotency.
@broker.subscriber(
    settings.KAFKA_COMMANDS_TOPIC,
    group_id=settings.KAFKA_CONSUMER_GROUP,
    auto_offset_reset="earliest",
    ack_policy=AckPolicy.NACK_ON_ERROR,
)
async def handle_commands(
    msg: dict,
    payment_service: FromDishka[PaymentServiceProtocol],
    idempotency_service: FromDishka[IdempotencyService],
):
    try:
        meta_dict = msg.get("metadata", {})
        metadata = CommandMetadata.model_validate(meta_dict)
        command_type = metadata.command_type
    except Exception as e:
        logger.error("invalid_command_metadata", error=str(e), msg=msg)
        return

    logger.info("routing_command", command_type=command_type)
    await router.handle(
        command_type=command_type,
        msg=msg,
        payment_service=payment_service,
        idempotency_service=idempotency_service,
    )


if __name__ == "__main__":
    import asyncio

    asyncio.run(app.run())
