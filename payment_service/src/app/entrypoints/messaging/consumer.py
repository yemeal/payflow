from typing import Callable

import structlog
from faststream import FastStream, AckPolicy
from faststream.kafka import KafkaBroker, KafkaMessage
from dishka_faststream import setup_dishka, FromDishka
from pydantic import ValidationError, BaseModel

from app.infrastructure.di import create_container
from app.core.logging import setup_logging
from app.core.settings import get_settings
from app.entrypoints.messaging.schemas.commands import (
    ProcessPaymentCommand,
    CommandMetadata,
)
from app.entrypoints.messaging.exceptions import UnknownCommandError
from app.entrypoints.http.schemas.payments import PaymentCreate, PaymentResponse
from app.application.exceptions.idempotency import (
    IdempotencyKeyPayloadMismatchError,
    IdempotencyStateInconsistencyError,
)
from app.application.ports.correlation import CommandCorrelationStoreProtocol
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


# Ошибки, при которых повтор бессмыслен: битые данные, неизвестный тип команды,
# логически несовместимый payload. Такие команды уводим в DLQ и коммитим offset,
# чтобы не потерять их молча и не заблокировать партицию бесконечными ретраями.
# Всё остальное (Redis/БД/провайдер недоступны и прочие временные сбои) считаем
# восстановимым: пробрасываем исключение -> NACK_ON_ERROR -> Kafka переигрывает
# команду до восстановления зависимости; дубли гасит Two-Level Idempotency.
NON_RETRIABLE_ERRORS: tuple[type[Exception], ...] = (
    ValidationError,
    UnknownCommandError,
    IdempotencyKeyPayloadMismatchError,
    IdempotencyStateInconsistencyError,
)


class CommandRouter:
    """Роутер команд: по типу выбирает схему валидации и обработчик."""

    def __init__(self):
        self._handlers = {}

    def register(self, command_type: str, schema: type):
        def decorator(func):
            self._handlers[command_type] = (schema, func)
            return func

        return decorator

    async def handle(
        self,
        command_type: str,
        msg: dict,
        payment_service: PaymentServiceProtocol,
        idempotency_service: IdempotencyService,
        correlations: CommandCorrelationStoreProtocol,
    ):
        entry = self._handlers.get(command_type)
        if entry is None:
            # раньше здесь молча возвращали None (offset коммитился, команда терялась);
            # теперь это явная невосстановимая ошибка -> уходит в DLQ
            raise UnknownCommandError(command_type)

        schema, handler = entry
        # Валидация здесь, в единой точке под политикой обработки ошибок:
        # ValidationError отсюда попадает в NON_RETRIABLE_ERRORS и уводится в DLQ,
        # а не всплывает необёрнутой в NACK (иначе битый payload = poison pill,
        # который переигрывается бесконечно и блокирует партицию).
        command = schema.model_validate(msg)
        return await handler(command, payment_service, idempotency_service, correlations)


router = CommandRouter()


@router.register("payment.process", ProcessPaymentCommand)
async def handle_process_payment_command(
    command: ProcessPaymentCommand,
    payment_service: PaymentServiceProtocol,
    idempotency_service: IdempotencyService,
    correlations: CommandCorrelationStoreProtocol,
):
    idempotency_key = str(command.metadata.command_id)

    # Транспортная корреляция саги (contracts/README п.1): запоминается ДО создания
    # платежа - иначе relay успеет опубликовать событие раньше, чем correlation
    # станет известна, и оркестратор потеряет ответ. Домен платежа о ней не знает:
    # подставит её в конверт транспортный адаптер (CorrelationEnrichingPublisher).
    if command.metadata.saga_id is not None and command.metadata.business_key:
        await correlations.remember(
            command_id=idempotency_key,
            correlation={
                "sagaId": str(command.metadata.saga_id),
                "businessKey": command.metadata.business_key,
                "commandId": idempotency_key,
            },
        )

    # TODO **command.data
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


async def send_command_to_dlq(
    msg: dict, error: Exception, message: KafkaMessage | None
) -> None:
    """
    Публикует невосстановимую команду в DLQ-топик с диагностическими заголовками.

    Если публикация в DLQ не удалась, пробрасываем исключение: тогда сработает NACK
    и команда не потеряется (будет доставлена повторно и снова попробует уйти в DLQ).
    """
    partition = ""
    offset = ""
    if message is not None:
        raw = getattr(message, "raw_message", None)
        partition = str(getattr(raw, "partition", ""))
        offset = str(getattr(raw, "offset", ""))

    headers = {
        "x-error-type": type(error).__name__,
        "x-error-detail": str(error)[:500],
        "x-original-topic": settings.KAFKA_COMMANDS_TOPIC,
        "x-original-partition": partition,
        "x-original-offset": offset,
    }
    await broker.publish(
        msg,
        topic=settings.KAFKA_COMMANDS_DLQ_TOPIC,
        headers=headers,
    )
    logger.error(
        "command_sent_to_dlq",
        dlq_topic=settings.KAFKA_COMMANDS_DLQ_TOPIC,
        error_type=type(error).__name__,
        error=str(error),
        original_partition=partition,
        original_offset=offset,
    )


async def process_command_message(
    msg: dict,
    payment_service: PaymentServiceProtocol,
    idempotency_service: IdempotencyService,
    correlations: CommandCorrelationStoreProtocol,
    message: KafkaMessage | None = None,
) -> None:
    """
    Граничная логика обработки одной команды с политикой ошибок.

    Вынесена из подписчика отдельной чистой функцией, чтобы её можно было
    тестировать без FastStream. Правило:
      - NON_RETRIABLE_ERRORS (битые данные / неизвестная команда) -> DLQ, затем ACK;
      - всё остальное -> исключение всплывает -> NACK_ON_ERROR -> переигрывание.
    """
    try:
        metadata = CommandMetadata.model_validate(msg.get("metadata", {}))
        logger.info("routing_command", command_type=metadata.command_type)
        await router.handle(
            command_type=metadata.command_type,
            msg=msg,
            payment_service=payment_service,
            idempotency_service=idempotency_service,
            correlations=correlations,
        )
    except NON_RETRIABLE_ERRORS as e:
        # невосстановимо (битые данные / неизвестная команда): в DLQ и ACK.
        # штатный возврат после успешной публикации коммитит offset.
        await send_command_to_dlq(msg, e, message)
    # Любая другая ошибка (Redis/БД/провайдер недоступны, неожиданный сбой)
    # НЕ перехватывается здесь: она всплывает -> NACK_ON_ERROR -> Kafka
    # переигрывает команду. Так временные сбои не приводят к потере команд.


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
    correlations: FromDishka[CommandCorrelationStoreProtocol],
    message: KafkaMessage,
):
    await process_command_message(
        msg=msg,
        payment_service=payment_service,
        idempotency_service=idempotency_service,
        correlations=correlations,
        message=message,
    )


if __name__ == "__main__":
    import asyncio

    asyncio.run(app.run())
