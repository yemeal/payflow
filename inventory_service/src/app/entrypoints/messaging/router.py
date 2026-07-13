"""
Роутер команд склада: чистая граничная логика без брокера и DI.

Вынесен из consumer.py, чтобы тестироваться без Kafka, FastStream и Postgres:
на вход - dict сообщения и сервис, на выход - вызов сервиса либо публикация
в DLQ через переданную функцию.

Политика ошибок (docs/saga-design.md, 9.10):
  - невалидный конверт / неизвестный тип команды - poison: ретрай даст тот же
    результат, поэтому DLQ и ACK (иначе бесконечный NACK-цикл травит партицию);
  - всё остальное (БД недоступна, гонка команд) - восстановимо: исключение
    всплывает, NACK_ON_ERROR переигрывает команду, дубль погасит журнал
    идемпотентности по commandId.
"""

from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

import structlog
from pydantic import ValidationError

from app.application.ports.dto.commands import (
    CancelReservationCommand,
    CommandCorrelation,
    CommitReservationCommand,
    ReserveCommand,
)
from app.application.services.inventory_service import InventoryServiceProtocol
from app.domain.reservations import ReservationItem
from app.entrypoints.messaging.exceptions import UnknownCommandError
from app.entrypoints.messaging.schemas.commands import (
    CancelReservationCommandEnvelope,
    CommandMetadata,
    CommitReservationCommandEnvelope,
    ReserveCommandEnvelope,
    extract_command_type,
)

logger = structlog.get_logger(__name__)

# Невосстановимые ошибки: битый конверт, отсутствие обязательных полей,
# неизвестный тип команды. Всё остальное считаем временным сбоем.
NON_RETRIABLE_ERRORS: tuple[type[Exception], ...] = (
    ValidationError,
    UnknownCommandError,
)

# (сообщение, ошибка) -> публикация в DLQ
DlqPublisher = Callable[[Any, Exception], Awaitable[None]]


def _correlation(metadata: CommandMetadata) -> CommandCorrelation:
    """Echo-блок: значения команды возвращаются в событие как есть"""
    return CommandCorrelation(
        saga_id=metadata.saga_id,
        business_key=metadata.business_key,
        command_id=metadata.command_id,
    )


async def _dispatch_reserve(
    service: InventoryServiceProtocol, envelope: ReserveCommandEnvelope
) -> None:
    await service.reserve(
        ReserveCommand(
            correlation=_correlation(envelope.metadata),
            order_id=envelope.data.order_id,
            items=[
                ReservationItem(product_id=item.product_id, quantity=item.quantity)
                for item in envelope.data.items
            ],
            ttl_seconds=envelope.data.ttl_seconds,
        )
    )


async def _dispatch_commit(
    service: InventoryServiceProtocol, envelope: CommitReservationCommandEnvelope
) -> None:
    await service.commit_reservation(
        CommitReservationCommand(
            correlation=_correlation(envelope.metadata),
            order_id=envelope.data.order_id,
        )
    )


async def _dispatch_cancel(
    service: InventoryServiceProtocol, envelope: CancelReservationCommandEnvelope
) -> None:
    await service.cancel_reservation(
        CancelReservationCommand(
            correlation=_correlation(envelope.metadata),
            order_id=envelope.data.order_id,
        )
    )


# тип команды -> (схема конверта, обработчик). Валидация здесь, в единой точке
# под политикой ошибок: ValidationError отсюда уходит в DLQ, а не в NACK-цикл
ROUTES: dict[str, tuple[type, Callable[..., Awaitable[None]]]] = {
    "inventory.reserve": (ReserveCommandEnvelope, _dispatch_reserve),
    "inventory.commit_reservation": (
        CommitReservationCommandEnvelope,
        _dispatch_commit,
    ),
    "inventory.cancel_reservation": (
        CancelReservationCommandEnvelope,
        _dispatch_cancel,
    ),
}


def build_dlq_envelope(
    original: Any,
    source_topic: str,
    consumer_group: str,
    error: Exception,
    partition: int | None = None,
    offset: int | None = None,
) -> dict[str, Any]:
    """Конверт contracts/envelope/dlq-envelope.v1"""
    dlq_meta: dict[str, Any] = {
        "sourceTopic": source_topic,
        "consumerGroup": consumer_group,
        "errorClass": type(error).__name__,
        "errorMessage": str(error)[:500],
        "retryCount": 0,
        # re-drive инкрементит счётчик; лимит переигровок - 2 (contracts/README)
        "redriveCount": 0,
        "failedAt": datetime.now(timezone.utc).isoformat(),
    }
    if partition is not None:
        dlq_meta["partition"] = partition
    if offset is not None:
        dlq_meta["offset"] = offset

    return {"original": original, "dlqMeta": dlq_meta}


async def process_command_message(
    message: Any,
    service: InventoryServiceProtocol,
    send_to_dlq: DlqPublisher,
) -> None:
    """Обработка одной команды под политикой ошибок"""
    command_type = extract_command_type(message)
    try:
        route = ROUTES.get(command_type)
        if route is None:
            raise UnknownCommandError(command_type)

        schema, dispatch = route
        envelope = schema.model_validate(message)
        logger.info(
            "inventory_command_received",
            command_type=command_type,
            command_id=envelope.metadata.command_id,
        )
        await dispatch(service, envelope)
    except NON_RETRIABLE_ERRORS as error:
        # poison: уводим в DLQ и штатно возвращаемся -> offset коммитится,
        # партиция продолжает жить
        await send_to_dlq(message, error)
