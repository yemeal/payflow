"""
Финализация заказа по событиям саги (saga.completed / saga.cancelled / saga.failed).

Инварианты:
 - дедупликация по event_id и смена статуса заказа - В ОДНОЙ транзакции;
 - кэш инвалидируется ПОСЛЕ коммита (инвалидация до коммита позволила бы
   конкуренту перечитать старый статус из БД и снова положить его в кэш);
 - saga.failed для заказа означает CANCELLED + ERROR-лог (алерт-сигнал):
   пользователь не должен видеть вечный PENDING.
"""

from typing import Any, Literal
from uuid import UUID

import structlog

from app.application.ports.cache import OrderCacheProtocol
from app.application.ports.repositories import (
    OrderRepositoryProtocol,
    ProcessedEventRepositoryProtocol,
)
from app.application.ports.uow import AsyncUOWProtocol
from app.domain.orders import OrderStatus
from app.domain.processed_events import ProcessedEvent

logger = structlog.get_logger()

HandleAction = Literal["processed", "duplicate", "ignored", "poison"]

_STATUS_BY_EVENT: dict[str, OrderStatus] = {
    "saga.completed": OrderStatus.COMPLETED,
    "saga.cancelled": OrderStatus.CANCELLED,
    "saga.failed": OrderStatus.CANCELLED,
}

_FINAL_STATUSES = frozenset({OrderStatus.COMPLETED, OrderStatus.CANCELLED})


class SagaEventsHandlerService:
    def __init__(
        self,
        orders: OrderRepositoryProtocol,
        processed_events: ProcessedEventRepositoryProtocol,
        cache: OrderCacheProtocol,
        uow: AsyncUOWProtocol,
    ) -> None:
        self._orders = orders
        self._processed_events = processed_events
        self._cache = cache
        self._uow = uow

    async def handle(self, message: dict[str, Any]) -> HandleAction:
        metadata = message.get("metadata")
        if not isinstance(metadata, dict):
            return "poison"
        event_type = str(metadata.get("event_type", ""))
        if event_type not in _STATUS_BY_EVENT:
            # общая шина orders.events: order.created, inventory.* - не наши
            return "ignored"

        try:
            event_id = UUID(str(metadata.get("event_id")))
        except (ValueError, TypeError):
            return "poison"
        data = message.get("data")
        try:
            order_id = UUID(str((data or {}).get("orderId")))
        except (ValueError, TypeError, AttributeError):
            return "poison"

        target_status = _STATUS_BY_EVENT[event_type]
        log = logger.bind(
            event_id=str(event_id), event_type=event_type, order_id=str(order_id)
        )

        async with self._uow:
            fresh = await self._processed_events.try_mark_processed(
                ProcessedEvent(event_id=event_id, event_type=event_type)
            )
            if not fresh:
                log.info("duplicate_event_skipped")
                return "duplicate"

            order = await self._orders.get_for_update(order_id)
            if order is None:
                # дедуп зафиксирован: повтор не поможет, событие ссылается в никуда
                log.warning("saga_event_for_unknown_order")
                return "ignored"
            if order.status in _FINAL_STATUSES:
                log.info("order_already_final", status=order.status.value)
                return "ignored"

            order.status = target_status
            await self._orders.update(order)

        # после коммита: следующий GET прочитает свежий статус из БД
        await self._cache.invalidate(order_id)

        if event_type == "saga.failed":
            # сигнал тревоги: сага не смогла завершиться штатно, нужен ручной разбор
            log.error("saga_failed_order_cancelled", reason=(data or {}).get("reason"))
        else:
            log.info("order_finalized", status=target_status.value)
        return "processed"
