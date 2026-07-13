"""
Ядро склада: три команды саги (contracts/inventory/*).

Транзакционный контур у всех трёх одинаков (UoW, одна транзакция):
  дедуп по commandId  ->  бизнес-эффект (сток + резерв)  ->  запись в outbox.
Разрыв этого контура ломает либо идемпотентность (эффект без журнала),
либо доставку (эффект без события) - dual write, ADR-002.

Домен склада не знает о сагах: correlation команды не попадает ни в
reservations, ни в stock_items. Она живёт в журнале processed_commands
(сохранённый конверт ответа) и в payload outbox-записи - это транспорт.
"""

import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Protocol
from uuid import UUID

import structlog

from app.application.ports.dto.commands import (
    CancelReservationCommand,
    CommandCorrelation,
    CommitReservationCommand,
    ReserveCommand,
)
from app.application.ports.repositories import (
    OutboxRepositoryProtocol,
    ProcessedCommandRepositoryProtocol,
    ReservationRepositoryProtocol,
    StockRepositoryProtocol,
)
from app.application.ports.uow import AsyncUOWProtocol
from app.core.settings import Settings
from app.domain.events import (
    EVENT_COMMIT_FAILED,
    EVENT_RESERVATION_CANCELLED,
    EVENT_RESERVATION_COMMITTED,
    EVENT_RESERVE_FAILED,
    EVENT_RESERVED,
    FAILURE_INSUFFICIENT_STOCK,
    FAILURE_RESERVATION_CONFLICT,
    FAILURE_RESERVATION_EXPIRED,
    FAILURE_RESERVATION_NOT_FOUND,
    FAILURE_UNKNOWN_PRODUCT,
    InventoryEvent,
    failure_block,
)
from app.domain.exceptions.inventory import ConcurrentCommandError
from app.domain.outbox import OutboxKind, OutboxMessage
from app.domain.processed_commands import ProcessedCommand
from app.domain.reservations import (
    Reservation,
    ReservationItem,
    ReservationStatus,
    utc_now,
)

logger = structlog.get_logger()

SOURCE = "inventory-service"
EVENT_VERSION = "1.0"


class InventoryServiceProtocol(Protocol):
    async def reserve(self, command: ReserveCommand) -> None: ...

    async def commit_reservation(self, command: CommitReservationCommand) -> None: ...

    async def cancel_reservation(self, command: CancelReservationCommand) -> None: ...


def _aggregate(items: list[ReservationItem]) -> dict[str, int]:
    """Один product_id может прийти несколькими строками - количества
    складываем, иначе вторая строка перезатрёт первую и сток разъедется"""
    required: dict[str, int] = defaultdict(int)
    for item in items:
        required[item.product_id] += item.quantity
    return dict(required)


class InventoryService:
    def __init__(
        self,
        stock: StockRepositoryProtocol,
        reservations: ReservationRepositoryProtocol,
        processed_commands: ProcessedCommandRepositoryProtocol,
        outbox: OutboxRepositoryProtocol,
        uow: AsyncUOWProtocol,
        settings: Settings,
    ) -> None:
        self._stock = stock
        self._reservations = reservations
        self._processed = processed_commands
        self._outbox = outbox
        self._uow = uow
        self._settings = settings

    # --- публичные команды ---

    async def reserve(self, command: ReserveCommand) -> None:
        await self._execute(command.correlation, lambda: self._do_reserve(command))

    async def commit_reservation(self, command: CommitReservationCommand) -> None:
        await self._execute(command.correlation, lambda: self._do_commit(command))

    async def cancel_reservation(self, command: CancelReservationCommand) -> None:
        await self._execute(command.correlation, lambda: self._do_cancel(command))

    # --- транзакционный контур: дедуп + эффект + outbox ---

    async def _execute(
        self,
        correlation: CommandCorrelation,
        business: Callable[[], Awaitable[InventoryEvent]],
    ) -> None:
        log = logger.bind(
            command_id=correlation.command_id,
            saga_id=correlation.saga_id,
            business_key=correlation.business_key,
        )

        async with self._uow:
            stored = await self._processed.get(correlation.command_id)
            if stored is not None:
                # дубль команды: бизнес-эффект НЕ повторяем, переиздаём
                # сохранённый ответ как есть (contracts/README, правило 2).
                # event_id тот же - дубль погасит дедуп оркестратора
                await self._enqueue(stored.result, correlation)
                log.info(
                    "inventory_command_duplicate_replayed",
                    event_type=self._event_type_of(stored.result),
                )
                return

            event = await business()
            envelope = self._event_envelope(event, correlation)

            # ON CONFLICT DO NOTHING: если журнал успел занять параллельный
            # обработчик той же команды - откатываем транзакцию целиком (иначе
            # эффект применится дважды). Команда переиграется по NACK и попадёт
            # в ветку дубля выше
            inserted = await self._processed.add_if_absent(
                ProcessedCommand(command_id=correlation.command_id, result=envelope)
            )
            if not inserted:
                raise ConcurrentCommandError(correlation.command_id)

            await self._enqueue(envelope, correlation)
            log.info("inventory_command_processed", event_type=event.event_type)

    async def _enqueue(
        self, envelope: dict[str, Any], correlation: CommandCorrelation
    ) -> None:
        await self._outbox.add(
            OutboxMessage(
                kind=OutboxKind.EVENT,
                topic=self._settings.KAFKA_EVENTS_TOPIC,
                # key = business_key саги (order_id): события одного заказа
                # идут в одну партицию и не обгоняют друг друга
                key=correlation.business_key,
                type=self._event_type_of(envelope),
                payload=envelope,
            )
        )

    @staticmethod
    def _event_type_of(envelope: dict[str, Any]) -> str:
        metadata = envelope.get("metadata")
        if isinstance(metadata, dict):
            return str(metadata.get("event_type", ""))
        return ""

    @staticmethod
    def _event_envelope(
        event: InventoryEvent, correlation: CommandCorrelation
    ) -> dict[str, Any]:
        """Конверт события (contracts/envelope/event-metadata.v1):
        metadata - snake_case, data - camelCase, correlation - echo команды"""
        return {
            "metadata": {
                "event_id": str(uuid.uuid7()),
                "event_type": event.event_type,
                "version": EVENT_VERSION,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "source": SOURCE,
                # echo: значения команды возвращаются непрозрачно, без интерпретации
                "correlation": {
                    "sagaId": correlation.saga_id,
                    "businessKey": correlation.business_key,
                    "commandId": correlation.command_id,
                },
            },
            "data": event.data,
        }

    # --- бизнес-логика ---

    async def _do_reserve(self, command: ReserveCommand) -> InventoryEvent:
        order_id = command.order_id
        existing = await self._reservations.get_by_order_id_for_update(order_id)
        if existing is not None:
            # резерв по заказу уже есть, а команда другая (иначе сработал бы
            # дедуп): ACTIVE - это тот же самый резерв, отвечаем успехом;
            # завершённый - повторно блокировать товар нельзя
            if existing.status is ReservationStatus.ACTIVE:
                logger.info("inventory_reserve_already_active", order_id=str(order_id))
                return self._reserved_event(order_id, existing.expires_at)
            return InventoryEvent(
                EVENT_RESERVE_FAILED,
                {
                    "orderId": str(order_id),
                    "failure": failure_block(
                        FAILURE_RESERVATION_CONFLICT,
                        f"reservation for order {order_id} is already "
                        f"{existing.status.value}",
                        retriable=False,
                    ),
                },
            )

        required = _aggregate(command.items)
        stock_items = await self._stock.get_for_update(sorted(required))
        by_product = {item.product_id: item for item in stock_items}

        unknown = sorted(pid for pid in required if pid not in by_product)
        if unknown:
            return InventoryEvent(
                EVENT_RESERVE_FAILED,
                {
                    "orderId": str(order_id),
                    "failure": failure_block(
                        FAILURE_UNKNOWN_PRODUCT,
                        f"unknown products: {', '.join(unknown)}",
                        retriable=False,
                    ),
                },
            )

        short = {
            pid: (by_product[pid].available, qty)
            for pid, qty in required.items()
            if by_product[pid].available < qty
        }
        if short:
            # БИЗНЕС-отказ: ретрай не поможет (товар сам не появится),
            # retriable=false -> оркестратор компенсирует сагу немедленно
            details = ", ".join(
                f"{pid}: available={available}, requested={quantity}"
                for pid, (available, quantity) in sorted(short.items())
            )
            logger.info(
                "inventory_reserve_insufficient_stock",
                order_id=str(order_id),
                details=details,
            )
            return InventoryEvent(
                EVENT_RESERVE_FAILED,
                {
                    "orderId": str(order_id),
                    "failure": failure_block(
                        FAILURE_INSUFFICIENT_STOCK,
                        f"insufficient stock ({details})",
                        retriable=False,
                    ),
                },
            )

        # резерв не списывает товар: количество переезжает available -> reserved
        for product_id, quantity in required.items():
            item = by_product[product_id]
            item.available -= quantity
            item.reserved += quantity
            await self._stock.update(item)

        ttl_seconds = (
            command.ttl_seconds or self._settings.RESERVATION_DEFAULT_TTL_SECONDS
        )
        expires_at = utc_now() + timedelta(seconds=ttl_seconds)
        await self._reservations.add(
            Reservation(
                order_id=order_id,
                status=ReservationStatus.ACTIVE,
                items=[
                    ReservationItem(product_id=product_id, quantity=quantity)
                    for product_id, quantity in sorted(required.items())
                ],
                expires_at=expires_at,
            )
        )
        logger.info(
            "inventory_reserved",
            order_id=str(order_id),
            ttl_seconds=ttl_seconds,
            expires_at=expires_at.isoformat(),
        )
        return self._reserved_event(order_id, expires_at)

    async def _do_commit(self, command: CommitReservationCommand) -> InventoryEvent:
        order_id = command.order_id
        reservation = await self._reservations.get_by_order_id_for_update(order_id)

        if reservation is None:
            # commit без резерва: рассинхрон саги и склада, ретрай не поможет
            logger.error(
                "inventory_commit_reservation_not_found", order_id=str(order_id)
            )
            return self._commit_failed_event(
                order_id,
                FAILURE_RESERVATION_NOT_FOUND,
                f"no reservation for order {order_id}",
            )

        if reservation.status is ReservationStatus.COMMITTED:
            # повторный commit НОВОЙ командой (дедуп её не ловит): эффект уже
            # применён, отвечаем успехом - иначе сага зависнет на ровном месте
            logger.info("inventory_commit_already_committed", order_id=str(order_id))
            return InventoryEvent(
                EVENT_RESERVATION_COMMITTED, {"orderId": str(order_id)}
            )

        if reservation.status is not ReservationStatus.ACTIVE:
            # истёк или отменён: нарушен инвариант TTL >= дедлайн оплаты + буфер
            # (docs/saga-design.md, 9.8) - оркестратор уводит сагу в FAILED
            logger.error(
                "inventory_commit_on_inactive_reservation",
                order_id=str(order_id),
                status=reservation.status.value,
            )
            return self._commit_failed_event(
                order_id,
                FAILURE_RESERVATION_EXPIRED,
                f"reservation for order {order_id} is {reservation.status.value}",
            )

        # списание: товар уезжает со склада. available не трогаем (он уменьшился
        # ещё при резерве), уменьшается только reserved
        required = _aggregate(reservation.items)
        stock_items = await self._stock.get_for_update(sorted(required))
        by_product = {item.product_id: item for item in stock_items}
        for product_id, quantity in required.items():
            item = by_product.get(product_id)
            if item is None:
                # товар исчез из каталога при живом резерве: чинить нечего,
                # сток по нему не двигаем, но факт фиксируем
                logger.error(
                    "inventory_commit_product_missing",
                    order_id=str(order_id),
                    product_id=product_id,
                )
                continue
            item.reserved = max(0, item.reserved - quantity)
            await self._stock.update(item)

        reservation.status = ReservationStatus.COMMITTED
        reservation.updated_at = utc_now()
        await self._reservations.update(reservation)
        logger.info("inventory_reservation_committed", order_id=str(order_id))
        return InventoryEvent(EVENT_RESERVATION_COMMITTED, {"orderId": str(order_id)})

    async def _do_cancel(self, command: CancelReservationCommand) -> InventoryEvent:
        order_id = command.order_id
        reservation = await self._reservations.get_by_order_id_for_update(order_id)
        cancelled = InventoryEvent(
            EVENT_RESERVATION_CANCELLED, {"orderId": str(order_id)}
        )

        # компенсация идемпотентна и коммутативна с автоистечением: отменять
        # нечего - это успех, а не ошибка (contracts/inventory/cancel-reservation.v1).
        # Иначе сага не смогла бы завершить компенсацию и зависла бы навсегда
        if reservation is None:
            logger.info("inventory_cancel_no_reservation", order_id=str(order_id))
            return cancelled

        if reservation.status is ReservationStatus.COMMITTED:
            # компенсация после списания: товар уже уехал, на склад его не вернуть.
            # Сага так не делает (commit - шаг после pivot), поэтому событие-успех
            # плюс громкий лог для ручного разбора
            logger.error(
                "inventory_cancel_on_committed_reservation", order_id=str(order_id)
            )
            return cancelled

        if reservation.status is not ReservationStatus.ACTIVE:
            logger.info(
                "inventory_cancel_already_released",
                order_id=str(order_id),
                status=reservation.status.value,
            )
            return cancelled

        await self._release_stock(reservation)
        reservation.status = ReservationStatus.CANCELLED
        reservation.updated_at = utc_now()
        await self._reservations.update(reservation)
        logger.info("inventory_reservation_cancelled", order_id=str(order_id))
        return cancelled

    async def _release_stock(self, reservation: Reservation) -> None:
        """Возврат стока по неиспользованному резерву: reserved -> available"""
        required = _aggregate(reservation.items)
        stock_items = await self._stock.get_for_update(sorted(required))
        by_product = {item.product_id: item for item in stock_items}
        for product_id, quantity in required.items():
            item = by_product.get(product_id)
            if item is None:
                logger.error(
                    "inventory_release_product_missing",
                    order_id=str(reservation.order_id),
                    product_id=product_id,
                )
                continue
            item.available += quantity
            item.reserved = max(0, item.reserved - quantity)
            await self._stock.update(item)

    # --- сборка событий ---

    @staticmethod
    def _reserved_event(order_id: UUID, expires_at: datetime) -> InventoryEvent:
        return InventoryEvent(
            EVENT_RESERVED,
            {
                "orderId": str(order_id),
                # в БД naive-UTC -> в конверт уходит явный UTC-суффикс
                "expiresAt": expires_at.replace(tzinfo=timezone.utc).isoformat(),
            },
        )

    @staticmethod
    def _commit_failed_event(order_id: UUID, code: str, message: str) -> InventoryEvent:
        return InventoryEvent(
            EVENT_COMMIT_FAILED,
            {
                "orderId": str(order_id),
                "failure": failure_block(code, message, retriable=False),
            },
        )
