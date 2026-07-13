"""
Общие фикстуры юнит-тестов склада: in-memory фейки портов вместо Postgres/Kafka
(по образцу orchestrator_service). Сервис тестируется как чистая машина:
команда на входе, остатки/резервы/outbox на выходе.

Фейки возвращают КОПИИ доменных моделей: иначе мутация объекта в сервисе
"сохранялась" бы сама собой, без вызова update(), и тесты не заметили бы
потерянную запись.
"""

import uuid
from datetime import datetime, timedelta
from typing import Any, Sequence
from uuid import UUID

import pytest

from app.application.ports.dto.commands import (
    CancelReservationCommand,
    CommandCorrelation,
    CommitReservationCommand,
    ReserveCommand,
)
from app.application.services.inventory_service import InventoryService
from app.core.settings import Settings
from app.domain.outbox import OutboxMessage, OutboxStatus
from app.domain.processed_commands import ProcessedCommand
from app.domain.reservations import (
    Reservation,
    ReservationItem,
    ReservationStatus,
    utc_now,
)
from app.domain.stock import StockItem


class FakeUOW:
    """Транзакция-заглушка: считает входы, ничего не коммитит и не откатывает"""

    def __init__(self) -> None:
        self.entered = 0
        self.failed = 0

    async def __aenter__(self) -> "FakeUOW":
        self.entered += 1
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if exc_type is not None:
            self.failed += 1
        return None


class FakeStockRepository:
    def __init__(self, items: dict[str, int] | None = None) -> None:
        self.items: dict[str, StockItem] = {
            product_id: StockItem(product_id=product_id, available=available, reserved=0)
            for product_id, available in (items or {}).items()
        }
        self.locked: list[list[str]] = []

    async def get_for_update(self, product_ids: Sequence[str]) -> list[StockItem]:
        self.locked.append(list(product_ids))
        return [
            self.items[product_id].model_copy(deep=True)
            for product_id in sorted(product_ids)
            if product_id in self.items
        ]

    async def update(self, item: StockItem) -> StockItem:
        if item.product_id not in self.items:
            raise ValueError(f"stock item {item.product_id} not found for update")
        self.items[item.product_id] = item.model_copy(deep=True)
        return item

    def available(self, product_id: str) -> int:
        return self.items[product_id].available

    def reserved(self, product_id: str) -> int:
        return self.items[product_id].reserved


class FakeReservationRepository:
    def __init__(self) -> None:
        self.by_order: dict[UUID, Reservation] = {}

    async def add(self, reservation: Reservation) -> Reservation:
        if reservation.order_id in self.by_order:
            # эмулируем UNIQUE (order_id) из схемы
            raise ValueError(f"reservation for order {reservation.order_id} exists")
        self.by_order[reservation.order_id] = reservation.model_copy(deep=True)
        return reservation

    async def update(self, reservation: Reservation) -> Reservation:
        if reservation.order_id not in self.by_order:
            raise ValueError(f"reservation {reservation.id} not found for update")
        self.by_order[reservation.order_id] = reservation.model_copy(deep=True)
        return reservation

    async def get_by_order_id(self, order_id: UUID) -> Reservation | None:
        reservation = self.by_order.get(order_id)
        return reservation.model_copy(deep=True) if reservation else None

    async def get_by_order_id_for_update(self, order_id: UUID) -> Reservation | None:
        return await self.get_by_order_id(order_id)

    async def find_expired_active(
        self, now: datetime, limit: int
    ) -> list[Reservation]:
        due = [
            reservation.model_copy(deep=True)
            for reservation in self.by_order.values()
            if reservation.status is ReservationStatus.ACTIVE
            and reservation.expires_at <= now
        ]
        due.sort(key=lambda reservation: reservation.expires_at)
        return due[:limit]

    def status_of(self, order_id: UUID) -> ReservationStatus:
        return self.by_order[order_id].status


class FakeProcessedCommandRepository:
    def __init__(self) -> None:
        self.by_command: dict[str, ProcessedCommand] = {}

    async def get(self, command_id: str) -> ProcessedCommand | None:
        stored = self.by_command.get(command_id)
        return stored.model_copy(deep=True) if stored else None

    async def add_if_absent(self, command: ProcessedCommand) -> bool:
        if command.command_id in self.by_command:
            return False
        self.by_command[command.command_id] = command.model_copy(deep=True)
        return True


class FakeOutboxRepository:
    def __init__(self) -> None:
        self.messages: list[OutboxMessage] = []

    async def add(self, message: OutboxMessage) -> OutboxMessage:
        self.messages.append(message.model_copy(deep=True))
        return message

    async def update(self, message: OutboxMessage) -> OutboxMessage:
        for index, existing in enumerate(self.messages):
            if existing.id == message.id:
                self.messages[index] = message.model_copy(deep=True)
                return message
        raise ValueError(f"outbox message {message.id} not found for update")

    async def get_unpublished(self, limit: int) -> list[OutboxMessage]:
        pending = [
            message.model_copy(deep=True)
            for message in self.messages
            if message.status is OutboxStatus.PENDING
        ]
        return pending[:limit]

    @property
    def event_types(self) -> list[str]:
        return [message.type for message in self.messages]

    @property
    def last_payload(self) -> dict[str, Any]:
        return self.messages[-1].payload

    @property
    def last_data(self) -> dict[str, Any]:
        return self.messages[-1].payload["data"]


@pytest.fixture
def settings() -> Settings:
    # значения задаём явно: тест не должен зависеть от .env разработчика
    return Settings(
        DATABASE_HOST="localhost",
        DATABASE_PORT=5432,
        DATABASE_USER="inventory_user",
        DATABASE_PASSWORD="inventory_password",
        DATABASE_NAME="inventory_db",
        DEV_LOGS=True,
        KAFKA_BOOTSTRAP_SERVERS="localhost:9092",
        RESERVATION_DEFAULT_TTL_SECONDS=2100,
    )


@pytest.fixture
def stock() -> FakeStockRepository:
    return FakeStockRepository({"sku-1": 100, "sku-2": 5})


@pytest.fixture
def reservations() -> FakeReservationRepository:
    return FakeReservationRepository()


@pytest.fixture
def processed_commands() -> FakeProcessedCommandRepository:
    return FakeProcessedCommandRepository()


@pytest.fixture
def outbox() -> FakeOutboxRepository:
    return FakeOutboxRepository()


@pytest.fixture
def uow() -> FakeUOW:
    return FakeUOW()


@pytest.fixture
def service(
    stock: FakeStockRepository,
    reservations: FakeReservationRepository,
    processed_commands: FakeProcessedCommandRepository,
    outbox: FakeOutboxRepository,
    uow: FakeUOW,
    settings: Settings,
) -> InventoryService:
    return InventoryService(
        stock=stock,
        reservations=reservations,
        processed_commands=processed_commands,
        outbox=outbox,
        uow=uow,
        settings=settings,
    )


# --- билдеры команд ---


def make_correlation(
    order_id: UUID, command_id: str | None = None
) -> CommandCorrelation:
    return CommandCorrelation(
        saga_id=str(uuid.uuid7()),
        business_key=str(order_id),
        command_id=command_id or str(uuid.uuid7()),
    )


def make_reserve(
    order_id: UUID,
    items: dict[str, int],
    ttl_seconds: int | None = 60,
    command_id: str | None = None,
) -> ReserveCommand:
    return ReserveCommand(
        correlation=make_correlation(order_id, command_id),
        order_id=order_id,
        items=[
            ReservationItem(product_id=product_id, quantity=quantity)
            for product_id, quantity in items.items()
        ],
        ttl_seconds=ttl_seconds,
    )


def make_commit(order_id: UUID, command_id: str | None = None) -> CommitReservationCommand:
    return CommitReservationCommand(
        correlation=make_correlation(order_id, command_id), order_id=order_id
    )


def make_cancel(order_id: UUID, command_id: str | None = None) -> CancelReservationCommand:
    return CancelReservationCommand(
        correlation=make_correlation(order_id, command_id), order_id=order_id
    )


def make_reservation(
    order_id: UUID,
    items: dict[str, int],
    status: ReservationStatus = ReservationStatus.ACTIVE,
    expires_in_seconds: int = 600,
) -> Reservation:
    return Reservation(
        order_id=order_id,
        status=status,
        items=[
            ReservationItem(product_id=product_id, quantity=quantity)
            for product_id, quantity in items.items()
        ],
        expires_at=utc_now() + timedelta(seconds=expires_in_seconds),
    )
