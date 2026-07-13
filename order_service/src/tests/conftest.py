"""
Общие фикстуры и in-memory фейки портов order_service.

Фейки намеренно моделируют транзакционность, а не просто складывают объекты в
список: запись идёт в "pending" и становится видимой снаружи только после
commit. Без этого утверждение "заказ и order.created сохраняются в ОДНОЙ
транзакции" нечем проверить - любой фейк-репозиторий покажет зелёный тест
даже там, где в бою произошёл бы dual write.

Postgres/Redis здесь не нужны: тестируется бизнес-логика поверх портов.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest

from app.application.services.order_service import OrderService
from app.application.services.saga_events import SagaEventsHandlerService
from app.domain.clock import utc_now
from app.domain.orders import Order, OrderItem, OrderStatus
from app.domain.outbox import OutboxMessage
from app.domain.processed_events import ProcessedEvent
from app.entrypoints.http.schemas.orders import OrderCreate, OrderItemSchema

EVENTS_TOPIC = "orders.events"


# ---------------------------------------------------------------------------
# Фейковая "сессия": committed-состояние + staged-запись до коммита
# ---------------------------------------------------------------------------


@dataclass
class FakeSession:
    """Транзакционная семантика в памяти: pending -> commit -> committed"""

    orders: dict[uuid.UUID, Order] = field(default_factory=dict)
    outbox: list[OutboxMessage] = field(default_factory=list)
    processed: dict[uuid.UUID, ProcessedEvent] = field(default_factory=dict)

    pending_orders: dict[uuid.UUID, Order] = field(default_factory=dict)
    pending_outbox: list[OutboxMessage] = field(default_factory=list)
    pending_processed: dict[uuid.UUID, ProcessedEvent] = field(default_factory=dict)

    commits: int = 0
    rollbacks: int = 0
    # снимок (заказов, outbox-записей) на момент каждого коммита:
    # так видно, что обе записи попали в ОДИН коммит, а не в два подряд
    commit_snapshots: list[tuple[int, int]] = field(default_factory=list)

    def commit(self) -> None:
        self.orders.update(self.pending_orders)
        self.outbox.extend(self.pending_outbox)
        self.processed.update(self.pending_processed)
        self._clear_pending()
        self.commits += 1
        self.commit_snapshots.append((len(self.orders), len(self.outbox)))

    def rollback(self) -> None:
        self._clear_pending()
        self.rollbacks += 1

    def _clear_pending(self) -> None:
        self.pending_orders = {}
        self.pending_outbox = []
        self.pending_processed = {}


class FakeUOW:
    """Контекст транзакции: выход без исключения - commit, с исключением - rollback"""

    def __init__(self, session: FakeSession) -> None:
        self._session = session

    async def __aenter__(self) -> "FakeUOW":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type is None:
            self._session.commit()
        else:
            self._session.rollback()


# ---------------------------------------------------------------------------
# Фейковые репозитории
# ---------------------------------------------------------------------------


class FakeOrderRepository:
    def __init__(self, session: FakeSession) -> None:
        self._session = session
        self.update_calls: list[uuid.UUID] = []
        self.reads: list[str] = []

    def _visible(self) -> dict[uuid.UUID, Order]:
        # read-your-writes внутри открытой транзакции
        return {**self._session.orders, **self._session.pending_orders}

    async def create(self, entity: Order) -> Order:
        self._session.pending_orders[entity.id] = entity
        return entity

    async def get(self, entity_id: uuid.UUID) -> Order | None:
        self.reads.append("get")
        return self._visible().get(entity_id)

    async def get_for_user(
        self, order_id: uuid.UUID, user_id: uuid.UUID
    ) -> Order | None:
        # владелец в самом запросе (WHERE user_id = ...), как в SQL-репозитории
        self.reads.append("get_for_user")
        order = self._visible().get(order_id)
        if order is None or order.user_id != user_id:
            return None
        return order

    async def list_for_user(
        self, user_id: uuid.UUID, limit: int = 50, offset: int = 0
    ) -> list[Order]:
        self.reads.append("list_for_user")
        rows = [o for o in self._visible().values() if o.user_id == user_id]
        return rows[offset : offset + limit]

    async def list_all(self, limit: int = 50, offset: int = 0) -> list[Order]:
        self.reads.append("list_all")
        rows = list(self._visible().values())
        return rows[offset : offset + limit]

    async def get_for_update(self, order_id: uuid.UUID) -> Order | None:
        self.reads.append("get_for_update")
        order = self._visible().get(order_id)
        if order is None:
            return None
        # копия: сервис меняет её и отдаёт в update, как в SQLAlchemy-репозитории
        return order.model_copy(deep=True)

    async def update(self, entity: Order) -> Order:
        self.update_calls.append(entity.id)
        self._session.pending_orders[entity.id] = entity
        return entity


class FakeOutboxRepository:
    def __init__(self, session: FakeSession, fail_on_add: bool = False) -> None:
        self._session = session
        self._fail_on_add = fail_on_add

    async def add(self, message: OutboxMessage) -> OutboxMessage:
        if self._fail_on_add:
            # имитируем сбой БД на записи в outbox: заказ обязан откатиться
            raise RuntimeError("outbox insert failed")
        self._session.pending_outbox.append(message)
        return message

    async def update(self, message: OutboxMessage) -> OutboxMessage:
        return message

    async def get_unpublished(self, limit: int) -> list[OutboxMessage]:
        return self._session.outbox[:limit]


class FakeProcessedEventRepository:
    """INSERT ... ON CONFLICT DO NOTHING в памяти"""

    def __init__(self, session: FakeSession) -> None:
        self._session = session

    async def try_mark_processed(self, event: ProcessedEvent) -> bool:
        seen = {**self._session.processed, **self._session.pending_processed}
        if event.event_id in seen:
            return False
        self._session.pending_processed[event.event_id] = event
        return True


class FakeOrderCache:
    def __init__(self) -> None:
        self.store: dict[uuid.UUID, dict[str, Any]] = {}
        self.get_calls = 0
        self.set_calls = 0
        self.invalidate_calls: list[uuid.UUID] = []

    async def get(self, order_id: uuid.UUID) -> dict[str, Any] | None:
        self.get_calls += 1
        return self.store.get(order_id)

    async def set(self, order_id: uuid.UUID, payload: dict[str, Any]) -> None:
        self.set_calls += 1
        self.store[order_id] = payload

    async def invalidate(self, order_id: uuid.UUID) -> None:
        self.invalidate_calls.append(order_id)
        self.store.pop(order_id, None)


# ---------------------------------------------------------------------------
# Фабрики доменных объектов
# ---------------------------------------------------------------------------


def make_order(
    user_id: uuid.UUID | None = None,
    status: OrderStatus = OrderStatus.PENDING,
    total_amount: str = "300.00",
) -> Order:
    return Order(
        user_id=user_id or uuid.uuid4(),
        status=status,
        items=[OrderItem(product_id="sku-1", quantity=2, price=Decimal("150.00"))],
        total_amount=Decimal(total_amount),
        currency="RUB",
        created_at=utc_now(),
    )


def make_order_create(currency: str = "RUB") -> OrderCreate:
    return OrderCreate(
        items=[
            OrderItemSchema(product_id="sku-1", quantity=2, price=Decimal("150.00")),
            OrderItemSchema(product_id="sku-2", quantity=1, price=Decimal("99.50")),
        ],
        currency=currency,
    )


def make_saga_event(
    event_type: str,
    order_id: uuid.UUID,
    event_id: uuid.UUID | None = None,
    status: str | None = None,
) -> dict[str, Any]:
    """Конверт contracts/orders/saga-finished.v1 (metadata snake_case, data camelCase)"""
    return {
        "metadata": {
            "event_id": str(event_id or uuid.uuid4()),
            "event_type": event_type,
            "version": "1.0",
            "timestamp": "2026-07-15T10:00:00",
            "source": "orchestrator-service",
            "correlation": {
                "sagaId": str(uuid.uuid4()),
                "businessKey": str(order_id),
                "commandId": str(uuid.uuid4()),
            },
        },
        "data": {
            "orderId": str(order_id),
            "status": status or event_type.split(".")[-1].upper(),
        },
    }


# ---------------------------------------------------------------------------
# Фикстуры
# ---------------------------------------------------------------------------


@pytest.fixture
def session() -> FakeSession:
    return FakeSession()


@pytest.fixture
def orders_repo(session: FakeSession) -> FakeOrderRepository:
    return FakeOrderRepository(session)


@pytest.fixture
def outbox_repo(session: FakeSession) -> FakeOutboxRepository:
    return FakeOutboxRepository(session)


@pytest.fixture
def processed_repo(session: FakeSession) -> FakeProcessedEventRepository:
    return FakeProcessedEventRepository(session)


@pytest.fixture
def cache() -> FakeOrderCache:
    return FakeOrderCache()


@pytest.fixture
def settings() -> SimpleNamespace:
    # реальный Settings потребовал бы .env (БД, Redis, JWT); сервису от настроек
    # нужен только топик, поэтому подставляем минимальный стаб
    return SimpleNamespace(KAFKA_EVENTS_TOPIC=EVENTS_TOPIC)


@pytest.fixture
def order_service(
    orders_repo: FakeOrderRepository,
    outbox_repo: FakeOutboxRepository,
    session: FakeSession,
    cache: FakeOrderCache,
    settings: SimpleNamespace,
) -> OrderService:
    return OrderService(
        orders=orders_repo,
        outbox=outbox_repo,
        uow=FakeUOW(session),
        cache=cache,
        settings=settings,
    )


@pytest.fixture
def saga_handler(
    orders_repo: FakeOrderRepository,
    processed_repo: FakeProcessedEventRepository,
    session: FakeSession,
    cache: FakeOrderCache,
) -> SagaEventsHandlerService:
    return SagaEventsHandlerService(
        orders=orders_repo,
        processed_events=processed_repo,
        cache=cache,
        uow=FakeUOW(session),
    )
