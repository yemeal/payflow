"""
Общие фикстуры юнит-тестов оркестратора: in-memory фейки портов вместо
Postgres/Kafka (по образцу analytics_service). Generic-исполнитель тестируется
как чистая машина: события на входе, состояние саги + outbox на выходе.
"""

import uuid
from datetime import datetime
from typing import Any

import pytest

from app.application.sagas.order_fulfillment import create_saga_registry
from app.application.services.saga_executor import SagaExecutorService
from app.core.settings import Settings
from app.domain.outbox import OutboxMessage, OutboxStatus
from app.domain.processed_events import ProcessedEvent
from app.domain.saga import TERMINAL_STATUSES, Saga, SagaTransition


class FakeUOW:
    """Транзакция-заглушка: считает входы/выходы, ничего не коммитит"""

    def __init__(self) -> None:
        self.entered = 0

    async def __aenter__(self) -> "FakeUOW":
        self.entered += 1
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class FakeSagaRepository:
    def __init__(self) -> None:
        self.by_id: dict[uuid.UUID, Saga] = {}

    def _copy(self, saga: Saga) -> Saga:
        return saga.model_copy(deep=True)

    async def create_if_absent(self, saga: Saga) -> bool:
        for existing in self.by_id.values():
            if (
                existing.saga_type == saga.saga_type
                and existing.business_key == saga.business_key
            ):
                return False
        self.by_id[saga.id] = self._copy(saga)
        return True

    async def get(self, saga_id: uuid.UUID) -> Saga | None:
        saga = self.by_id.get(saga_id)
        return self._copy(saga) if saga else None

    async def get_for_update(self, saga_id: uuid.UUID) -> Saga | None:
        return await self.get(saga_id)

    async def get_by_business_key_for_update(
        self, saga_type: str, business_key: str
    ) -> Saga | None:
        for saga in self.by_id.values():
            if saga.saga_type == saga_type and saga.business_key == business_key:
                return self._copy(saga)
        return None

    async def update(self, saga: Saga) -> Saga:
        self.by_id[saga.id] = self._copy(saga)
        return saga

    async def find_retry_due(self, now: datetime, limit: int) -> list[Saga]:
        due = [
            self._copy(s)
            for s in self.by_id.values()
            if s.retry_after is not None
            and s.retry_after <= now
            and s.status not in TERMINAL_STATUSES
        ]
        return due[:limit]

    async def find_deadline_due(self, now: datetime, limit: int) -> list[Saga]:
        due = [
            self._copy(s)
            for s in self.by_id.values()
            if s.deadline_at is not None
            and s.deadline_at <= now
            and s.retry_after is None
            and s.status not in TERMINAL_STATUSES
        ]
        return due[:limit]

    async def list_sagas(self, saga_type, status, limit, offset) -> list[Saga]:
        found = [
            self._copy(s)
            for s in self.by_id.values()
            if (saga_type is None or s.saga_type == saga_type)
            and (status is None or s.status.value == status)
        ]
        return found[offset : offset + limit]

    async def list_stuck(self, older_than: datetime, limit: int) -> list[Saga]:
        found = [
            self._copy(s)
            for s in self.by_id.values()
            if s.status not in TERMINAL_STATUSES
            and (s.updated_at or s.created_at) < older_than
        ]
        return found[:limit]

    def single(self) -> Saga:
        assert len(self.by_id) == 1, f"ожидалась одна сага, есть {len(self.by_id)}"
        return next(iter(self.by_id.values()))


class FakeSagaTransitionRepository:
    def __init__(self) -> None:
        self.items: list[SagaTransition] = []

    async def add(self, transition: SagaTransition) -> SagaTransition:
        self.items.append(transition)
        return transition

    async def list_for_saga(self, saga_id: uuid.UUID) -> list[SagaTransition]:
        return [t for t in self.items if t.saga_id == saga_id]


class FakeProcessedEventRepository:
    def __init__(self) -> None:
        self.seen: set[uuid.UUID] = set()

    async def try_mark_processed(self, event: ProcessedEvent) -> bool:
        if event.event_id in self.seen:
            return False
        self.seen.add(event.event_id)
        return True


class FakeOutboxRepository:
    def __init__(self) -> None:
        self.messages: list[OutboxMessage] = []

    async def add(self, message: OutboxMessage) -> OutboxMessage:
        self.messages.append(message.model_copy(deep=True))
        return message

    async def update(self, message: OutboxMessage) -> OutboxMessage:
        for i, existing in enumerate(self.messages):
            if existing.id == message.id:
                self.messages[i] = message.model_copy(deep=True)
        return message

    async def get_unpublished(self, limit: int) -> list[OutboxMessage]:
        pending = [m for m in self.messages if m.status == OutboxStatus.PENDING]
        return pending[:limit]

    def by_type(self, message_type: str) -> list[OutboxMessage]:
        """Записи по типу из конверта: "inventory.reserve", "saga.completed", ..."""
        return [m for m in self.messages if m.type == message_type]


@pytest.fixture
def settings() -> Settings:
    return Settings(
        DATABASE_HOST="localhost",
        DATABASE_PORT=5436,
        DATABASE_USER="test",
        DATABASE_PASSWORD="test",
        DATABASE_NAME="test",
        DEV_LOGS=True,
        KAFKA_BOOTSTRAP_SERVERS="localhost:9092",
        JWT_SECRET="test-secret",
    )


@pytest.fixture
def saga_repo() -> FakeSagaRepository:
    return FakeSagaRepository()


@pytest.fixture
def transitions_repo() -> FakeSagaTransitionRepository:
    return FakeSagaTransitionRepository()


@pytest.fixture
def processed_repo() -> FakeProcessedEventRepository:
    return FakeProcessedEventRepository()


@pytest.fixture
def outbox_repo() -> FakeOutboxRepository:
    return FakeOutboxRepository()


@pytest.fixture
def executor(
    settings: Settings,
    saga_repo: FakeSagaRepository,
    transitions_repo: FakeSagaTransitionRepository,
    processed_repo: FakeProcessedEventRepository,
    outbox_repo: FakeOutboxRepository,
) -> SagaExecutorService:
    return SagaExecutorService(
        registry=create_saga_registry(settings),
        sagas=saga_repo,
        transitions=transitions_repo,
        processed_events=processed_repo,
        outbox=outbox_repo,
        uow=FakeUOW(),
        settings=settings,
    )


# --- конструкторы событий (контракты contracts/) ---


def order_created_event(order_id: str, event_id: uuid.UUID | None = None) -> dict[str, Any]:
    return {
        "metadata": {
            "event_id": str(event_id or uuid.uuid7()),
            "event_type": "order.created",
            "version": "1.0",
            "timestamp": "2026-07-14T12:00:00",
            "source": "order-service",
        },
        "data": {
            "orderId": order_id,
            "userId": str(uuid.uuid7()),
            "items": [{"productId": "sku-1", "quantity": 2, "price": "10.50"}],
            "totalAmount": "21.00",
            "currency": "RUB",
        },
    }


def participant_event(
    event_type: str,
    saga_id: uuid.UUID,
    business_key: str,
    command_id: uuid.UUID,
    failure: dict[str, Any] | None = None,
    correlation: bool = True,
    event_id: uuid.UUID | None = None,
) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "event_id": str(event_id or uuid.uuid7()),
        "event_type": event_type,
        "version": "1.0",
        "timestamp": "2026-07-14T12:00:01",
        "source": "test-participant",
    }
    if correlation:
        metadata["correlation"] = {
            "sagaId": str(saga_id),
            "businessKey": business_key,
            "commandId": str(command_id),
        }
    data: dict[str, Any] = {"orderId": business_key}
    if failure is not None:
        data["failure"] = failure
    return {"metadata": metadata, "data": data}
