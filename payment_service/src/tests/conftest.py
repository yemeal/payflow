"""
Общие фикстуры и фейки для тестов payment_service.

Здесь лежит всё, что переиспользуется между тестовыми модулями:
фабрики доменных объектов и лёгкие in-memory реализации портов
(хранилище идемпотентности, репозитории, UOW, publisher).

Идея простая: юнит-тесты гоняем на фейках без реальной инфраструктуры
(ни Postgres, ни Redis, ни Kafka), а интеграционные - связываем настоящие
сервисы с этими же фейками вместо адаптеров. Так один и тот же контракт
портов проверяется и по частям, и в сборке.
"""

from __future__ import annotations

import contextlib
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

import pytest

from app.domain.payments import Payment, PaymentStatus
from app.domain.outbox import OutboxEvent, OutboxStatus
from app.application.services.idempotency.domain import (
    AcquireLockResult,
    IdempotencyEntry,
)
from app.application.services.idempotency.enums import LockAcquireStatus


# ---------------------------------------------------------------------------
# Настройки
# ---------------------------------------------------------------------------

@pytest.fixture
def idempotency_settings() -> SimpleNamespace:
    """
    Минимальный заменитель Settings для IdempotencyGuard.
    Guard читает только два поля, полноценный Settings (с env) не нужен.
    """
    return SimpleNamespace(
        IDEMPOTENCY_LOCK_TTL=60,
        IDEMPOTENCY_RESULT_TTL=3600,
    )


# ---------------------------------------------------------------------------
# Фабрики доменных объектов
# ---------------------------------------------------------------------------

def make_payment(**overrides) -> Payment:
    """Фабрика платежа с разумными дефолтами (переопределяем через kwargs)."""
    defaults = dict(
        idempotency_key="key-default",
        amount=Decimal("100.00"),
        currency="RUB",
        status=PaymentStatus.PENDING,
    )
    defaults.update(overrides)
    return Payment(**defaults)


def make_outbox_event(event_type: str = "payment.pending", attempts: int = 0) -> OutboxEvent:
    """Фабрика outbox-события."""
    return OutboxEvent(
        event_type=event_type,
        payload={"id": str(uuid4())},
        attempts=attempts,
    )


@pytest.fixture
def payment_factory():
    """Отдаём фабрику как фикстуру, чтобы тест сам задавал нужные поля."""
    return make_payment


# ---------------------------------------------------------------------------
# In-memory хранилище идемпотентности
# ---------------------------------------------------------------------------

class InMemoryIdempotencyStorage:
    """
    Реализация IdempotencyStorageProtocol в памяти.
    Повторяет семантику Redis+Lua адаптера, но без Redis:
      - acquire_lock атомарно ставит ключ, если его нет;
      - если ключ уже есть - возвращает ENTRY_EXISTS с текущей записью;
      - release_lock удаляет ключ только если значение совпадает (compare-and-delete);
      - save_result перезаписывает значение.

    TTL здесь не эмулируется намеренно: в юнит-тестах время не крутим,
    проверяем только логику переходов.
    """

    def __init__(self) -> None:
        self._data: dict[str, str] = {}
        # счётчики для проверок в тестах
        self.acquire_calls = 0
        self.release_calls = 0
        self.save_calls = 0

    async def acquire_lock(self, key: str, lock_value: str, ttl: int) -> AcquireLockResult:
        self.acquire_calls += 1
        current = self._data.get(key)
        if current is not None:
            entry = IdempotencyEntry.model_validate_json(current)
            return AcquireLockResult(
                status=LockAcquireStatus.ENTRY_EXISTS, existing_entry=entry
            )
        self._data[key] = lock_value
        return AcquireLockResult(status=LockAcquireStatus.LOCK_ACQUIRED)

    async def release_lock(self, key: str, expected_value: str) -> bool:
        self.release_calls += 1
        if self._data.get(key) == expected_value:
            del self._data[key]
            return True
        return False

    async def save_result(self, key: str, entry: IdempotencyEntry, ttl: int) -> None:
        self.save_calls += 1
        self._data[key] = entry.model_dump_json()

    # хелперы для ассертов
    def raw(self, key: str) -> str | None:
        return self._data.get(key)

    def entry(self, key: str) -> IdempotencyEntry | None:
        raw = self._data.get(key)
        return IdempotencyEntry.model_validate_json(raw) if raw else None


@pytest.fixture
def in_memory_storage() -> InMemoryIdempotencyStorage:
    return InMemoryIdempotencyStorage()


# ---------------------------------------------------------------------------
# In-memory репозитории и UOW (для интеграционных тестов)
# ---------------------------------------------------------------------------

class InMemoryUOW:
    """
    UOW-заглушка: считает вход/выход и глубину вложенности.
    Ничего не коммитит (репозитории пишут сразу в память),
    но позволяет проверить, что вызовы делаются внутри транзакции.
    """

    def __init__(self) -> None:
        self.depth = 0
        self.enters = 0
        self.commits = 0
        self.rollbacks = 0

    async def __aenter__(self) -> "InMemoryUOW":
        self.depth += 1
        self.enters += 1
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self.depth -= 1
        if exc_type is None:
            self.commits += 1
        else:
            self.rollbacks += 1


class InMemoryPaymentRepository:
    """Хранит платежи в dict по id, поддерживает поиск по ключу идемпотентности."""

    def __init__(self) -> None:
        self.payments: dict[str, Payment] = {}

    async def create(self, entity: Payment) -> Payment:
        self.payments[str(entity.id)] = entity
        return entity

    async def get(self, entity_id: str) -> Payment | None:
        return self.payments.get(str(entity_id))

    async def update(self, entity: Payment) -> Payment:
        self.payments[str(entity.id)] = entity
        return entity

    async def find_by_idempotency_key(self, idempotency_key: str) -> Payment | None:
        for payment in self.payments.values():
            if payment.idempotency_key == idempotency_key:
                return payment
        return None

    async def get_processing_payments(
        self, threshold_seconds: int = 10, limit: int = 100
    ) -> list[Payment]:
        result = [
            p for p in self.payments.values()
            if p.status == PaymentStatus.PROCESSING
        ]
        return result[:limit]


class InMemoryOutboxRepository:
    """Копит outbox-события; get_unpublished_events отдаёт PENDING по порядку создания."""

    def __init__(self) -> None:
        self.events: list[OutboxEvent] = []

    async def create(self, entity: OutboxEvent) -> OutboxEvent:
        self.events.append(entity)
        return entity

    async def update(self, entity: OutboxEvent) -> OutboxEvent:
        return entity

    async def get_unpublished_events(self, limit: int = 100):
        pending = [e for e in self.events if e.status == OutboxStatus.PENDING]
        pending.sort(key=lambda e: (e.created_at, str(e.id)))
        return pending[:limit]


class RecordingPublisher:
    """Publisher, складывающий опубликованные конверты в список."""

    def __init__(self) -> None:
        self.published = []

    async def publish(self, envelope) -> None:
        self.published.append(envelope)


@pytest.fixture
def in_memory_payment_repo() -> InMemoryPaymentRepository:
    return InMemoryPaymentRepository()


@pytest.fixture
def in_memory_outbox_repo() -> InMemoryOutboxRepository:
    return InMemoryOutboxRepository()


@pytest.fixture
def in_memory_uow() -> InMemoryUOW:
    return InMemoryUOW()


@pytest.fixture
def recording_publisher() -> RecordingPublisher:
    return RecordingPublisher()


def make_scope_factory(uow, outbox_repo):
    """
    Собирает OutboxScopeFactory поверх готовых uow и repo.
    Используется в тестах OutboxRelayService.
    """
    scope = SimpleNamespace(uow=uow, outbox_repo=outbox_repo)

    @contextlib.asynccontextmanager
    async def factory():
        yield scope

    return factory
