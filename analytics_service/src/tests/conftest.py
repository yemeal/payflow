"""
Общие фикстуры и фейки для тестов analytics_service.

Каталог тестов раньше был пустым (долг из TECH_DEBT п.8).
Здесь заводим базу: фабрики событий/платежей и in-memory реализации
портов (репозитории, UOW, кэш), чтобы гонять бизнес-логику без Postgres и Redis.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

import pytest


# ---------------------------------------------------------------------------
# Фабрики входящих событий
# ---------------------------------------------------------------------------

def make_event_dict(**overrides) -> dict:
    """
    Сырой конверт события в том виде, в каком его шлёт payment_service
    (metadata + data). Это ровно контракт EventEnvelope из payment_service.
    """
    event_id = str(overrides.pop("event_id", uuid4()))
    payment_id = str(overrides.pop("payment_id", uuid4()))
    status = overrides.pop("status", "COMPLETED")
    data = {
        "id": payment_id,
        "status": status,
        "amount": overrides.pop("amount", "100.00"),
        "currency": overrides.pop("currency", "RUB"),
        "customerId": overrides.pop("customer_id", "cust-1"),
        "description": overrides.pop("description", "test"),
        "createdAt": overrides.pop("created_at", "2026-07-10T10:00:00"),
        "updatedAt": overrides.pop("updated_at", None),
    }
    data.update(overrides)
    # metadata payment_service шлёт в snake_case (EventEnvelopeMetadata - обычная модель),
    # data - в camelCase (PaymentResponse). Analytics принимает оба варианта
    # (populate_by_name=True), но здесь воспроизводим ровно формат провода.
    return {
        "metadata": {
            "event_id": event_id,
            "event_type": f"payment.{status.lower()}",
            "version": "1.0",
            "timestamp": "2026-07-10T10:00:00Z",
            "source": "payment-service",
        },
        "data": data,
    }


@pytest.fixture
def event_dict_factory():
    return make_event_dict


# ---------------------------------------------------------------------------
# In-memory реализации портов
# ---------------------------------------------------------------------------

class InMemoryUOW:
    """UOW-заглушка: считает вход/выход, отражает commit/rollback."""

    def __init__(self) -> None:
        self.enters = 0
        self.commits = 0
        self.rollbacks = 0

    async def __aenter__(self) -> "InMemoryUOW":
        self.enters += 1
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type is None:
            self.commits += 1
        else:
            self.rollbacks += 1


class InMemoryProcessedEventRepository:
    """
    Дедупликация в памяти: save_if_not_exists возвращает True на новом
    event_id и False на повторе (эмуляция ON CONFLICT DO NOTHING).
    """

    def __init__(self) -> None:
        self.seen: set[str] = set()

    async def save_if_not_exists(self, event_id: str) -> bool:
        if event_id in self.seen:
            return False
        self.seen.add(event_id)
        return True


class InMemoryPaymentRepository:
    """Read-модель платежей: upsert по id, плюс простые выборки."""

    def __init__(self) -> None:
        self.payments: dict[str, dict] = {}

    async def upsert(self, payment_data: dict) -> None:
        self.payments[str(payment_data["id"])] = dict(payment_data)

    async def get(self, entity_id: str):
        return self.payments.get(str(entity_id))


class InMemoryCache:
    """Кэш в памяти с журналом операций для ассертов."""

    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.deleted_patterns: list[str] = []

    async def get(self, key: str) -> str | None:
        return self.store.get(key)

    async def set(self, key: str, value: str, expire: int) -> None:
        self.store[key] = value

    async def delete_by_pattern(self, pattern: str) -> None:
        self.deleted_patterns.append(pattern)
        # грубое сопоставление префикса до "*"
        prefix = pattern.rstrip("*")
        for key in [k for k in self.store if k.startswith(prefix)]:
            del self.store[key]


@pytest.fixture
def in_memory_uow() -> InMemoryUOW:
    return InMemoryUOW()


@pytest.fixture
def in_memory_processed_events() -> InMemoryProcessedEventRepository:
    return InMemoryProcessedEventRepository()


@pytest.fixture
def in_memory_payment_repo() -> InMemoryPaymentRepository:
    return InMemoryPaymentRepository()


@pytest.fixture
def in_memory_cache() -> InMemoryCache:
    return InMemoryCache()
