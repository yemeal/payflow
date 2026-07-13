import uuid
from typing import Protocol

from app.domain.orders import Order
from app.domain.outbox import OutboxMessage
from app.domain.processed_events import ProcessedEvent


class AsyncRepositoryProtocol[EntityT](Protocol):
    """Базовый протокол для всех репозиториев"""

    async def create(self, entity: EntityT) -> EntityT: ...

    async def get(self, entity_id: str | int) -> EntityT | None: ...

    async def update(self, entity: EntityT) -> EntityT: ...


class OrderRepositoryProtocol(AsyncRepositoryProtocol[Order]):
    """Протокол, специфичный для Order.

    RBAC живёт на уровне запросов: выборки для пользователя всегда содержат
    WHERE user_id = :current, а не if в коде."""

    async def get_for_user(
        self, order_id: uuid.UUID, user_id: uuid.UUID
    ) -> Order | None: ...

    async def list_for_user(
        self, user_id: uuid.UUID, limit: int = 50, offset: int = 0
    ) -> list[Order]: ...

    async def list_all(self, limit: int = 50, offset: int = 0) -> list[Order]: ...

    async def get_for_update(self, order_id: uuid.UUID) -> Order | None:
        """SELECT FOR UPDATE: блокировка строки на смену статуса
        (отмена владельцем, применение финального события саги)"""
        ...


class OutboxRepositoryProtocol(Protocol):
    """Единая outbox-таблица команд и событий (ADR-006)"""

    async def add(self, message: OutboxMessage) -> OutboxMessage: ...

    async def update(self, message: OutboxMessage) -> OutboxMessage: ...

    async def get_unpublished(self, limit: int) -> list[OutboxMessage]:
        """PENDING в порядке создания, FOR UPDATE SKIP LOCKED"""
        ...


class ProcessedEventRepositoryProtocol(Protocol):
    """Реестр обработанных событий (Idempotent Consumer, финализация саги)"""

    async def try_mark_processed(self, event: ProcessedEvent) -> bool:
        """INSERT ... ON CONFLICT DO NOTHING, атомарно.

        True - событие новое; False - дубль. Вызывается строго в одной
        транзакции с обновлением статуса заказа."""
        ...
