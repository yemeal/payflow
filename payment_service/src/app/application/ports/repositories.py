from typing import Protocol, Sequence
from app.domain.payments import Payment
from app.domain.outbox import OutboxEvent


class AsyncRepositoryProtocol[EntityT](Protocol):
    """Базовый протокол для всех репозиториев"""

    async def create(self, entity: EntityT) -> EntityT: ...
    async def get(self, entity_id: str | int) -> EntityT | None: ...
    async def update(self, entity: EntityT) -> EntityT: ...

    # async def delete(self, entity_id: str | int) -> None: ...


class PaymentRepositoryProtocol(AsyncRepositoryProtocol[Payment]):
    """Протокол специфичный для Payment"""

    async def find_by_idempotency_key(self, idempotency_key: str) -> Payment | None: ...

    async def get_processing_payments(
        self, threshold_seconds: int = 10, limit: int = 100
    ) -> list[Payment]: ...


class OutboxRepositoryProtocol(AsyncRepositoryProtocol[OutboxEvent]):
    """Протокол специфичный для Outbox"""

    async def get_unpublished_events(
        self, limit: int = 100
    ) -> Sequence[OutboxEvent]: ...
