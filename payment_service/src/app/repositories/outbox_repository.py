from typing import Protocol, Sequence

from sqlalchemy import select

from app.models.outbox_events import OutboxEvent, OutboxStatus
from app.repositories.base_repository import (
    SQLAlchemyAsyncRepository,
    AsyncRepositoryProtocol,
)


class OutboxRepositoryProtocol(AsyncRepositoryProtocol[OutboxEvent]):
    async def get_unpublished_events(self, limit: int = 100) -> Sequence[OutboxEvent]: ...


class OutboxRepository(SQLAlchemyAsyncRepository[OutboxEvent]):
    """Специализированный репозиторий для Outbox-событий"""

    async def get_unpublished_events(self, limit: int = 100) -> Sequence[OutboxEvent]:
        stmt = (
            select(OutboxEvent)
            .where(OutboxEvent.status == OutboxStatus.PENDING)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        result = await self._session.execute(stmt)
        return result.scalars().all()
