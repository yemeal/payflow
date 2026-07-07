from typing import Sequence

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.outbox import OutboxEvent, OutboxStatus
from app.infrastructure.database.models.outbox import OutboxEventORM
from app.infrastructure.database.repositories.base_repository import (
    SQLAlchemyAsyncRepository,
)


class OutboxRepository(SQLAlchemyAsyncRepository[OutboxEvent, OutboxEventORM]):
    """Специализированный репозиторий для Outbox-событий"""

    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, OutboxEvent, OutboxEventORM)

    async def get_unpublished_events(self, limit: int = 100) -> Sequence[OutboxEvent]:
        # Используем SELECT FOR UPDATE + скипаем залоченные
        # -> несколько воркеров разгребают таблицу аутбокс ивентов параллельно
        #
        # ORDER BY обязателен: без него Postgres не гарантирует порядок строк,
        # и события одного платежа могут уйти в Kafka в неправильной последовательности.
        # id (UUID v7) — time-ordered tiebreak для событий с одинаковым created_at.
        stmt = (
            select(OutboxEventORM)
            .where(OutboxEventORM.status == OutboxStatus.PENDING)
            .order_by(OutboxEventORM.created_at.asc(), OutboxEventORM.id.asc())
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        result = await self._session.execute(stmt)
        return [
            OutboxEvent.model_validate(orm_model, from_attributes=True)
            for orm_model in result.scalars().all()
        ]
