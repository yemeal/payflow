from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.outbox import OutboxMessage, OutboxStatus
from app.infrastructure.database.models.outbox import OutboxMessageORM
from app.infrastructure.database.repositories.base_repository import (
    SQLAlchemyAsyncRepository,
)


class OutboxRepository(SQLAlchemyAsyncRepository[OutboxMessage, OutboxMessageORM]):
    """Единая outbox-таблица (ADR-006); у склада в ней только события"""

    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, OutboxMessage, OutboxMessageORM)

    async def add(self, message: OutboxMessage) -> OutboxMessage:
        return await self.create(message)

    async def get_unpublished(self, limit: int) -> list[OutboxMessage]:
        # FOR UPDATE SKIP LOCKED: несколько релеев разгребают очередь параллельно.
        # ORDER BY обязателен: без него порядок публикации не гарантирован и
        # события одного заказа могут уйти в Kafka в неверной последовательности;
        # id (uuid7) - time-ordered tiebreak при одинаковом created_at
        stmt = (
            select(OutboxMessageORM)
            .where(OutboxMessageORM.status == OutboxStatus.PENDING)
            .order_by(OutboxMessageORM.created_at.asc(), OutboxMessageORM.id.asc())
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        result = await self._session.execute(stmt)
        return [
            OutboxMessage.model_validate(orm_model, from_attributes=True)
            for orm_model in result.scalars().all()
        ]
