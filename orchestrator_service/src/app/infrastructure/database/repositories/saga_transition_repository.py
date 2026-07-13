import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.saga import SagaTransition
from app.infrastructure.database.models.saga_transition import SagaTransitionORM
from app.infrastructure.database.repositories.base_repository import (
    SQLAlchemyAsyncRepository,
)


class SagaTransitionRepository(
    SQLAlchemyAsyncRepository[SagaTransition, SagaTransitionORM]
):
    """Append-only история переходов саги"""

    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, SagaTransition, SagaTransitionORM)

    async def add(self, transition: SagaTransition) -> SagaTransition:
        return await self.create(transition)

    async def list_for_saga(self, saga_id: uuid.UUID) -> list[SagaTransition]:
        # id (uuid7) - time-ordered tiebreak при одинаковом created_at
        stmt = (
            select(SagaTransitionORM)
            .where(SagaTransitionORM.saga_id == saga_id)
            .order_by(SagaTransitionORM.created_at.asc(), SagaTransitionORM.id.asc())
        )
        result = await self._session.execute(stmt)
        return [
            SagaTransition.model_validate(orm_model, from_attributes=True)
            for orm_model in result.scalars().all()
        ]
