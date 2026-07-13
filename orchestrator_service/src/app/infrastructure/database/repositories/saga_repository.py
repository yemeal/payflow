import uuid
from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.saga import TERMINAL_STATUSES, Saga, SagaStatus
from app.infrastructure.database.models.saga import SagaORM
from app.infrastructure.database.repositories.base_repository import (
    SQLAlchemyAsyncRepository,
)


class SagaRepository(SQLAlchemyAsyncRepository[Saga, SagaORM]):
    """Репозиторий generic-саг (ADR-006)"""

    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, Saga, SagaORM)

    async def create_if_absent(self, saga: Saga) -> bool:
        # атомарный INSERT ... ON CONFLICT DO NOTHING по (saga_type, business_key):
        # дубль стартового события не создаёт вторую сагу и не роняет обработчик
        stmt = (
            pg_insert(SagaORM)
            .values(**saga.model_dump())
            .on_conflict_do_nothing(index_elements=["saga_type", "business_key"])
        )
        result = await self._session.execute(stmt)
        return bool(result.rowcount)

    async def get_for_update(self, saga_id: uuid.UUID) -> Saga | None:
        stmt = select(SagaORM).where(SagaORM.id == saga_id).with_for_update()
        result = await self._session.execute(stmt)
        orm_model = result.scalar_one_or_none()
        if orm_model is None:
            return None
        return Saga.model_validate(orm_model, from_attributes=True)

    async def get_by_business_key_for_update(
        self, saga_type: str, business_key: str
    ) -> Saga | None:
        # переход всегда делается под блокировкой строки: конкурирующие события
        # одной саги обрабатываются строго последовательно
        stmt = (
            select(SagaORM)
            .where(
                SagaORM.saga_type == saga_type,
                SagaORM.business_key == business_key,
            )
            .with_for_update()
        )
        result = await self._session.execute(stmt)
        orm_model = result.scalar_one_or_none()
        if orm_model is None:
            return None
        return Saga.model_validate(orm_model, from_attributes=True)

    async def find_retry_due(self, now: datetime, limit: int) -> list[Saga]:
        # skip_locked: несколько инстансов поллера не конфликтуют
        stmt = (
            select(SagaORM)
            .where(
                SagaORM.retry_after.is_not(None),
                SagaORM.retry_after <= now,
                SagaORM.status.not_in(list(TERMINAL_STATUSES)),
            )
            .order_by(SagaORM.retry_after.asc())
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        result = await self._session.execute(stmt)
        return [
            Saga.model_validate(orm_model, from_attributes=True)
            for orm_model in result.scalars().all()
        ]

    async def find_deadline_due(self, now: datetime, limit: int) -> list[Saga]:
        # retry_after IS NULL: если ретрай уже запланирован, дедлайн неактивен
        stmt = (
            select(SagaORM)
            .where(
                SagaORM.deadline_at.is_not(None),
                SagaORM.deadline_at <= now,
                SagaORM.retry_after.is_(None),
                SagaORM.status.not_in(list(TERMINAL_STATUSES)),
            )
            .order_by(SagaORM.deadline_at.asc())
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        result = await self._session.execute(stmt)
        return [
            Saga.model_validate(orm_model, from_attributes=True)
            for orm_model in result.scalars().all()
        ]

    async def list_sagas(
        self,
        saga_type: str | None,
        status: str | None,
        limit: int,
        offset: int,
    ) -> list[Saga]:
        stmt = select(SagaORM).order_by(SagaORM.created_at.desc(), SagaORM.id.desc())
        if saga_type:
            stmt = stmt.where(SagaORM.saga_type == saga_type)
        if status:
            stmt = stmt.where(SagaORM.status == SagaStatus(status))
        stmt = stmt.limit(limit).offset(offset)
        result = await self._session.execute(stmt)
        return [
            Saga.model_validate(orm_model, from_attributes=True)
            for orm_model in result.scalars().all()
        ]

    async def list_stuck(self, older_than: datetime, limit: int) -> list[Saga]:
        # "застрявшие": нетерминальные, не менявшиеся дольше порога
        last_touched = func.coalesce(SagaORM.updated_at, SagaORM.created_at)
        stmt = (
            select(SagaORM)
            .where(
                SagaORM.status.not_in(list(TERMINAL_STATUSES)),
                last_touched < older_than,
            )
            .order_by(last_touched.asc())
            .limit(limit)
        )
        result = await self._session.execute(stmt)
        return [
            Saga.model_validate(orm_model, from_attributes=True)
            for orm_model in result.scalars().all()
        ]
