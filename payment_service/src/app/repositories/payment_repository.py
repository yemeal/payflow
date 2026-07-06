from datetime import datetime, timezone, timedelta

from sqlalchemy import select

from app.models import Payment, PaymentStatus
from app.repositories.base_repository import (
    SQLAlchemyAsyncRepository,
    AsyncRepositoryProtocol,
)
from app.repositories.postgres.models import PaymentORM


class PaymentRepositoryProtocol(AsyncRepositoryProtocol[Payment]):
    async def find_by_idempotency_key(self, idempotency_key: str) -> Payment | None: ...
    async def get_processing_payments(
        self, threshold_seconds: int = 10, limit: int = 100
    ) -> list[Payment]: ...
    async def update(self, entity: Payment) -> Payment: ...


class PaymentRepository(SQLAlchemyAsyncRepository[PaymentORM]):
    """Специализированный репозиторий для платежей"""

    async def create(self, entity: Payment) -> Payment:
        orm_model = PaymentORM(**entity.model_dump())
        self._session.add(orm_model)
        await self._session.flush()
        return Payment.model_validate(orm_model)

    async def update(self, entity: Payment) -> Payment:
        # обазетельно юзаем SELECT FOR UPDATE дабы избежать race condition
        stmt = select(PaymentORM).where(PaymentORM.id == entity.id).with_for_update()
        result = await self._session.execute(stmt)
        orm_model = result.scalar_one_or_none()
        if not orm_model:
            raise ValueError(f"Payment with id {entity.id} not found for update")

        for key, value in entity.model_dump().items():
            setattr(orm_model, key, value)

        await self._session.flush()
        return Payment.model_validate(orm_model)

    async def get(self, entity_id: str | int) -> Payment | None:
        orm_model = await self._session.get(entity=self._model, ident=entity_id)
        if not orm_model:
            return None
        return Payment.model_validate(orm_model)

    async def find_by_idempotency_key(self, key: str) -> Payment | None:
        stmt = select(PaymentORM).where(PaymentORM.idempotency_key == key)
        result = await self._session.execute(stmt)
        orm_model = result.scalar_one_or_none()
        if not orm_model:
            return None
        return Payment.model_validate(orm_model)

    async def get_processing_payments(
        self, threshold_seconds: int = 10, limit: int = 100
    ) -> list[Payment]:
        threshold = timedelta(seconds=threshold_seconds)
        stmt = (
            select(PaymentORM)
            .where(
                PaymentORM.status == PaymentStatus.PROCESSING,
                PaymentORM.external_id.is_not(None),
                PaymentORM.created_at
                <= datetime.now(timezone.utc).replace(tzinfo=None) - threshold,
            )
            .limit(limit)
        )
        result = await self._session.execute(stmt)
        return [
            Payment.model_validate(orm_model) for orm_model in result.scalars().all()
        ]
