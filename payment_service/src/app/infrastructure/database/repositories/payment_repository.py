from datetime import datetime, timezone, timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.payments import Payment, PaymentStatus
from app.application.ports.repositories import AsyncRepositoryProtocol
from app.infrastructure.database.repositories.base_repository import (
    SQLAlchemyAsyncRepository,
)
from app.infrastructure.database.models.payments import PaymentORM


class PaymentRepository(SQLAlchemyAsyncRepository[Payment, PaymentORM]):
    """Специализированный репозиторий для платежей"""

    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, Payment, PaymentORM)

    async def find_by_idempotency_key(self, key: str) -> Payment | None:
        stmt = select(PaymentORM).where(PaymentORM.idempotency_key == key)
        result = await self._session.execute(stmt)
        orm_model = result.scalar_one_or_none()
        if not orm_model:
            return None
        return Payment.model_validate(orm_model, from_attributes=True)

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
            Payment.model_validate(orm_model, from_attributes=True)
            for orm_model in result.scalars().all()
        ]
