from typing import Protocol, Sequence
from datetime import datetime

from sqlalchemy import select, func
from sqlalchemy.dialects.postgresql import insert

from app.models import Payment
from app.repositories.base_repository import (
    SQLAlchemyAsyncRepository,
    AsyncRepositoryProtocol,
)


class PaymentRepositoryProtocol(AsyncRepositoryProtocol[Payment], Protocol):
    async def upsert(self, payment_data: dict) -> None:
        """
        INSERT ... ON CONFLICT (id) DO UPDATE ...
        """
        ...
        
    async def get_summary(
        self,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
        currency: str | None = None,
    ) -> dict: ...
    
    async def get_payments(
        self,
        status: str | None = None,
        currency: str | None = None,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
        limit: int = 10,
        offset: int = 0,
    ) -> tuple[Sequence[Payment], int]: ...


class PaymentRepository(SQLAlchemyAsyncRepository[Payment]):
    async def upsert(self, payment_data: dict) -> None:
        stmt = insert(Payment).values(**payment_data)

        upsert_stmt = stmt.on_conflict_do_update(
            index_elements=["id"],  # поле(я), по которому ищем конфликт
            set_={
                # указываем, какие поля нужно обновить, если запись уже существует.
                # Мы берем новые значения из stmt.excluded
                "status": stmt.excluded.status,
                "amount": stmt.excluded.amount,
                "currency": stmt.excluded.currency,
                "customer_id": stmt.excluded.customer_id,
                "description": stmt.excluded.description,
                "updated_at": stmt.excluded.updated_at,
            },
        )

        await self._session.execute(upsert_stmt)

    def _apply_filters(self, stmt, status: str | None = None, currency: str | None = None, date_from: datetime | None = None, date_to: datetime | None = None):
        if status:
            stmt = stmt.where(Payment.status == status)
        if currency:
            stmt = stmt.where(Payment.currency == currency)
        if date_from:
            stmt = stmt.where(Payment.created_at >= date_from)
        if date_to:
            stmt = stmt.where(Payment.created_at <= date_to)
        return stmt

    async def get_summary(
        self,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
        currency: str | None = None,
    ) -> dict:
        stmt = select(
            func.count(Payment.id).label("total_transactions"),
            func.sum(Payment.amount).label("total_amount"),
            func.count(Payment.id).filter(Payment.status == "COMPLETED").label("completed_count"),
            func.count(Payment.id).filter(Payment.status == "FAILED").label("failed_count"),
        )
        
        stmt = self._apply_filters(stmt, currency=currency, date_from=date_from, date_to=date_to)
        
        result = await self._session.execute(stmt)
        row = result.fetchone()
        
        if not row:
            return {
                "total_transactions": 0,
                "total_amount": 0.0,
                "completed_count": 0,
                "failed_count": 0,
            }
            
        return {
            "total_transactions": row.total_transactions or 0,
            "total_amount": row.total_amount or 0.0,
            "completed_count": row.completed_count or 0,
            "failed_count": row.failed_count or 0,
        }

    async def get_payments(
        self,
        status: str | None = None,
        currency: str | None = None,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
        limit: int = 10,
        offset: int = 0,
    ) -> tuple[Sequence[Payment], int]:
        # Count query
        count_stmt = select(func.count(Payment.id))
        count_stmt = self._apply_filters(count_stmt, status, currency, date_from, date_to)
        total_count = await self._session.scalar(count_stmt) or 0
        
        if total_count == 0:
            return [], 0

        # Select query
        stmt = select(Payment)
        stmt = self._apply_filters(stmt, status, currency, date_from, date_to)
        stmt = stmt.order_by(Payment.created_at.desc()).limit(limit).offset(offset)
        
        result = await self._session.execute(stmt)
        return result.scalars().all(), total_count
