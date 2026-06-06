from typing import Protocol

from sqlalchemy import select

from app.models import Payment
from app.repositories.base_repository import SQLAlchemyAsyncRepository

class PaymentRepositoryProtocol(Protocol):
    async def create(self, payment: Payment) -> Payment:
        ...
    async def get(self, payment_id: str) -> Payment | None:
        ...
    async def find_by_idempotency_key(self, idempotency_key: str) -> Payment | None:
        ...

class PaymentRepository(SQLAlchemyAsyncRepository[Payment]):
    """Специализированный репозиторий для платежей"""

    async def find_by_idempotency_key(self, key: str) -> Payment | None:
        stmt = select(Payment).where(Payment.idempotency_key == key)
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()
