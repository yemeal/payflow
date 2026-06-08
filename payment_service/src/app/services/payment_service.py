from typing import Protocol, Awaitable, Callable

import structlog

from app.core.exceptions.payment import PaymentNotFoundError
from app.models import Payment
from app.repositories.payment_repository import PaymentRepositoryProtocol
from app.schemas.payments import PaymentResponse
from app.services.idempotency import IdempotencyCachedResult
from app.utils.unit_of_work import AsyncUOWProtocol

logger = structlog.get_logger()

class PaymentServiceProtocol(Protocol):
    async def create(self, payment: Payment) -> Payment:
        ...

    async def get(self, payment_id: str) -> Payment | None:
        ...

    def build_idempotency_db_lookup(self) -> Callable[[str], Awaitable[IdempotencyCachedResult | None]]:
        ...


class PaymentService:
    def __init__(
            self,
            payment_repository: PaymentRepositoryProtocol,
            uow: AsyncUOWProtocol,
    ) -> None:
        self._payment_repository = payment_repository
        self._uow = uow

    async def create(self, new_payment: Payment) -> Payment:
        """
        Создать новый платеж
        """
        async with self._uow:
            created_payment = await self._payment_repository.create(new_payment)

        logger.info(
            "payment_created",
            payment_id=str(created_payment.id),
            amount=str(created_payment.amount),
            currency=str(created_payment.currency),
            status=str(created_payment.status),
        )
        return created_payment

    async def get(self, payment_id: str) -> Payment | None:
        """
        Получить платеж по его айди
        """
        payment = await self._payment_repository.get(payment_id)
        if not payment:
            raise PaymentNotFoundError(f"Платеж с id={payment_id} не существует")
        return payment

    def build_idempotency_db_lookup(self) -> Callable[[str], Awaitable[IdempotencyCachedResult | None]]:
        """
        создает callback для IdempotencyGuard - поиск платежа по клбючу идемпотентности
        замыкание захватывает self._payment_repository
        """
        async def lookup(key: str) -> IdempotencyCachedResult | None:
            payment = await self._payment_repository.find_by_idempotency_key(key)
            if payment is None:
                return None
            return IdempotencyCachedResult(
                status_code=201,
                response=PaymentResponse.model_validate(payment).model_dump(mode="json"),
            )
        return lookup