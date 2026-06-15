from typing import Protocol, Awaitable, Callable
from datetime import datetime, timezone

import structlog

from app.core.exceptions.payment import PaymentNotFoundError
from app.core.exceptions.payment_provider import (
    ProviderIntegrationError,
    ProviderUnavailableError,
)
from app.models import Payment, OutboxEvent
from app.models.payments import PaymentStatus
from app.repositories.payment_repository import PaymentRepositoryProtocol
from app.repositories.outbox_repository import OutboxRepositoryProtocol
from app.schemas.payments import PaymentResponse
from app.services.idempotency import IdempotencyCachedResult
from app.utils.unit_of_work import AsyncUOWProtocol
from app.integrations.payment_provider_client import (
    PaymentProviderProtocol,
    ProviderPaymentRequest,
)

logger = structlog.get_logger()


class PaymentServiceProtocol(Protocol):
    async def create(self, payment: Payment) -> Payment: ...

    async def get(self, payment_id: str) -> Payment | None: ...

    def build_idempotency_db_lookup(
        self,
    ) -> Callable[[str], Awaitable[IdempotencyCachedResult | None]]: ...


class PaymentService:
    def __init__(
        self,
        payment_repository: PaymentRepositoryProtocol,
        uow: AsyncUOWProtocol,
        payment_provider: PaymentProviderProtocol,
        outbox_repository: OutboxRepositoryProtocol,
    ) -> None:
        self._payment_repository = payment_repository
        self._uow = uow
        self._payment_provider = payment_provider
        self._outbox_repository = outbox_repository

    async def create(self, new_payment: Payment) -> Payment:
        """
        Создать новый платеж
        """
        new_payment.status = PaymentStatus.PROCESSING
        provider_error = None

        async with self._uow:
            created_payment = await self._payment_repository.create(new_payment)

            logger.info(
                "payment_created_and_processing",
                payment_id=str(created_payment.id),
                amount=str(created_payment.amount),
                currency=str(created_payment.currency),
                status=str(created_payment.status.value),
            )

            logger.info("provider_request_started", payment_id=str(created_payment.id))

            try:
                provider_request = ProviderPaymentRequest(
                    amount=created_payment.amount,
                    currency=created_payment.currency,
                    customer_id=created_payment.customer_id or "unknown",
                )
                response = await self._payment_provider.process_payment(
                    provider_request
                )

                created_payment.status = PaymentStatus.COMPLETED
                if "transaction_id" in response:
                    created_payment.external_id = response["transaction_id"]

                logger.info(
                    "payment_status_changed",
                    payment_id=str(created_payment.id),
                    status="COMPLETED",
                )

            except (ProviderIntegrationError, ProviderUnavailableError) as e:
                created_payment.status = PaymentStatus.FAILED
                logger.info(
                    "payment_status_changed",
                    payment_id=str(created_payment.id),
                    status="FAILED",
                )
                provider_error = e

            outbox_event = OutboxEvent(
                event_type=f"payment.{created_payment.status.value.lower()}",
                payload={
                    "payment_id": str(created_payment.id),
                    "amount": str(created_payment.amount),
                    "currency": created_payment.currency,
                    "status": created_payment.status.value,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
            )
            await self._outbox_repository.create(outbox_event)

        # Пробрасываем ошибку дальше уже после того, как UOW успешно закоммитил статус FAILED
        if provider_error:
            raise provider_error

        return created_payment

    async def get(self, payment_id: str) -> Payment | None:
        """
        Получить платеж по его айди
        """
        payment = await self._payment_repository.get(payment_id)
        if not payment:
            raise PaymentNotFoundError(f"Платеж с id={payment_id} не существует")
        return payment

    def build_idempotency_db_lookup(
        self,
    ) -> Callable[[str], Awaitable[IdempotencyCachedResult | None]]:
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
                response=PaymentResponse.model_validate(payment).model_dump(
                    mode="json"
                ),
            )

        return lookup
