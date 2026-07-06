from typing import Protocol, Awaitable, Callable
from datetime import datetime, timezone
from uuid import UUID

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
from app.schemas.payments import PaymentResponse, PaymentCreate
from app.services.idempotency import IdempotencyCachedResult
from app.utils.unit_of_work import AsyncUOWProtocol
from app.integrations.payment_providers.ports import PaymentProviderProtocol
from app.integrations.payment_providers.domain import ProviderTransactionRequest

logger = structlog.get_logger()


class PaymentServiceProtocol(Protocol):
    async def create(self, payload: PaymentCreate, idempotency_key: str) -> Payment: ...

    async def get(self, payment_id: str) -> Payment | None: ...

    async def sync_payment_with_provider(self, payment: Payment) -> None: ...

    async def get_processing_payments(
        self, threshold_seconds: int = 10, limit: int = 100
    ) -> list[Payment]: ...

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

    async def create(self, payload: PaymentCreate, idempotency_key: str) -> Payment:
        """
        Создать новый платеж
        """
        new_payment = Payment(
            idempotency_key=idempotency_key,
            **payload.model_dump(),
            status=PaymentStatus.PENDING,
        )
        provider_error = None

        async with self._uow:
            created_payment = await self._payment_repository.create(new_payment)
            logger.info(
                "payment created and pending",
                payment_id=str(created_payment.id),
                amount=str(created_payment.amount),
                currency=str(created_payment.currency),
                status=str(created_payment.status.value),
            )

            try:
                provider_request = ProviderTransactionRequest(
                    amount=created_payment.amount,
                    currency=created_payment.currency,
                )
                response = await self._payment_provider.initiate_transaction(
                    provider_request
                )
                if response.transaction_id:
                    created_payment.external_id = response.transaction_id
                    created_payment.status = PaymentStatus.PROCESSING
                    logger.info(
                        "payment_status_changed",
                        payment_id=str(created_payment.id),
                        status="PROCESSING",
                    )
            except (ProviderIntegrationError, ProviderUnavailableError) as e:
                created_payment.status = PaymentStatus.FAILED
                logger.info(
                    "payment_status_changed",
                    payment_id=str(created_payment.id),
                    status="FAILED",
                )
                provider_error = e

            created_payment = await self._payment_repository.update(created_payment)

            outbox_event = OutboxEvent(
                event_type=f"payment.{created_payment.status.value.lower()}",
                payload=PaymentResponse.model_validate(created_payment).model_dump(
                    mode="json"
                ),
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

    async def sync_payment_with_provider(self, payment: Payment) -> None:
        """
        Синхронизировать статус платежа со статусом платежа у провайдера
        """
        if not payment.external_id:
            logger.warning(
                "payment sync attempt without external_id", payment_id=str(payment.id)
            )
            return

        try:
            status_response = await self._payment_provider.get_transaction_status(
                payment.external_id
            )
        except ProviderUnavailableError as e:
            logger.warning(
                "provider unavailable during sync",
                payment_id=str(payment.id),
                error=str(e),
            )
            return  # провайдер недоступен, попробуем в следующий раз
        except ProviderIntegrationError as e:
            logger.error(
                "provider integration error during sync",
                payment_id=str(payment.id),
                error=str(e),
            )
            # если провайдер вернул ошибку, которую не отретраить (например 404),
            # мы не можем быть уверены, что платеж не прошел.
            # поэтому оставляем его в статусе PROCESSING для дальнейших попыток или ручного разбора.
            return
        else:
            if status_response.status == PaymentStatus.COMPLETED.value:
                new_status = PaymentStatus.COMPLETED
            elif status_response.status == PaymentStatus.FAILED.value:
                new_status = PaymentStatus.FAILED
            else:
                return  # все еще в процессе

        if payment.status != new_status:
            payment.status = new_status
            async with self._uow:
                # в одной транзакции обновляем статус и генерим событие в таблицу аутбокса о смене статуса
                updated_payment = await self._payment_repository.update(payment)
                outbox_event = OutboxEvent(
                    event_type=f"payment.{updated_payment.status.value.lower()}",
                    payload=PaymentResponse.model_validate(updated_payment).model_dump(
                        mode="json"
                    ),
                )
                await self._outbox_repository.create(outbox_event)
                logger.info(
                    "payment status synced",
                    payment_id=str(updated_payment.id),
                    status=updated_payment.status.value,
                )

    async def get_processing_payments(
        self, threshold_seconds: int = 10, limit: int = 100
    ) -> list[Payment]:
        """
        Прокси-метод для получения платежей

        Клиенты сервиса не должны напрямую ходить в репозиторий
        """
        return await self._payment_repository.get_processing_payments(
            threshold_seconds=threshold_seconds, limit=limit
        )

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
