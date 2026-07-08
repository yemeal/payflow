from typing import Protocol, Awaitable, Callable

import structlog

from app.domain.exceptions.payments import PaymentNotFoundError
from app.infrastructure.exceptions.payment_providers import (
    ProviderIntegrationError,
    ProviderUnavailableError,
)  # TODO исправить напраление зависмости
from app.domain.payments import Payment
from app.domain.outbox import OutboxEvent
from app.domain.payments import PaymentStatus
from app.application.ports.repositories import PaymentRepositoryProtocol
from app.application.ports.repositories import OutboxRepositoryProtocol
from app.entrypoints.http.schemas.payments import (
    PaymentResponse,
    PaymentCreate,
)  # TODO исправить напраление зависмости
from app.application.services.idempotency import IdempotencyCachedResult
from app.application.ports.uow import AsyncUOWProtocol
from app.application.ports.payment_provider import PaymentProviderProtocol
from app.application.ports.dto import ProviderTransactionRequest

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
        Создать новый платеж.

        ВАЖНО: вызов провайдера выполняется ВНЕ транзакции БД.
        Удержание соединения на время внешнего HTTP-вызова (таймауты * ретраи -
        до десятков секунд) исчерпывает пул под нагрузкой и роняет весь сервис.

        Порядок:
        1) короткая транзакция: PENDING-платеж + payment.pending в outbox;
        2) без транзакции: вызов провайдера;
        3) короткая транзакция: финальный статус + событие в outbox.

        Известный edge case: при падении процесса между (1) и (3) платеж останется
        PENDING без external_id — подбирается reconciliation'ом (расширение
        get_processing_payments на "зависшие" PENDING — см. TECH_DEBT.md).
        """
        new_payment = Payment(
            idempotency_key=idempotency_key,
            **payload.model_dump(),
            status=PaymentStatus.PENDING,
        )

        # Транзакция 1: фиксируем PENDING + событие о создании
        async with self._uow:
            created_payment = await self._payment_repository.create(new_payment)
            await self._create_status_event(created_payment)
            logger.info(
                "payment and event created",
                payment_id=str(created_payment.id),
                amount=str(created_payment.amount),
                currency=str(created_payment.currency),
                status=str(created_payment.status.value),
            )

        # Вне транзакции: внешний HTTP-вызов провайдера
        provider_error = None
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
        except (ProviderIntegrationError, ProviderUnavailableError) as e:
            created_payment.status = PaymentStatus.FAILED
            provider_error = e

        logger.info(
            "payment status changed",
            payment_id=str(created_payment.id),
            status=created_payment.status.value,
        )

        # Транзакция 2: финальный статус + событие
        async with self._uow:
            created_payment = await self._payment_repository.update(created_payment)
            await self._create_status_event(created_payment)

        # Пробрасываем ошибку дальше уже после того, как UOW успешно закоммитил статус FAILED
        if provider_error:
            raise provider_error
        return created_payment

    async def _create_status_event(self, payment: Payment) -> None:
        """Записать событие о текущем статусе платежа в outbox (в рамках активной транзакции)"""
        outbox_event = OutboxEvent(
            event_type=f"payment.{payment.status.value.lower()}",
            payload=PaymentResponse.model_validate(payment).model_dump(mode="json"),
        )
        await self._outbox_repository.create(outbox_event)

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
