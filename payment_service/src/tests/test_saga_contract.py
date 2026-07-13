"""
Тесты контракта саги: блок failure в событиях payment.failed.

Оркестратор принимает решение "ретраить шаг или компенсировать сагу" исключительно
по data.failure.retriable, поэтому каждое payment.failed обязано нести
failure {code, message, retriable} (contracts/payments/payment-result.v1):
  - provider_unavailable      -> retriable=true  (технический сбой, провайдер оживёт);
  - provider_integration_error -> retriable=false (битая интеграция, повтор бессмыслен);
  - payment_declined          -> retriable=false (бизнес-отказ провайдера при сверке).
Успешные события (payment.completed и промежуточные) блока failure нести не должны.

Формат документации: Проверяем / Успех / Нежелательное поведение.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock
from decimal import Decimal

from app.application.services.payment_service import PaymentService
from app.domain.payments import Payment, PaymentStatus
from app.infrastructure.exceptions.payment_providers import (
    ProviderUnavailableError,
    ProviderIntegrationError,
)


# ---------------------------------------------------------------------------
# Фикстуры
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_payment_repo():
    repo = AsyncMock()
    repo.create.side_effect = lambda p: p
    repo.update.side_effect = lambda p: p
    return repo


@pytest.fixture
def mock_outbox_repo():
    return AsyncMock()


@pytest.fixture
def mock_provider():
    return AsyncMock()


@pytest.fixture
def mock_uow():
    uow = AsyncMock()
    uow.__aenter__.return_value = None
    uow.__aexit__.return_value = None
    return uow


@pytest.fixture
def service(mock_payment_repo, mock_uow, mock_provider, mock_outbox_repo):
    return PaymentService(
        payment_repository=mock_payment_repo,
        uow=mock_uow,
        payment_provider=mock_provider,
        outbox_repository=mock_outbox_repo,
    )


def make_payment_create(amount="100.00", currency="RUB"):
    """Заглушка PaymentCreate: create() дергает у payload только model_dump()."""
    payload = MagicMock()
    payload.model_dump.return_value = {
        "amount": Decimal(amount),
        "currency": currency,
        "customer_id": "cust_1",
        "description": None,
    }
    return payload


def emitted_events(mock_outbox_repo):
    """Все outbox-события, записанные сервисом, по порядку."""
    return [call.args[0] for call in mock_outbox_repo.create.call_args_list]


def last_event(mock_outbox_repo):
    """Финальное событие статуса - именно его увидит оркестратор."""
    return emitted_events(mock_outbox_repo)[-1]


def make_status_response(status: str):
    """Ответ провайдера на запрос статуса транзакции."""
    response = MagicMock()
    response.status = status
    return response


# ---------------------------------------------------------------------------
# create(): сбой провайдера
# ---------------------------------------------------------------------------


class TestCreateFailureContract:
    """payment.failed на этапе инициации транзакции у провайдера."""

    @pytest.mark.asyncio
    async def test_provider_unavailable_is_retriable(
        self, service, mock_provider, mock_outbox_repo
    ):
        """
        Проверяем: провайдер недоступен (ретраи и circuit breaker исчерпаны).
        Успех: payment.failed несёт failure.code=provider_unavailable и retriable=true -
               оркестратор повторит шаг оплаты, а не отменит заказ.
        Нежелательное поведение: retriable=false на временном сбое -> сага
               компенсируется (заказ отменён) из-за пятиминутной недоступности провайдера.
        """
        mock_provider.initiate_transaction.side_effect = ProviderUnavailableError(
            "circuit breaker is open"
        )

        with pytest.raises(ProviderUnavailableError):
            await service.create(make_payment_create(), idempotency_key="key-1")

        event = last_event(mock_outbox_repo)
        assert event.event_type == "payment.failed"
        failure = event.payload["failure"]
        assert failure["code"] == "provider_unavailable"
        assert failure["retriable"] is True
        assert failure["message"]

    @pytest.mark.asyncio
    async def test_provider_integration_error_is_not_retriable(
        self, service, mock_provider, mock_outbox_repo
    ):
        """
        Проверяем: ошибка интеграции с провайдером (4xx/битый ответ).
        Успех: payment.failed несёт failure.code=provider_integration_error
               и retriable=false - повтор даст ровно тот же результат.
        Нежелательное поведение: retriable=true -> оркестратор бесконечно ретраит
               шаг, который не может завершиться успехом, и сага висит до таймаута.
        """
        mock_provider.initiate_transaction.side_effect = ProviderIntegrationError(
            "unexpected 422 from provider"
        )

        with pytest.raises(ProviderIntegrationError):
            await service.create(make_payment_create(), idempotency_key="key-2")

        event = last_event(mock_outbox_repo)
        assert event.event_type == "payment.failed"
        failure = event.payload["failure"]
        assert failure["code"] == "provider_integration_error"
        assert failure["retriable"] is False

    @pytest.mark.asyncio
    async def test_unavailable_not_swallowed_by_parent_except(
        self, service, mock_provider, mock_outbox_repo
    ):
        """
        Проверяем: ProviderUnavailableError - подкласс ProviderIntegrationError,
               поэтому порядок except-веток в create() определяет retriable.
        Успех: недоступность классифицируется как provider_unavailable/retriable=true,
               а не проваливается в родительскую ветку интеграционной ошибки.
        Нежелательное поведение: перестановка except-веток тихо превращает
               восстановимый сбой в невосстановимый и роняет саги на ровном месте.
        """
        assert issubclass(ProviderUnavailableError, ProviderIntegrationError)

        mock_provider.initiate_transaction.side_effect = ProviderUnavailableError("down")

        with pytest.raises(ProviderUnavailableError):
            await service.create(make_payment_create(), idempotency_key="key-3")

        failure = last_event(mock_outbox_repo).payload["failure"]
        assert failure["code"] == "provider_unavailable"
        assert failure["retriable"] is True

    @pytest.mark.asyncio
    async def test_failure_block_has_full_contract_shape(
        self, service, mock_provider, mock_outbox_repo
    ):
        """
        Проверяем: форма блока failure.
        Успех: ровно три обязательных поля контракта - code, message, retriable
               нужных типов.
        Нежелательное поведение: недостающее поле - и потребитель падает
               на разборе конверта либо принимает решение по умолчанию.
        """
        mock_provider.initiate_transaction.side_effect = ProviderUnavailableError("x")

        with pytest.raises(ProviderUnavailableError):
            await service.create(make_payment_create(), idempotency_key="key-4")

        failure = last_event(mock_outbox_repo).payload["failure"]
        assert set(failure) >= {"code", "message", "retriable"}
        assert isinstance(failure["code"], str)
        assert isinstance(failure["message"], str)
        assert isinstance(failure["retriable"], bool)

    @pytest.mark.asyncio
    async def test_success_events_carry_no_failure_block(
        self, service, mock_provider, mock_outbox_repo
    ):
        """
        Проверяем: успешный флоу создания платежа.
        Успех: ни payment.pending, ни payment.processing не несут блок failure.
        Нежелательное поведение: пустой/мусорный failure в успешном событии -
               оркестратор может принять его за отказ.
        """
        response = MagicMock()
        response.transaction_id = "ext-1"
        mock_provider.initiate_transaction.return_value = response

        await service.create(make_payment_create(), idempotency_key="key-5")

        events = emitted_events(mock_outbox_repo)
        assert [e.event_type for e in events] == [
            "payment.pending",
            "payment.processing",
        ]
        for event in events:
            assert "failure" not in event.payload


# ---------------------------------------------------------------------------
# sync_payment_with_provider(): бизнес-отказ провайдера
# ---------------------------------------------------------------------------


class TestSyncFailureContract:
    """
    payment.failed рождается и при сверке (reconciliation), где контекста входящей
    команды уже нет: correlation берётся из журнала, а failure - бизнес-отказ.
    """

    @pytest.mark.asyncio
    async def test_declined_by_provider_is_not_retriable(
        self, service, mock_provider, mock_outbox_repo
    ):
        """
        Проверяем: провайдер отклонил транзакцию (карта отклонена и т.п.).
        Успех: payment.failed несёт failure.code=payment_declined и retriable=false -
               оркестратор немедленно компенсирует сагу, а не ретраит оплату.
        Нежелательное поведение: retriable=true -> повторные попытки списать
               по отклонённой карте, шаг саги не завершается.
        """
        payment = Payment(
            idempotency_key="key-sync-1",
            amount=Decimal("100.00"),
            currency="RUB",
            status=PaymentStatus.PROCESSING,
            external_id="ext-1",
            customer_id="cust_1",
        )
        mock_provider.get_transaction_status.return_value = make_status_response("FAILED")

        await service.sync_payment_with_provider(payment)

        event = last_event(mock_outbox_repo)
        assert event.event_type == "payment.failed"
        failure = event.payload["failure"]
        assert failure["code"] == "payment_declined"
        assert failure["retriable"] is False
        assert failure["message"]

    @pytest.mark.asyncio
    async def test_completed_has_no_failure_block(
        self, service, mock_provider, mock_outbox_repo
    ):
        """
        Проверяем: провайдер подтвердил транзакцию.
        Успех: событие payment.completed, блока failure в data нет.
        Нежелательное поведение: failure в успешном событии - оркестратор
               истолкует успешную оплату как отказ и откатит заказ.
        """
        payment = Payment(
            idempotency_key="key-sync-2",
            amount=Decimal("100.00"),
            currency="RUB",
            status=PaymentStatus.PROCESSING,
            external_id="ext-2",
            customer_id="cust_1",
        )
        mock_provider.get_transaction_status.return_value = make_status_response(
            "COMPLETED"
        )

        await service.sync_payment_with_provider(payment)

        event = last_event(mock_outbox_repo)
        assert event.event_type == "payment.completed"
        assert "failure" not in event.payload
        assert event.payload["status"] == "COMPLETED"

    @pytest.mark.asyncio
    async def test_status_unchanged_emits_nothing(
        self, service, mock_provider, mock_outbox_repo
    ):
        """
        Проверяем: сверка не нашла изменения статуса (всё ещё PENDING у провайдера).
        Успех: событий не пишем вовсе.
        Нежелательное поведение: дубли payment.* на каждом цикле reconciliation
               забьют топик и заставят оркестратор дедуплицировать лишнее.
        """
        payment = Payment(
            idempotency_key="key-sync-3",
            amount=Decimal("100.00"),
            currency="RUB",
            status=PaymentStatus.PROCESSING,
            external_id="ext-3",
            customer_id="cust_1",
        )
        mock_provider.get_transaction_status.return_value = make_status_response("PENDING")

        await service.sync_payment_with_provider(payment)

        mock_outbox_repo.create.assert_not_called()
