"""
Тесты PaymentService - ядро флоу создания и сверки платежа.

Формат документации у каждого теста единый:
    Проверяем: какое поведение под контролем.
    Успех: что должно произойти, чтобы тест был зелёным.
    Нежелательное поведение: что мы этим тестом ловим (ради чего он существует).

Ключевые инварианты (см. AGENTS.md, раздел "Вызов провайдера"):
  1) create() разбит на две короткие транзакции вокруг HTTP-вызова провайдера;
     сам вызов провайдера идёт ВНЕ транзакции БД.
  2) На каждый переход статуса пишется отдельное outbox-событие.
  3) Ошибка провайдера сначала коммитит статус FAILED, и только потом пробрасывается.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock
from decimal import Decimal
from uuid import uuid4

from app.application.services.payment_service import PaymentService
from app.domain.payments import Payment, PaymentStatus
from app.application.services.idempotency import IdempotencyCachedResult
from app.domain.exceptions.payments import PaymentNotFoundError
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
    # по умолчанию репозиторий отдаёт то, что ему передали
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
    """
    Заглушка PaymentCreate: create() дергает у payload только model_dump().
    Полноценную схему тут не строим, чтобы не тащить валидацию в юнит-тест.
    """
    payload = MagicMock()
    payload.model_dump.return_value = {
        "amount": Decimal(amount),
        "currency": currency,
        "customer_id": None,
        "description": None,
    }
    return payload


def provider_initiated(transaction_id="ext-1"):
    """Ответ провайдера на инициацию транзакции."""
    response = MagicMock()
    response.transaction_id = transaction_id
    return response


# ---------------------------------------------------------------------------
# create(): успешный флоу
# ---------------------------------------------------------------------------

class TestCreateSuccess:
    """POST-флоу: платеж создаётся, провайдер отвечает, статус становится PROCESSING."""

    @pytest.mark.asyncio
    async def test_happy_path_transitions_to_processing(
        self, service, mock_provider, mock_outbox_repo
    ):
        """
        Проверяем: успешное создание платежа с ответом провайдера.
        Успех: статус PROCESSING, external_id взят из ответа провайдера,
               в outbox ровно два события (payment.pending и payment.processing).
        Нежелательное поведение: потеря external_id, единственное событие
               вместо двух, статус, отличный от PROCESSING.
        """
        mock_provider.initiate_transaction.return_value = provider_initiated("ext-42")

        payment = await service.create(make_payment_create(), idempotency_key="key-1")

        assert payment.status == PaymentStatus.PROCESSING
        assert payment.external_id == "ext-42"
        assert mock_outbox_repo.create.await_count == 2

    @pytest.mark.asyncio
    async def test_provider_called_outside_transaction(
        self, service, mock_provider, mock_uow
    ):
        """
        Проверяем: регресс к TECH_DEBT п.6 - HTTP-вызов провайдера не должен
                   идти внутри открытой транзакции БД.
        Успех: на момент вызова провайдера глубина открытых транзакций равна 0.
        Нежелательное поведение: провайдер вызван внутри UOW - под деградацией
                   провайдера соединения висят и пул БД исчерпывается.
        """
        depth = 0
        provider_called_at_depth = None

        async def uow_enter(*args):
            nonlocal depth
            depth += 1

        async def uow_exit(*args):
            nonlocal depth
            depth -= 1

        mock_uow.__aenter__.side_effect = uow_enter
        mock_uow.__aexit__.side_effect = uow_exit

        async def initiate(*args, **kwargs):
            nonlocal provider_called_at_depth
            provider_called_at_depth = depth
            return provider_initiated("ext-1")

        mock_provider.initiate_transaction.side_effect = initiate

        await service.create(make_payment_create(), idempotency_key="key-2")

        assert provider_called_at_depth == 0

    @pytest.mark.asyncio
    async def test_two_transactions_opened(self, service, mock_provider, mock_uow):
        """
        Проверяем: create() открывает ровно две транзакции (до и после вызова провайдера).
        Успех: __aenter__ у UOW вызван дважды.
        Нежелательное поведение: одна длинная транзакция вокруг вызова провайдера.
        """
        mock_provider.initiate_transaction.return_value = provider_initiated()

        await service.create(make_payment_create(), idempotency_key="key-3")

        assert mock_uow.__aenter__.await_count == 2

    @pytest.mark.asyncio
    async def test_empty_transaction_id_keeps_pending(
        self, service, mock_provider, mock_outbox_repo
    ):
        """
        Проверяем: провайдер ответил без transaction_id (пустая строка).
        Успех: статус остаётся PENDING, external_id не проставлен, ошибка не летит,
               второе событие всё равно записано (финальный статус зафиксирован).
        Нежелательное поведение: молчаливый перевод в PROCESSING без external_id.
        """
        mock_provider.initiate_transaction.return_value = provider_initiated("")

        payment = await service.create(make_payment_create(), idempotency_key="key-4")

        assert payment.status == PaymentStatus.PENDING
        assert payment.external_id is None
        assert mock_outbox_repo.create.await_count == 2


# ---------------------------------------------------------------------------
# create(): ошибки провайдера
# ---------------------------------------------------------------------------

class TestCreateProviderFailure:
    """Провайдер недоступен или вернул невосстановимую ошибку."""

    @pytest.mark.asyncio
    async def test_unavailable_commits_failed_then_raises(
        self, service, mock_provider, mock_payment_repo, mock_outbox_repo
    ):
        """
        Проверяем: провайдер недоступен (ProviderUnavailableError).
        Успех: платеж закоммичен со статусом FAILED, записаны два события
               (payment.pending + payment.failed), затем исходная ошибка проброшена.
        Нежелательное поведение: ошибка проброшена ДО коммита FAILED (платеж
               завис бы в PENDING без следа), либо ошибка проглочена.
        """
        mock_provider.initiate_transaction.side_effect = ProviderUnavailableError(
            message="unavailable", details=""
        )

        with pytest.raises(ProviderUnavailableError):
            await service.create(make_payment_create(), idempotency_key="key-5")

        updated = mock_payment_repo.update.await_args.args[0]
        assert updated.status == PaymentStatus.FAILED
        assert mock_outbox_repo.create.await_count == 2

    @pytest.mark.asyncio
    async def test_integration_error_commits_failed_then_raises(
        self, service, mock_provider, mock_payment_repo, mock_outbox_repo
    ):
        """
        Проверяем: провайдер вернул невосстановимую ошибку интеграции.
        Успех: статус FAILED закоммичен, два события записаны, ошибка проброшена.
        Нежелательное поведение: платеж остаётся PENDING, событие FAILED не создано.
        """
        mock_provider.initiate_transaction.side_effect = ProviderIntegrationError(
            message="integration error", details=""
        )

        with pytest.raises(ProviderIntegrationError):
            await service.create(make_payment_create(), idempotency_key="key-6")

        updated = mock_payment_repo.update.await_args.args[0]
        assert updated.status == PaymentStatus.FAILED
        assert mock_outbox_repo.create.await_count == 2


# ---------------------------------------------------------------------------
# get()
# ---------------------------------------------------------------------------

class TestGet:
    @pytest.mark.asyncio
    async def test_get_existing(self, service, mock_payment_repo):
        """
        Проверяем: получение существующего платежа по id.
        Успех: возвращается тот же объект, что отдал репозиторий.
        Нежелательное поведение: подмена объекта или лишний поход в БД.
        """
        payment = Payment(
            idempotency_key="k", amount=Decimal("1"), currency="RUB",
            status=PaymentStatus.PROCESSING,
        )
        mock_payment_repo.get.return_value = payment

        result = await service.get(str(payment.id))

        assert result is payment

    @pytest.mark.asyncio
    async def test_get_missing_raises(self, service, mock_payment_repo):
        """
        Проверяем: запрос несуществующего платежа.
        Успех: поднимается PaymentNotFoundError.
        Нежелательное поведение: возврат None вместо явной ошибки.
        """
        mock_payment_repo.get.return_value = None

        with pytest.raises(PaymentNotFoundError):
            await service.get("no-such-id")


# ---------------------------------------------------------------------------
# sync_payment_with_provider(): reconciliation одного платежа
# ---------------------------------------------------------------------------

class TestSyncPaymentWithProvider:
    """Сверка статуса PROCESSING-платежа с провайдером."""

    def _processing_payment(self, external_id="ext-1"):
        return Payment(
            id=uuid4(),
            idempotency_key="key-sync",
            amount=Decimal("100.00"),
            currency="RUB",
            status=PaymentStatus.PROCESSING,
            external_id=external_id,
        )

    @pytest.mark.asyncio
    async def test_completed_updates_status_and_emits_event(
        self, service, mock_provider, mock_payment_repo, mock_outbox_repo
    ):
        """
        Проверяем: провайдер сообщил COMPLETED.
        Успех: статус платежа переходит в COMPLETED, делается update и ровно одно
               outbox-событие о смене статуса.
        Нежелательное поведение: статус не обновлён, либо событие не отправлено
               (analytics не узнает о завершении платежа).
        """
        payment = self._processing_payment()
        status_response = MagicMock()
        status_response.status = "COMPLETED"
        mock_provider.get_transaction_status.return_value = status_response
        mock_payment_repo.update.return_value = payment

        await service.sync_payment_with_provider(payment)

        assert payment.status == PaymentStatus.COMPLETED
        mock_payment_repo.update.assert_awaited_once_with(payment)
        mock_outbox_repo.create.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_failed_updates_status_and_emits_event(
        self, service, mock_provider, mock_payment_repo, mock_outbox_repo
    ):
        """
        Проверяем: провайдер сообщил FAILED.
        Успех: статус переходит в FAILED, есть update и одно outbox-событие.
        Нежелательное поведение: FAILED не зафиксирован в read-модели.
        """
        payment = self._processing_payment()
        status_response = MagicMock()
        status_response.status = "FAILED"
        mock_provider.get_transaction_status.return_value = status_response
        mock_payment_repo.update.return_value = payment

        await service.sync_payment_with_provider(payment)

        assert payment.status == PaymentStatus.FAILED
        mock_outbox_repo.create.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_still_pending_does_not_update(
        self, service, mock_provider, mock_payment_repo, mock_outbox_repo
    ):
        """
        Проверяем: провайдер вернул промежуточный статус (ещё в процессе).
        Успех: статус не меняется, update и событие не создаются.
        Нежелательное поведение: лишний update/событие на каждый цикл сверки.
        """
        payment = self._processing_payment()
        status_response = MagicMock()
        status_response.status = "PENDING"
        mock_provider.get_transaction_status.return_value = status_response

        await service.sync_payment_with_provider(payment)

        assert payment.status == PaymentStatus.PROCESSING
        mock_payment_repo.update.assert_not_called()
        mock_outbox_repo.create.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_external_id_skips_provider_call(
        self, service, mock_provider, mock_payment_repo
    ):
        """
        Проверяем: платеж без external_id (PENDING-orphan) попал в сверку.
        Успех: провайдер не вызывается, update не делается.
        Нежелательное поведение: запрос к провайдеру с пустым id.
        """
        payment = self._processing_payment(external_id=None)

        await service.sync_payment_with_provider(payment)

        mock_provider.get_transaction_status.assert_not_called()
        mock_payment_repo.update.assert_not_called()

    @pytest.mark.asyncio
    async def test_provider_unavailable_keeps_processing(
        self, service, mock_provider, mock_payment_repo
    ):
        """
        Проверяем: провайдер временно недоступен во время сверки.
        Успех: статус остаётся PROCESSING (повторим в следующий цикл), update нет.
        Нежелательное поведение: платеж помечен FAILED из-за недоступности провайдера.
        """
        payment = self._processing_payment()
        mock_provider.get_transaction_status.side_effect = ProviderUnavailableError(
            message="unavailable", details=""
        )

        await service.sync_payment_with_provider(payment)

        assert payment.status == PaymentStatus.PROCESSING
        mock_payment_repo.update.assert_not_called()

    @pytest.mark.asyncio
    async def test_provider_integration_error_keeps_processing(
        self, service, mock_provider, mock_payment_repo
    ):
        """
        Проверяем: провайдер вернул невосстановимую ошибку (например 404) при сверке.
        Успех: статус остаётся PROCESSING (нельзя гарантировать, что платеж не прошёл),
               update не делается - разбор откладывается.
        Нежелательное поведение: автоматический перевод в FAILED по неоднозначной ошибке.
        """
        payment = self._processing_payment()
        mock_provider.get_transaction_status.side_effect = ProviderIntegrationError(
            message="integration error", details=""
        )

        await service.sync_payment_with_provider(payment)

        assert payment.status == PaymentStatus.PROCESSING
        mock_payment_repo.update.assert_not_called()


# ---------------------------------------------------------------------------
# Прокси и вспомогательные методы
# ---------------------------------------------------------------------------

class TestHelpers:
    @pytest.mark.asyncio
    async def test_get_processing_payments_proxies_to_repo(
        self, service, mock_payment_repo
    ):
        """
        Проверяем: get_processing_payments проксирует запрос в репозиторий.
        Успех: параметры threshold/limit доходят до репозитория без изменений.
        Нежелательное поведение: клиент сервиса ходит в репозиторий напрямую.
        """
        mock_payment_repo.get_processing_payments.return_value = []

        await service.get_processing_payments(threshold_seconds=30, limit=25)

        mock_payment_repo.get_processing_payments.assert_awaited_once_with(
            threshold_seconds=30, limit=25
        )

    @pytest.mark.asyncio
    async def test_idempotency_db_lookup_found(self, service, mock_payment_repo):
        """
        Проверяем: db_lookup для второго уровня идемпотентности нашёл платеж в БД.
        Успех: возвращается IdempotencyCachedResult со status_code 201 и телом ответа.
        Нежелательное поведение: возврат сырого Payment вместо кэшируемого результата.
        """
        payment = Payment(
            id=uuid4(), idempotency_key="key-x", amount=Decimal("100.00"),
            currency="RUB", status=PaymentStatus.PROCESSING,
        )
        mock_payment_repo.find_by_idempotency_key.return_value = payment

        lookup = service.build_idempotency_db_lookup()
        result = await lookup("key-x")

        assert isinstance(result, IdempotencyCachedResult)
        assert result.status_code == 201
        assert result.response["id"] == str(payment.id)

    @pytest.mark.asyncio
    async def test_idempotency_db_lookup_missing(self, service, mock_payment_repo):
        """
        Проверяем: db_lookup не нашёл платеж по ключу.
        Успех: возвращается None (guard пойдёт выполнять бизнес-логику).
        Нежелательное поведение: исключение вместо None на отсутствии записи.
        """
        mock_payment_repo.find_by_idempotency_key.return_value = None

        lookup = service.build_idempotency_db_lookup()
        result = await lookup("absent-key")

        assert result is None
