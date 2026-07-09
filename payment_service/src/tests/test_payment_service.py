import pytest
from unittest.mock import AsyncMock, MagicMock
from decimal import Decimal
from uuid import uuid4

from app.application.services.payment_service import PaymentService
from app.domain.payments import Payment
from app.domain.outbox import OutboxEvent
from app.domain.payments import PaymentStatus
from app.infrastructure.exceptions.payment_providers import ProviderUnavailableError, ProviderIntegrationError

@pytest.fixture
def mock_payment_repo():
    return AsyncMock()

@pytest.fixture
def mock_uow():
    uow = AsyncMock()
    uow.__aenter__.return_value = None
    uow.__aexit__.return_value = None
    return uow

@pytest.fixture
def mock_provider():
    return AsyncMock()

@pytest.fixture
def mock_outbox_repo():
    return AsyncMock()

@pytest.fixture
def service(mock_payment_repo, mock_uow, mock_provider, mock_outbox_repo):
    return PaymentService(
        payment_repository=mock_payment_repo,
        uow=mock_uow,
        payment_provider=mock_provider,
        outbox_repository=mock_outbox_repo,
    )

def make_payment_create():
    """Мок PaymentCreate: create() зовет только payload.model_dump()"""
    payload = MagicMock()
    payload.model_dump.return_value = {
        "amount": Decimal("100.00"),
        "currency": "RUB",
        "customer_id": None,
        "description": None,
    }
    return payload


@pytest.mark.asyncio
async def test_create_provider_call_outside_transaction(
    service, mock_provider, mock_payment_repo, mock_outbox_repo, mock_uow
):
    """
    Регресс-тест к TECH_DEBT п.6: HTTP-вызов провайдера не должен выполняться
    внутри открытой транзакции БД (иначе пул исчерпывается при деградации провайдера).
    Проверяем: на момент вызова провайдера первая транзакция уже закрыта.
    """
    uow_depth = 0
    provider_called_at_depth = None

    async def uow_enter(*args):
        nonlocal uow_depth
        uow_depth += 1

    async def uow_exit(*args):
        nonlocal uow_depth
        uow_depth -= 1

    mock_uow.__aenter__.side_effect = uow_enter
    mock_uow.__aexit__.side_effect = uow_exit

    async def initiate(*args, **kwargs):
        nonlocal provider_called_at_depth
        provider_called_at_depth = uow_depth
        response = MagicMock()
        response.transaction_id = "ext-42"
        return response

    mock_provider.initiate_transaction.side_effect = initiate
    mock_payment_repo.create.side_effect = lambda p: p
    mock_payment_repo.update.side_effect = lambda p: p

    payment = await service.create(make_payment_create(), idempotency_key="key-42")

    assert provider_called_at_depth == 0  # провайдер вызван вне транзакции
    assert payment.status == PaymentStatus.PROCESSING
    assert payment.external_id == "ext-42"
    # два outbox-события: payment.pending и payment.processing
    assert mock_outbox_repo.create.await_count == 2


@pytest.mark.asyncio
async def test_create_provider_failure_commits_failed_status_then_raises(
    service, mock_provider, mock_payment_repo, mock_outbox_repo
):
    mock_provider.initiate_transaction.side_effect = ProviderUnavailableError(
        message="unavailable", details=""
    )
    mock_payment_repo.create.side_effect = lambda p: p
    mock_payment_repo.update.side_effect = lambda p: p

    with pytest.raises(ProviderUnavailableError):
        await service.create(make_payment_create(), idempotency_key="key-43")

    # FAILED-статус закоммичен до проброса ошибки
    updated_payment = mock_payment_repo.update.await_args.args[0]
    assert updated_payment.status == PaymentStatus.FAILED
    # события: payment.pending + payment.failed
    assert mock_outbox_repo.create.await_count == 2


@pytest.mark.asyncio
async def test_sync_payment_with_provider_completed(service, mock_provider, mock_payment_repo, mock_outbox_repo):
    payment = Payment(
        id=uuid4(),
        idempotency_key="key-1",
        amount=Decimal("100.00"),
        currency="RUB",
        status=PaymentStatus.PROCESSING,
        external_id="ext-1"
    )
    
    status_response = MagicMock()
    status_response.status = "COMPLETED"
    mock_provider.get_transaction_status.return_value = status_response
    
    mock_payment_repo.update.return_value = payment
    
    await service.sync_payment_with_provider(payment)
    
    assert payment.status == PaymentStatus.COMPLETED
    mock_payment_repo.update.assert_called_once_with(payment)
    mock_outbox_repo.create.assert_called_once()
    
@pytest.mark.asyncio
async def test_sync_payment_with_provider_unavailable(service, mock_provider, mock_payment_repo, mock_outbox_repo):
    payment = Payment(
        id=uuid4(),
        idempotency_key="key-2",
        amount=Decimal("100.00"),
        currency="RUB",
        status=PaymentStatus.PROCESSING,
        external_id="ext-2"
    )
    
    mock_provider.get_transaction_status.side_effect = ProviderUnavailableError(message="unavailable", details="")
    
    await service.sync_payment_with_provider(payment)
    
    assert payment.status == PaymentStatus.PROCESSING
    mock_payment_repo.update.assert_not_called()

@pytest.mark.asyncio
async def test_sync_payment_with_provider_integration_error(service, mock_provider, mock_payment_repo, mock_outbox_repo):
    payment = Payment(
        id=uuid4(),
        idempotency_key="key-3",
        amount=Decimal("100.00"),
        currency="RUB",
        status=PaymentStatus.PROCESSING,
        external_id="ext-3"
    )
    
    mock_provider.get_transaction_status.side_effect = ProviderIntegrationError(message="integration error", details="")
    
    await service.sync_payment_with_provider(payment)
    
    assert payment.status == PaymentStatus.PROCESSING
    mock_payment_repo.update.assert_not_called()
