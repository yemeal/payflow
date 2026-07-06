import pytest
from unittest.mock import AsyncMock, MagicMock
from decimal import Decimal
from uuid import uuid4

from app.services.payment_service import PaymentService
from app.models import Payment, OutboxEvent
from app.models.payments import PaymentStatus
from app.core.exceptions.payment_provider import ProviderUnavailableError, ProviderIntegrationError

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
