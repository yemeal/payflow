import pytest
from unittest.mock import AsyncMock, MagicMock
from decimal import Decimal

from app.entrypoints.messaging.consumer import router
from app.domain.payments import Payment


@pytest.mark.asyncio
async def test_handle_process_payment_command_success():
    # Arrange
    payment_service = AsyncMock()
    idempotency_service = MagicMock()

    # Мокаем контекстный менеджер IdempotencyGuard
    guard = MagicMock()
    guard.has_cached_result = False
    guard.cached_status_code = None

    # Настраиваем асинхронный контекстный менеджер
    async_context = MagicMock()
    async_context.__aenter__.return_value = guard
    async_context.__aexit__.return_value = None
    idempotency_service.return_value = async_context

    from uuid import uuid4
    from datetime import datetime
    from app.domain.payments import PaymentStatus

    # Настраиваем результат создания платежа с использованием реального Payment объекта
    created_payment = Payment(
        id=uuid4(),
        idempotency_key="12345678-1234-5678-1234-567812345678",
        amount=Decimal("150.00"),
        currency="USD",
        status=PaymentStatus.PROCESSING,
        customer_id="cust_12345",
        description="Integration Test",
        created_at=datetime.utcnow(),
    )

    payment_service.create.return_value = created_payment

    msg = {
        "metadata": {
            "commandId": "12345678-1234-5678-1234-567812345678",
            "commandType": "payment.process",
            "timestamp": "2026-07-08T04:29:50Z",
            "source": "orchestrator",
        },
        "data": {
            "amount": "150.00",
            "currency": "USD",
            "customerId": "cust_12345",
            "description": "Integration Test",
        },
    }

    # Act
    await router.handle(
        command_type="payment.process",
        msg=msg,
        payment_service=payment_service,
        idempotency_service=idempotency_service,
    )

    # Assert
    payment_service.create.assert_called_once()
    called_payload, called_idempotency_key = payment_service.create.call_args[0]
    assert called_idempotency_key == "12345678-1234-5678-1234-567812345678"
    assert called_payload.amount == Decimal("150.00")
    assert called_payload.currency == "USD"
    assert called_payload.customer_id == "cust_12345"
    assert called_payload.description == "Integration Test"

    guard.set_result.assert_called_once()


@pytest.mark.asyncio
async def test_handle_process_payment_command_idempotency_hit():
    # Arrange
    payment_service = AsyncMock()
    idempotency_service = MagicMock()

    # Мокаем контекстный менеджер IdempotencyGuard для случая хита в кэше
    guard = MagicMock()
    guard.has_cached_result = True
    guard.cached_status_code = 201
    guard.cached_response = {"id": "payment-123", "status": "PROCESSING"}

    async_context = MagicMock()
    async_context.__aenter__.return_value = guard
    async_context.__aexit__.return_value = None
    idempotency_service.return_value = async_context

    msg = {
        "metadata": {
            "commandId": "12345678-1234-5678-1234-567812345678",
            "commandType": "payment.process",
            "timestamp": "2026-07-08T04:29:50Z",
            "source": "orchestrator",
        },
        "data": {"amount": "150.00", "currency": "USD"},
    }

    # Act
    res = await router.handle(
        command_type="payment.process",
        msg=msg,
        payment_service=payment_service,
        idempotency_service=idempotency_service,
    )

    # Assert
    assert res == {"id": "payment-123", "status": "PROCESSING"}
    payment_service.create.assert_not_called()
    guard.set_result.assert_not_called()


@pytest.mark.asyncio
async def test_handle_unknown_command():
    # Arrange
    payment_service = AsyncMock()
    idempotency_service = MagicMock()

    # Act
    res = await router.handle(
        command_type="unknown.command",
        msg={},
        payment_service=payment_service,
        idempotency_service=idempotency_service,
    )

    # Assert
    assert res is None
    payment_service.create.assert_not_called()
