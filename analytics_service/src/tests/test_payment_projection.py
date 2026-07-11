"""
Тесты PaymentProjectionService - применение события к read-модели платежей.

Проекция делегирует запись репозиторию (upsert по id: INSERT ... ON CONFLICT DO UPDATE).
Проверяем, что payload корректно конвертируется в dict и уходит в upsert.

Формат документации: Проверяем / Успех / Нежелательное поведение.
"""

import pytest
from decimal import Decimal
from uuid import uuid4
from datetime import datetime

from app.services.payment_projection import PaymentProjectionService
from app.schemas.payments import PaymentPayload


def make_payload(status="COMPLETED"):
    return PaymentPayload(
        id=uuid4(),
        status=status,
        amount=Decimal("100.00"),
        currency="RUB",
        customer_id="cust-1",
        description="test",
        created_at=datetime(2026, 7, 10, 10, 0, 0),
        updated_at=None,
    )


@pytest.mark.asyncio
async def test_project_payment_calls_upsert(in_memory_payment_repo):
    """
    Проверяем: проекция сохраняет платеж через upsert.
    Успех: в репозитории появляется запись с тем же id и статусом из payload.
    Нежелательное поведение: потеря данных или запись под другим id.
    """
    payload = make_payload(status="COMPLETED")
    service = PaymentProjectionService(in_memory_payment_repo)

    await service.project_payment(payload)

    stored = in_memory_payment_repo.payments[str(payload.id)]
    assert stored["id"] == payload.id
    assert stored["status"] == "COMPLETED"


@pytest.mark.asyncio
async def test_project_payment_is_idempotent_upsert(in_memory_payment_repo):
    """
    Проверяем: повторная проекция того же платежа с новым статусом.
    Успех: запись перезаписывается (upsert), в read-модели один платеж с последним статусом.
    Нежелательное поведение: дублирование строки платежа или застревание старого статуса.
    """
    payload = make_payload(status="PROCESSING")
    service = PaymentProjectionService(in_memory_payment_repo)

    await service.project_payment(payload)
    payload.status = "COMPLETED"
    await service.project_payment(payload)

    assert len(in_memory_payment_repo.payments) == 1
    assert in_memory_payment_repo.payments[str(payload.id)]["status"] == "COMPLETED"
