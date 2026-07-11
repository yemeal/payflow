"""
Интеграционные тесты analytics - связка реальных сервисов обработки события.

Работают НАСТОЯЩИЕ PaymentEventHandler, EventDeduplicationService и
PaymentProjectionService вместе, подменены только адаптеры (репозитории, кэш, UoW).
Проверяем сквозной путь события:

    event (формат провода payment_service) -> дедупликация -> проекция в read-модель
                                            -> инвалидация кэша.

Отдельно фиксируем контракт: конверт, который шлёт payment_service
(metadata в snake_case + data в camelCase), корректно принимается схемой analytics.

Формат документации: Проверяем / Успех / Нежелательное поведение.
"""

import pytest
from uuid import uuid4

from app.services.event_handler import PaymentEventHandler
from app.services.deduplication import EventDeduplicationService
from app.services.payment_projection import PaymentProjectionService
from app.schemas.events import PaymentEvent


@pytest.fixture
def handler(in_memory_uow, in_memory_processed_events, in_memory_payment_repo, in_memory_cache):
    """Связка реальных сервисов analytics на in-memory портах."""
    dedup = EventDeduplicationService(in_memory_processed_events)
    projection = PaymentProjectionService(in_memory_payment_repo)
    return PaymentEventHandler(
        uow=in_memory_uow,
        deduplication_service=dedup,
        projection_service=projection,
        cache=in_memory_cache,
    )


# ---------------------------------------------------------------------------
# Контракт события
# ---------------------------------------------------------------------------

def test_wire_envelope_is_accepted_by_schema(event_dict_factory):
    """
    Проверяем: конверт в формате провода payment_service парсится схемой analytics.
    Успех: PaymentEvent.model_validate принимает metadata (snake_case) и data (camelCase);
           поля доезжают без потерь.
    Нежелательное поведение: рассинхрон контрактов между сервисами - события падают в DLQ.
    """
    raw = event_dict_factory(status="COMPLETED", customer_id="cust-9")

    event = PaymentEvent.model_validate(raw)

    assert event.metadata.event_type == "payment.completed"
    assert event.data.status == "COMPLETED"
    assert event.data.customer_id == "cust-9"


# ---------------------------------------------------------------------------
# Сквозная проекция
# ---------------------------------------------------------------------------

class TestProjectionFlow:
    @pytest.mark.asyncio
    async def test_event_is_projected_into_read_model(self, handler, in_memory_payment_repo, in_memory_cache):
        """
        Проверяем: новое событие доезжает до read-модели.
        Успех: handle вернул True, платеж появился в репозитории со статусом из события,
               кэш сводной аналитики инвалидирован.
        Нежелательное поведение: событие принято, но проекция не применена (аналитика врёт).
        """
        payment_id = uuid4()
        raw = event_dict_factory_status(payment_id, "COMPLETED")

        ok = await handler.handle(PaymentEvent.model_validate(raw))

        assert ok is True
        assert in_memory_payment_repo.payments[str(payment_id)]["status"] == "COMPLETED"
        assert "analytics:summary:*" in in_memory_cache.deleted_patterns

    @pytest.mark.asyncio
    async def test_redelivery_is_deduplicated(self, handler, in_memory_payment_repo):
        """
        Проверяем: то же событие пришло дважды (at-least-once redelivery).
        Успех: первый handle True, второй False; в read-модели ровно один платеж.
        Нежелательное поведение: двойной учёт платежа при повторной доставке из Kafka.
        """
        event_id = uuid4()
        payment_id = uuid4()
        raw = event_dict_factory_status(payment_id, "COMPLETED", event_id=event_id)

        first = await handler.handle(PaymentEvent.model_validate(raw))
        second = await handler.handle(PaymentEvent.model_validate(raw))

        assert first is True
        assert second is False
        assert len(in_memory_payment_repo.payments) == 1

    @pytest.mark.asyncio
    async def test_status_update_via_new_event(self, handler, in_memory_payment_repo):
        """
        Проверяем: по одному платежу приходят два события (PROCESSING, затем COMPLETED).
        Успех: разные event_id обрабатываются оба; read-модель хранит один платеж
               с последним статусом COMPLETED (upsert по id).
        Нежелательное поведение: застревание старого статуса или дублирование платежа.
        """
        payment_id = uuid4()
        raw_proc = event_dict_factory_status(payment_id, "PROCESSING", event_id=uuid4())
        raw_done = event_dict_factory_status(payment_id, "COMPLETED", event_id=uuid4())

        await handler.handle(PaymentEvent.model_validate(raw_proc))
        await handler.handle(PaymentEvent.model_validate(raw_done))

        assert len(in_memory_payment_repo.payments) == 1
        assert in_memory_payment_repo.payments[str(payment_id)]["status"] == "COMPLETED"


# ---------------------------------------------------------------------------
# Локальный хелпер (нужен фиксированный payment_id/event_id)
# ---------------------------------------------------------------------------

def event_dict_factory_status(payment_id, status, event_id=None):
    """
    Конверт с явными payment_id/event_id - для проверки дедупликации и upsert.
    Формат провода: metadata в snake_case, data в camelCase.
    """
    return {
        "metadata": {
            "event_id": str(event_id or uuid4()),
            "event_type": f"payment.{status.lower()}",
            "version": "1.0",
            "timestamp": "2026-07-10T10:00:00Z",
            "source": "payment-service",
        },
        "data": {
            "id": str(payment_id),
            "status": status,
            "amount": "100.00",
            "currency": "RUB",
            "customerId": "cust-1",
            "description": "test",
            "createdAt": "2026-07-10T10:00:00",
            "updatedAt": None,
        },
    }
