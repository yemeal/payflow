"""
Тесты EventDeduplicationService - второй уровень exactly-once в analytics.

Kafka-консьюмер работает at-least-once (NACK_ON_ERROR), поэтому одно и то же событие
может прийти повторно (redelivery). Дедупликация по event_id в таблице processed_events
гарантирует, что проекция применяется ровно один раз.

Формат документации: Проверяем / Успех / Нежелательное поведение.
"""

import pytest

from app.services.deduplication import EventDeduplicationService


@pytest.mark.asyncio
async def test_new_event_is_registered(in_memory_processed_events):
    """
    Проверяем: событие с новым event_id.
    Успех: register_event возвращает True (событие впервые, можно обрабатывать).
    Нежелательное поведение: новое событие принято за дубль и пропущено.
    """
    service = EventDeduplicationService(in_memory_processed_events)

    assert await service.register_event("evt-1") is True


@pytest.mark.asyncio
async def test_duplicate_event_is_rejected(in_memory_processed_events):
    """
    Проверяем: повторный event_id (redelivery из Kafka).
    Успех: первый вызов True, второй с тем же id - False (дубль отфильтрован).
    Нежелательное поведение: повторная обработка -> двойной учёт платежа в аналитике.
    """
    service = EventDeduplicationService(in_memory_processed_events)

    assert await service.register_event("evt-2") is True
    assert await service.register_event("evt-2") is False
