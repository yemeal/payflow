"""
Тесты PaymentEventHandler - фасад обработки события в analytics.

Хендлер оркеструет: открывает UoW, проверяет дедупликацию, применяет проекцию,
после коммита инвалидирует кэш аналитики. Сам ничего не считает - только порядок и границы.

Проверяем два ключевых пути: новое событие (полный цикл) и дубль (ранний выход).

Формат документации: Проверяем / Успех / Нежелательное поведение.
"""

import pytest
from unittest.mock import AsyncMock

from app.services.event_handler import PaymentEventHandler
from app.schemas.events import PaymentEvent


def build_handler(uow, dedup_result=True):
    dedup = AsyncMock()
    dedup.register_event.return_value = dedup_result
    projection = AsyncMock()
    cache = AsyncMock()
    handler = PaymentEventHandler(
        uow=uow,
        deduplication_service=dedup,
        projection_service=projection,
        cache=cache,
    )
    return handler, dedup, projection, cache


@pytest.mark.asyncio
async def test_new_event_projects_and_invalidates_cache(in_memory_uow, event_dict_factory):
    """
    Проверяем: пришло новое событие (дедупликация пропускает).
    Успех: проекция применена, кэш аналитики инвалидирован по паттерну summary,
           handle возвращает True, транзакция закоммичена.
    Нежелательное поведение: пропуск проекции или устаревший кэш после обновления данных.
    """
    event = PaymentEvent.model_validate(event_dict_factory(status="COMPLETED"))
    handler, dedup, projection, cache = build_handler(in_memory_uow, dedup_result=True)

    result = await handler.handle(event)

    assert result is True
    projection.project_payment.assert_awaited_once()
    cache.delete_by_pattern.assert_awaited_once_with("analytics:summary:*")
    assert in_memory_uow.commits == 1


@pytest.mark.asyncio
async def test_duplicate_event_is_skipped(in_memory_uow, event_dict_factory):
    """
    Проверяем: событие уже обрабатывалось (дедупликация вернула False).
    Успех: проекция НЕ вызывается, кэш НЕ инвалидируется, handle возвращает False.
    Нежелательное поведение: повторная проекция дубля и лишний сброс кэша.
    """
    event = PaymentEvent.model_validate(event_dict_factory(status="COMPLETED"))
    handler, dedup, projection, cache = build_handler(in_memory_uow, dedup_result=False)

    result = await handler.handle(event)

    assert result is False
    projection.project_payment.assert_not_called()
    cache.delete_by_pattern.assert_not_called()
