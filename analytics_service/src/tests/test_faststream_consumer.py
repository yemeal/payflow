"""
Тесты Kafka-консьюмера analytics (topic payments.events) - приём событий и DLQ.

Инвариант (AGENTS.md, "Kafka-консьюмеры"): молча терять сообщения запрещено.
  - валидное событие -> отдаём в PaymentEventHandler;
  - невалидный payload (ValidationError) -> уходит в DLQ, обработка не падает;
  - ошибка обработки -> событие уходит в DLQ;
  - если и DLQ недоступна при ошибке обработки -> raise (сообщение переиграется, не потеряется).

FastStream HandlerCallWrapper вызываем напрямую (он проксирует в исходную функцию).
Kafka не поднимаем: broker.publish подменяем.

Формат документации: Проверяем / Успех / Нежелательное поведение.
"""

import pytest
from unittest.mock import AsyncMock

import app.faststream_app as consumer
from app.faststream_app import handle_payment_event, DLQ_TOPIC


@pytest.mark.asyncio
async def test_valid_event_goes_to_handler(monkeypatch, event_dict_factory):
    """
    Проверяем: пришло валидное событие.
    Успех: событие распарсено и передано в handler.handle; в DLQ ничего не уходит.
    Нежелательное поведение: валидное событие отправлено в DLQ или потеряно.
    """
    publish = AsyncMock()
    monkeypatch.setattr(consumer.broker, "publish", publish)
    handler = AsyncMock()

    await handle_payment_event(event_dict_factory(status="COMPLETED"), handler=handler)

    handler.handle.assert_awaited_once()
    publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_invalid_payload_goes_to_dlq(monkeypatch):
    """
    Проверяем: событие не проходит валидацию схемы.
    Успех: сообщение уходит в DLQ, handler.handle не вызывается, обработчик не падает.
    Нежелательное поведение: молчаливая потеря битого события или падение консьюмера.
    """
    publish = AsyncMock()
    monkeypatch.setattr(consumer.broker, "publish", publish)
    handler = AsyncMock()

    await handle_payment_event({"metadata": {}, "data": {}}, handler=handler)

    handler.handle.assert_not_called()
    publish.assert_awaited_once()
    assert publish.call_args.kwargs["topic"] == DLQ_TOPIC


@pytest.mark.asyncio
async def test_processing_error_goes_to_dlq(monkeypatch, event_dict_factory):
    """
    Проверяем: обработка валидного события упала (ошибка проекции/БД).
    Успех: событие уходит в DLQ, обработчик не пробрасывает исключение наружу.
    Нежелательное поведение: потеря события или бесконечный цикл переигрывания.
    """
    publish = AsyncMock()
    monkeypatch.setattr(consumer.broker, "publish", publish)
    handler = AsyncMock()
    handler.handle.side_effect = RuntimeError("projection failed")

    await handle_payment_event(event_dict_factory(status="COMPLETED"), handler=handler)

    publish.assert_awaited_once()
    assert publish.call_args.kwargs["topic"] == DLQ_TOPIC


@pytest.mark.asyncio
async def test_dlq_failure_on_processing_error_reraises(monkeypatch, event_dict_factory):
    """
    Проверяем: обработка упала И DLQ недоступна.
    Успех: исключение пробрасывается наружу - сообщение не ackается и переиграется
           (не теряется молча).
    Нежелательное поведение: тихое проглатывание события при недоступной DLQ.
    """
    publish = AsyncMock(side_effect=RuntimeError("dlq down"))
    monkeypatch.setattr(consumer.broker, "publish", publish)
    handler = AsyncMock()
    handler.handle.side_effect = RuntimeError("projection failed")

    with pytest.raises(Exception):
        await handle_payment_event(event_dict_factory(status="COMPLETED"), handler=handler)
