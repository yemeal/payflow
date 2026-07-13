"""
Тесты роутера команд склада - граничная политика ошибок без Kafka и БД.

Формат документации у каждого теста единый:
    Проверяем: какое поведение под контролем.
    Успех: что должно произойти, чтобы тест был зелёным.
    Нежелательное поведение: что мы этим тестом ловим (ради чего он существует).

Политика (docs/saga-design.md, 9.10):
  - невалидный конверт / неизвестный тип - poison: DLQ + ACK (не NACK-цикл);
  - временный сбой сервиса (БД недоступна) - исключение всплывает -> NACK.
"""

import uuid

import pytest

from app.application.ports.dto.commands import ReserveCommand
from app.entrypoints.messaging.router import (
    build_dlq_envelope,
    process_command_message,
)


class _RecordingService:
    """Фейк InventoryService: запоминает вызовы, может симулировать сбой"""

    def __init__(self, raise_on_reserve: Exception | None = None) -> None:
        self.reserve_calls: list[ReserveCommand] = []
        self.commit_calls: list = []
        self.cancel_calls: list = []
        self._raise_on_reserve = raise_on_reserve

    async def reserve(self, command: ReserveCommand) -> None:
        if self._raise_on_reserve is not None:
            raise self._raise_on_reserve
        self.reserve_calls.append(command)

    async def commit_reservation(self, command) -> None:
        self.commit_calls.append(command)

    async def cancel_reservation(self, command) -> None:
        self.cancel_calls.append(command)


class _DlqRecorder:
    def __init__(self) -> None:
        self.calls: list[tuple] = []

    async def __call__(self, original, error: Exception) -> None:
        self.calls.append((original, error))


def _reserve_message(order_id: uuid.UUID, command_id: str | None = None) -> dict:
    return {
        "metadata": {
            "commandId": command_id or str(uuid.uuid7()),
            "commandType": "inventory.reserve",
            "version": "1.0",
            "timestamp": "2026-07-15T10:00:00+00:00",
            "source": "orchestrator",
            "sagaId": str(uuid.uuid7()),
            "businessKey": str(order_id),
        },
        "data": {
            "orderId": str(order_id),
            "items": [{"productId": "sku-1", "quantity": 2}],
            "ttlSeconds": 60,
        },
    }


async def test_valid_reserve_is_dispatched_with_echo():
    """
    Проверяем: валидная команда inventory.reserve доходит до сервиса с echo-данными.
    Успех: reserve вызван один раз, order_id и commandId прокинуты, DLQ пуст.
    Нежелательное поведение: корректная команда теряется или уходит в DLQ.
    """
    order_id = uuid.uuid7()
    command_id = str(uuid.uuid7())
    service = _RecordingService()
    dlq = _DlqRecorder()

    await process_command_message(
        _reserve_message(order_id, command_id), service, dlq
    )

    assert len(service.reserve_calls) == 1
    dispatched = service.reserve_calls[0]
    assert dispatched.order_id == order_id
    assert dispatched.correlation.command_id == command_id
    assert dispatched.correlation.business_key == str(order_id)
    assert dispatched.items[0].product_id == "sku-1"
    assert dlq.calls == []


async def test_unknown_command_type_goes_to_dlq():
    """
    Проверяем: неизвестный тип команды - poison, уходит в DLQ и не роняет консюмер.
    Успех: send_to_dlq вызван один раз, сервис не тронут, исключение не всплыло.
    Нежелательное поведение: неизвестная команда травит партицию вечным NACK.
    """
    service = _RecordingService()
    dlq = _DlqRecorder()
    message = {
        "metadata": {"commandId": str(uuid.uuid7()), "commandType": "inventory.explode"},
        "data": {},
    }

    await process_command_message(message, service, dlq)

    assert len(dlq.calls) == 1
    assert service.reserve_calls == []
    assert service.commit_calls == []
    assert service.cancel_calls == []


async def test_invalid_envelope_goes_to_dlq():
    """
    Проверяем: правильный тип, но битые data (пустой items) - валидация в DLQ.
    Успех: send_to_dlq вызван один раз, сервис не вызван.
    Нежелательное поведение: ValidationError уходит в NACK-цикл вместо DLQ.
    """
    order_id = uuid.uuid7()
    service = _RecordingService()
    dlq = _DlqRecorder()
    message = _reserve_message(order_id)
    message["data"]["items"] = []  # нарушает minItems=1

    await process_command_message(message, service, dlq)

    assert len(dlq.calls) == 1
    assert service.reserve_calls == []


async def test_missing_required_metadata_goes_to_dlq():
    """
    Проверяем: отсутствие обязательных sagaId/businessKey - невалидный конверт в DLQ.
    Успех: send_to_dlq вызван один раз, сервис не вызван.
    Нежелательное поведение: команда без корреляции обрабатывается и рождает событие без echo.
    """
    order_id = uuid.uuid7()
    service = _RecordingService()
    dlq = _DlqRecorder()
    message = _reserve_message(order_id)
    del message["metadata"]["sagaId"]
    del message["metadata"]["businessKey"]

    await process_command_message(message, service, dlq)

    assert len(dlq.calls) == 1
    assert service.reserve_calls == []


async def test_transient_service_error_propagates_for_nack():
    """
    Проверяем: временный сбой сервиса (например БД) всплывает как исключение (NACK).
    Успех: process_command_message пробрасывает ошибку, в DLQ ничего не уходит.
    Нежелательное поведение: технический сбой проглатывается и команда теряется.
    """
    order_id = uuid.uuid7()
    service = _RecordingService(raise_on_reserve=RuntimeError("db is down"))
    dlq = _DlqRecorder()

    with pytest.raises(RuntimeError):
        await process_command_message(_reserve_message(order_id), service, dlq)

    assert dlq.calls == []  # временный сбой в DLQ не уходит - его ретраит NACK


def test_build_dlq_envelope_shape():
    """
    Проверяем: конверт DLQ соответствует contracts/envelope/dlq-envelope.v1.
    Успех: есть original и dlqMeta с sourceTopic/errorClass/errorMessage/failedAt.
    Нежелательное поведение: dlq-watcher не сможет переиграть сообщение из-за формата.
    """
    original = {"metadata": {"commandType": "inventory.reserve"}, "data": {}}
    envelope = build_dlq_envelope(
        original=original,
        source_topic="inventory.commands",
        consumer_group="inventory-service-commands",
        error=ValueError("bad envelope"),
        partition=3,
        offset=42,
    )

    assert envelope["original"] == original
    meta = envelope["dlqMeta"]
    assert meta["sourceTopic"] == "inventory.commands"
    assert meta["consumerGroup"] == "inventory-service-commands"
    assert meta["errorClass"] == "ValueError"
    assert meta["errorMessage"] == "bad envelope"
    assert meta["partition"] == 3
    assert meta["offset"] == 42
    assert meta["redriveCount"] == 0
    assert "failedAt" in meta
