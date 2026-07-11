"""
Тесты KafkaOutboxPublisher и EventEnvelope - контракт исходящих событий в Kafka.

Ключевой инвариант (AGENTS.md, "Партиционирование"): key = payment id, чтобы все
события одного платежа попадали в один partition и порядок сохранялся. Плюс проверяем
корректную сборку конверта (metadata + data) из доменного OutboxEvent.

Kafka не поднимаем: продюсер - AsyncMock, смотрим на аргументы send_and_wait.

Формат документации: Проверяем / Успех / Нежелательное поведение.
"""

import json
import pytest
from unittest.mock import AsyncMock
from uuid import uuid4
from datetime import datetime, timezone

from app.infrastructure.brokers.adapters import KafkaOutboxPublisher
from app.application.ports.dto.events import EventEnvelope
from app.domain.outbox import OutboxEvent


# ---------------------------------------------------------------------------
# EventEnvelope.from_outbox_event
# ---------------------------------------------------------------------------

class TestEventEnvelope:
    def test_from_outbox_event_maps_fields(self):
        """
        Проверяем: сборка конверта из доменного OutboxEvent.
        Успех: event_id/event_type/timestamp взяты из события, data = payload,
               version="1.0", source="payment-service" по умолчанию.
        Нежелательное поведение: расхождение контракта с тем, что ждёт analytics
                   (metadata.eventId, eventType, data...).
        """
        payment_id = str(uuid4())
        created = datetime(2026, 7, 10, 10, 0, 0, tzinfo=timezone.utc)
        event = OutboxEvent(
            event_type="payment.completed",
            payload={"id": payment_id, "status": "COMPLETED"},
            created_at=created,
        )

        envelope = EventEnvelope.from_outbox_event(event)

        assert envelope.metadata.event_id == event.id
        assert envelope.metadata.event_type == "payment.completed"
        assert envelope.metadata.timestamp == created
        assert envelope.metadata.version == "1.0"
        assert envelope.metadata.source == "payment-service"
        assert envelope.data == {"id": payment_id, "status": "COMPLETED"}


# ---------------------------------------------------------------------------
# KafkaOutboxPublisher.publish
# ---------------------------------------------------------------------------

class TestKafkaPublish:
    @pytest.mark.asyncio
    async def test_key_is_payment_id(self):
        """
        Проверяем: ключ сообщения Kafka равен id платежа (партиционирование).
        Успех: send_and_wait получил key = payment_id в байтах и правильный топик.
        Нежелательное поведение: пустой/чужой ключ - события платежа расползутся
                   по partition'ам и потеряют порядок.
        """
        payment_id = str(uuid4())
        producer = AsyncMock()
        publisher = KafkaOutboxPublisher(producer, topic="payments.events")

        event = OutboxEvent(event_type="payment.pending", payload={"id": payment_id})
        await publisher.publish(EventEnvelope.from_outbox_event(event))

        producer.send_and_wait.assert_awaited_once()
        _, kwargs = producer.send_and_wait.call_args
        assert kwargs["topic"] == "payments.events"
        assert kwargs["key"] == payment_id.encode("utf-8")

    @pytest.mark.asyncio
    async def test_value_is_valid_json_envelope(self):
        """
        Проверяем: тело сообщения - валидный JSON с metadata и data.
        Успех: value десериализуется, содержит metadata.eventType и data.id.
        Нежелательное поведение: невалидный JSON или потеря части конверта.
        """
        payment_id = str(uuid4())
        producer = AsyncMock()
        publisher = KafkaOutboxPublisher(producer, topic="payments.events")

        event = OutboxEvent(event_type="payment.pending", payload={"id": payment_id})
        await publisher.publish(EventEnvelope.from_outbox_event(event))

        _, kwargs = producer.send_and_wait.call_args
        decoded = json.loads(kwargs["value"].decode("utf-8"))
        # metadata у EventEnvelope - обычная snake_case модель (не camel),
        # data приходит уже готовым payload'ом (camelCase из PaymentResponse)
        assert decoded["metadata"]["event_type"] == "payment.pending"
        assert decoded["data"]["id"] == payment_id

    @pytest.mark.asyncio
    async def test_missing_id_falls_back_to_empty_key(self):
        """
        Проверяем: в payload нет id (аномалия).
        Успех: publish не падает, ключ становится пустым (b"").
        Нежелательное поведение: исключение внутри publisher рушит relay-батч.
        """
        producer = AsyncMock()
        publisher = KafkaOutboxPublisher(producer, topic="payments.events")

        envelope = EventEnvelope(
            metadata={
                "event_id": uuid4(),
                "event_type": "payment.pending",
                "timestamp": datetime.now(timezone.utc),
            },
            data={},
        )
        await publisher.publish(envelope)

        _, kwargs = producer.send_and_wait.call_args
        assert kwargs["key"] == b""
