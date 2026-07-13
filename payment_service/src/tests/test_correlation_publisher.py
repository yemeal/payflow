"""
Тесты CorrelationEnrichingPublisher - транспортное обогащение конверта корреляцией.

Корреляция саги живёт НЕ в домене (Payment/OutboxEvent о ней не знают), а в журнале
command_correlations. Подставляет её в metadata.correlation транспортный декоратор
при публикации, резолвя по data.id платежа. Ключевые инварианты:
  1) есть корреляция -> она попадает в metadata.correlation и уезжает в Kafka;
  2) нет корреляции (платёж вне саги, HTTP API) -> конверт уходит БЕЗ ключа
     correlation вовсе (exclude_none), а не с correlation: null;
  3) событие публикуется в любом случае: журнал корреляций не может стать
     причиной потери события.

Формат документации: Проверяем / Успех / Нежелательное поведение.
"""

import json
import pytest
from unittest.mock import AsyncMock
from uuid import uuid4
from datetime import datetime, timezone

from app.infrastructure.brokers.adapters import (
    CorrelationEnrichingPublisher,
    KafkaOutboxPublisher,
)
from app.application.ports.dto.events import EventEnvelope
from app.domain.outbox import OutboxEvent


# ---------------------------------------------------------------------------
# Хелперы
# ---------------------------------------------------------------------------


def make_correlation(saga_id=None, business_key="order-42", command_id=None):
    """Echo-блок команды саги: ровно те три поля, что требует контракт."""
    return {
        "sagaId": str(saga_id or uuid4()),
        "businessKey": business_key,
        "commandId": str(command_id or uuid4()),
    }


def make_envelope(payment_id=None, event_type="payment.completed") -> EventEnvelope:
    """Конверт события ровно так, как его собирает relay - из доменного OutboxEvent."""
    event = OutboxEvent(
        event_type=event_type,
        payload={"id": str(payment_id or uuid4()), "status": "COMPLETED"},
        created_at=datetime(2026, 7, 15, 10, 0, 0, tzinfo=timezone.utc),
    )
    return EventEnvelope.from_outbox_event(event)


def make_store(correlation=None):
    """Журнал корреляций: resolve_for_payment отдаёт заданное значение."""
    store = AsyncMock()
    store.resolve_for_payment.return_value = correlation
    return store


# ---------------------------------------------------------------------------
# Обогащение конверта
# ---------------------------------------------------------------------------


class TestCorrelationAttached:
    """Платёж, рождённый командой саги: correlation обязана уехать в событии."""

    @pytest.mark.asyncio
    async def test_correlation_from_store_lands_in_metadata(self):
        """
        Проверяем: стор знает корреляцию платежа.
        Успех: inner.publish получил конверт с metadata.correlation, равным
               тому, что вернул стор (echo, без интерпретации полей).
        Нежелательное поведение: correlation потеряна -> оркестратор не сматчит
               ответ с шагом саги и повиснет до таймаута.
        """
        payment_id = uuid4()
        correlation = make_correlation()
        inner = AsyncMock()
        publisher = CorrelationEnrichingPublisher(inner, make_store(correlation))

        await publisher.publish(make_envelope(payment_id))

        inner.publish.assert_awaited_once()
        published = inner.publish.await_args.args[0]
        assert published.metadata.correlation == correlation

    @pytest.mark.asyncio
    async def test_resolves_by_payment_id_from_data(self):
        """
        Проверяем: по какому ключу декоратор ищет корреляцию.
        Успех: resolve_for_payment вызван со строковым data.id платежа.
        Нежелательное поведение: поиск по чужому полю (event_id, customerId) -
               корреляция не найдётся, событие уедет без неё.
        """
        payment_id = uuid4()
        store = make_store(make_correlation())
        publisher = CorrelationEnrichingPublisher(AsyncMock(), store)

        await publisher.publish(make_envelope(payment_id))

        store.resolve_for_payment.assert_awaited_once_with(str(payment_id))

    @pytest.mark.asyncio
    async def test_correlation_is_echoed_verbatim(self):
        """
        Проверяем: правило echo - участник не интерпретирует значения корреляции.
        Успех: незнакомые/дополнительные поля стора доезжают до конверта как есть.
        Нежелательное поведение: фильтрация или переименование полей ломает echo
               и делает контракт хрупким к расширению.
        """
        correlation = make_correlation()
        correlation["someFutureField"] = "opaque-value"
        inner = AsyncMock()
        publisher = CorrelationEnrichingPublisher(inner, make_store(correlation))

        await publisher.publish(make_envelope())

        published = inner.publish.await_args.args[0]
        assert published.metadata.correlation == correlation


# ---------------------------------------------------------------------------
# Платёж вне саги
# ---------------------------------------------------------------------------


class TestNoCorrelation:
    """Платёж из HTTP API корреляции не имеет: событие уходит без блока."""

    @pytest.mark.asyncio
    async def test_publishes_without_correlation_when_store_returns_none(self):
        """
        Проверяем: стор не нашёл корреляцию (платёж создан вне саги).
        Успех: событие всё равно опубликовано, metadata.correlation остался None.
        Нежелательное поведение: событие проглочено или упало - платежи вне саги
               перестанут попадать в аналитику.
        """
        inner = AsyncMock()
        publisher = CorrelationEnrichingPublisher(inner, make_store(None))

        await publisher.publish(make_envelope())

        inner.publish.assert_awaited_once()
        assert inner.publish.await_args.args[0].metadata.correlation is None

    @pytest.mark.asyncio
    async def test_envelope_without_payment_id_skips_lookup(self):
        """
        Проверяем: в data нет id (аномальное событие).
        Успех: в стор не ходим вовсе, событие публикуется как есть.
        Нежелательное поведение: лишний запрос в БД на каждое битое событие
               либо падение publish -> застревание relay-батча.
        """
        store = make_store(make_correlation())
        inner = AsyncMock()
        publisher = CorrelationEnrichingPublisher(inner, store)

        envelope = EventEnvelope(
            metadata={
                "event_id": uuid4(),
                "event_type": "payment.pending",
                "timestamp": datetime.now(timezone.utc),
            },
            data={},
        )
        await publisher.publish(envelope)

        store.resolve_for_payment.assert_not_awaited()
        inner.publish.assert_awaited_once()


# ---------------------------------------------------------------------------
# Сквозная проверка формата на проводе (декоратор + реальный Kafka-паблишер)
# ---------------------------------------------------------------------------


class TestWireFormat:
    """
    Собираем реальную цепочку CorrelationEnrichingPublisher -> KafkaOutboxPublisher
    и смотрим на байты, которые уходят в продюсер: только так видно, что
    exclude_none действительно убирает пустую корреляцию из JSON.
    """

    @pytest.mark.asyncio
    async def test_correlation_present_in_kafka_payload(self):
        """
        Проверяем: JSON на проводе для платежа в саге.
        Успех: metadata.correlation содержит sagaId/businessKey/commandId.
        Нежелательное поведение: корреляция теряется при сериализации
               (например, если бы её положили в поле, не входящее в модель).
        """
        producer = AsyncMock()
        correlation = make_correlation(business_key="order-77")
        publisher = CorrelationEnrichingPublisher(
            KafkaOutboxPublisher(producer, topic="payments.events"),
            make_store(correlation),
        )

        await publisher.publish(make_envelope())

        _, kwargs = producer.send_and_wait.call_args
        decoded = json.loads(kwargs["value"].decode("utf-8"))
        assert decoded["metadata"]["correlation"] == correlation
        assert decoded["metadata"]["correlation"]["businessKey"] == "order-77"

    @pytest.mark.asyncio
    async def test_correlation_key_absent_when_unknown(self):
        """
        Проверяем: JSON на проводе для платежа вне саги (exclude_none).
        Успех: ключа "correlation" в metadata НЕТ вообще (не null).
        Нежелательное поведение: correlation: null в конверте - оркестратор
               обязан игнорировать такие события, а лишний null засоряет контракт
               и ломает потребителей, различающих отсутствие ключа и null.
        """
        producer = AsyncMock()
        publisher = CorrelationEnrichingPublisher(
            KafkaOutboxPublisher(producer, topic="payments.events"),
            make_store(None),
        )

        await publisher.publish(make_envelope())

        _, kwargs = producer.send_and_wait.call_args
        decoded = json.loads(kwargs["value"].decode("utf-8"))
        assert "correlation" not in decoded["metadata"]

    @pytest.mark.asyncio
    async def test_partition_key_survives_decoration(self):
        """
        Проверяем: декоратор не ломает партиционирование.
        Успех: key сообщения по-прежнему равен id платежа.
        Нежелательное поведение: события одного платежа разъезжаются по partition'ам
               и оркестратор видит их не в том порядке.
        """
        payment_id = uuid4()
        producer = AsyncMock()
        publisher = CorrelationEnrichingPublisher(
            KafkaOutboxPublisher(producer, topic="payments.events"),
            make_store(make_correlation()),
        )

        await publisher.publish(make_envelope(payment_id))

        _, kwargs = producer.send_and_wait.call_args
        assert kwargs["key"] == str(payment_id).encode("utf-8")
