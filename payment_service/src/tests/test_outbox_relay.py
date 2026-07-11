"""
Тесты OutboxRelayService - транзакционный Outbox, публикация событий в Kafka.

Покрываем: успешную публикацию батча и маркировку SUCCESS, сохранение порядка
при сбое (упавшее событие не даёт обогнать себя новым), обработку "ядовитых"
событий (FAILED после max attempts), инкремент attempts и last_error.

Формат документации: Проверяем / Успех / Нежелательное поведение.
"""

import pytest
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

from app.application.services.outbox_relay import OutboxRelayService
from app.domain.outbox import OutboxEvent, OutboxStatus


def make_event(event_type: str = "payment.pending", attempts: int = 0) -> OutboxEvent:
    return OutboxEvent(
        event_type=event_type,
        payload={"id": "00000000-0000-0000-0000-000000000001"},
        attempts=attempts,
    )


def make_relay(events, publisher=None, max_attempts=3):
    """Собирает relay поверх in-memory scope (uow + outbox_repo) и publisher."""
    outbox_repo = AsyncMock()
    outbox_repo.get_unpublished_events.return_value = events

    scope = MagicMock()
    scope.uow = AsyncMock()
    scope.outbox_repo = outbox_repo

    @asynccontextmanager
    async def scope_factory():
        yield scope

    publisher = publisher or AsyncMock()
    relay = OutboxRelayService(publisher, scope_factory, max_publish_attempts=max_attempts)
    return relay, publisher, outbox_repo


class TestSuccessfulPublish:
    @pytest.mark.asyncio
    async def test_all_events_published_and_marked_success(self):
        """
        Проверяем: все события батча успешно уходят в брокер.
        Успех: publish вызван на каждое событие, все помечены SUCCESS, все обновлены в БД.
        Нежелательное поведение: событие опубликовано, но не помечено SUCCESS
                   (при следующем поллинге уйдёт повторно - дубль в Kafka).
        """
        events = [make_event("payment.pending"), make_event("payment.processing")]
        relay, publisher, outbox_repo = make_relay(events)

        await relay._process_batch(50)

        assert publisher.publish.await_count == 2
        assert all(e.status == OutboxStatus.SUCCESS for e in events)
        assert outbox_repo.update.await_count == 2

    @pytest.mark.asyncio
    async def test_publish_preserves_input_order(self):
        """
        Проверяем: порядок публикации совпадает с порядком выборки из БД.
        Успех: события уходят в брокер строго в том порядке, в каком отдал репозиторий
               (ORDER BY created_at, id + партиционирование по payment id).
        Нежелательное поведение: перестановка событий одного платежа
                   (например completed раньше processing).
        """
        first = make_event("payment.pending")
        second = make_event("payment.processing")
        relay, publisher, _ = make_relay([first, second])

        await relay._process_batch(50)

        published_types = [
            call.args[0].metadata.event_type for call in publisher.publish.await_args_list
        ]
        assert published_types == ["payment.pending", "payment.processing"]

    @pytest.mark.asyncio
    async def test_empty_batch_does_nothing(self):
        """
        Проверяем: в outbox нет неопубликованных событий.
        Успех: ни publish, ни update не вызываются.
        Нежелательное поведение: лишние обращения к брокеру или БД на пустом батче.
        """
        relay, publisher, outbox_repo = make_relay([])

        await relay._process_batch(50)

        publisher.publish.assert_not_awaited()
        outbox_repo.update.assert_not_awaited()


class TestPublishFailure:
    @pytest.mark.asyncio
    async def test_failure_increments_attempts_and_stops_batch(self):
        """
        Проверяем: первое событие батча не публикуется (брокер недоступен).
        Успех: у него attempts=1, статус остаётся PENDING (будет ретрай), записан last_error;
               батч прерван - второе событие не публиковалось и не менялось.
        Нежелательное поведение: более новое событие обгоняет упавшее (нарушение порядка).
        """
        events = [make_event("payment.pending"), make_event("payment.processing")]
        publisher = AsyncMock()
        publisher.publish.side_effect = RuntimeError("kafka down")
        relay, publisher, outbox_repo = make_relay(events, publisher=publisher)

        await relay._process_batch(50)

        first, second = events
        assert first.attempts == 1
        assert first.status == OutboxStatus.PENDING
        assert "kafka down" in first.last_error
        assert publisher.publish.await_count == 1
        assert second.attempts == 0
        assert second.status == OutboxStatus.PENDING
        outbox_repo.update.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_partial_success_before_failure_is_persisted(self):
        """
        Проверяем: первое событие ушло, второе упало.
        Успех: первое помечено SUCCESS и закоммичено, второе осталось PENDING с attempts=1;
               оба изменения сохранены (update вызван дважды).
        Нежелательное поведение: откат уже опубликованного события (при ретрае уйдёт дважды).
        """
        events = [make_event("payment.pending"), make_event("payment.processing")]
        publisher = AsyncMock()
        publisher.publish.side_effect = [None, RuntimeError("kafka down")]
        relay, publisher, outbox_repo = make_relay(events, publisher=publisher)

        await relay._process_batch(50)

        first, second = events
        assert first.status == OutboxStatus.SUCCESS
        assert second.status == OutboxStatus.PENDING
        assert second.attempts == 1
        assert outbox_repo.update.await_count == 2


class TestPoisonEvents:
    @pytest.mark.asyncio
    async def test_marked_failed_after_max_attempts(self):
        """
        Проверяем: событие уже имело 2 неудачи, max_attempts=3, снова падает.
        Успех: attempts становится 3, статус FAILED - событие исключается из выборки
               relay и разбирается вручную, не блокируя очередь бесконечными ретраями.
        Нежелательное поведение: вечные ретраи ядовитого события, стопор всей очереди.
        """
        event = make_event(attempts=2)
        publisher = AsyncMock()
        publisher.publish.side_effect = RuntimeError("payload too large")
        relay, publisher, outbox_repo = make_relay([event], publisher=publisher)

        await relay._process_batch(50)

        assert event.attempts == 3
        assert event.status == OutboxStatus.FAILED
        assert "payload too large" in event.last_error
