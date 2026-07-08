"""
Тесты OutboxRelayService: порядок публикации, обработка "ядовитых" событий,
маркировка SUCCESS/FAILED, инкремент attempts.
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
    outbox_repo = AsyncMock()
    outbox_repo.get_unpublished_events.return_value = events

    scope = MagicMock()
    scope.uow = AsyncMock()  # поддерживает async with
    scope.outbox_repo = outbox_repo

    @asynccontextmanager
    async def scope_factory():
        yield scope

    publisher = publisher or AsyncMock()
    relay = OutboxRelayService(
        publisher, scope_factory, max_publish_attempts=max_attempts
    )
    return relay, publisher, outbox_repo


@pytest.mark.asyncio
async def test_all_events_published_and_marked_success():
    events = [make_event("payment.pending"), make_event("payment.processing")]
    relay, publisher, outbox_repo = make_relay(events)

    await relay._process_batch(50)

    assert publisher.publish.await_count == 2
    assert all(e.status == OutboxStatus.SUCCESS for e in events)
    assert outbox_repo.update.await_count == 2


@pytest.mark.asyncio
async def test_publish_failure_increments_attempts_and_stops_batch():
    """Упавшее событие не должно давать более новым обогнать себя."""
    events = [make_event("payment.pending"), make_event("payment.processing")]
    publisher = AsyncMock()
    publisher.publish.side_effect = RuntimeError("kafka down")
    relay, publisher, outbox_repo = make_relay(events, publisher=publisher)

    await relay._process_batch(50)

    first, second = events
    assert first.attempts == 1
    assert first.status == OutboxStatus.PENDING  # ещё будет ретрай
    assert "kafka down" in first.last_error
    # батч прерван: второе событие не публиковалось и не трогалось
    assert publisher.publish.await_count == 1
    assert second.attempts == 0
    assert second.status == OutboxStatus.PENDING
    outbox_repo.update.assert_awaited_once()


@pytest.mark.asyncio
async def test_poison_event_marked_failed_after_max_attempts():
    event = make_event(attempts=2)  # уже 2 неудачи, max = 3
    publisher = AsyncMock()
    publisher.publish.side_effect = RuntimeError("payload too large")
    relay, publisher, outbox_repo = make_relay([event], publisher=publisher)

    await relay._process_batch(50)

    assert event.attempts == 3
    assert event.status == OutboxStatus.FAILED
    assert "payload too large" in event.last_error


@pytest.mark.asyncio
async def test_partial_batch_success_before_failure_is_persisted():
    """Опубликованные до сбоя события помечаются SUCCESS и коммитятся."""
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


@pytest.mark.asyncio
async def test_empty_batch_does_nothing():
    relay, publisher, outbox_repo = make_relay([])

    await relay._process_batch(50)

    publisher.publish.assert_not_awaited()
    outbox_repo.update.assert_not_awaited()
