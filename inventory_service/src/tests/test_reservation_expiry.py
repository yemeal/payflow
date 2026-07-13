"""
Тесты ReservationExpiryService - фоновое автоистечение резервов (TTL).

Формат документации у каждого теста единый:
    Проверяем: какое поведение под контролем.
    Успех: что должно произойти, чтобы тест был зелёным.
    Нежелательное поведение: что мы этим тестом ловим (ради чего он существует).

Инварианты под контролем:
  1) ACTIVE-резерв с expires_at <= now переходит в EXPIRED, сток возвращается;
  2) поллер НЕ публикует событий (оркестратор узнаёт по своему дедлайну оплаты);
  3) не истёкшие и уже терминальные резервы поллер не трогает.
"""

import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta
from typing import AsyncIterator

from app.application.ports.repositories import (
    ReservationRepositoryProtocol,
    StockRepositoryProtocol,
)
from app.application.ports.uow import AsyncUOWProtocol
from app.application.services.reservation_expiry import ReservationExpiryService
from app.domain.reservations import ReservationStatus, utc_now
from tests.conftest import make_reserve, make_reservation


@dataclass
class _Scope:
    uow: AsyncUOWProtocol
    reservations: ReservationRepositoryProtocol
    stock: StockRepositoryProtocol


def _make_scope_factory(uow, reservations, stock):
    """Фабрика per-tick scope поверх in-memory фейков (замена Dishka-адаптера)"""

    @asynccontextmanager
    async def factory() -> AsyncIterator[_Scope]:
        yield _Scope(uow=uow, reservations=reservations, stock=stock)

    return factory


def _poller(uow, reservations, stock) -> ReservationExpiryService:
    return ReservationExpiryService(
        scope_factory=_make_scope_factory(uow, reservations, stock),
        interval_seconds=0.01,
        batch_size=100,
    )


async def test_expiry_returns_stock_and_marks_expired(
    service, stock, reservations, uow
):
    """
    Проверяем: истёкший ACTIVE-резерв возвращает товар и становится EXPIRED.
    Успех: tick вернул 1, available 96->100, reserved 0, статус EXPIRED.
    Нежелательное поведение: сток остаётся заблокированным навсегда после TTL.
    """
    order_id = uuid.uuid7()
    await service.reserve(make_reserve(order_id, {"sku-1": 4}))
    assert stock.available("sku-1") == 96
    # форсируем истечение TTL
    reservations.by_order[order_id].expires_at = utc_now() - timedelta(seconds=1)

    expired = await _poller(uow, reservations, stock).tick()

    assert expired == 1
    assert stock.available("sku-1") == 100
    assert stock.reserved("sku-1") == 0
    assert reservations.status_of(order_id) is ReservationStatus.EXPIRED


async def test_expiry_publishes_no_events(service, stock, reservations, uow, outbox):
    """
    Проверяем: поллер не публикует событий - у него вообще нет доступа к outbox.
    Успех: после tick в outbox лежит только событие резерва, ничего сверх.
    Нежелательное поведение: второй источник правды о таймауте, гонка отменителей.
    """
    order_id = uuid.uuid7()
    await service.reserve(make_reserve(order_id, {"sku-1": 2}))
    outbox_len_before = len(outbox.messages)
    reservations.by_order[order_id].expires_at = utc_now() - timedelta(seconds=1)

    await _poller(uow, reservations, stock).tick()

    assert len(outbox.messages) == outbox_len_before


async def test_expiry_skips_not_yet_expired(service, stock, reservations, uow):
    """
    Проверяем: живой ACTIVE-резерв (expires_at в будущем) поллер не трогает.
    Успех: tick вернул 0, статус ACTIVE, сток остаётся заблокированным.
    Нежелательное поведение: досрочное истечение живого резерва, оплата бьётся о пустоту.
    """
    order_id = uuid.uuid7()
    await service.reserve(make_reserve(order_id, {"sku-1": 4}, ttl_seconds=600))

    expired = await _poller(uow, reservations, stock).tick()

    assert expired == 0
    assert stock.available("sku-1") == 96
    assert reservations.status_of(order_id) is ReservationStatus.ACTIVE


async def test_expiry_ignores_terminal_reservations(stock, reservations, uow):
    """
    Проверяем: уже CANCELLED/COMMITTED резервы поллер не переводит в EXPIRED.
    Успех: tick вернул 0, статус резерва не изменился.
    Нежелательное поведение: повторное движение стока по завершённому резерву.
    """
    order_id = uuid.uuid7()
    reservation = make_reservation(
        order_id, {"sku-1": 3}, status=ReservationStatus.CANCELLED
    )
    reservation.expires_at = utc_now() - timedelta(seconds=1)  # истёк, но терминален
    await reservations.add(reservation)

    expired = await _poller(uow, reservations, stock).tick()

    assert expired == 0
    assert reservations.status_of(order_id) is ReservationStatus.CANCELLED


async def test_expiry_empty_batch_is_noop(stock, reservations, uow):
    """
    Проверяем: пустой прогон поллера безопасен (нет истёкших резервов).
    Успех: tick вернул 0 без исключений.
    Нежелательное поведение: поллер падает на пустой выборке и умирает.
    """
    expired = await _poller(uow, reservations, stock).tick()

    assert expired == 0
