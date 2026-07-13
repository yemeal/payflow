"""
Фоновое автоистечение резервов (TTL).

Раз в EXPIRY_POLLER_INTERVAL_SECONDS переводит ACTIVE-резервы с
expires_at <= now в EXPIRED и возвращает сток (available += qty).

Событий НЕ публикует: оркестратор узнаёт о неоплаченном заказе по СВОЕМУ
дедлайну шага оплаты и сам шлёт компенсацию (cancel_reservation), которая
на уже истёкшем резерве отработает как успех. Публиковать отсюда события -
значит завести второй источник правды о таймауте и словить гонку двух
"отменителей" одного заказа.
"""

import asyncio
from contextlib import AbstractAsyncContextManager
from typing import Protocol

import structlog

from app.application.ports.repositories import (
    ReservationRepositoryProtocol,
    StockRepositoryProtocol,
)
from app.application.ports.uow import AsyncUOWProtocol
from app.domain.reservations import Reservation, ReservationStatus, utc_now

logger = structlog.get_logger()


def _aggregate(reservation: Reservation) -> dict[str, int]:
    """Строки резерва по одному товару складываются (см. InventoryService)"""
    required: dict[str, int] = {}
    for item in reservation.items:
        required[item.product_id] = required.get(item.product_id, 0) + item.quantity
    return required


class ExpiryScope(Protocol):
    """Per-tick зависимости поллера: свежая сессия и транзакция на каждый тик"""

    uow: AsyncUOWProtocol
    reservations: ReservationRepositoryProtocol
    stock: StockRepositoryProtocol


class ExpiryScopeFactory(Protocol):
    def __call__(self) -> AbstractAsyncContextManager[ExpiryScope]: ...


class ReservationExpiryService:
    def __init__(
        self,
        scope_factory: ExpiryScopeFactory,
        interval_seconds: float,
        batch_size: int = 100,
    ) -> None:
        self._scope_factory = scope_factory
        self._interval = interval_seconds
        self._batch_size = batch_size
        self._is_running = False

    async def run(self) -> None:
        self._is_running = True
        logger.info("reservation_expiry_started", interval_seconds=self._interval)

        while self._is_running:
            try:
                await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                # поллер обязан пережить любой сбой тика: следующий тик - новая
                # транзакция, необработанные резервы никуда не денутся
                logger.exception("reservation_expiry_tick_error")

            if self._is_running:
                await asyncio.sleep(self._interval)

        logger.info("reservation_expiry_stopped")

    async def tick(self) -> int:
        """Один проход: сколько резервов истекло. Возвращает счётчик (для тестов)"""
        async with self._scope_factory() as scope:
            async with scope.uow:
                now = utc_now()
                expired = await scope.reservations.find_expired_active(
                    now=now, limit=self._batch_size
                )
                if not expired:
                    return 0

                for reservation in expired:
                    # FOR UPDATE SKIP LOCKED в find_expired_active держит строки
                    # до конца транзакции: сток и статус меняются атомарно
                    for product_id, quantity in _aggregate(reservation).items():
                        items = await scope.stock.get_for_update([product_id])
                        if not items:
                            logger.error(
                                "reservation_expiry_product_missing",
                                order_id=str(reservation.order_id),
                                product_id=product_id,
                            )
                            continue
                        item = items[0]
                        item.available += quantity
                        item.reserved = max(0, item.reserved - quantity)
                        await scope.stock.update(item)

                    reservation.status = ReservationStatus.EXPIRED
                    reservation.updated_at = now
                    await scope.reservations.update(reservation)
                    logger.info(
                        "reservation_expired",
                        order_id=str(reservation.order_id),
                        expires_at=reservation.expires_at.isoformat(),
                    )

                logger.info("reservation_expiry_batch", count=len(expired))
                return len(expired)

    def stop(self) -> None:
        self._is_running = False
        logger.info("reservation_expiry_stopping")
