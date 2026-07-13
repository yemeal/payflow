from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncIterator

from dishka import AsyncContainer

from app.application.ports.repositories import (
    ReservationRepositoryProtocol,
    StockRepositoryProtocol,
)
from app.application.ports.uow import AsyncUOWProtocol


@dataclass
class DishkaExpiryScope:
    """Per-tick зависимости поллера автоистечения: свежая сессия и транзакция"""

    uow: AsyncUOWProtocol
    reservations: ReservationRepositoryProtocol
    stock: StockRepositoryProtocol


class DishkaExpiryScopeFactory:
    """Адаптер ExpiryScopeFactory для Dishka (та же идея, что у relay)"""

    def __init__(self, container: AsyncContainer) -> None:
        self._container = container

    @asynccontextmanager
    async def __call__(self) -> AsyncIterator[DishkaExpiryScope]:
        async with self._container() as request_container:
            uow = await request_container.get(AsyncUOWProtocol)
            reservations = await request_container.get(ReservationRepositoryProtocol)
            stock = await request_container.get(StockRepositoryProtocol)
            yield DishkaExpiryScope(
                uow=uow, reservations=reservations, stock=stock
            )
