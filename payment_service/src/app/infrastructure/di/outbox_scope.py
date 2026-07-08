from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncIterator

from dishka import AsyncContainer

from app.application.ports.repositories import OutboxRepositoryProtocol
from app.application.ports.uow import AsyncUOWProtocol


@dataclass
class DishkaOutboxScope:
    """
    Конкретная реализация OutboxScope для Dishka.
    Хранит per-batch зависимости, полученные из request-контейнера.
    """

    uow: AsyncUOWProtocol
    outbox_repo: OutboxRepositoryProtocol


class DishkaOutboxScopeFactory:
    """
    Адаптер OutboxScopeFactory для Dishka.

    Вместо прямой зависимости OutboxRelayService от AsyncContainer (утечка DI-фреймворка),
    relay зависит от абстрактного OutboxScopeFactory,
    а этот адаптер оборачивает Dishka и резолвит per-batch зависимости.
    """

    def __init__(self, container: AsyncContainer) -> None:
        self._container = container

    @asynccontextmanager
    async def __call__(self) -> AsyncIterator[DishkaOutboxScope]:
        # создаем новый REQUEST scope для каждого batch - свежая сессия и транзакция
        async with self._container() as request_container:
            uow = await request_container.get(AsyncUOWProtocol)
            outbox_repo = await request_container.get(OutboxRepositoryProtocol)
            yield DishkaOutboxScope(uow=uow, outbox_repo=outbox_repo)
