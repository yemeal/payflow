from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncIterator

from dishka import AsyncContainer

from app.application.ports.repositories import OutboxRepositoryProtocol
from app.application.ports.uow import AsyncUOWProtocol


@dataclass
class DishkaOutboxScope:
    """Per-batch зависимости релея из request-контейнера"""

    uow: AsyncUOWProtocol
    outbox_repo: OutboxRepositoryProtocol


class DishkaOutboxScopeFactory:
    """
    Адаптер OutboxScopeFactory для Dishka: relay зависит от абстрактной фабрики,
    а не от DI-фреймворка. Каждый батч - новый REQUEST scope (свежая сессия).
    """

    def __init__(self, container: AsyncContainer) -> None:
        self._container = container

    @asynccontextmanager
    async def __call__(self) -> AsyncIterator[DishkaOutboxScope]:
        async with self._container() as request_container:
            uow = await request_container.get(AsyncUOWProtocol)
            outbox_repo = await request_container.get(OutboxRepositoryProtocol)
            yield DishkaOutboxScope(uow=uow, outbox_repo=outbox_repo)
