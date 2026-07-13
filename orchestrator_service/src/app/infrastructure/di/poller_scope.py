from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncIterator

from dishka import AsyncContainer

from app.application.services.saga_executor import SagaExecutorService


@dataclass
class DishkaSagaPollerScope:
    """Per-tick зависимости поллера: свежий executor со свежей сессией"""

    executor: SagaExecutorService


class DishkaSagaPollerScopeFactory:
    """Адаптер SagaPollerScopeFactory для Dishka (та же идея, что у relay)"""

    def __init__(self, container: AsyncContainer) -> None:
        self._container = container

    @asynccontextmanager
    async def __call__(self) -> AsyncIterator[DishkaSagaPollerScope]:
        async with self._container() as request_container:
            executor = await request_container.get(SagaExecutorService)
            yield DishkaSagaPollerScope(executor=executor)
