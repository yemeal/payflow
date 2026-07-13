"""
Фоновый поллер саг: один цикл, два дела (docs/saga-design.md, 9.3):
  1) retry: саги с наступившим retry_after - переотправить команду шага
     (тот же commandId, дедуп участника вернёт сохранённый результат);
  2) deadline: саги, чей участник молчит дольше дедлайна шага, - таймаут
     по политике шага (RETRY / BUSINESS_FAIL).

Каждый тик работает в СВЕЖЕМ request-scope DI (новая сессия и транзакция),
по образцу outbox-релея. Никакого глобального состояния: экземпляр собирается
в DI, стоп-флаг - атрибут экземпляра.
"""

import asyncio
from contextlib import AbstractAsyncContextManager
from typing import Protocol

import structlog

logger = structlog.get_logger()


class SagaPollerScope(Protocol):
    """Per-tick зависимости поллера"""

    executor: "SagaExecutorLike"


class SagaExecutorLike(Protocol):
    async def process_due_retries(self) -> int: ...

    async def process_due_deadlines(self) -> int: ...


class SagaPollerScopeFactory(Protocol):
    def __call__(self) -> AbstractAsyncContextManager[SagaPollerScope]: ...


class SagaPollerService:
    def __init__(
        self,
        scope_factory: SagaPollerScopeFactory,
        interval_seconds: float,
    ) -> None:
        self._scope_factory = scope_factory
        self._interval = interval_seconds
        self._is_running = False

    async def run(self) -> None:
        self._is_running = True
        logger.info("saga_poller_started", interval_seconds=self._interval)
        while self._is_running:
            try:
                await self._tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                # поллер обязан пережить любой сбой тика: следующий тик - новая транзакция
                logger.exception("saga_poller_tick_error")
            if self._is_running:
                await asyncio.sleep(self._interval)
        logger.info("saga_poller_stopped")

    async def _tick(self) -> None:
        async with self._scope_factory() as scope:
            resent = await scope.executor.process_due_retries()
            timed_out = await scope.executor.process_due_deadlines()
        if resent or timed_out:
            logger.info("saga_poller_tick", resent=resent, timed_out=timed_out)

    def stop(self) -> None:
        self._is_running = False
        logger.info("saga_poller_stopping")
