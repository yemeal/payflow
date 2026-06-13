import asyncio
import time
from enum import Enum
from typing import Any, Callable

import structlog

from app.core.exceptions import AppError

logger = structlog.getLogger()


class CircuitState(str, Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF-OPEN"


class CircuitBreakerError(AppError):
    def __init__(self, message: str = "Circuit Breaker открыт") -> None:
        super().__init__(message)


class CircuitBreaker:
    def __init__(
        self,
        fail_max: int,
        recovery_timeout: float,
        name: str,
    ) -> None:
        self._fail_max = fail_max
        self._recovery_timeout = recovery_timeout
        self.name = name

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._opened_at: float | None = None

        # lock гарантирует, что если одновременно придет например 100 запросов в момент перехода состояний
        # то только один запрос проскочит как пробный например (залочим стейт и проверим),
        # остальные 99 просто подождут или отвалятся по fail fast.
        # Что решает проблему конкурентного перехода состояний.
        self._lock = asyncio.Lock()

    async def _check_state(self) -> None:
        """Проверяет время ожидания в OPEN и переводит в HALF_OPEN при необходимости"""
        if self._state == CircuitState.OPEN:
            now = time.monotonic()
            if now - self._opened_at >= self._recovery_timeout:
                logger.info(
                    "circuit_breaker_cooldown_elapsed",
                    name=self.name,
                    opened_at=self._opened_at,
                    state=self._state,
                    recovery_timeout=self._recovery_timeout,
                )
                self._state = CircuitState.HALF_OPEN
                logger.info(
                    "circuit_breaker_half_open",
                    name=self.name,
                    state=self._state,
                )

    def _trip(self) -> None:
        """Размыкание цепи (переходим в OPEN)"""
        self._state = CircuitState.OPEN
        self._opened_at = time.monotonic()
        logger.error(
            "circuit_breaker_trip",
            name=self.name,
            recovery_timeout=self._recovery_timeout,
            state=self._state,
            opened_at=self._opened_at,
        )

    async def _handle_success(self) -> None:
        """Обработка успешного выполнения запроса"""
        if self._state == CircuitState.HALF_OPEN:
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._opened_at = None
            logger.info(
                "circuit_breaker_probe_request_succeeded",
                name=self.name,
                state=self._state,
            )
        elif self._state == CircuitState.CLOSED:
            # не забываем сбрасывать счетчик ошибок при успешном выполнении запроса
            self._failure_count = 0

    async def _handle_failure(self, exc: Exception) -> None:
        """Обработка сбоя при выполнении запроса"""
        self._failure_count += 1
        logger.warning(
            "circuit_breaker_recorded_failure",
            name=self.name,
            failure_count=self._failure_count,
            state=self._state,
            error=str(exc),
        )

        if self._state == CircuitState.CLOSED:
            # если превысили максимальное количество ошибок, то переходим в OPEN
            if self._failure_count >= self._fail_max:
                self._trip()

        elif self._state == CircuitState.HALF_OPEN:
            # любая ошибка в полуоткрытом состоянии возвращает в OPEN
            logger.warning(
                "circuit_breaker_probe_request_failed_in_half_open",
                name=self.name,
                failure_count=self._failure_count,
                state=self._state,
                error=str(exc),
            )
            self._trip()

    async def call(
        self,
        func: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Обертка для вызова асинхронной функцуии"""
        async with self._lock:
            await self._check_state()

            if self._state == CircuitState.OPEN:
                # fail fast
                time_left = self._recovery_timeout - (
                    time.monotonic() - self._opened_at
                )
                raise CircuitBreakerError(
                    f'Circuit Breaker "{self.name}" открыт. Осталось {time_left:.1f} секунд'
                )

        try:
            # выполняем защищаемый асинхронный вызов переданной функции
            result = await func(*args, **kwargs)
        except Exception as e:
            # в случае ошибки обрабатываем сбой
            async with self._lock:
                await self._handle_failure(e)
            raise e

        # в случае успеха сбрасываем состояние
        async with self._lock:
            await self._handle_success()
        return result
