"""
Тесты Circuit Breaker.

Проверяем полный жизненный цикл состояний CLOSED -> OPEN -> HALF_OPEN -> CLOSED,
fail-fast в OPEN, фильтрацию ошибок через is_failure и конкурентный доступ.

Circuit Breaker - собственная реализация без внешних библиотек, тестируем напрямую
через call() без моков asyncio.Lock.

Формат документации: Проверяем / Успех / Нежелательное поведение.
"""

import asyncio
import pytest
from unittest.mock import AsyncMock, patch

from app.infrastructure.resilience.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerError,
    CircuitState,
)


@pytest.fixture
def cb() -> CircuitBreaker:
    """CB с порогом 3 ошибки и коротким таймаутом восстановления 0.1 c (для скорости тестов)."""
    return CircuitBreaker(fail_max=3, recovery_timeout=0.1, name="test-breaker")


@pytest.fixture
def cb_with_filter() -> CircuitBreaker:
    """CB, который считает сбоем только ValueError; ConnectionError игнорирует."""
    return CircuitBreaker(
        fail_max=2,
        recovery_timeout=0.1,
        name="filtered-breaker",
        is_failure=lambda e: isinstance(e, ValueError),
    )


async def _success():
    return "ok"


async def _fail():
    raise ConnectionError("connection refused")


async def _fail_value_error():
    raise ValueError("bad value")


class TestClosedToOpen:
    """При накоплении ошибок до порога CB размыкается (CLOSED -> OPEN)."""

    @pytest.mark.asyncio
    async def test_stays_closed_below_threshold(self, cb):
        """
        Проверяем: число ошибок меньше fail_max.
        Успех: состояние остаётся CLOSED, счётчик равен числу ошибок.
        Нежелательное поведение: преждевременное размыкание до достижения порога.
        """
        for _ in range(cb._fail_max - 1):
            with pytest.raises(ConnectionError):
                await cb.call(_fail)

        assert cb._state == CircuitState.CLOSED
        assert cb._failure_count == cb._fail_max - 1

    @pytest.mark.asyncio
    async def test_trips_to_open_at_threshold(self, cb):
        """
        Проверяем: число ошибок достигло fail_max.
        Успех: CB переходит в OPEN, счётчик равен fail_max.
        Нежелательное поведение: остаться в CLOSED и продолжать бить по сбойному провайдеру.
        """
        for _ in range(cb._fail_max):
            with pytest.raises(ConnectionError):
                await cb.call(_fail)

        assert cb._state == CircuitState.OPEN
        assert cb._failure_count == cb._fail_max

    @pytest.mark.asyncio
    async def test_success_resets_failure_count(self, cb):
        """
        Проверяем: успешный вызов после серии ошибок (но до порога).
        Успех: счётчик ошибок сбрасывается в 0, состояние CLOSED.
        Нежелательное поведение: накопление ошибок не обнуляется и CB рано или поздно
                   ложно размыкается на редких единичных сбоях.
        """
        for _ in range(cb._fail_max - 1):
            with pytest.raises(ConnectionError):
                await cb.call(_fail)
        assert cb._failure_count == cb._fail_max - 1

        result = await cb.call(_success)

        assert result == "ok"
        assert cb._failure_count == 0
        assert cb._state == CircuitState.CLOSED


class TestOpenRejects:
    """В состоянии OPEN запросы мгновенно отклоняются, не доходя до функции."""

    @pytest.mark.asyncio
    async def test_open_rejects_without_calling_func(self, cb):
        """
        Проверяем: вызов в состоянии OPEN.
        Успех: поднимается CircuitBreakerError, защищаемая функция не вызвана вообще.
        Нежелательное поведение: запрос всё-таки уходит к недоступному провайдеру.
        """
        for _ in range(cb._fail_max):
            with pytest.raises(ConnectionError):
                await cb.call(_fail)
        assert cb._state == CircuitState.OPEN

        guarded = AsyncMock()
        with pytest.raises(CircuitBreakerError):
            await cb.call(guarded)

        guarded.assert_not_called()


class TestOpenToHalfOpen:
    """После recovery_timeout CB пробует один пробный запрос (HALF_OPEN)."""

    @pytest.mark.asyncio
    async def test_transitions_to_half_open_after_timeout(self, cb):
        """
        Проверяем: истёк recovery_timeout, приходит успешный пробный запрос.
        Успех: CB пропускает вызов, при успехе сразу закрывается (CLOSED), счётчик 0.
        Нежелательное поведение: вечная блокировка в OPEN даже после восстановления провайдера.
        """
        for _ in range(cb._fail_max):
            with pytest.raises(ConnectionError):
                await cb.call(_fail)
        assert cb._state == CircuitState.OPEN

        opened_at = cb._opened_at
        with patch("app.infrastructure.resilience.circuit_breaker.time") as mock_time:
            mock_time.monotonic.return_value = opened_at + cb._recovery_timeout + 0.01
            result = await cb.call(_success)

        assert result == "ok"
        assert cb._state == CircuitState.CLOSED
        assert cb._failure_count == 0


class TestHalfOpenTransitions:
    """Пробный запрос в HALF_OPEN решает судьбу цепи."""

    @pytest.mark.asyncio
    async def test_success_in_half_open_closes_circuit(self, cb):
        """
        Проверяем: пробный запрос в HALF_OPEN прошёл успешно.
        Успех: CB закрывается (CLOSED), счётчик 0, opened_at сброшен.
        Нежелательное поведение: остаться в HALF_OPEN или OPEN после доказанного восстановления.
        """
        for _ in range(cb._fail_max):
            with pytest.raises(ConnectionError):
                await cb.call(_fail)
        cb._state = CircuitState.HALF_OPEN

        result = await cb.call(_success)

        assert result == "ok"
        assert cb._state == CircuitState.CLOSED
        assert cb._failure_count == 0
        assert cb._opened_at is None

    @pytest.mark.asyncio
    async def test_failure_in_half_open_returns_to_open(self, cb):
        """
        Проверяем: пробный запрос в HALF_OPEN снова упал.
        Успех: CB возвращается в OPEN, opened_at заново выставлен (отсчёт таймаута сначала).
        Нежелательное поведение: закрыться при неудачной пробе и пропустить лавину запросов.
        """
        for _ in range(cb._fail_max):
            with pytest.raises(ConnectionError):
                await cb.call(_fail)
        cb._state = CircuitState.HALF_OPEN

        with pytest.raises(ConnectionError):
            await cb.call(_fail)

        assert cb._state == CircuitState.OPEN
        assert cb._opened_at is not None


class TestIsFailureFilter:
    """is_failure отделяет реальные сбои от ошибок, которые не должны трогать цепь."""

    @pytest.mark.asyncio
    async def test_non_failure_errors_dont_count(self, cb_with_filter):
        """
        Проверяем: ошибки, не прошедшие is_failure (ConnectionError).
        Успех: счётчик не растёт, состояние остаётся CLOSED даже после многих ошибок.
        Нежелательное поведение: посторонние ошибки размыкают цепь и рубят рабочий провайдер.
        """
        for _ in range(5):
            with pytest.raises(ConnectionError):
                await cb_with_filter.call(_fail)

        assert cb_with_filter._failure_count == 0
        assert cb_with_filter._state == CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_matching_errors_trip_breaker(self, cb_with_filter):
        """
        Проверяем: ошибки, прошедшие is_failure (ValueError).
        Успех: при достижении порога CB размыкается (OPEN).
        Нежелательное поведение: реальные сбои игнорируются фильтром.
        """
        for _ in range(cb_with_filter._fail_max):
            with pytest.raises(ValueError):
                await cb_with_filter.call(_fail_value_error)

        assert cb_with_filter._state == CircuitState.OPEN


class TestConcurrency:
    """Внутренний lock защищает от гонок при залповом трафике."""

    @pytest.mark.asyncio
    async def test_concurrent_failures_trip_exactly_once(self, cb):
        """
        Проверяем: пачка одновременных сбоев больше порога.
        Успех: CB оказывается в OPEN, счётчик не меньше порога; гонок за состояние нет
               (lock сериализует переходы).
        Нежелательное поведение: рассинхронизация счётчика и состояния при параллельных вызовах.
        """
        async def call_and_swallow():
            try:
                await cb.call(_fail)
            except (ConnectionError, CircuitBreakerError):
                pass

        await asyncio.gather(*[call_and_swallow() for _ in range(20)])

        assert cb._state == CircuitState.OPEN
        assert cb._failure_count >= cb._fail_max
