"""
Тесты Circuit Breaker

Проверяем полный жизненный цикл состояний:
CLOSED → OPEN → HALF_OPEN → CLOSED

Circuit Breaker — кастомная реализация, не зависит от внешних библиотек.
Тестируем напрямую через метод call() без моков asyncio.Lock.
"""

import pytest
import time
from unittest.mock import AsyncMock, patch

from app.infrastructure.resilience.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerError,
    CircuitState,
)


# ---------------------------------------------------------------------------
# Фикстуры
# ---------------------------------------------------------------------------

@pytest.fixture
def cb() -> CircuitBreaker:
    """
    Circuit Breaker с порогом 3 ошибки и таймаутом восстановления 0.1 секунды.
    Маленький таймаут, чтобы тесты проходили быстро.
    """
    return CircuitBreaker(
        fail_max=3,
        recovery_timeout=0.1,
        name="test-breaker",
    )


@pytest.fixture
def cb_with_filter() -> CircuitBreaker:
    """
    Circuit Breaker, который считает ошибками только ValueError.
    ConnectionError, например, игнорируется.
    """
    return CircuitBreaker(
        fail_max=2,
        recovery_timeout=0.1,
        name="filtered-breaker",
        is_failure=lambda e: isinstance(e, ValueError),
    )


# ---------------------------------------------------------------------------
# Вспомогательные функции
# ---------------------------------------------------------------------------

async def _success():
    """Имитация успешного вызова"""
    return "ok"


async def _fail():
    """Имитация неудачного вызова"""
    raise ConnectionError("connection refused")


async def _fail_value_error():
    """Имитация ошибки, которую is_failure считает реальным сбоем"""
    raise ValueError("bad value")


# ---------------------------------------------------------------------------
# Тесты: Переход CLOSED → OPEN
# ---------------------------------------------------------------------------

class TestClosedToOpen:
    """При превышении порога ошибок CB переходит из CLOSED в OPEN"""

    @pytest.mark.asyncio
    async def test_stays_closed_below_threshold(self, cb):
        """CB остаётся CLOSED пока ошибок меньше fail_max"""
        for _ in range(cb._fail_max - 1):
            with pytest.raises(ConnectionError):
                await cb.call(_fail)

        assert cb._state == CircuitState.CLOSED
        assert cb._failure_count == cb._fail_max - 1

    @pytest.mark.asyncio
    async def test_trips_to_open_at_threshold(self, cb):
        """CB переходит в OPEN когда количество ошибок достигает fail_max"""
        for _ in range(cb._fail_max):
            with pytest.raises(ConnectionError):
                await cb.call(_fail)

        assert cb._state == CircuitState.OPEN
        assert cb._failure_count == cb._fail_max

    @pytest.mark.asyncio
    async def test_success_resets_failure_count(self, cb):
        """Успешный вызов в CLOSED сбрасывает счётчик ошибок"""
        # накапливаем 2 ошибки (из 3 для trip)
        for _ in range(cb._fail_max - 1):
            with pytest.raises(ConnectionError):
                await cb.call(_fail)

        assert cb._failure_count == cb._fail_max - 1

        # успешный вызов должен сбросить счётчик
        result = await cb.call(_success)
        assert result == "ok"
        assert cb._failure_count == 0
        assert cb._state == CircuitState.CLOSED


# ---------------------------------------------------------------------------
# Тесты: В состоянии OPEN запросы отклоняются
# ---------------------------------------------------------------------------

class TestOpenRejects:
    """В состоянии OPEN все запросы мгновенно отклоняются (fail fast)"""

    @pytest.mark.asyncio
    async def test_open_rejects_without_calling_func(self, cb):
        """В OPEN запросы отклоняются CircuitBreakerError без вызова функции"""
        # доводим до OPEN
        for _ in range(cb._fail_max):
            with pytest.raises(ConnectionError):
                await cb.call(_fail)

        assert cb._state == CircuitState.OPEN

        # следующий вызов должен быть отклонён без реального вызова функции
        mock_func = AsyncMock()
        with pytest.raises(CircuitBreakerError):
            await cb.call(mock_func)

        # функция НЕ должна была быть вызвана
        mock_func.assert_not_called()


# ---------------------------------------------------------------------------
# Тесты: Переход OPEN → HALF_OPEN после таймаута
# ---------------------------------------------------------------------------

class TestOpenToHalfOpen:
    """После истечения recovery_timeout CB переходит из OPEN в HALF_OPEN"""

    @pytest.mark.asyncio
    async def test_transitions_to_half_open_after_timeout(self, cb):
        """CB переходит в HALF_OPEN после recovery_timeout"""
        # доводим до OPEN
        for _ in range(cb._fail_max):
            with pytest.raises(ConnectionError):
                await cb.call(_fail)

        assert cb._state == CircuitState.OPEN

        # подменяем время, чтобы recovery_timeout истёк
        original_opened_at = cb._opened_at
        with patch("app.infrastructure.resilience.circuit_breaker.time") as mock_time:
            mock_time.monotonic.return_value = original_opened_at + cb._recovery_timeout + 0.01

            # следующий вызов должен перевести в HALF_OPEN и выполнить функцию
            result = await cb.call(_success)

        assert result == "ok"
        assert cb._state == CircuitState.CLOSED  # успех → сразу CLOSED
        assert cb._failure_count == 0


# ---------------------------------------------------------------------------
# Тесты: Успешный запрос в HALF_OPEN → CLOSED
# ---------------------------------------------------------------------------

class TestHalfOpenToClosed:
    """Успешный пробный запрос в HALF_OPEN переводит CB обратно в CLOSED"""

    @pytest.mark.asyncio
    async def test_success_in_half_open_closes_circuit(self, cb):
        """Успех в HALF_OPEN → CLOSED, счётчик ошибок сбрасывается"""
        # доводим до OPEN
        for _ in range(cb._fail_max):
            with pytest.raises(ConnectionError):
                await cb.call(_fail)

        # вручную переводим в HALF_OPEN (имитируем истечение таймаута)
        cb._state = CircuitState.HALF_OPEN

        # успешный пробный запрос
        result = await cb.call(_success)
        assert result == "ok"
        assert cb._state == CircuitState.CLOSED
        assert cb._failure_count == 0
        assert cb._opened_at is None

    @pytest.mark.asyncio
    async def test_failure_in_half_open_returns_to_open(self, cb):
        """Ошибка в HALF_OPEN возвращает CB в OPEN"""
        # доводим до OPEN
        for _ in range(cb._fail_max):
            with pytest.raises(ConnectionError):
                await cb.call(_fail)

        # вручную переводим в HALF_OPEN
        cb._state = CircuitState.HALF_OPEN

        # неудачный пробный запрос
        with pytest.raises(ConnectionError):
            await cb.call(_fail)

        assert cb._state == CircuitState.OPEN
        assert cb._opened_at is not None


# ---------------------------------------------------------------------------
# Тесты: Фильтрация ошибок (is_failure)
# ---------------------------------------------------------------------------

class TestIsFailureFilter:
    """Проверяем, что is_failure корректно фильтрует ошибки"""

    @pytest.mark.asyncio
    async def test_non_failure_errors_dont_count(self, cb_with_filter):
        """Ошибки, не прошедшие is_failure, не увеличивают счётчик"""
        # ConnectionError не считается сбоем для cb_with_filter
        for _ in range(5):
            with pytest.raises(ConnectionError):
                await cb_with_filter.call(_fail)

        # счётчик не должен был увеличиться
        assert cb_with_filter._failure_count == 0
        assert cb_with_filter._state == CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_matching_errors_trip_breaker(self, cb_with_filter):
        """Ошибки, прошедшие is_failure (ValueError), трипают CB"""
        for _ in range(cb_with_filter._fail_max):
            with pytest.raises(ValueError):
                await cb_with_filter.call(_fail_value_error)

        assert cb_with_filter._state == CircuitState.OPEN
