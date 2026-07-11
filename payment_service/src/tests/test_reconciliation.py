"""
Тесты reconciliation-задачи sync_pending_payments_task (TaskIQ cron).

Проверяем устойчивость периодической сверки (AGENTS.md, "Reconciliation"):
  - Distributed lock (Redis SET NX EX): пока идёт один запуск, второй не стартует,
    иначе параллельная сверка одного платежа даёт гонки статусов и дубли событий;
  - лок освобождается в finally (compare-and-delete через Lua);
  - падение сверки одного платежа не отменяет обработку остального батча.

Инфраструктуру подменяем: DI-контейнер и Redis - моки, функцию сверки одного
платежа инжектим как параметр задачи.

Формат документации: Проверяем / Успех / Нежелательное поведение.
"""

import contextlib
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.domain.payments import Payment, PaymentStatus
from app.entrypoints.workers import taskiq as worker
from app.entrypoints.workers.tasks.sync_payments import (
    sync_pending_payments_task,
    _sync_one_payment,
)


# достаём исходную корутину из-под декоратора TaskIQ
_task_fn = getattr(sync_pending_payments_task, "original_func", sync_pending_payments_task)


def _payment(status=PaymentStatus.PROCESSING):
    return Payment(
        idempotency_key="k",
        amount=Decimal("100.00"),
        currency="RUB",
        status=status,
        external_id="ext",
    )


def install_fake_container(monkeypatch, redis_mock, payment_service):
    """
    Подменяет worker.container так, чтобы:
      - `await worker.container.get(Redis)` вернул redis_mock;
      - `async with worker.container() as rc: rc.get(...)` вернул payment_service.
    """
    request_container = MagicMock()
    request_container.get = AsyncMock(return_value=payment_service)

    @contextlib.asynccontextmanager
    async def request_scope():
        yield request_container

    container = MagicMock(side_effect=request_scope)
    container.get = AsyncMock(return_value=redis_mock)

    monkeypatch.setattr(worker, "container", container)
    return container


# ---------------------------------------------------------------------------
# Distributed lock
# ---------------------------------------------------------------------------

class TestDistributedLock:
    @pytest.mark.asyncio
    async def test_lock_acquired_processes_and_releases(self, monkeypatch):
        """
        Проверяем: лок свободен, в батче два PROCESSING-платежа.
        Успех: сверка вызвана для каждого платежа, по завершении лок освобождается
               (redis.eval с compare-and-delete).
        Нежелательное поведение: пропуск платежей или неосвобождённый лок (следующий
                   запуск cron будет молча простаивать до истечения TTL).
        """
        redis_mock = AsyncMock()
        redis_mock.set.return_value = True  # NX удался - лок наш
        payment_service = AsyncMock()
        payment_service.get_processing_payments.return_value = [_payment(), _payment()]
        install_fake_container(monkeypatch, redis_mock, payment_service)

        sync_one = AsyncMock()
        await _task_fn(sync_one_payment_func=sync_one)

        assert sync_one.await_count == 2
        redis_mock.eval.assert_awaited_once()  # лок освобождён в finally

    @pytest.mark.asyncio
    async def test_lock_not_acquired_skips_run(self, monkeypatch):
        """
        Проверяем: лок уже занят другим запуском (SET NX вернул None).
        Успех: задача выходит сразу, платежи не выбираются и не сверяются.
        Нежелательное поведение: параллельная сверка тех же платежей двумя запусками.
        """
        redis_mock = AsyncMock()
        redis_mock.set.return_value = None  # NX не удался - лок занят
        payment_service = AsyncMock()
        install_fake_container(monkeypatch, redis_mock, payment_service)

        sync_one = AsyncMock()
        await _task_fn(sync_one_payment_func=sync_one)

        sync_one.assert_not_called()
        payment_service.get_processing_payments.assert_not_called()

    @pytest.mark.asyncio
    async def test_empty_batch_still_releases_lock(self, monkeypatch):
        """
        Проверяем: лок взят, но сверять некого (пустой батч).
        Успех: задача корректно завершается и всё равно освобождает лок в finally.
        Нежелательное поведение: лок остаётся висеть после холостого запуска.
        """
        redis_mock = AsyncMock()
        redis_mock.set.return_value = True
        payment_service = AsyncMock()
        payment_service.get_processing_payments.return_value = []
        install_fake_container(monkeypatch, redis_mock, payment_service)

        sync_one = AsyncMock()
        await _task_fn(sync_one_payment_func=sync_one)

        sync_one.assert_not_called()
        redis_mock.eval.assert_awaited_once()


# ---------------------------------------------------------------------------
# Изоляция ошибок и параллелизм
# ---------------------------------------------------------------------------

class TestBatchResilience:
    @pytest.mark.asyncio
    async def test_processes_whole_batch_above_parallel_limit(self, monkeypatch):
        """
        Проверяем: батч больше лимита параллелизма (RECONCILIATION_MAX_PARALLEL).
        Успех: обработаны все платежи (semaphore ограничивает одновременность, но не теряет задачи).
        Нежелательное поведение: часть платежей не обработана из-за ограничения параллелизма.
        """
        redis_mock = AsyncMock()
        redis_mock.set.return_value = True
        payment_service = AsyncMock()
        payments = [_payment() for _ in range(25)]
        payment_service.get_processing_payments.return_value = payments
        install_fake_container(monkeypatch, redis_mock, payment_service)

        sync_one = AsyncMock()
        await _task_fn(sync_one_payment_func=sync_one)

        assert sync_one.await_count == 25

    @pytest.mark.asyncio
    async def test_sync_one_payment_swallows_errors(self, monkeypatch):
        """
        Проверяем: сверка одного платежа упала с исключением.
        Успех: _sync_one_payment гасит ошибку (не пробрасывает), чтобы падение одного
               платежа не отменило всю TaskGroup остальных.
        Нежелательное поведение: исключение из одного платежа рушит весь батч сверки.
        """
        payment_service = AsyncMock()
        payment_service.sync_payment_with_provider.side_effect = RuntimeError("boom")

        request_container = MagicMock()
        request_container.get = AsyncMock(return_value=payment_service)

        @contextlib.asynccontextmanager
        async def request_scope():
            yield request_container

        container = MagicMock(side_effect=request_scope)
        monkeypatch.setattr(worker, "container", container)

        # не должно поднять исключение
        await _sync_one_payment(_payment())
