"""
Общая обвязка фоновых воркеров оркестратора (relay, poller).

Оба процесса живут одинаково: крутят бесконечный цикл, ловят SIGINT/SIGTERM
и обязаны дать текущему батчу (тику) доработать, а не оборваться посередине
транзакции. Логика вынесена сюда, чтобы семантика остановки у них не разъехалась.
"""

import asyncio
import signal
from collections.abc import Awaitable, Callable
from contextlib import suppress

import structlog

logger = structlog.get_logger(__name__)

# сколько ждём завершения текущего батча/тика после команды stop
DEFAULT_SHUTDOWN_TIMEOUT = 10.0


def install_stop_signal() -> asyncio.Event:
    """Событие, взводимое по SIGINT/SIGTERM (docker stop шлёт SIGTERM)"""
    stop_signal = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_signal.set)
        except NotImplementedError:
            # Windows-хост при локальном запуске вне Docker: сигналы не поддержаны,
            # остановка идёт через KeyboardInterrupt в asyncio.run
            pass
    return stop_signal


async def run_worker(
    name: str,
    run: Callable[[], Awaitable[None]],
    stop: Callable[[], None],
    shutdown_timeout: float = DEFAULT_SHUTDOWN_TIMEOUT,
) -> None:
    """Запускает цикл воркера и держит процесс до сигнала остановки.

    Если цикл упадёт сам, wait вернётся по FIRST_COMPLETED, а wait_for ниже
    пробросит исключение наверх: процесс должен умереть громко и быть перезапущен
    оркестратором контейнеров, а не притворяться живым с мёртвым циклом.
    """
    stop_signal = install_stop_signal()
    worker_task = asyncio.create_task(run(), name=name)
    stop_task = asyncio.create_task(stop_signal.wait())
    logger.info("worker_starting", worker=name)

    try:
        await asyncio.wait(
            {worker_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
        )
    finally:
        # graceful: сервис дообрабатывает текущий батч/тик и выходит из цикла сам
        stop()
        stop_task.cancel()
        with suppress(asyncio.CancelledError):
            await stop_task
        if not worker_task.done():
            try:
                await asyncio.wait_for(worker_task, timeout=shutdown_timeout)
            except asyncio.TimeoutError:
                logger.warning("worker_shutdown_timeout", worker=name)
                worker_task.cancel()
                with suppress(asyncio.CancelledError):
                    await worker_task
        logger.info("worker_stopped", worker=name)
