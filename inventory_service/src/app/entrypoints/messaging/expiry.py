"""
Поллер автоистечения резервов: отдельный процесс.

Раз в EXPIRY_POLLER_INTERVAL_SECONDS переводит ACTIVE-резервы с истёкшим TTL
в EXPIRED и возвращает товар на склад. Событий не публикует (см. докстринг
ReservationExpiryService): о неоплаченном заказе оркестратор узнаёт по своему
дедлайну шага оплаты.

Отдельный процесс, а не задача внутри консьюмера: поллер не должен умирать
вместе с ребалансировкой Kafka и не должен конкурировать с обработкой команд
за event loop.
"""

import asyncio
import signal

import structlog

from app.application.services.reservation_expiry import ReservationExpiryService
from app.core.logging import setup_logging
from app.core.settings import get_settings
from app.infrastructure.di import create_container

logger = structlog.get_logger(__name__)


async def main() -> None:
    setup_logging()
    settings = get_settings()
    container = create_container(settings)
    poller = await container.get(ReservationExpiryService)

    stop_signal = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_signal.set)
        except NotImplementedError:
            # Windows-хост при локальном запуске вне Docker
            pass

    logger.info("reservation_expiry_worker_starting")
    poller_task = asyncio.create_task(poller.run())
    stop_task = asyncio.create_task(stop_signal.wait())

    try:
        await asyncio.wait(
            {poller_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
        )
    finally:
        # graceful: поллер дорабатывает текущий тик и выходит из цикла
        poller.stop()
        stop_task.cancel()
        try:
            await asyncio.wait_for(poller_task, timeout=10.0)
        except asyncio.TimeoutError:
            logger.warning("reservation_expiry_shutdown_timeout")
            poller_task.cancel()
        await container.close()
        logger.info("reservation_expiry_worker_stopped")


if __name__ == "__main__":
    asyncio.run(main())
