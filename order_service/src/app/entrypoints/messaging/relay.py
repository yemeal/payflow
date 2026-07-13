"""
Outbox relay процесс: публикует PENDING-записи единого outbox (order.created)
в orders.events. Гарантия: событие уходит тогда и только тогда, когда
транзакция создания заказа закоммичена.

Собирается фабрикой, состояние - в main(); глобалей уровня модуля нет.
"""

import asyncio
import signal

import structlog

from app.application.services.outbox_relay import OutboxRelayService
from app.core.logging import setup_logging
from app.core.settings import get_settings
from app.infrastructure.di import create_container

logger = structlog.get_logger(__name__)


async def main() -> None:
    setup_logging()
    settings = get_settings()
    container = create_container(settings)
    relay = await container.get(OutboxRelayService)

    stop_signal = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_signal.set)
        except NotImplementedError:
            # Windows-хост при локальном запуске вне Docker
            pass

    logger.info("outbox_relay_worker_starting")
    relay_task = asyncio.create_task(relay.run())
    stop_task = asyncio.create_task(stop_signal.wait())

    try:
        await asyncio.wait({relay_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        relay.stop()
        stop_task.cancel()
        try:
            await asyncio.wait_for(relay_task, timeout=10.0)
        except asyncio.TimeoutError:
            logger.warning("outbox_relay_shutdown_timeout")
            relay_task.cancel()
        await container.close()
        logger.info("outbox_relay_worker_stopped")


if __name__ == "__main__":
    asyncio.run(main())
