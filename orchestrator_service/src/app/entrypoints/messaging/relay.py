"""
Relay единого outbox оркестратора: отдельный процесс, публикует PENDING-записи
(команды участникам и события саги) в их топики. Гарантия ADR-002: сообщение
уходит тогда и только тогда, когда переход саги закоммичен.

Отдельный энтрипоинт (а не поток внутри консюмера), чтобы compose масштабировал
relay независимо: очередь outbox разгребают несколько инстансов, конкуренцию
за строки снимает FOR UPDATE SKIP LOCKED.

Запуск: python -m app.entrypoints.messaging.relay
Всё состояние собирается в main(); глобалей уровня модуля нет.
"""

import asyncio

import structlog

from app.application.services.outbox_relay import OutboxRelayService
from app.core.logging import setup_logging
from app.core.settings import get_settings
from app.entrypoints.messaging.worker_runtime import run_worker
from app.infrastructure.di import create_container

logger = structlog.get_logger(__name__)


async def main() -> None:
    setup_logging()
    settings = get_settings()
    container = create_container(settings)
    relay = await container.get(OutboxRelayService)

    try:
        await run_worker(
            name="outbox_relay",
            run=lambda: relay.run(
                poll_interval=settings.OUTBOX_RELAY_POLL_INTERVAL_SECONDS,
                batch_size=settings.OUTBOX_RELAY_BATCH_SIZE,
            ),
            stop=relay.stop,
        )
    finally:
        # в контейнере живут продюсер Kafka и пул БД: закрываем последним
        await container.close()


if __name__ == "__main__":
    asyncio.run(main())
