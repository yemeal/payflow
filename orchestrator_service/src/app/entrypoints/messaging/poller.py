"""
Поллер саг: отдельный процесс, тикает retry и deadline (docs/saga-design.md, 9.3).

Без него сага, чей участник промолчал, зависла бы навсегда: событий больше нет,
а значит, консюмер её не разбудит. Время здесь - такой же триггер перехода,
как и событие, поэтому источник правды один и тот же (таблица sagas).

Отдельный энтрипоинт (а не поток внутри консюмера): у поллера свой профиль
нагрузки и свой масштаб. Несколько инстансов безопасны - выборки идут
FOR UPDATE SKIP LOCKED, одну сагу возьмёт ровно один тик.

Запуск: python -m app.entrypoints.messaging.poller
Всё состояние собирается в main(); глобалей уровня модуля нет.
"""

import asyncio

import structlog

from app.application.services.saga_poller import SagaPollerService
from app.core.logging import setup_logging
from app.core.settings import get_settings
from app.entrypoints.messaging.worker_runtime import run_worker
from app.infrastructure.di import create_container

logger = structlog.get_logger(__name__)


async def main() -> None:
    setup_logging()
    settings = get_settings()
    container = create_container(settings)
    poller = await container.get(SagaPollerService)

    try:
        await run_worker(
            name="saga_poller",
            run=poller.run,
            stop=poller.stop,
        )
    finally:
        # поллер сам ничего не публикует (команды уходят через outbox),
        # но в контейнере живёт пул БД: закрываем последним
        await container.close()


if __name__ == "__main__":
    asyncio.run(main())
