import asyncio
import uuid
from typing import Callable, Coroutine, Any

import structlog
from redis.asyncio import Redis

from app.domain.payments import Payment
from app.application.services.payment_service import PaymentServiceProtocol
from app.entrypoints.workers import taskiq as worker
from app.entrypoints.workers.taskiq import broker

logger = structlog.get_logger(__name__)

# --- reconciliation lock ---
RECONCILIATION_LOCK_KEY = "payments:reconciliation:lock"
# TTL страхует от вечного лока при падении воркера;
# должен превышать максимальную длительность батча
RECONCILIATION_LOCK_TTL_SEC = 300
# ограничиваем параллелизм: 50 одновременных sync'ов - это 50 сессий БД
# из пула и залповый расход RPS-лимита провайдера
RECONCILIATION_MAX_PARALLEL = 10

# освобождаем лок, только если он всё ещё наш
# чтобы не удалить лок следующего запуска, если наш уже истёк по TTL
_RELEASE_LOCK_SCRIPT = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""


async def _sync_one_payment(payment: Payment) -> None:
    async with worker.container() as request_container:
        payment_service = await request_container.get(PaymentServiceProtocol)
        try:
            await payment_service.sync_payment_with_provider(payment)
        except Exception as e:
            # внутри TaskGroup важно перехватывать исключения,
            # чтобы падение одного платежа не отменило всю группу
            logger.error(
                "error syncing payment",
                payment_id=str(payment.id),
                error=str(e),
            )


@broker.task(schedule=[{"cron": "* * * * *"}])
async def sync_pending_payments_task(
    sync_one_payment_func: Callable[
        [Payment], Coroutine[Any, Any, None]
    ] = _sync_one_payment,
) -> None:
    redis: Redis = await worker.container.get(Redis)

    # Distributed lock: в кластере одновременно работает только один запуск задачи.
    # Без него батч, работающий дольше минуты, пересекается со следующим запуском cron
    # -> параллельные sync одного платежа
    # → гонки статусов и дублирующиеся outbox-события.
    lock_token = str(uuid.uuid4())
    acquired = await redis.set(
        RECONCILIATION_LOCK_KEY,
        lock_token,
        nx=True,
        ex=RECONCILIATION_LOCK_TTL_SEC,
    )
    if not acquired:
        logger.info("reconciliation_skipped_previous_run_in_progress")
        return

    try:
        async with worker.container() as request_container:
            payment_service = await request_container.get(PaymentServiceProtocol)
            # Берем порцию платежей (например, 50 за раз)
            processing_payments = await payment_service.get_processing_payments(
                limit=50
            )

        if not processing_payments:
            return

        logger.info(
            "syncing processing payments batch",
            count=len(processing_payments),
        )

        semaphore = asyncio.Semaphore(RECONCILIATION_MAX_PARALLEL)

        async def sync_with_limit(payment: Payment) -> None:
            async with semaphore:
                await sync_one_payment_func(payment)

        # Обрабатываем батч параллельно, но не более RECONCILIATION_MAX_PARALLEL за раз
        async with asyncio.TaskGroup() as task_group:
            for payment in processing_payments:
                task_group.create_task(sync_with_limit(payment))
    finally:
        try:
            await redis.eval(
                _RELEASE_LOCK_SCRIPT, 1, RECONCILIATION_LOCK_KEY, lock_token
            )
        except Exception as e:
            # не критично: лок истечет по TTL самостоятельно
            logger.warning("reconciliation_lock_release_failed", error=str(e))
