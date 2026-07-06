import asyncio
from typing import Callable, Awaitable, Coroutine, Any

import structlog

from app.models import Payment
from app.services.payment_service import PaymentServiceProtocol
from app import taskiq as worker
from app.taskiq import broker

logger = structlog.get_logger(__name__)

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
    async with worker.container() as request_container:
        payment_service = await request_container.get(PaymentServiceProtocol)
        # Берем порцию платежей (например, 50 за раз)
        processing_payments = await payment_service.get_processing_payments(limit=50)

    if not processing_payments:
        return

    logger.info(
        "syncing processing payments batch",
        count=len(processing_payments),
    )

    # Обрабатываем батч параллельно
    async with asyncio.TaskGroup() as task_group:
        for payment in processing_payments:
            task_group.create_task(sync_one_payment_func(payment))
