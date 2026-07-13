"""
Health-пробы Admin API.

/health/live - процесс жив (для restart-политики): никаких внешних вызовов,
иначе моргнувшая БД перезапускала бы здоровый процесс.
/health/ready - готов обслуживать запросы: Postgres (источник правды саг) и
Kafka-продюсер (без него outbox-relay не публикует команды). Любая недоступность
даёт 503, чтобы балансировщик увёл трафик.
"""

import asyncio

from aiokafka import AIOKafkaProducer
from dishka import FromDishka
from dishka.integrations.fastapi import inject
from fastapi import APIRouter, Response, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

router = APIRouter(prefix="/health", tags=["health"])

# проба не должна висеть дольше интервала самой пробы у оркестратора контейнеров
_PROBE_TIMEOUT_SECONDS = 2.0


async def check_postgres(engine: AsyncEngine) -> bool:
    try:
        async with engine.connect() as conn:
            await asyncio.wait_for(
                conn.execute(text("SELECT 1")), timeout=_PROBE_TIMEOUT_SECONDS
            )
        return True
    except Exception:
        return False


async def check_kafka(producer: AIOKafkaProducer) -> bool:
    # спрашиваем метаданные у уже поднятого продюсера: проверка живого соединения,
    # а не просто открытого TCP-порта брокера
    try:
        await asyncio.wait_for(
            producer.client.fetch_all_metadata(), timeout=_PROBE_TIMEOUT_SECONDS
        )
        return True
    except Exception:
        return False


@router.get("/live")
async def live() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/ready")
@inject
async def ready(
    response: Response,
    engine: FromDishka[AsyncEngine],
    producer: FromDishka[AIOKafkaProducer],
) -> dict[str, str]:
    # проверки независимы -> гоняем параллельно, чтобы уложиться в таймаут пробы
    postgres_ok, kafka_ok = await asyncio.gather(
        check_postgres(engine),
        check_kafka(producer),
    )

    if not (postgres_ok and kafka_ok):
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE

    return {
        "postgres": "ok" if postgres_ok else "unavailable",
        "kafka": "ok" if kafka_ok else "unavailable",
    }
