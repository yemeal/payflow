import asyncio

from dishka import FromDishka
from dishka.integrations.fastapi import inject
from fastapi import APIRouter, Response
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.core.settings import Settings

router = APIRouter(prefix="/health", tags=["health"])


async def check_kafka(bootstrap_servers: str) -> bool:
    try:
        servers = bootstrap_servers.split(",")
        for server in servers:
            host, port = server.split(":")
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, int(port)), timeout=2.0
            )
            writer.close()
            await writer.wait_closed()
            return True
        return False
    except Exception:
        return False


async def check_postgres(engine: AsyncEngine) -> bool:
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


async def check_redis(redis: Redis) -> bool:
    try:
        return await redis.ping()
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
    redis: FromDishka[Redis],
    settings: FromDishka[Settings],
) -> dict[str, str]:

    # запускаем проверки конкурентно
    postgres_ok, kafka_ok, redis_ok = await asyncio.gather(
        check_postgres(engine),
        check_kafka(settings.KAFKA_BOOTSTRAP_SERVERS),
        check_redis(redis),
    )

    status = {
        "postgres": "ok" if postgres_ok else "unavailable",
        "kafka": "ok" if kafka_ok else "unavailable",
        "redis": "ok" if redis_ok else "unavailable",
    }

    # если хотя бы один из критичных компонентов недоступен - возвращаем 503
    if not all((postgres_ok, kafka_ok, redis_ok)):
        response.status_code = 503

    return status
