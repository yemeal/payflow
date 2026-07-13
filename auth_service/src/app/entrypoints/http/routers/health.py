import asyncio

from dishka import FromDishka
from dishka.integrations.fastapi import inject
from fastapi import APIRouter, Response
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

router = APIRouter(prefix="/health", tags=["health"])


async def check_postgres(engine: AsyncEngine) -> bool:
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


async def check_redis(redis_client: Redis) -> bool:
    try:
        return bool(await redis_client.ping())
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
    redis_client: FromDishka[Redis],
) -> dict[str, str]:
    # без базы или Redis refresh-flow не может нормально обслуживать запросы
    postgres_ok, redis_ok = await asyncio.gather(
        check_postgres(engine),
        check_redis(redis_client),
    )

    if not postgres_ok or not redis_ok:
        response.status_code = 503

    return {
        "postgres": "ok" if postgres_ok else "unavailable",
        "redis": "ok" if redis_ok else "unavailable",
    }
