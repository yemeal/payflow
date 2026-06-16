import asyncio
from typing import Dict

from dishka import FromDishka
from dishka.integrations.fastapi import inject
from fastapi import APIRouter, Response
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


@router.get("/live")
async def live() -> Dict[str, str]:
    return {"status": "ok"}


@router.get("/ready")
@inject
async def ready(
    response: Response,
    engine: FromDishka[AsyncEngine],
    settings: FromDishka[Settings],
) -> Dict[str, str]:
    postgres_ok = await check_postgres(engine)
    kafka_ok = await check_kafka(settings.KAFKA_BOOTSTRAP_SERVERS)

    status = {
        "postgres": "ok" if postgres_ok else "unavailable",
        "kafka": "ok" if kafka_ok else "unavailable",
    }

    if not postgres_ok or not kafka_ok:
        response.status_code = 503

    return status
