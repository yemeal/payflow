from typing import cast
from unittest.mock import AsyncMock

from dishka import Provider, Scope, make_async_container, provide
from dishka.integrations.fastapi import setup_dishka
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient, Response
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncEngine

from app.entrypoints.http.routers import health


class _HealthProvider(Provider):
    @provide(scope=Scope.APP)
    def get_engine(self) -> AsyncEngine:
        return cast(AsyncEngine, object())

    @provide(scope=Scope.APP)
    def get_redis(self) -> Redis:
        return cast(Redis, object())


async def _request_readiness(
    monkeypatch,
    *,
    postgres_ok: bool,
    redis_ok: bool,
) -> Response:
    monkeypatch.setattr(
        health,
        "check_postgres",
        AsyncMock(return_value=postgres_ok),
    )
    monkeypatch.setattr(
        health,
        "check_redis",
        AsyncMock(return_value=redis_ok),
    )

    app = FastAPI()
    app.include_router(health.router)
    container = make_async_container(_HealthProvider())
    setup_dishka(container, app)

    try:
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            return await client.get("/health/ready")
    finally:
        await container.close()


class TestReadiness:
    async def test_ready_when_postgres_and_redis_are_available(
        self,
        monkeypatch,
    ) -> None:
        response = await _request_readiness(
            monkeypatch,
            postgres_ok=True,
            redis_ok=True,
        )

        assert response.status_code == 200
        assert response.json() == {"postgres": "ok", "redis": "ok"}

    async def test_not_ready_when_redis_is_unavailable(
        self,
        monkeypatch,
    ) -> None:
        response = await _request_readiness(
            monkeypatch,
            postgres_ok=True,
            redis_ok=False,
        )

        assert response.status_code == 503
        assert response.json() == {
            "postgres": "ok",
            "redis": "unavailable",
        }

    async def test_not_ready_when_postgres_is_unavailable(
        self,
        monkeypatch,
    ) -> None:
        response = await _request_readiness(
            monkeypatch,
            postgres_ok=False,
            redis_ok=True,
        )

        assert response.status_code == 503
        assert response.json() == {
            "postgres": "unavailable",
            "redis": "ok",
        }
