from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from dishka import Provider, Scope, make_async_container, provide
from dishka.integrations.fastapi import setup_dishka
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from app.application.services.auth_service import (
    AuthServiceProtocol,
    TokenPair,
)
from app.application.services.idempotency import (
    AcquireLockResult,
    IdempotencyEntry,
    IdempotencyService,
)
from app.application.services.idempotency.enums import LockAcquireStatus
from app.domain.exceptions import DomainErrors
from app.entrypoints.http.routers import create_api_router
from app.entrypoints.http.routers.exception_handlers import (
    register_exception_handlers,
)


class InMemoryIdempotencyStorage:
    """Повторяет атомарный Redis-контракт без внешней инфраструктуры."""

    def __init__(self) -> None:
        self._data: dict[str, str] = {}

    async def acquire_lock(
        self,
        key: str,
        lock_value: str,
        ttl: int,
    ) -> AcquireLockResult:
        del ttl
        current = self._data.get(key)
        if current is not None:
            return AcquireLockResult(
                status=LockAcquireStatus.ENTRY_EXISTS,
                existing_entry=IdempotencyEntry.model_validate_json(current),
            )
        self._data[key] = lock_value
        return AcquireLockResult(status=LockAcquireStatus.LOCK_ACQUIRED)

    async def release_lock(
        self,
        key: str,
        expected_value: str,
    ) -> bool:
        if self._data.get(key) != expected_value:
            return False
        del self._data[key]
        return True

    async def save_result(
        self,
        key: str,
        entry: IdempotencyEntry,
        ttl: int,
    ) -> None:
        del ttl
        self._data[key] = entry.model_dump_json()

    def expire_all(self) -> None:
        self._data.clear()


class _TestProvider(Provider):
    def __init__(
        self,
        auth_service: AsyncMock,
        idempotency_service: IdempotencyService,
    ) -> None:
        super().__init__()
        self._auth_service = auth_service
        self._idempotency_service = idempotency_service

    @provide(scope=Scope.REQUEST)
    def get_auth_service(self) -> AuthServiceProtocol:
        return self._auth_service

    @provide(scope=Scope.REQUEST)
    def get_idempotency_service(self) -> IdempotencyService:
        return self._idempotency_service


@pytest_asyncio.fixture
async def refresh_http():
    auth_service = AsyncMock(spec=AuthServiceProtocol)
    storage = InMemoryIdempotencyStorage()
    settings = SimpleNamespace(
        IDEMPOTENCY_LOCK_TTL=30,
        IDEMPOTENCY_RESULT_TTL=300,
    )
    idempotency_service = IdempotencyService(storage, settings)

    app = FastAPI()
    app.include_router(create_api_router())
    register_exception_handlers(
        app,
        idempotency_retry_after_seconds=settings.IDEMPOTENCY_LOCK_TTL,
    )
    container = make_async_container(
        _TestProvider(auth_service, idempotency_service)
    )
    setup_dishka(container, app)

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        yield client, auth_service, storage

    await container.close()


def _pair(suffix: str = "1") -> TokenPair:
    return TokenPair(
        access_token=f"access-{suffix}",
        refresh_token=f"refresh-{suffix}",
    )


def _request_headers(key: str = "refresh-request-0001") -> dict[str, str]:
    return {"Idempotency-Key": key}


class TestRefreshEndpoint:
    async def test_requires_idempotency_key(self, refresh_http) -> None:
        client, auth_service, _storage = refresh_http

        response = await client.post(
            "/api/v1/auth/refresh",
            json={"refreshToken": "old-refresh"},
        )

        assert response.status_code == 422
        auth_service.refresh.assert_not_awaited()

    async def test_first_request_rotates_and_caches_pair(
        self,
        refresh_http,
    ) -> None:
        client, auth_service, _storage = refresh_http
        auth_service.refresh.return_value = _pair()

        response = await client.post(
            "/api/v1/auth/refresh",
            json={"refreshToken": "old-refresh"},
            headers=_request_headers(),
        )

        assert response.status_code == 200
        assert response.json() == {
            "accessToken": "access-1",
            "refreshToken": "refresh-1",
            "tokenType": "bearer",
        }
        auth_service.refresh.assert_awaited_once_with("old-refresh")

    async def test_same_token_and_key_returns_same_pair_without_reuse(
        self,
        refresh_http,
    ) -> None:
        client, auth_service, _storage = refresh_http
        auth_service.refresh.return_value = _pair()

        first = await client.post(
            "/api/v1/auth/refresh",
            json={"refreshToken": "old-refresh"},
            headers=_request_headers(),
        )
        second = await client.post(
            "/api/v1/auth/refresh",
            json={"refreshToken": "old-refresh"},
            headers=_request_headers(),
        )

        assert first.status_code == second.status_code == 200
        assert second.json() == first.json()
        auth_service.refresh.assert_awaited_once_with("old-refresh")

    async def test_same_key_with_another_token_is_conflict(
        self,
        refresh_http,
    ) -> None:
        client, auth_service, _storage = refresh_http
        auth_service.refresh.return_value = _pair()

        await client.post(
            "/api/v1/auth/refresh",
            json={"refreshToken": "old-refresh"},
            headers=_request_headers(),
        )
        response = await client.post(
            "/api/v1/auth/refresh",
            json={"refreshToken": "another-refresh"},
            headers=_request_headers(),
        )

        assert response.status_code == 409
        assert response.json()["code"] == "auth.idempotency_key_conflict"
        auth_service.refresh.assert_awaited_once()

    async def test_same_token_with_another_key_uses_normal_reuse_flow(
        self,
        refresh_http,
    ) -> None:
        client, auth_service, _storage = refresh_http
        auth_service.refresh.side_effect = [
            _pair(),
            DomainErrors.Session.REFRESH_TOKEN_REUSED(),
        ]

        first = await client.post(
            "/api/v1/auth/refresh",
            json={"refreshToken": "old-refresh"},
            headers=_request_headers("refresh-request-0001"),
        )
        second = await client.post(
            "/api/v1/auth/refresh",
            json={"refreshToken": "old-refresh"},
            headers=_request_headers("refresh-request-0002"),
        )

        assert first.status_code == 200
        assert second.status_code == 401
        assert second.json()["code"] == "auth.invalid_refresh_token"
        assert auth_service.refresh.await_count == 2

    async def test_expired_window_uses_normal_reuse_flow(
        self,
        refresh_http,
    ) -> None:
        client, auth_service, storage = refresh_http
        auth_service.refresh.side_effect = [
            _pair(),
            DomainErrors.Session.REFRESH_TOKEN_REUSED(),
        ]

        first = await client.post(
            "/api/v1/auth/refresh",
            json={"refreshToken": "old-refresh"},
            headers=_request_headers(),
        )
        storage.expire_all()
        second = await client.post(
            "/api/v1/auth/refresh",
            json={"refreshToken": "old-refresh"},
            headers=_request_headers(),
        )

        assert first.status_code == 200
        assert second.status_code == 401
        assert auth_service.refresh.await_count == 2

    async def test_concurrent_same_key_invokes_refresh_only_once(
        self,
        refresh_http,
    ) -> None:
        client, auth_service, _storage = refresh_http
        refresh_started = asyncio.Event()
        allow_refresh_to_finish = asyncio.Event()

        async def slow_refresh(_refresh_token: str) -> TokenPair:
            refresh_started.set()
            await allow_refresh_to_finish.wait()
            return _pair()

        auth_service.refresh.side_effect = slow_refresh
        first_request = asyncio.create_task(
            client.post(
                "/api/v1/auth/refresh",
                json={"refreshToken": "old-refresh"},
                headers=_request_headers(),
            )
        )
        await refresh_started.wait()

        concurrent_response = await client.post(
            "/api/v1/auth/refresh",
            json={"refreshToken": "old-refresh"},
            headers=_request_headers(),
        )
        allow_refresh_to_finish.set()
        first_response = await first_request

        assert first_response.status_code == 200
        assert concurrent_response.status_code == 423
        assert concurrent_response.headers["Retry-After"] == "30"
        auth_service.refresh.assert_awaited_once_with("old-refresh")
