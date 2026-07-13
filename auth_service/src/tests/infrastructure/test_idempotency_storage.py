from unittest.mock import AsyncMock

import pytest
import redis.exceptions

from app.application.exceptions.idempotency import (
    IdempotencyStorageUnavailableError,
)
from app.application.services.idempotency.domain import IdempotencyEntry
from app.application.services.idempotency.enums import (
    IdempotencyKeyStatus,
    LockAcquireStatus,
)
from app.infrastructure.idempotency import RedisIdempotencyStorage


def _done_entry() -> IdempotencyEntry:
    return IdempotencyEntry(
        status=IdempotencyKeyStatus.DONE,
        payload_hash="payload-hash",
        status_code=200,
        response={
            "accessToken": "access",
            "refreshToken": "refresh",
            "tokenType": "bearer",
        },
    )


class TestRedisIdempotencyStorage:
    async def test_acquire_lock_parses_existing_result(self) -> None:
        redis_client = AsyncMock()
        redis_client.eval.return_value = [
            LockAcquireStatus.ENTRY_EXISTS,
            _done_entry().model_dump_json(),
        ]
        storage = RedisIdempotencyStorage(redis_client)

        result = await storage.acquire_lock(
            key="idempotency:auth:refresh:key",
            lock_value="processing",
            ttl=30,
        )

        assert result.status is LockAcquireStatus.ENTRY_EXISTS
        assert result.existing_entry == _done_entry()

    async def test_acquire_failure_becomes_application_error(self) -> None:
        redis_client = AsyncMock()
        redis_client.eval.side_effect = redis.exceptions.ConnectionError()
        storage = RedisIdempotencyStorage(redis_client)

        with pytest.raises(IdempotencyStorageUnavailableError):
            await storage.acquire_lock(
                key="idempotency:auth:refresh:key",
                lock_value="processing",
                ttl=30,
            )

    async def test_result_cache_write_uses_ttl(self) -> None:
        redis_client = AsyncMock()
        storage = RedisIdempotencyStorage(redis_client)
        entry = _done_entry()

        await storage.save_result(
            key="idempotency:auth:refresh:key",
            entry=entry,
            ttl=300,
        )

        redis_client.set.assert_awaited_once_with(
            "idempotency:auth:refresh:key",
            entry.model_dump_json(),
            ex=300,
        )

    async def test_result_cache_failure_does_not_hide_success(self) -> None:
        redis_client = AsyncMock()
        redis_client.set.side_effect = redis.exceptions.ConnectionError()
        storage = RedisIdempotencyStorage(redis_client)

        await storage.save_result(
            key="idempotency:auth:refresh:key",
            entry=_done_entry(),
            ttl=300,
        )
