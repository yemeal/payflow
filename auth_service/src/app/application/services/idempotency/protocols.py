from typing import Protocol

from app.application.services.idempotency.domain import (
    AcquireLockResult,
    IdempotencyEntry,
)


class IdempotencyStorageProtocol(Protocol):
    """Storage-контракт generic idempotency guard."""

    async def acquire_lock(
        self,
        key: str,
        lock_value: str,
        ttl: int,
    ) -> AcquireLockResult: ...

    async def release_lock(
        self,
        key: str,
        expected_value: str,
    ) -> bool: ...

    async def save_result(
        self,
        key: str,
        entry: IdempotencyEntry,
        ttl: int,
    ) -> None: ...
