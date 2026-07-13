from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict

from app.application.services.idempotency.enums import (
    IdempotencyKeyStatus,
    LockAcquireStatus,
)


class AcquireLockResult(BaseModel):
    """Результат атомарной попытки занять idempotency key."""

    model_config = ConfigDict(frozen=True)

    status: LockAcquireStatus
    existing_entry: IdempotencyEntry | None = None


class IdempotencyEntry(BaseModel):
    """Запись lock/result, которую хранит idempotency storage."""

    status: IdempotencyKeyStatus
    payload_hash: str
    status_code: int | None = None
    response: dict[str, Any] | None = None
