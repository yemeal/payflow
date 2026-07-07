from __future__ import annotations

from pydantic import BaseModel

from app.application.services.idempotency.enums import IdempotencyKeyStatus, LockAcquireStatus


class AcquireLockResult(BaseModel):
    """
    Результат попытки захвата lock.

    Guard работает с этим объектом вместо raw-ответа Redis
    (преобразование из формата Redis происходит в адаптере)
    """

    model_config = {"frozen": True}

    status: LockAcquireStatus
    existing_entry: IdempotencyEntry | None = None


class IdempotencyEntry(BaseModel):
    """
    Структура записи идемпотентности, хранящаяся в storage.

    Поля status_code и response заполняются только при status == DONE.
    """

    status: IdempotencyKeyStatus
    payload_hash: str

    # заполнены только при status == DONE
    status_code: int | None = None
    response: dict | None = None


class IdempotencyCachedResult(BaseModel):
    """Результат, возвращаемый из DB fallback для кеширования"""

    status_code: int
    response: dict
