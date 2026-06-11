from pydantic import BaseModel

from app.services.idempotency.enums import IdempotencyKeyStatus


class IdempotencyEntry(BaseModel):
    """Структура данных, которая хранится в Redis под ключом idempotency:{key}"""

    status: IdempotencyKeyStatus
    payload_hash: str

    # эти поля заполнены только тогда, когда статус == DONE
    status_code: int | None = None
    response: dict | None = None


class IdempotencyCachedResult(BaseModel):
    """Результат, возвращаемый из DB fallback для кеширования"""

    status_code: int
    response: dict
