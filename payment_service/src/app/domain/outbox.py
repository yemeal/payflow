import enum
import uuid
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field, ConfigDict


class OutboxStatus(enum.Enum):
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


def _default_now() -> datetime:
    return datetime.now(timezone.utc)


class OutboxEvent(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID = Field(default_factory=uuid.uuid7)
    event_type: str
    payload: dict[str, Any]
    status: OutboxStatus = OutboxStatus.PENDING
    reserved_to: datetime | None = None
    # счётчик неудачных попыток публикации; после OUTBOX_MAX_PUBLISH_ATTEMPTS
    # событие помечается FAILED ("ядовитое") и исключается из выборки relay
    attempts: int = 0
    last_error: str | None = None
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc).replace(tzinfo=None)
    )
    updated_at: datetime | None = None
