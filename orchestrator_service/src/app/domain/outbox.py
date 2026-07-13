import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class OutboxStatus(Enum):
    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    # запись исчерпала попытки публикации и требует ручного разбора
    FAILED = "FAILED"


class OutboxKind(Enum):
    """Класс сообщения: команда участнику или факт для шины"""

    COMMAND = "COMMAND"
    EVENT = "EVENT"


class OutboxMessage(BaseModel):
    """
    Единая outbox-запись для команд и событий (ADR-006).

    Роутинг задаёт topic (у оркестратора адресатов несколько), порядок сообщений
    одной саги - key (партиционирование по business_key).

    Именование полей: kind - класс сообщения (COMMAND / EVENT), type - тип из
    конверта (то, что участник видит как commandType / event_type: "inventory.reserve",
    "saga.completed"). id - идентификатор строки outbox; идентификатор самого
    сообщения (commandId / event_id) живёт внутри payload-конверта, поэтому
    переотправка команды создаёт новую строку с ТЕМ ЖЕ commandId.
    """

    model_config = ConfigDict(from_attributes=True)

    id: UUID = Field(default_factory=uuid.uuid7)
    kind: OutboxKind
    topic: str
    key: str
    type: str
    # полный конверт сообщения: {"metadata": {...}, "data": {...}}
    payload: dict[str, Any]
    status: OutboxStatus = OutboxStatus.PENDING
    attempts: int = 0
    last_error: str | None = None
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc).replace(tzinfo=None)
    )
    updated_at: datetime | None = None
