import uuid
from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from app.domain.reservations import utc_now


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
    Единая outbox-запись команд и событий (ADR-006, конвенция репозитория).

    Склад публикует только события (orders.events), но структура таблицы
    общая для всех сервисов: kind - класс сообщения, type - тип из конверта
    ("inventory.reserved"), topic/key - адресация и партиционирование.

    id - идентификатор СТРОКИ outbox; идентификатор самого сообщения
    (event_id) живёт внутри payload-конверта.
    """

    model_config = ConfigDict(from_attributes=True)

    id: UUID = Field(default_factory=uuid.uuid7)
    kind: OutboxKind
    topic: str
    # ключ партиционирования = business_key саги (order_id): порядок событий
    # одного заказа Kafka сохраняет внутри партиции
    key: str
    type: str
    # полный конверт сообщения: {"metadata": {...}, "data": {...}}
    payload: dict[str, Any]
    status: OutboxStatus = OutboxStatus.PENDING
    attempts: int = 0
    last_error: str | None = None
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime | None = None
