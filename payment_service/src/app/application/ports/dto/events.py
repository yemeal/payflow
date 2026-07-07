from uuid import UUID
from datetime import datetime
from typing import Any
from pydantic import BaseModel

from app.domain.outbox import OutboxEvent

# ---------------------------------------------------------------------------
# Event Envelope — типизированная Pydantic-схема исходящего события
# ---------------------------------------------------------------------------
# отделяет контракт формата сообщений от relay-сервиса (SRP):
# если структура envelope меняется — меняется только эта схема, не сервис


class EventEnvelopeMetadata(BaseModel):
    """Метаданные события — кто отправил, когда, какой тип"""
    event_id: UUID
    event_type: str
    version: str = "1.0"
    timestamp: datetime
    source: str = "payment-service"


class EventEnvelope(BaseModel):
    """
    Полный конверт события для публикации в брокер сообщений.
    Содержит метаданные + бизнес-данные (payload) из OutboxEvent.
    """
    metadata: EventEnvelopeMetadata
    data: dict[str, Any]

    @classmethod
    def from_outbox_event(cls, event: OutboxEvent) -> "EventEnvelope":
        """Фабричный метод — собирает envelope из доменного OutboxEvent"""
        return cls(
            metadata=EventEnvelopeMetadata(
                event_id=event.id,
                event_type=event.event_type,
                timestamp=event.created_at,
            ),
            data=event.payload,
        )
