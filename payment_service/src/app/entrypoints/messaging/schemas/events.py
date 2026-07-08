import uuid
from datetime import datetime
from typing import Any
from app.entrypoints.http.schemas.base import CamelCaseBase


class EventMetadata(CamelCaseBase):
    """Метаданные события для Outbox"""

    event_id: uuid.UUID
    event_type: str
    version: str = "1.0"
    timestamp: datetime
    source: str = "payment-service"


class EventEnvelope(CamelCaseBase):
    """Конверт события для отправки в шину сообщений"""

    metadata: EventMetadata
    data: dict[str, Any]
