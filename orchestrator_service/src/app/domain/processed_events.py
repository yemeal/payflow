import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict


class ProcessedEvent(BaseModel):
    """
    Запись об обработанном событии (Idempotent Consumer).

    Kafka гарантирует at-least-once: одно событие может прийти несколько раз
    (ребалансинг, сбой commit offset'а). Уникальность event_id + вставка
    ON CONFLICT DO NOTHING в одной транзакции с бизнес-обработкой дают
    exactly-once processing.
    """

    model_config = ConfigDict(from_attributes=True)

    event_id: uuid.UUID
    # для отладки: по saga_id восстанавливается весь путь саги в логах
    saga_id: uuid.UUID | None = None
    event_type: str
    # проставляется БД (server_default now)
    processed_at: datetime | None = None
