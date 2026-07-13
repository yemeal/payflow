import uuid
from datetime import datetime

from sqlalchemy import String, func
from sqlalchemy.orm import Mapped, mapped_column

from app.infrastructure.database.models.base import Base


class ProcessedEventORM(Base):
    __tablename__ = "processed_events"

    # PK по event_id: ON CONFLICT DO NOTHING по этому ключу и есть
    # атомарная проверка "событие уже обработано"
    event_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    saga_id: Mapped[uuid.UUID | None] = mapped_column(index=True, default=None)
    event_type: Mapped[str] = mapped_column(String(255))
    processed_at: Mapped[datetime] = mapped_column(server_default=func.now())
