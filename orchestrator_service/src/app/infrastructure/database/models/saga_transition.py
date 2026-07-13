import uuid
from datetime import datetime

from sqlalchemy import ForeignKey, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from app.infrastructure.database.models.base import Base, UuidMixin


class SagaTransitionORM(Base, UuidMixin):
    """Append-only история переходов (Admin API, отладка, аудит)"""

    __tablename__ = "saga_transitions"

    # CASCADE: retention-скрипты удаляют сагу - история уходит вместе с ней
    saga_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("sagas.id", ondelete="CASCADE"), index=True
    )
    from_status: Mapped[str | None] = mapped_column(String(50), default=None)
    from_step: Mapped[str | None] = mapped_column(String(100), default=None)
    to_status: Mapped[str] = mapped_column(String(50))
    to_step: Mapped[str | None] = mapped_column(String(100), default=None)
    # событие-триггер перехода; NULL - переход инициировал поллер (retry/timeout)
    event_id: Mapped[uuid.UUID | None] = mapped_column(default=None)
    event_type: Mapped[str | None] = mapped_column(String(100), default=None)
    detail: Mapped[str | None] = mapped_column(Text, default=None)
    # без TimestampMixin: история неизменяема, updated_at ей не нужен
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
