from datetime import datetime

from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import Enum as SAEnum, Text

from app.infrastructure.database.models.base import Base, UuidMixin, TimestampMixin
from app.domain.outbox import OutboxStatus


class OutboxEventORM(Base, UuidMixin, TimestampMixin):
    __tablename__ = "outbox_events"

    event_type: Mapped[str]
    payload: Mapped[dict] = mapped_column(JSONB)
    status: Mapped[OutboxStatus] = mapped_column(
        SAEnum(OutboxStatus), default=OutboxStatus.PENDING
    )
    reserved_to: Mapped[datetime | None] = mapped_column(default=None)
    attempts: Mapped[int] = mapped_column(default=0, server_default="0")
    last_error: Mapped[str | None] = mapped_column(Text, default=None)
