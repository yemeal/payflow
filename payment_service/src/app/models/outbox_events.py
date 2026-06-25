import enum
from datetime import datetime

from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import Enum as SAEnum

from app.models.base import Base, UuidMixin, TimestampMixin


class OutboxStatus(enum.Enum):
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class OutboxEvent(Base, UuidMixin, TimestampMixin):
    event_type: Mapped[str]
    payload: Mapped[str] = mapped_column(JSONB)
    status: Mapped[OutboxStatus] = mapped_column(
        SAEnum(OutboxStatus), default=OutboxStatus.PENDING
    )
    reserved_to: Mapped[datetime | None] = mapped_column(default=None)
