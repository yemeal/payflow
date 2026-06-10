from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.dialects.postgresql import JSONB

from app.models.base import Base, UuidMixin, TimestampMixin


class OutboxEvent(Base, UuidMixin, TimestampMixin):
    event_type: Mapped[str]
    payload: Mapped[str] = mapped_column(JSONB)
    published: Mapped[bool] = mapped_column(default=False)
