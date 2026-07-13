from sqlalchemy import Enum as SAEnum
from sqlalchemy import String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.domain.outbox import OutboxKind, OutboxStatus
from app.infrastructure.database.models.base import Base, TimestampMixin, UuidMixin


class OutboxMessageORM(Base, UuidMixin, TimestampMixin):
    """Единая outbox-таблица команд и событий (ADR-006)"""

    __tablename__ = "outbox"

    # kind - класс сообщения (COMMAND / EVENT), семантика для метрик и отладки
    kind: Mapped[OutboxKind] = mapped_column(SAEnum(OutboxKind))
    # адресат: у оркестратора их несколько, поэтому топик - атрибут записи
    topic: Mapped[str] = mapped_column(String(255))
    # ключ партиционирования (business_key): порядок сообщений одной саги
    key: Mapped[str] = mapped_column(String(255))
    # type - тип из конверта: "order.created"
    type: Mapped[str] = mapped_column(String(100))
    # полный конверт: {"metadata": ..., "data": ...}
    payload: Mapped[dict] = mapped_column(JSONB)
    status: Mapped[OutboxStatus] = mapped_column(
        SAEnum(OutboxStatus), default=OutboxStatus.PENDING, index=True
    )
    attempts: Mapped[int] = mapped_column(default=0, server_default="0")
    last_error: Mapped[str | None] = mapped_column(Text, default=None)
