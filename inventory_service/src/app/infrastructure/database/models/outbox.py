from sqlalchemy import Enum as SAEnum
from sqlalchemy import Index, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.domain.outbox import OutboxKind, OutboxStatus
from app.infrastructure.database.models.base import Base, TimestampMixin, UuidMixin


class OutboxMessageORM(Base, UuidMixin, TimestampMixin):
    """Единая outbox-таблица команд и событий (ADR-006)"""

    __tablename__ = "outbox"

    # ровно под выборку релея: WHERE status = 'PENDING' ORDER BY created_at
    __table_args__ = (
        Index("ix_outbox_status_created_at", "status", "created_at"),
    )

    kind: Mapped[OutboxKind] = mapped_column(SAEnum(OutboxKind))
    # адресат - атрибут записи: релей не знает бизнес-логики маршрутизации
    topic: Mapped[str] = mapped_column(String(255))
    # ключ партиционирования (business_key): порядок событий одного заказа
    key: Mapped[str] = mapped_column(String(255))
    # тип из конверта: "inventory.reserved", "inventory.commit-failed", ...
    type: Mapped[str] = mapped_column(String(100))
    # полный конверт: {"metadata": ..., "data": ...}
    payload: Mapped[dict] = mapped_column(JSONB)
    status: Mapped[OutboxStatus] = mapped_column(
        SAEnum(OutboxStatus), default=OutboxStatus.PENDING
    )
    attempts: Mapped[int] = mapped_column(default=0, server_default="0")
    last_error: Mapped[str | None] = mapped_column(Text, default=None)
