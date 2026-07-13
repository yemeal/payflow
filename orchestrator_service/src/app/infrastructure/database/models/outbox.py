from sqlalchemy import Enum as SAEnum
from sqlalchemy import String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.domain.outbox import OutboxKind, OutboxStatus
from app.infrastructure.database.models.base import Base, TimestampMixin, UuidMixin


class OutboxMessageORM(Base, UuidMixin, TimestampMixin):
    """Единая outbox-таблица команд и событий (ADR-006)"""

    __tablename__ = "outbox"

    # класс сообщения: команда участнику (COMMAND) или факт для шины (EVENT)
    kind: Mapped[OutboxKind] = mapped_column(SAEnum(OutboxKind))
    # адресат: у оркестратора их несколько, поэтому топик - атрибут записи
    topic: Mapped[str] = mapped_column(String(255))
    # ключ партиционирования (business_key): порядок сообщений одной саги.
    # index нужен разбору инцидентов ("все сообщения по заказу X")
    key: Mapped[str] = mapped_column(String(255), index=True)
    # тип из конверта: commandType ("inventory.reserve") или event_type ("saga.completed")
    type: Mapped[str] = mapped_column(String(100))
    # полный конверт: {"metadata": ..., "data": ...}
    payload: Mapped[dict] = mapped_column(JSONB)
    status: Mapped[OutboxStatus] = mapped_column(
        SAEnum(OutboxStatus), default=OutboxStatus.PENDING, index=True
    )
    attempts: Mapped[int] = mapped_column(default=0, server_default="0")
    last_error: Mapped[str | None] = mapped_column(Text, default=None)
