import uuid
from datetime import datetime

from sqlalchemy import Enum as SAEnum
from sqlalchemy import String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.domain.saga import SagaStatus
from app.infrastructure.database.models.base import Base, TimestampMixin, UuidMixin


class SagaORM(Base, UuidMixin, TimestampMixin):
    """Состояние саги: источник правды, события Kafka - лишь триггеры переходов"""

    __tablename__ = "sagas"
    __table_args__ = (
        # идемпотентное создание саги: дубль стартового события упирается
        # в уникальность и превращается в INSERT ... ON CONFLICT DO NOTHING
        UniqueConstraint("saga_type", "business_key", name="uq_sagas_type_business_key"),
    )

    saga_type: Mapped[str] = mapped_column(String(100), index=True)
    # ключ корреляции с внешним миром (для заказа - order_id)
    business_key: Mapped[str] = mapped_column(String(255))
    status: Mapped[SagaStatus] = mapped_column(
        SAEnum(SagaStatus), default=SagaStatus.RUNNING, index=True
    )
    current_step: Mapped[str | None] = mapped_column(String(100), default=None)
    # минимальный снапшот данных для команд и компенсаций (без PII/токенов)
    payload: Mapped[dict] = mapped_column(JSONB)
    retry_count: Mapped[int] = mapped_column(default=0, server_default="0")
    # индексы retry_after/deadline_at нужны выборкам фонового поллера
    retry_after: Mapped[datetime | None] = mapped_column(default=None, index=True)
    deadline_at: Mapped[datetime | None] = mapped_column(default=None, index=True)
    # commandId активной команды: ответы на устаревшие команды отбрасываются
    active_command_id: Mapped[uuid.UUID | None] = mapped_column(default=None)
    last_error: Mapped[str | None] = mapped_column(Text, default=None)
