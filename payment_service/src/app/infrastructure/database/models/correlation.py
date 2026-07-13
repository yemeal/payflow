from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.infrastructure.database.models.base import Base, TimestampMixin


class CommandCorrelationORM(Base, TimestampMixin):
    """
    Журнал транспортной корреляции входящих команд (инфраструктура, не домен).

    command_id = idempotency_key платежа: связь с платежом идёт через него,
    поэтому в таблице payments не появляется ни одного лишнего поля.
    """

    __tablename__ = "command_correlations"

    command_id: Mapped[str] = mapped_column(primary_key=True)
    # непрозрачный echo-блок команды: {sagaId, businessKey, commandId}
    correlation: Mapped[dict] = mapped_column(JSONB)
