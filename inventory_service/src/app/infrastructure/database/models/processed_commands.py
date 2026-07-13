from datetime import datetime

from sqlalchemy import String, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.infrastructure.database.models.base import Base


class ProcessedCommandORM(Base):
    """Журнал идемпотентности участника: command_id -> сохранённый ответ"""

    __tablename__ = "processed_commands"

    # PK по command_id: INSERT ... ON CONFLICT DO NOTHING по этому ключу и есть
    # атомарная проверка "команда уже обработана"
    command_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    # готовый конверт ответного события вместе с echo-корреляцией: на дубль
    # команды он переиздаётся в outbox как есть
    result: Mapped[dict] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
