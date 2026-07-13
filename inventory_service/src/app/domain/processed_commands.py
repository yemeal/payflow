from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from app.domain.reservations import utc_now


class ProcessedCommand(BaseModel):
    """
    Журнал идемпотентности участника (contracts/README, правило 2):
    command_id -> сохранённый результат.

    Дубль команды НЕ выполняется повторно: сохранённый ответ (готовый конверт
    события вместе с echo-корреляцией) переиздаётся в outbox как есть.
    Поэтому здесь, а не в доменных моделях склада, живёт correlation: это
    инфраструктурный журнал сообщений, а не состояние склада.
    """

    model_config = ConfigDict(from_attributes=True)

    # строка, а не UUID: commandId из чужого конверта - непрозрачное значение,
    # журнал не должен падать на нестандартном формате
    command_id: str
    # конверт ответного события: {"metadata": {...}, "data": {...}}
    result: dict[str, Any]
    created_at: datetime = Field(default_factory=utc_now)
