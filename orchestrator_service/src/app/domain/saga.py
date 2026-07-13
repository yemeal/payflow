import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class SagaStatus(Enum):
    """
    Generic-статусы (ADR-006): фаза процесса, а не бизнес-шаг.
    Конкретный бизнес-шаг хранится в Saga.current_step и осмысляется
    через SagaDefinition соответствующего saga_type.
    """

    RUNNING = "RUNNING"
    COMPENSATING = "COMPENSATING"
    COMPLETED = "COMPLETED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"


# из терминальных статусов переходов нет: событие к завершённой саге
# логируется и игнорируется, а не роняет обработчик
TERMINAL_STATUSES: frozenset[SagaStatus] = frozenset(
    {SagaStatus.COMPLETED, SagaStatus.CANCELLED, SagaStatus.FAILED}
)


def utc_now() -> datetime:
    """Naive-UTC, как во всех таблицах проекта (колонки без таймзоны)"""
    return datetime.now(timezone.utc).replace(tzinfo=None)


class Saga(BaseModel):
    """
    Состояние одной саги. Источник правды - эта запись в PostgreSQL;
    события Kafka лишь триггеры переходов.
    """

    model_config = ConfigDict(from_attributes=True)

    id: UUID = Field(default_factory=uuid.uuid7)
    saga_type: str
    # ключ корреляции с внешним миром (для заказа - order_id);
    # UNIQUE(saga_type, business_key) даёт идемпотентное создание саги
    business_key: str
    status: SagaStatus = SagaStatus.RUNNING
    # имя текущего шага из SagaDefinition;
    # в статусе COMPENSATING - имя компенсируемого сейчас шага
    current_step: str | None = None
    # минимальный снапшот данных для команд и компенсаций (без PII/токенов)
    payload: dict[str, Any]
    # попытки текущего шага; сбрасывается в 0 при переходе на следующий шаг
    retry_count: int = 0
    # когда переотправить команду текущего шага (exponential backoff + jitter);
    # None - ретрай не запланирован
    retry_after: datetime | None = None
    # дедлайн ответа участника на активную команду; None - ответа не ждём
    deadline_at: datetime | None = None
    # commandId активной команды: переотправка идёт с ТЕМ ЖЕ id (дедуп участника
    # вернёт тот же результат), ответы на устаревшие команды отбрасываются
    active_command_id: UUID | None = None
    last_error: str | None = None
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime | None = None

    @property
    def is_terminal(self) -> bool:
        return self.status in TERMINAL_STATUSES


class SagaTransition(BaseModel):
    """Append-only история переходов: аудит для Admin API и отладки"""

    model_config = ConfigDict(from_attributes=True)

    id: UUID = Field(default_factory=uuid.uuid7)
    saga_id: UUID
    from_status: str | None = None
    from_step: str | None = None
    to_status: str
    to_step: str | None = None
    # событие-триггер перехода; None - переход инициировал поллер (retry/timeout)
    event_id: UUID | None = None
    event_type: str | None = None
    detail: str | None = None
    created_at: datetime = Field(default_factory=utc_now)
