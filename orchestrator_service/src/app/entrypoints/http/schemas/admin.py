from datetime import datetime
from typing import Any
from uuid import UUID

from app.domain.saga import SagaStatus
from app.entrypoints.http.schemas.base import CamelCaseOrmBase


class SagaResponse(CamelCaseOrmBase):
    """Состояние саги без payload: список саг не должен раздуваться снапшотами"""

    id: UUID
    saga_type: str
    business_key: str
    status: SagaStatus
    current_step: str | None
    retry_count: int
    retry_after: datetime | None
    deadline_at: datetime | None
    active_command_id: UUID | None
    last_error: str | None
    created_at: datetime
    updated_at: datetime | None


class SagaTransitionResponse(CamelCaseOrmBase):
    """Строка append-only истории переходов (аудит для оператора)"""

    id: UUID
    saga_id: UUID
    from_status: str | None
    from_step: str | None
    to_status: str
    to_step: str | None
    event_id: UUID | None
    event_type: str | None
    detail: str | None
    created_at: datetime


class SagaDetailResponse(CamelCaseOrmBase):
    """Карточка саги: состояние + снапшот payload + вся история переходов"""

    saga: SagaResponse
    # payload саги - минимальный снапшот для команд и компенсаций, PII в нём нет
    # по построению (docs/saga-design.md, 9.2), поэтому отдаём оператору как есть
    payload: dict[str, Any]
    transitions: list[SagaTransitionResponse]
