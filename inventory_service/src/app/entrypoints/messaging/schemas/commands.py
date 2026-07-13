"""
Входной контракт команд склада (contracts/inventory/*.v1.schema.json).

Свои Pydantic-модели, а не импорт contracts/ - осознанная дупликация, цена
автономии команд (ADR-007). Tolerant reader: неизвестные поля игнорируются,
обязательные валидируются; невалидный конверт уходит в DLQ, а не в NACK-цикл.
"""

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel


class _CamelCaseBase(BaseModel):
    # metadata и data команд - camelCase (contracts/README);
    # populate_by_name принимает и snake_case-вариант
    model_config = ConfigDict(
        populate_by_name=True,
        alias_generator=to_camel,
        extra="ignore",
    )


class CommandMetadata(_CamelCaseBase):
    """metadata команды; sagaId и businessKey обязательны для команд склада"""

    # строка, а не UUID: commandId - непрозрачный ключ журнала идемпотентности
    command_id: str
    command_type: str
    version: str = "1.0"
    timestamp: datetime | None = None
    source: str = ""
    # echo-значения: возвращаются в metadata.correlation ответного события
    saga_id: str
    business_key: str


class ReserveItem(_CamelCaseBase):
    product_id: str
    quantity: int = Field(ge=1)


class ReserveData(_CamelCaseBase):
    order_id: uuid.UUID
    items: list[ReserveItem] = Field(min_length=1)
    # по схеме обязателен, но толерантный ридер не обязан падать на его
    # отсутствии: None -> дефолт RESERVATION_DEFAULT_TTL_SECONDS из настроек
    ttl_seconds: int | None = Field(default=None, ge=1)


class OrderRefData(_CamelCaseBase):
    """data команд commit_reservation / cancel_reservation"""

    order_id: uuid.UUID


class ReserveCommandEnvelope(_CamelCaseBase):
    metadata: CommandMetadata
    data: ReserveData


class CommitReservationCommandEnvelope(_CamelCaseBase):
    metadata: CommandMetadata
    data: OrderRefData


class CancelReservationCommandEnvelope(_CamelCaseBase):
    metadata: CommandMetadata
    data: OrderRefData


def extract_command_type(message: Any) -> str:
    """Тип команды до валидации - по нему выбирается схема конверта"""
    if not isinstance(message, dict):
        return ""
    metadata = message.get("metadata")
    if not isinstance(metadata, dict):
        return ""
    return str(metadata.get("commandType") or metadata.get("command_type") or "")
