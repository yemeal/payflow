import uuid
from datetime import datetime, timezone
from enum import Enum
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


def utc_now() -> datetime:
    """Naive-UTC, как во всех таблицах проекта (колонки без таймзоны)"""
    return datetime.now(timezone.utc).replace(tzinfo=None)


class ReservationStatus(Enum):
    """
    Жизненный цикл резерва:
      ACTIVE -> COMMITTED  (оплата прошла, товар списан)
      ACTIVE -> CANCELLED  (компенсация саги, сток возвращён)
      ACTIVE -> EXPIRED    (истёк TTL, сток возвращён фоновым поллером)
    Терминальные статусы неизменяемы.
    """

    ACTIVE = "ACTIVE"
    COMMITTED = "COMMITTED"
    CANCELLED = "CANCELLED"
    EXPIRED = "EXPIRED"


# из этих статусов сток уже возвращён или списан: повторное движение
# остатков по такому резерву - двойной учёт
TERMINAL_STATUSES: frozenset[ReservationStatus] = frozenset(
    {
        ReservationStatus.COMMITTED,
        ReservationStatus.CANCELLED,
        ReservationStatus.EXPIRED,
    }
)


class ReservationItem(BaseModel):
    """Строка резерва: сколько какого товара заблокировано"""

    product_id: str
    quantity: int = Field(gt=0)


class Reservation(BaseModel):
    """
    Резерв товара под заказ с автоистечением (TTL).

    Домен склада НЕ знает о сагах: ни saga_id, ни correlation здесь нет.
    Корреляция - транспортная метадата команды, живёт в processed_commands
    и в конверте outbox-записи (contracts/README, правило 1).
    """

    model_config = ConfigDict(from_attributes=True)

    id: UUID = Field(default_factory=uuid.uuid7)
    # бизнес-ключ склада: на заказ - максимум один резерв (UNIQUE в БД)
    order_id: UUID
    status: ReservationStatus = ReservationStatus.ACTIVE
    items: list[ReservationItem]
    # момент автоистечения; инвариант конфигурации (docs/saga-design.md, 9.8):
    # TTL резерва >= дедлайн оплаты + буфер
    expires_at: datetime
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime | None = None

    def is_expired_at(self, moment: datetime) -> bool:
        return self.expires_at <= moment
