from dataclasses import dataclass
from uuid import UUID

from app.domain.reservations import ReservationItem


@dataclass(frozen=True, slots=True)
class CommandCorrelation:
    """
    Echo-блок из метаданных команды (contracts/README, правило 1).

    Участник возвращает эти значения в metadata.correlation ответного события
    как непрозрачные - не интерпретируя. Это транспортная метадата: она приходит
    с уровня entrypoints/messaging и НЕ попадает в доменные модели склада.
    """

    saga_id: str
    business_key: str
    command_id: str


@dataclass(frozen=True, slots=True)
class ReserveCommand:
    """inventory.reserve: заблокировать товар под заказ на ttl_seconds"""

    correlation: CommandCorrelation
    order_id: UUID
    items: list[ReservationItem]
    # ttlSeconds обязателен по контракту, но толерантный ридер не обязан падать
    # на его отсутствии: None -> RESERVATION_DEFAULT_TTL_SECONDS из настроек
    ttl_seconds: int | None = None


@dataclass(frozen=True, slots=True)
class CommitReservationCommand:
    """inventory.commit_reservation: списать товар, резерв -> COMMITTED"""

    correlation: CommandCorrelation
    order_id: UUID


@dataclass(frozen=True, slots=True)
class CancelReservationCommand:
    """inventory.cancel_reservation: компенсация, вернуть сток"""

    correlation: CommandCorrelation
    order_id: UUID
