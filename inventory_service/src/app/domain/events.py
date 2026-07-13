from dataclasses import dataclass, field
from typing import Any

# --- типы ответных событий (contracts/inventory/*) ---
EVENT_RESERVED = "inventory.reserved"
EVENT_RESERVE_FAILED = "inventory.reserve-failed"
EVENT_RESERVATION_COMMITTED = "inventory.reservation-committed"
EVENT_COMMIT_FAILED = "inventory.commit-failed"
EVENT_RESERVATION_CANCELLED = "inventory.reservation-cancelled"

# --- машиночитаемые коды отказов (contracts/envelope/failure.v1) ---
# нехватка товара: бизнес-отказ, ретрай бессмыслен - оркестратор компенсирует
FAILURE_INSUFFICIENT_STOCK = "insufficient_stock"
# товара нет в каталоге склада: тоже бизнес-отказ, но причина - данные заказа,
# а не остатки; отдельный код, чтобы отличать в алертах
FAILURE_UNKNOWN_PRODUCT = "unknown_product"
# резерв истёк/отменён до commit: нарушение инварианта TTL >= дедлайн оплаты
# (docs/saga-design.md, 9.8) - сага уходит в FAILED на ручной разбор
FAILURE_RESERVATION_EXPIRED = "reservation_expired"
# commit по заказу, для которого резерва вообще не было: рассинхрон саги
FAILURE_RESERVATION_NOT_FOUND = "reservation_not_found"
# повторный резерв по заказу с уже завершённым (не ACTIVE) резервом
FAILURE_RESERVATION_CONFLICT = "reservation_conflict"


@dataclass(frozen=True, slots=True)
class InventoryEvent:
    """
    Результат обработки команды: тип события + data-часть конверта.

    Транспорт (metadata, correlation, outbox) добавляется поверх - домен склада
    о конверте и сагах не знает.
    """

    event_type: str
    data: dict[str, Any] = field(default_factory=dict)


def failure_block(code: str, message: str, retriable: bool) -> dict[str, Any]:
    """Обязательный блок data.failure каждого *.failed события"""
    return {"code": code, "message": message, "retriable": retriable}
