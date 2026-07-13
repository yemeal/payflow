import uuid
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field


class OrderStatus(Enum):
    """
    Статусы заказа. PENDING - semantic lock (ADR-004): пока сага активна,
    заказ меняет только она. Финализируют заказ события саги:
    saga.completed -> COMPLETED, saga.cancelled / saga.failed -> CANCELLED.

    Промежуточных статусов саги здесь нет: заказ process-agnostic (ADR-006),
    шаги саги (резерв, оплата) живут в оркестраторе.
    """

    PENDING = "PENDING"
    COMPLETED = "COMPLETED"
    CANCELLED = "CANCELLED"


class OrderItem(BaseModel):
    """Позиция заказа. Хранится внутри orders.items (JSONB)."""

    product_id: str
    quantity: int = Field(gt=0)
    price: Decimal = Field(gt=0)


class Order(BaseModel):
    """
    Доменная модель заказа.

    user_id приходит только из JWT (никогда из тела запроса).
    saga_id здесь НЕТ (ADR-006): заказ ничего не знает о процессе;
    сагу создаёт оркестратор, корреляция - по order_id (business key).
    """

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID = Field(default_factory=uuid.uuid7)
    user_id: uuid.UUID
    status: OrderStatus = OrderStatus.PENDING
    items: list[OrderItem]
    total_amount: Decimal
    # до появления каталога валюта заказа фиксируется клиентом (упрощение MVP)
    currency: str = "RUB"
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc).replace(tzinfo=None)
    )
    updated_at: datetime | None = None
