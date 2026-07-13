import uuid
from datetime import datetime
from decimal import Decimal

from pydantic import Field

from app.domain.orders import OrderStatus
from app.entrypoints.http.schemas.base import CamelCaseBase, CamelCaseOrmBase


class OrderItemSchema(CamelCaseOrmBase):
    # CamelCaseOrmBase (from_attributes=True): схема наполняется и из JSON-тела,
    # и из доменного OrderItem при сериализации OrderResponse - без него
    # ответ падал ValidationError на вложенных позициях
    product_id: str = Field(min_length=1, max_length=255)
    quantity: int = Field(gt=0)
    price: Decimal = Field(gt=0, max_digits=10, decimal_places=2)


class OrderCreate(CamelCaseBase):
    """Тело POST /orders.

    user_id здесь сознательно отсутствует: владелец заказа берётся только
    из JWT (см. entrypoints/http/security.py), подменить его нельзя.
    total_amount тоже не принимаем - сумма считается на сервере по позициям
    (до появления каталога цены позиций - от клиента, упрощение MVP).
    """

    items: list[OrderItemSchema] = Field(min_length=1)
    currency: str = Field(default="RUB", pattern="^[A-Z]{3}$")


class OrderResponse(CamelCaseOrmBase):
    id: uuid.UUID
    user_id: uuid.UUID
    status: OrderStatus
    items: list[OrderItemSchema]
    total_amount: Decimal
    currency: str
    created_at: datetime
    updated_at: datetime | None = None
