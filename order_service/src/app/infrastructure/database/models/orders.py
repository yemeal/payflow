import uuid
from decimal import Decimal

from sqlalchemy import Enum as SAEnum
from sqlalchemy import Numeric, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.domain.orders import OrderStatus
from app.infrastructure.database.models.base import Base, TimestampMixin, UuidMixin


class OrderORM(Base, UuidMixin, TimestampMixin):
    __tablename__ = "orders"

    user_id: Mapped[uuid.UUID] = mapped_column(index=True)
    status: Mapped[OrderStatus] = mapped_column(
        SAEnum(OrderStatus), default=OrderStatus.PENDING, index=True
    )
    # позиции заказа; значения json-совместимые: price сериализуется строкой
    # (конвертацию Decimal -> str делает репозиторий через model_dump(mode="json"))
    items: Mapped[list[dict]] = mapped_column(JSONB)
    total_amount: Mapped[Decimal] = mapped_column(Numeric(10, 2))
    currency: Mapped[str] = mapped_column(String(3), default="RUB", server_default="RUB")
    # saga_id удалён (ADR-006): заказ process-agnostic, корреляция по order_id
