from sqlalchemy import CheckConstraint, String
from sqlalchemy.orm import Mapped, mapped_column

from app.infrastructure.database.models.base import Base, TimestampMixin


class StockItemORM(Base, TimestampMixin):
    """Остаток по товару. PK - product_id (естественный ключ каталога)"""

    __tablename__ = "stock_items"

    product_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    # CHECK на уровне БД - последний рубеж: даже если сервис ошибётся в
    # арифметике, отрицательный остаток не запишется, транзакция упадёт
    available: Mapped[int] = mapped_column(
        CheckConstraint("available >= 0", name="check_available_non_negative"),
        default=0,
        server_default="0",
    )
    reserved: Mapped[int] = mapped_column(
        CheckConstraint("reserved >= 0", name="check_reserved_non_negative"),
        default=0,
        server_default="0",
    )
