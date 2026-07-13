import uuid
from datetime import datetime

from sqlalchemy import Enum as SAEnum
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.domain.reservations import ReservationStatus
from app.infrastructure.database.models.base import Base, TimestampMixin, UuidMixin


class ReservationORM(Base, UuidMixin, TimestampMixin):
    """Резерв товара под заказ с TTL"""

    __tablename__ = "reservations"

    # UNIQUE: на заказ - максимум один резерв; это и есть защита от двойного
    # резерва при повторной команде с новым commandId
    order_id: Mapped[uuid.UUID] = mapped_column(unique=True, index=True)
    status: Mapped[ReservationStatus] = mapped_column(
        SAEnum(ReservationStatus),
        default=ReservationStatus.ACTIVE,
        index=True,
    )
    # снапшот позиций резерва: [{"product_id": "sku-1", "quantity": 2}, ...].
    # JSONB, а не отдельная таблица: строки резерва не живут своей жизнью,
    # читаются и пишутся только целиком вместе с резервом
    items: Mapped[list[dict]] = mapped_column(JSONB)
    # индекс обязателен: выборка поллера идёт по expires_at <= now
    expires_at: Mapped[datetime] = mapped_column(index=True)
