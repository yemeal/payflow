from decimal import Decimal
from datetime import datetime

from sqlalchemy import Enum as SAEnum, Numeric, String, CheckConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.infrastructure.database.models.base import Base, UuidMixin, TimestampMixin
from app.domain.payments import PaymentStatus


class PaymentORM(Base, UuidMixin, TimestampMixin):
    __tablename__ = "payments"

    idempotency_key: Mapped[str] = mapped_column(
        unique=True,
        index=True,
    )
    status: Mapped[PaymentStatus] = mapped_column(
        SAEnum(PaymentStatus),
    )
    amount: Mapped[Decimal] = mapped_column(
        Numeric(10, 2),
        CheckConstraint(
            "amount > 0",
            name="check_amount",
        ),
    )
    currency: Mapped[str] = mapped_column(
        String(3),
        CheckConstraint(
            "currency ~ '^[A-Z]{3}$'",
            name="check_currency_format",
        ),
    )

    external_id: Mapped[str | None] = mapped_column(
        String(255),
        unique=True,
        index=True,
        default=None,
    )
    customer_id: Mapped[str | None] = mapped_column(
        String(255),
        index=True,
        default=None,
    )
    description: Mapped[str | None] = mapped_column(
        String(1000),
        default=None,
    )
