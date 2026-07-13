import re
import uuid
from datetime import datetime

from sqlalchemy import DateTime, func
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    declared_attr,
    mapped_column,
)


class Base(DeclarativeBase):
    __abstract__ = True

    @declared_attr.directive
    def __tablename__(cls) -> str:
        return f"{re.sub(r'([a-z])([A-Z])', r'\1_\2', cls.__name__).lower()}s"

    __mapper_args__ = {"eager_defaults": True}


class UuidMixin:
    __abstract__ = True

    id: Mapped[uuid.UUID] = mapped_column(
        default=uuid.uuid7,
        primary_key=True,
    )


class CreatedAtMixin:
    __abstract__ = True

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )


class TimestampMixin(CreatedAtMixin):
    __abstract__ = True

    updated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        onupdate=func.now(),
    )
