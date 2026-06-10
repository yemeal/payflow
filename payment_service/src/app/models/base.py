import uuid
from datetime import datetime

from sqlalchemy import func
from sqlalchemy.orm import (
    DeclarativeBase,
    declared_attr,
    mapped_column,
    Mapped,
)
import re


class Base(DeclarativeBase):
    __abstract__ = True

    @declared_attr.directive
    def __tablename__(cls) -> str:
        return f"{re.sub(r'([a-z])([A-Z])', r'\1_\2', cls.__name__).lower()}s"


class UuidMixin:
    __abstract__ = True

    id: Mapped[uuid.UUID] = mapped_column(
        default=uuid.uuid7,
        primary_key=True,
    )


class TimestampMixin:
    __abstract__ = True

    created_at: Mapped[datetime] = mapped_column(
        server_default=func.now(),
    )
    updated_at: Mapped[datetime | None] = mapped_column(
        default=None,
        onupdate=func.now(),
    )
