from decimal import Decimal
from enum import Enum
from datetime import datetime, timezone
from uuid import UUID
import uuid

from pydantic import BaseModel, ConfigDict, Field


class PaymentStatus(Enum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    REFUNDED = "REFUNDED"
    CANCELED = "CANCELED"


class Payment(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID = Field(default_factory=uuid.uuid7)
    idempotency_key: str
    status: PaymentStatus
    amount: Decimal
    currency: str
    external_id: str | None = None
    customer_id: str | None = None
    description: str | None = None
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc).replace(tzinfo=None)
    )
    updated_at: datetime | None = None
