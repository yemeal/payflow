import uuid
from datetime import datetime
from decimal import Decimal

from app.schemas.base import CamelCaseOrmBase


class PaymentPayload(CamelCaseOrmBase):
    id: uuid.UUID
    status: str
    amount: Decimal
    currency: str

    customer_id: str | None
    description: str | None

    created_at: datetime
    updated_at: datetime | None
