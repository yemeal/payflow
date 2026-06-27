import uuid

from app.schemas.base import CamelCaseOrmBase
from app.schemas.payments import PaymentPayload


class PaymentEvent(CamelCaseOrmBase):
    id: uuid.UUID
    event_type: str
    payload: PaymentPayload
    status: str
