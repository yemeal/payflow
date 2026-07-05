import uuid

from pydantic import BaseModel
from app.schemas.base import CamelCaseOrmBase
from app.schemas.payments import PaymentPayload


class EventMetadata(CamelCaseOrmBase):
    event_id: uuid.UUID
    event_type: str
    version: str
    timestamp: str
    source: str


class PaymentEvent(BaseModel):
    metadata: EventMetadata
    data: PaymentPayload
