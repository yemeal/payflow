import uuid
from datetime import datetime
from decimal import Decimal
from pydantic import Field
from app.entrypoints.http.schemas.base import CamelCaseBase


class CommandMetadata(CamelCaseBase):
    """Метаданные команды от оркестратора"""

    command_id: uuid.UUID
    command_type: str
    version: str = "1.0"
    timestamp: datetime
    source: str


class ProcessPaymentPayload(CamelCaseBase):
    """Полезная нагрузка для создания платежа"""

    amount: Decimal = Field(gt=0, max_digits=10, decimal_places=2)
    currency: str = Field(min_length=3, max_length=3, pattern="^[A-Z]{3}$")
    customer_id: str | None = Field(default=None, max_length=255)
    description: str | None = Field(default=None, max_length=1000)


class ProcessPaymentCommand(CamelCaseBase):
    """Конверт команды payment.process"""

    metadata: CommandMetadata
    data: ProcessPaymentPayload
