from decimal import Decimal
from datetime import datetime
from uuid import UUID
from pydantic import Field

from app.domain.payments import PaymentStatus
from app.entrypoints.http.schemas.base import CamelCaseBase, CamelCaseOrmBase


class PaymentCreate(CamelCaseBase):
    """
    Схема для создания нового платежа (входящий POST-запрос)
    Ключ идемпотентности указывать в хедере `Idempotency-Key: <key>`
    """

    # почему в заголовке? потому что идемпотентность - это инфраструктурный механизм HTTP уровня API,
    # а тело запроса (Payload/DTO) относится строго к бизнес-логике приложения
    amount: Decimal = Field(gt=0, max_digits=10, decimal_places=2)
    currency: str = Field(min_length=3, max_length=3, pattern="^[A-Z]{3}$")
    customer_id: str | None = Field(default=None, max_length=255)
    description: str | None = Field(default=None, max_length=1000)


class PaymentResponse(CamelCaseOrmBase):
    """
    Схема для возврата инфы о платеже клиенту (исходящий ответ)
    Включает серверные поля (id, даты, статус...)
    """

    id: UUID
    status: PaymentStatus
    amount: Decimal
    currency: str

    customer_id: str | None
    description: str | None

    created_at: datetime
    updated_at: datetime | None

    # external_id и idempotency_key клиенту API знать обычно не обязательно,
    # поэтому их в Response не отдаем из соображений безопасности.
