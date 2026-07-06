from decimal import Decimal
from typing import Literal, Annotated

from pydantic import BaseModel, Field

# доменные модели (Pydantic используется намеренно для валидации)
# в принципе можно и датаклассы юзать


class ProviderTransactionRequest(BaseModel):
    """
    Модель запроса на проведение транзакции
    """

    amount: Decimal
    currency: str


class ProviderTransactionInitiated(BaseModel):
    """
    Модель ответа внешнего провайдера на инициацию платежа
    (status обычно "PENDING")
    """

    transaction_id: str = Field(validation_alias="id")
    status: str
    redirect_url: str | None = None


class ProviderTransactionPending(BaseModel):
    transaction_id: str = Field(validation_alias="id")
    status: Literal["PENDING"]


class ProviderTransactionCompleted(BaseModel):
    transaction_id: str = Field(validation_alias="id")
    status: Literal["COMPLETED"]
    amount: Decimal
    currency: str


class ProviderTransactionFailed(BaseModel):
    transaction_id: str = Field(validation_alias="id")
    status: Literal["FAILED"]
    error_message: str | None


# discriminated union
# В зависимости от значения поля discriminator="status"
# автоматически маппим на подходящую модель (pending, failed, ...)
ProviderTransactionStatus = Annotated[
    ProviderTransactionPending
    | ProviderTransactionCompleted
    | ProviderTransactionFailed,
    Field(discriminator="status"),
]
