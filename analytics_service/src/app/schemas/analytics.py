from decimal import Decimal

from pydantic import Field

from app.schemas.base import CamelCaseOrmBase
from app.schemas.payments import PaymentPayload

from datetime import datetime

class PaginationParams(CamelCaseOrmBase):
    limit: int = Field(default=10, ge=1, le=100, description="Лимит элементов для пагинации")
    offset: int = Field(default=0, ge=0, description="Смещение элементов для пагинации")

class DateFilterParams(CamelCaseOrmBase):
    date_from: datetime | None = Field(default=None, description="Начальная дата для фильтрации")
    date_to: datetime | None = Field(default=None, description="Конечная дата для фильтрации")

class SummaryFilterParams(DateFilterParams):
    currency: str | None = Field(default=None, description="Фильтр по валюте")

class PaymentFilterParams(SummaryFilterParams):
    status: str | None = Field(default=None, description="Фильтр по статусу платежа")

class AnalyticsSummary(CamelCaseOrmBase):
    total_transactions: int = Field(
        default=0, description="Общее количество обработанных транзакций"
    )
    total_amount: Decimal = Field(
        default=Decimal("0.0"), description="Сумма всех транзакций"
    )
    currency: str = Field(description="Валюта суммы")
    status_breakdown: dict[str, int] = Field(
        default_factory=dict, description="Количество транзакций по статусам"
    )


class PaginatedPaymentsResponse(CamelCaseOrmBase):
    items: list[PaymentPayload]
    total: int
    page: int
    size: int
