import uuid
from typing import Annotated

from fastapi import APIRouter, Path, Depends
from dishka.integrations.fastapi import FromDishka, inject

from app.schemas.analytics import (
    AnalyticsSummary, 
    PaginatedPaymentsResponse, 
    PaginationParams, 
    SummaryFilterParams,
    PaymentFilterParams
)
from app.schemas.payments import PaymentPayload
from app.services.analytics import AnalyticsServiceProtocol

router = APIRouter(tags=["analytics"])


@router.get(
    "/summary",
    response_model=AnalyticsSummary,
    summary="Получить сводную аналитику",
)
@inject
async def get_summary(
    service: FromDishka[AnalyticsServiceProtocol],
    filters: Annotated[SummaryFilterParams, Depends()],
):
    return await service.get_summary(
        date_from=filters.date_from,
        date_to=filters.date_to,
        currency=filters.currency,
    )


@router.get(
    "/payments",
    response_model=PaginatedPaymentsResponse,
    summary="Получить список платежей с фильтрацией",
)
@inject
async def get_payments(
    service: FromDishka[AnalyticsServiceProtocol],
    filters: Annotated[PaymentFilterParams, Depends()],
    pagination: Annotated[PaginationParams, Depends()],
):
    return await service.get_payments(
        status=filters.status,
        currency=filters.currency,
        date_from=filters.date_from,
        date_to=filters.date_to,
        limit=pagination.limit,
        offset=pagination.offset,
    )


@router.get(
    "/payments/{payment_id}",
    response_model=PaymentPayload,
    summary="Получить детали платежа",
)
@inject
async def get_payment_by_id(
    service: FromDishka[AnalyticsServiceProtocol],
    payment_id: Annotated[uuid.UUID, Path(description="ID платежа")],
):
    return await service.get_payment_by_id(str(payment_id))
