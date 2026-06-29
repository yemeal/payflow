import structlog
from datetime import datetime
from typing import Protocol

from app.core.exceptions.payment import PaymentNotFoundError
from app.core.settings import Settings
from app.schemas.analytics import AnalyticsSummary, PaginatedPaymentsResponse
from app.schemas.payments import PaymentPayload
from app.repositories.payments import PaymentRepositoryProtocol
from app.services.cache import CacheServiceProtocol

logger = structlog.get_logger()

class AnalyticsServiceProtocol(Protocol):
    async def get_summary(
        self,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
        currency: str | None = None,
    ) -> AnalyticsSummary: ...

    async def get_payments(
        self,
        status: str | None = None,
        currency: str | None = None,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
        limit: int = 10,
        offset: int = 0,
    ) -> PaginatedPaymentsResponse: ...

    async def get_payment_by_id(self, payment_id: str) -> PaymentPayload: ...


class AnalyticsService:
    def __init__(
        self, 
        repo: PaymentRepositoryProtocol,
        cache: CacheServiceProtocol,
        settings: Settings,
    ):
        self._repo = repo
        self._cache = cache
        self._settings = settings

    async def get_summary(
        self,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
        currency: str | None = None,
    ) -> AnalyticsSummary:
        cache_key = f"analytics:summary:{date_from}:{date_to}:{currency}"
        
        cached_data = await self._cache.get(cache_key)
        if cached_data:
            logger.info("analytics_summary_cache_hit", key=cache_key)
            return AnalyticsSummary.model_validate_json(cached_data)

        logger.info("analytics_summary_cache_miss", key=cache_key)
        
        summary_data = await self._repo.get_summary(
            date_from=date_from,
            date_to=date_to,
            currency=currency,
        )

        total_transactions = summary_data["total_transactions"]
        completed = summary_data["completed_count"]
        failed = summary_data["failed_count"]
        total_amount = summary_data["total_amount"]

        status_breakdown = {
            "COMPLETED": completed,
            "FAILED": failed,
        }

        summary = AnalyticsSummary(
            total_transactions=total_transactions,
            total_amount=total_amount,
            currency=currency or "MIXED",
            status_breakdown=status_breakdown,
        )

        await self._cache.set(
            cache_key, 
            summary.model_dump_json(), 
            expire=self._settings.CACHE_TTL
        )

        return summary

    async def get_payments(
        self,
        status: str | None = None,
        currency: str | None = None,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
        limit: int = 10,
        offset: int = 0,
    ) -> PaginatedPaymentsResponse:
        payments, total_count = await self._repo.get_payments(
            status=status,
            currency=currency,
            date_from=date_from,
            date_to=date_to,
            limit=limit,
            offset=offset,
        )

        items = [
            PaymentPayload.model_validate(p, from_attributes=True) for p in payments
        ]

        page = (offset // limit) + 1 if limit > 0 else 1

        return PaginatedPaymentsResponse(
            items=items,
            total=total_count,
            page=page,
            size=limit,
        )

    async def get_payment_by_id(self, payment_id: str) -> PaymentPayload:
        payment = await self._repo.get(payment_id)
        if not payment:
            raise PaymentNotFoundError(f"Payment with id={payment_id} not found")
            
        return PaymentPayload.model_validate(payment, from_attributes=True)
