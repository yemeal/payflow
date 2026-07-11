"""
Тесты AnalyticsService - чтение аналитики (сводка, список платежей, платеж по id).

Проверяем кэширование сводки (hit не ходит в БД, miss считает и кэширует),
пагинацию списка и поведение при отсутствии платежа.

Репозиторий и кэш - моки, БД/Redis не поднимаем.

Формат документации: Проверяем / Успех / Нежелательное поведение.
"""

import pytest
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4
from datetime import datetime

from unittest.mock import AsyncMock

from app.services.analytics import AnalyticsService
from app.schemas.analytics import AnalyticsSummary
from app.core.exceptions.payment import PaymentNotFoundError


def make_service(repo=None, cache=None):
    repo = repo or AsyncMock()
    cache = cache or AsyncMock()
    settings = SimpleNamespace(CACHE_TTL=60)
    return AnalyticsService(repo=repo, cache=cache, settings=settings), repo, cache


def make_payment_row(status="COMPLETED"):
    return SimpleNamespace(
        id=uuid4(),
        status=status,
        amount=Decimal("100.00"),
        currency="RUB",
        customer_id="cust-1",
        description="test",
        created_at=datetime(2026, 7, 10, 10, 0, 0),
        updated_at=None,
    )


# ---------------------------------------------------------------------------
# get_summary: кэш
# ---------------------------------------------------------------------------

class TestGetSummary:
    @pytest.mark.asyncio
    async def test_cache_hit_skips_repository(self):
        """
        Проверяем: сводка уже лежит в кэше.
        Успех: возвращается распарсенная из кэша сводка, репозиторий НЕ вызывается.
        Нежелательное поведение: лишний тяжёлый запрос агрегата в БД при живом кэше.
        """
        cached = AnalyticsSummary(
            total_transactions=5,
            total_amount=Decimal("500.00"),
            currency="RUB",
            status_breakdown={"COMPLETED": 4, "FAILED": 1},
        )
        cache = AsyncMock()
        cache.get.return_value = cached.model_dump_json()
        service, repo, _ = make_service(cache=cache)

        result = await service.get_summary(currency="RUB")

        assert result.total_transactions == 5
        repo.get_summary.assert_not_called()

    @pytest.mark.asyncio
    async def test_cache_miss_computes_and_caches(self):
        """
        Проверяем: сводки в кэше нет.
        Успех: агрегат считается репозиторием, собирается AnalyticsSummary,
               результат кладётся в кэш (cache.set вызван).
        Нежелательное поведение: пропуск кэширования (каждый запрос бьёт в БД).
        """
        cache = AsyncMock()
        cache.get.return_value = None
        repo = AsyncMock()
        repo.get_summary.return_value = {
            "total_transactions": 3,
            "completed_count": 2,
            "failed_count": 1,
            "total_amount": Decimal("300.00"),
        }
        service, _, _ = make_service(repo=repo, cache=cache)

        result = await service.get_summary(currency="RUB")

        assert result.total_transactions == 3
        assert result.status_breakdown == {"COMPLETED": 2, "FAILED": 1}
        cache.set.assert_awaited_once()


# ---------------------------------------------------------------------------
# get_payments: пагинация
# ---------------------------------------------------------------------------

class TestGetPayments:
    @pytest.mark.asyncio
    async def test_pagination_page_computed(self):
        """
        Проверяем: постраничная выдача платежей.
        Успех: items соответствуют строкам из репозитория, total прокинут,
               page рассчитан как offset/limit + 1.
        Нежелательное поведение: неверный номер страницы или потеря элементов.
        """
        rows = [make_payment_row(), make_payment_row()]
        repo = AsyncMock()
        repo.get_payments.return_value = (rows, 12)
        service, _, _ = make_service(repo=repo)

        result = await service.get_payments(limit=10, offset=10)

        assert result.total == 12
        assert len(result.items) == 2
        assert result.page == 2
        assert result.size == 10


# ---------------------------------------------------------------------------
# get_payment_by_id
# ---------------------------------------------------------------------------

class TestGetPaymentById:
    @pytest.mark.asyncio
    async def test_found(self):
        """
        Проверяем: платеж существует в read-модели.
        Успех: возвращается PaymentPayload с тем же id.
        Нежелательное поведение: подмена данных или лишний поиск.
        """
        row = make_payment_row()
        repo = AsyncMock()
        repo.get.return_value = row
        service, _, _ = make_service(repo=repo)

        result = await service.get_payment_by_id(str(row.id))

        assert result.id == row.id

    @pytest.mark.asyncio
    async def test_missing_raises(self):
        """
        Проверяем: платеж не найден.
        Успех: поднимается PaymentNotFoundError.
        Нежелательное поведение: возврат None вместо явной ошибки 404.
        """
        repo = AsyncMock()
        repo.get.return_value = None
        service, _, _ = make_service(repo=repo)

        with pytest.raises(PaymentNotFoundError):
            await service.get_payment_by_id("absent")
