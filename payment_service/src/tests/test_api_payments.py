"""
Тесты API эндпоинтов платежей (POST /api/v1/payments/, GET /api/v1/payments/{id})

Тестируем роутеры через отдельное FastAPI-приложение с подменёнными зависимостями Dishka.
Это позволяет полностью изолировать тесты от реальной инфраструктуры (БД, Redis, Kafka).
"""

import pytest
from decimal import Decimal
from uuid import uuid4
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest_asyncio
from dishka import make_async_container, Provider, Scope, provide
from dishka.integrations.fastapi import setup_dishka
from fastapi import FastAPI
from httpx import AsyncClient, ASGITransport

from app.domain.payments import Payment, PaymentStatus
from app.domain.exceptions.payments import PaymentNotFoundError
from app.application.services.idempotency import IdempotencyService
from app.application.services.payment_service import PaymentServiceProtocol
from app.entrypoints.http.routers import api_router
from app.entrypoints.http.routers.exception_handlers import register_exception_handlers


# ---------------------------------------------------------------------------
# Фабрика тестовых платежей
# ---------------------------------------------------------------------------

def _make_payment(**overrides) -> Payment:
    """Фабрика для создания тестового платежа с дефолтными значениями"""
    defaults = dict(
        id=uuid4(),
        idempotency_key="test-key-123",
        amount=Decimal("100.00"),
        currency="RUB",
        status=PaymentStatus.PROCESSING,
        external_id="ext-1",
        customer_id="customer-1",
        description="Test payment",
        created_at=datetime.now(timezone.utc),
        updated_at=None,
    )
    defaults.update(overrides)
    return Payment(**defaults)


# ---------------------------------------------------------------------------
# Мок IdempotencyService — контекстный менеджер
# ---------------------------------------------------------------------------

class MockIdempotencyGuard:
    """Мок guard, который возвращает IdempotencyService.__call__"""

    def __init__(self):
        self.has_cached_result = False
        self.cached_status_code = None
        self.cached_response = None
        self._result_set = False

    def set_result(self, status_code: int, response: dict):
        self._result_set = True


class MockIdempotencyService:
    """Мок IdempotencyService, реализующий протокол контекстного менеджера"""

    def __init__(self):
        self.guard = MockIdempotencyGuard()

    def __call__(self, key, payload, db_lookup):
        return self

    async def __aenter__(self):
        return self.guard

    async def __aexit__(self, *args):
        pass


# ---------------------------------------------------------------------------
# DI-провайдер с моками для тестов
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_payment_service():
    service = AsyncMock(spec=PaymentServiceProtocol)
    service.build_idempotency_db_lookup.return_value = AsyncMock(return_value=None)
    return service


@pytest.fixture
def mock_idempotency_service():
    return MockIdempotencyService()


class MockDIProvider(Provider):
    """Dishka-провайдер, подменяющий зависимости моками"""

    def __init__(self, payment_service, idempotency_service):
        super().__init__()
        self._payment_service = payment_service
        self._idempotency_service = idempotency_service

    @provide(scope=Scope.REQUEST)
    def provide_payment_service(self) -> PaymentServiceProtocol:
        return self._payment_service

    @provide(scope=Scope.REQUEST)
    def provide_idempotency_service(self) -> IdempotencyService:
        return self._idempotency_service


@pytest_asyncio.fixture
async def client(mock_payment_service, mock_idempotency_service):
    """Создаёт тестовое FastAPI-приложение с подменённым DI-контейнером"""
    app = FastAPI()
    app.include_router(api_router)
    register_exception_handlers(app)

    container = make_async_container(
        MockDIProvider(mock_payment_service, mock_idempotency_service)
    )
    setup_dishka(container, app)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac

    await container.close()


# ---------------------------------------------------------------------------
# Тесты: Создание платежа
# ---------------------------------------------------------------------------

class TestCreatePayment:
    """POST /api/v1/payments/"""

    @pytest.mark.asyncio
    async def test_create_payment_happy_path(
        self, client, mock_payment_service, mock_idempotency_service
    ):
        """Создание платежа (happy path) – 201, корректный ответ"""
        payment = _make_payment(status=PaymentStatus.PROCESSING)
        mock_payment_service.create.return_value = payment

        response = await client.post(
            "/api/v1/payments/",
            json={
                "amount": "100.00",
                "currency": "RUB",
                "customerId": "customer-1",
                "description": "Test payment",
            },
            headers={"Idempotency-Key": "test-key-123"},
        )

        assert response.status_code == 201
        data = response.json()
        assert data["id"] == str(payment.id)
        assert data["status"] == "PROCESSING"
        assert data["amount"] == "100.00"
        assert data["currency"] == "RUB"

    @pytest.mark.asyncio
    async def test_create_payment_validation_negative_amount(self, client):
        """Валидация: отрицательная сумма – 422"""
        response = await client.post(
            "/api/v1/payments/",
            json={
                "amount": "-50.00",
                "currency": "RUB",
            },
            headers={"Idempotency-Key": "key-negative"},
        )

        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_create_payment_validation_zero_amount(self, client):
        """Валидация: нулевая сумма – 422 (amount должен быть gt=0)"""
        response = await client.post(
            "/api/v1/payments/",
            json={
                "amount": "0.00",
                "currency": "RUB",
            },
            headers={"Idempotency-Key": "key-zero"},
        )

        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_create_payment_validation_invalid_currency(self, client):
        """Валидация: невалидная валюта (больше 3 символов) – 422"""
        response = await client.post(
            "/api/v1/payments/",
            json={
                "amount": "100.00",
                "currency": "invalid",
            },
            headers={"Idempotency-Key": "key-currency"},
        )

        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_create_payment_validation_currency_lowercase(self, client):
        """Валидация: валюта в нижнем регистре – 422 (паттерн ^[A-Z]{3}$)"""
        response = await client.post(
            "/api/v1/payments/",
            json={
                "amount": "100.00",
                "currency": "rub",
            },
            headers={"Idempotency-Key": "key-lowercase"},
        )

        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_create_payment_missing_idempotency_key(self, client):
        """Отсутствие заголовка Idempotency-Key – 422"""
        response = await client.post(
            "/api/v1/payments/",
            json={
                "amount": "100.00",
                "currency": "RUB",
            },
        )

        assert response.status_code == 422


# ---------------------------------------------------------------------------
# Тесты: Получение платежа
# ---------------------------------------------------------------------------

class TestGetPayment:
    """GET /api/v1/payments/{payment_id}"""

    @pytest.mark.asyncio
    async def test_get_payment_by_id(self, client, mock_payment_service):
        """Получение платежа по ID – 200"""
        payment = _make_payment()
        mock_payment_service.get.return_value = payment

        response = await client.get(f"/api/v1/payments/{payment.id}")

        assert response.status_code == 200
        data = response.json()
        assert data["id"] == str(payment.id)
        assert data["status"] == payment.status.value

    @pytest.mark.asyncio
    async def test_get_payment_not_found(self, client, mock_payment_service):
        """Получение несуществующего платежа – 404"""
        fake_id = uuid4()
        mock_payment_service.get.side_effect = PaymentNotFoundError(
            f"Платеж с id={fake_id} не существует"
        )

        response = await client.get(f"/api/v1/payments/{fake_id}")

        assert response.status_code == 404
        assert "error" in response.json()


# ---------------------------------------------------------------------------
# Тесты: Идемпотентность
# ---------------------------------------------------------------------------

class TestIdempotency:
    """Проверка идемпотентности при создании платежа"""

    @pytest.mark.asyncio
    async def test_idempotent_duplicate_returns_cached(
        self, client, mock_payment_service, mock_idempotency_service
    ):
        """Повторный запрос с тем же idempotency_key не создаёт дубликат"""
        payment = _make_payment()

        # Эмулируем: guard обнаружил кэшированный результат
        mock_idempotency_service.guard.has_cached_result = True
        mock_idempotency_service.guard.cached_status_code = 201
        mock_idempotency_service.guard.cached_response = {
            "id": str(payment.id),
            "status": "PROCESSING",
            "amount": "100.00",
            "currency": "RUB",
            "customerId": "customer-1",
            "description": "Test payment",
            "createdAt": payment.created_at.isoformat(),
            "updatedAt": None,
        }

        response = await client.post(
            "/api/v1/payments/",
            json={
                "amount": "100.00",
                "currency": "RUB",
                "customerId": "customer-1",
                "description": "Test payment",
            },
            headers={"Idempotency-Key": "duplicate-key"},
        )

        assert response.status_code == 201
        assert response.json()["id"] == str(payment.id)
        # create НЕ должен был вызваться — ответ из кэша
        mock_payment_service.create.assert_not_called()
