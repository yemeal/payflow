"""
Тесты HTTP API платежей (POST /api/v1/payments/, GET /api/v1/payments/{id}).

Роутеры тестируем через отдельное FastAPI-приложение с подменёнными зависимостями
Dishka - полная изоляция от инфраструктуры (БД, Redis, Kafka). Проверяем валидацию
входа, коды ответов и поведение Two-Level Idempotency на уровне API (кэш-хит,
конфликт payload -> 409, параллельная обработка -> 423).

Формат документации: Проверяем / Успех / Нежелательное поведение.
"""

import pytest
from decimal import Decimal
from uuid import uuid4
from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest_asyncio
from dishka import make_async_container, Provider, Scope, provide
from dishka.integrations.fastapi import setup_dishka
from fastapi import FastAPI
from httpx import AsyncClient, ASGITransport

from app.domain.payments import Payment, PaymentStatus
from app.domain.exceptions.payments import PaymentNotFoundError
from app.application.exceptions.idempotency import (
    IdempotencyKeyPayloadMismatchError,
    IdempotencyKeyAlreadyProcessingError,
)
from app.application.services.idempotency import IdempotencyService
from app.application.services.payment_service import PaymentServiceProtocol
from app.entrypoints.http.routers import api_router
from app.entrypoints.http.routers.exception_handlers import register_exception_handlers


# ---------------------------------------------------------------------------
# Фабрика тестовых платежей
# ---------------------------------------------------------------------------

def _make_payment(**overrides) -> Payment:
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
# Мок IdempotencyService (контекстный менеджер)
# ---------------------------------------------------------------------------

class MockIdempotencyGuard:
    """Guard-заглушка: по умолчанию кэша нет, результат просто запоминается."""

    def __init__(self):
        self.has_cached_result = False
        self.cached_status_code = None
        self.cached_response = None
        self._result_set = False

    def set_result(self, status_code: int, response: dict):
        self._result_set = True


class MockIdempotencyService:
    """
    Заглушка IdempotencyService - контекстный менеджер.
    raise_on_enter имитирует конфликт payload (409) или параллельную обработку (423).
    """

    def __init__(self, raise_on_enter=None):
        self.guard = MockIdempotencyGuard()
        self._raise_on_enter = raise_on_enter

    def __call__(self, key, payload, db_lookup):
        return self

    async def __aenter__(self):
        if self._raise_on_enter is not None:
            raise self._raise_on_enter
        return self.guard

    async def __aexit__(self, *args):
        pass


# ---------------------------------------------------------------------------
# DI-провайдер с моками
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


def build_client(payment_service, idempotency_service) -> AsyncClient:
    """Собирает тестовое приложение с подменённым DI-контейнером."""
    app = FastAPI()
    app.include_router(api_router)
    register_exception_handlers(app)

    container = make_async_container(
        MockDIProvider(payment_service, idempotency_service)
    )
    setup_dishka(container, app)

    transport = ASGITransport(app=app)
    client = AsyncClient(transport=transport, base_url="http://test")
    client._container = container  # держим ссылку, чтобы закрыть в фикстуре
    return client


@pytest_asyncio.fixture
async def client(mock_payment_service, mock_idempotency_service):
    ac = build_client(mock_payment_service, mock_idempotency_service)
    async with ac:
        yield ac
    await ac._container.close()


# ---------------------------------------------------------------------------
# Создание платежа: happy path
# ---------------------------------------------------------------------------

class TestCreatePayment:
    @pytest.mark.asyncio
    async def test_happy_path(self, client, mock_payment_service):
        """
        Проверяем: корректный POST с заголовком Idempotency-Key.
        Успех: код 201, тело содержит id/статус/сумму/валюту из созданного платежа.
        Нежелательное поведение: неверный код или искажение полей ответа.
        """
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


# ---------------------------------------------------------------------------
# Создание платежа: валидация входа
# ---------------------------------------------------------------------------

class TestCreateValidation:
    """Некорректный вход должен отсекаться на границе (422), не доходя до сервиса."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "payload, header, case",
        [
            ({"amount": "-50.00", "currency": "RUB"}, {"Idempotency-Key": "k1"}, "negative amount"),
            ({"amount": "0.00", "currency": "RUB"}, {"Idempotency-Key": "k2"}, "zero amount"),
            ({"amount": "100.00", "currency": "invalid"}, {"Idempotency-Key": "k3"}, "currency too long"),
            ({"amount": "100.00", "currency": "rub"}, {"Idempotency-Key": "k4"}, "currency lowercase"),
            ({"amount": "100.00", "currency": "RUB"}, {}, "no idempotency header"),
        ],
    )
    async def test_invalid_requests_return_422(self, client, payload, header, case):
        """
        Проверяем: разные варианты некорректного запроса (см. параметр case).
        Успех: код 422 (ошибка валидации до бизнес-логики).
        Нежелательное поведение: пропуск невалидных данных в сервис или БД.
        """
        response = await client.post("/api/v1/payments/", json=payload, headers=header)
        assert response.status_code == 422


# ---------------------------------------------------------------------------
# Получение платежа
# ---------------------------------------------------------------------------

class TestGetPayment:
    @pytest.mark.asyncio
    async def test_get_by_id(self, client, mock_payment_service):
        """
        Проверяем: получение существующего платежа по id.
        Успех: код 200, id и статус в ответе совпадают с платежом.
        Нежелательное поведение: подмена данных или неверный код.
        """
        payment = _make_payment()
        mock_payment_service.get.return_value = payment

        response = await client.get(f"/api/v1/payments/{payment.id}")

        assert response.status_code == 200
        data = response.json()
        assert data["id"] == str(payment.id)
        assert data["status"] == payment.status.value

    @pytest.mark.asyncio
    async def test_get_not_found(self, client, mock_payment_service):
        """
        Проверяем: запрос несуществующего платежа.
        Успех: код 404, тело содержит поле error.
        Нежелательное поведение: 200 с пустым телом или 500.
        """
        fake_id = uuid4()
        mock_payment_service.get.side_effect = PaymentNotFoundError(
            f"Платеж с id={fake_id} не существует"
        )

        response = await client.get(f"/api/v1/payments/{fake_id}")

        assert response.status_code == 404
        assert "error" in response.json()


# ---------------------------------------------------------------------------
# Идемпотентность на уровне API
# ---------------------------------------------------------------------------

class TestIdempotency:
    @pytest.mark.asyncio
    async def test_duplicate_returns_cached(self, client, mock_payment_service, mock_idempotency_service):
        """
        Проверяем: повторный запрос с тем же ключом и тем же payload (кэш-хит).
        Успех: код 201, тело из кэша, create() НЕ вызывается (дубль не создаётся).
        Нежелательное поведение: повторное создание платежа при retry клиента.
        """
        payment = _make_payment()
        guard = mock_idempotency_service.guard
        guard.has_cached_result = True
        guard.cached_status_code = 201
        guard.cached_response = {"id": str(payment.id), "status": "PROCESSING"}

        response = await client.post(
            "/api/v1/payments/",
            json={"amount": "100.00", "currency": "RUB"},
            headers={"Idempotency-Key": "duplicate-key"},
        )

        assert response.status_code == 201
        assert response.json()["id"] == str(payment.id)
        mock_payment_service.create.assert_not_called()

    @pytest.mark.asyncio
    async def test_payload_mismatch_returns_409(self, mock_payment_service):
        """
        Проверяем: тот же ключ идемпотентности пришёл с другим payload.
        Успех: код 409 (конфликт), create() не вызывается.
        Нежелательное поведение: обработка запроса под чужим ключом
                   (нарушение привязки ключ-payload).
        """
        idem = MockIdempotencyService(raise_on_enter=IdempotencyKeyPayloadMismatchError())
        ac = build_client(mock_payment_service, idem)
        async with ac:
            response = await ac.post(
                "/api/v1/payments/",
                json={"amount": "100.00", "currency": "RUB"},
                headers={"Idempotency-Key": "reused-key"},
            )
        await ac._container.close()

        assert response.status_code == 409
        mock_payment_service.create.assert_not_called()

    @pytest.mark.asyncio
    async def test_already_processing_returns_423(self, mock_payment_service):
        """
        Проверяем: запрос с ключом, который прямо сейчас обрабатывается (стоит лок).
        Успех: код 423 Locked (клиенту предлагается повторить позже), create() не вызывается.
        Нежелательное поведение: параллельная двойная обработка одного ключа.
        """
        idem = MockIdempotencyService(raise_on_enter=IdempotencyKeyAlreadyProcessingError())
        ac = build_client(mock_payment_service, idem)
        async with ac:
            response = await ac.post(
                "/api/v1/payments/",
                json={"amount": "100.00", "currency": "RUB"},
                headers={"Idempotency-Key": "in-flight-key"},
            )
        await ac._container.close()

        assert response.status_code == 423
        mock_payment_service.create.assert_not_called()
