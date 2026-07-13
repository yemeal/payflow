"""
Тесты Admin API: авторизация по JWT (роль admin), выдача саг и истории переходов.
Postgres и Kafka заменены фейками портов из conftest, приложение поднимается
целиком через фабрику create_http_app поверх ASGITransport.
"""

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncIterator

import httpx
import jwt
import pytest
from dishka import Provider, Scope, make_async_container, provide
from fastapi import FastAPI

from app.application.ports.repositories import (
    SagaRepositoryProtocol,
    SagaTransitionRepositoryProtocol,
)
from app.core.settings import Settings
from app.domain.saga import Saga, SagaStatus, SagaTransition, utc_now
from app.entrypoints.http.main import create_http_app
from tests.conftest import FakeSagaRepository, FakeSagaTransitionRepository

SAGA_TYPE = "order_fulfillment"


class FakeRepositoriesProvider(Provider):
    """Отдаёт роутерам те же экземпляры фейков, что видит тест"""

    def __init__(
        self,
        settings: Settings,
        sagas: FakeSagaRepository,
        transitions: FakeSagaTransitionRepository,
    ) -> None:
        super().__init__()
        self._settings = settings
        self._sagas = sagas
        self._transitions = transitions

    @provide(scope=Scope.APP)
    def get_settings(self) -> Settings:
        return self._settings

    @provide(scope=Scope.REQUEST)
    def get_sagas(self) -> SagaRepositoryProtocol:
        return self._sagas

    @provide(scope=Scope.REQUEST)
    def get_transitions(self) -> SagaTransitionRepositoryProtocol:
        return self._transitions


def make_token(
    settings: Settings,
    role: str = "ADMIN",
    expires_in_seconds: int = 300,
    secret: str | None = None,
) -> str:
    claims = {
        "sub": str(uuid.uuid7()),
        "role": role,
        "exp": datetime.now(tz=timezone.utc) + timedelta(seconds=expires_in_seconds),
    }
    return jwt.encode(
        claims,
        secret if secret is not None else settings.JWT_SECRET,
        algorithm=settings.JWT_ALGORITHM,
    )


def auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def make_saga(
    business_key: str,
    status: SagaStatus = SagaStatus.RUNNING,
    current_step: str | None = "reserve_inventory",
    updated_at: datetime | None = None,
) -> Saga:
    return Saga(
        saga_type=SAGA_TYPE,
        business_key=business_key,
        status=status,
        current_step=current_step,
        payload={"orderId": business_key, "totalAmount": "21.00"},
        updated_at=updated_at or utc_now(),
    )


@pytest.fixture
def app(
    settings: Settings,
    saga_repo: FakeSagaRepository,
    transitions_repo: FakeSagaTransitionRepository,
) -> FastAPI:
    container = make_async_container(
        FakeRepositoriesProvider(settings, saga_repo, transitions_repo)
    )
    return create_http_app(settings, container)


@pytest.fixture
async def client(app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://orchestrator"
    ) as http_client:
        yield http_client


@pytest.fixture
def admin_token(settings: Settings) -> str:
    return make_token(settings, role="ADMIN")


# --- авторизация ---


async def test_sagas_without_token_rejected(client: httpx.AsyncClient) -> None:
    """
    Проверяем: запрос к Admin API без Bearer-токена.
    Успех: 401, тело саг не отдаётся.
    Нежелательное поведение: 200 или 403 - анонимный доступ к состоянию саг.
    """
    response = await client.get("/admin/v1/sagas")

    assert response.status_code == 401


async def test_token_with_foreign_secret_rejected(
    client: httpx.AsyncClient, settings: Settings
) -> None:
    """
    Проверяем: токен подписан чужим секретом (подделка или чужой стенд).
    Успех: 401, подпись не проходит проверку.
    Нежелательное поведение: 200 - оркестратор верит любой подписи.
    """
    forged = make_token(settings, role="ADMIN", secret="not-our-secret")

    response = await client.get("/admin/v1/sagas", headers=auth(forged))

    assert response.status_code == 401


async def test_expired_token_rejected(
    client: httpx.AsyncClient, settings: Settings
) -> None:
    """
    Проверяем: валидно подписанный, но просроченный access-токен.
    Успех: 401 по истёкшему exp.
    Нежелательное поведение: 200 - протухший токен живёт вечно.
    """
    expired = make_token(settings, role="ADMIN", expires_in_seconds=-60)

    response = await client.get("/admin/v1/sagas", headers=auth(expired))

    assert response.status_code == 401


async def test_user_role_forbidden(
    client: httpx.AsyncClient, settings: Settings
) -> None:
    """
    Проверяем: валидный токен обычного пользователя (role=USER).
    Успех: 403 - роль admin обязательна, 401 не подходит (токен-то валиден).
    Нежелательное поведение: 200 - любой залогиненный видит внутренности саг.
    """
    user_token = make_token(settings, role="USER")

    response = await client.get("/admin/v1/sagas", headers=auth(user_token))

    assert response.status_code == 403


# --- список саг ---


async def test_admin_lists_sagas_in_camel_case(
    client: httpx.AsyncClient,
    saga_repo: FakeSagaRepository,
    admin_token: str,
) -> None:
    """
    Проверяем: роль admin получает список саг с camelCase-сериализацией.
    Успех: 200, поля sagaType/businessKey/currentStep, значения из репозитория.
    Нежелательное поведение: snake_case в ответе или падение сериализации.
    """
    saga = make_saga("order-1")
    saga_repo.by_id[saga.id] = saga

    response = await client.get("/admin/v1/sagas", headers=auth(admin_token))

    assert response.status_code == 200
    body: list[dict[str, Any]] = response.json()
    assert len(body) == 1
    item = body[0]
    assert item["id"] == str(saga.id)
    assert item["sagaType"] == SAGA_TYPE
    assert item["businessKey"] == "order-1"
    assert item["status"] == "RUNNING"
    assert item["currentStep"] == "reserve_inventory"
    assert item["retryCount"] == 0
    # payload в списке не отдаём: он живёт только в карточке саги
    assert "payload" not in item


async def test_status_filter_narrows_result(
    client: httpx.AsyncClient,
    saga_repo: FakeSagaRepository,
    admin_token: str,
) -> None:
    """
    Проверяем: фильтр ?status=FAILED прокидывается в репозиторий.
    Успех: 200 и только сага в статусе FAILED.
    Нежелательное поведение: фильтр игнорируется и оператор видит все саги.
    """
    running = make_saga("order-running", status=SagaStatus.RUNNING)
    failed = make_saga("order-failed", status=SagaStatus.FAILED, current_step=None)
    saga_repo.by_id[running.id] = running
    saga_repo.by_id[failed.id] = failed

    response = await client.get(
        "/admin/v1/sagas",
        params={"status": "FAILED"},
        headers=auth(admin_token),
    )

    assert response.status_code == 200
    body = response.json()
    assert [item["businessKey"] for item in body] == ["order-failed"]
    assert body[0]["currentStep"] is None


async def test_unknown_status_value_rejected(
    client: httpx.AsyncClient, admin_token: str
) -> None:
    """
    Проверяем: фильтр по статусу, которого нет в SagaStatus.
    Успех: 422 - валидация запроса, а не тихий пустой список.
    Нежелательное поведение: 200 с пустым списком (оператор решит, что саг нет).
    """
    response = await client.get(
        "/admin/v1/sagas",
        params={"status": "ALMOST_DONE"},
        headers=auth(admin_token),
    )

    assert response.status_code == 422


# --- карточка саги ---


async def test_saga_detail_returns_payload_and_transitions(
    client: httpx.AsyncClient,
    saga_repo: FakeSagaRepository,
    transitions_repo: FakeSagaTransitionRepository,
    admin_token: str,
) -> None:
    """
    Проверяем: карточка саги отдаёт состояние, payload и историю переходов.
    Успех: 200, переходы только этой саги, поля в camelCase.
    Нежелательное поведение: чужие переходы в истории или потеря payload.
    """
    saga = make_saga("order-42")
    other = make_saga("order-99")
    saga_repo.by_id[saga.id] = saga
    saga_repo.by_id[other.id] = other

    await transitions_repo.add(
        SagaTransition(
            saga_id=saga.id,
            from_status=None,
            to_status=SagaStatus.RUNNING.value,
            to_step="reserve_inventory",
            event_type="order.created",
        )
    )
    await transitions_repo.add(
        SagaTransition(
            saga_id=other.id,
            to_status=SagaStatus.RUNNING.value,
            to_step="reserve_inventory",
        )
    )

    response = await client.get(
        f"/admin/v1/sagas/{saga.id}", headers=auth(admin_token)
    )

    assert response.status_code == 200
    body = response.json()
    assert body["saga"]["businessKey"] == "order-42"
    assert body["payload"] == {"orderId": "order-42", "totalAmount": "21.00"}
    assert len(body["transitions"]) == 1
    transition = body["transitions"][0]
    assert transition["sagaId"] == str(saga.id)
    assert transition["toStatus"] == "RUNNING"
    assert transition["toStep"] == "reserve_inventory"
    assert transition["eventType"] == "order.created"


async def test_unknown_saga_returns_404(
    client: httpx.AsyncClient, admin_token: str
) -> None:
    """
    Проверяем: запрос карточки несуществующей саги.
    Успех: 404 с понятным detail.
    Нежелательное поведение: 200 с пустой карточкой или 500.
    """
    response = await client.get(
        f"/admin/v1/sagas/{uuid.uuid7()}", headers=auth(admin_token)
    )

    assert response.status_code == 404
    assert response.json()["detail"] == "saga not found"


# --- застрявшие саги ---


async def test_stuck_returns_only_stale_non_terminal(
    client: httpx.AsyncClient,
    saga_repo: FakeSagaRepository,
    admin_token: str,
) -> None:
    """
    Проверяем: /sagas/stuck отбирает нетерминальные саги старше порога.
    Успех: 200, свежая и завершённая саги отсеяны, застрявшая - в ответе.
    Нежелательное поведение: путь "stuck" матчится как saga_id (422) или выдача
    завершённых саг, из-за которой оператор гоняется за призраками.
    """
    long_ago = utc_now() - timedelta(seconds=3600)
    stuck = make_saga("order-stuck", updated_at=long_ago)
    fresh = make_saga("order-fresh")
    done = make_saga("order-done", status=SagaStatus.COMPLETED, updated_at=long_ago)
    for saga in (stuck, fresh, done):
        saga_repo.by_id[saga.id] = saga

    response = await client.get(
        "/admin/v1/sagas/stuck",
        params={"older_than_seconds": 300},
        headers=auth(admin_token),
    )

    assert response.status_code == 200
    assert [item["businessKey"] for item in response.json()] == ["order-stuck"]


# --- health и метрики ---


async def test_health_live_is_public(client: httpx.AsyncClient) -> None:
    """
    Проверяем: liveness-проба не требует токена и не ходит во внешние системы.
    Успех: 200 {"status": "ok"} без Authorization.
    Нежелательное поведение: 401 - kubelet не сможет проверить процесс.
    """
    response = await client.get("/health/live")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


async def test_metrics_endpoint_exposes_saga_families(
    client: httpx.AsyncClient,
) -> None:
    """
    Проверяем: /metrics отдаёт экспозицию prometheus с метриками саг.
    Успех: 200 и семейства sagas_started_total / outbox_pending в теле.
    Нежелательное поведение: 404 (метрики не смонтированы) или пустой реестр.
    """
    response = await client.get("/metrics")

    assert response.status_code == 200
    assert "sagas_started_total" in response.text
    assert "outbox_pending" in response.text
