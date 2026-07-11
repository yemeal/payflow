"""
Тесты MockPaymentProviderAdapter - обвязка вызова внешнего провайдера.

Проверяем связку устойчивости (AGENTS.md, "Вызов провайдера"):
  - CircuitBreaker снаружи: при открытой цепи запрос вообще не уходит,
    техническая ошибка превращается в доменную ProviderUnavailableError;
  - Retry внутри: ретраим только сетевые сбои и 5xx, на 4xx не ретраим;
  - исчерпание ретраев / прочие httpx-ошибки -> ProviderIntegrationError.

HTTP не делаем: подменяем httpx-клиент адаптера и убираем задержки ретраев,
чтобы тесты были быстрыми.

Формат документации: Проверяем / Успех / Нежелательное поведение.
"""

import httpx
import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock

from tenacity import AsyncRetrying, stop_after_attempt, wait_none, retry_if_exception

from app.infrastructure.payment_providers.adapters import (
    MockPaymentProviderAdapter,
    _is_retriable_error,
)
from app.infrastructure.resilience.circuit_breaker import CircuitBreaker
from app.application.ports.dto import ProviderTransactionRequest
from app.infrastructure.exceptions.payment_providers import (
    ProviderUnavailableError,
    ProviderIntegrationError,
)


# ---------------------------------------------------------------------------
# Хелперы
# ---------------------------------------------------------------------------

def make_settings(max_retries=3):
    return SimpleNamespace(
        PAYMENT_PROVIDER_URL="http://provider.local",
        PAYMENT_PROVIDER_TIMEOUT=1.0,
        PAYMENT_PROVIDER_MAX_RETRIES=max_retries,
        PAYMENT_PROVIDER_MAX_RPS=1000,
    )


def make_adapter(circuit_breaker=None, max_retries=3):
    cb = circuit_breaker or CircuitBreaker(
        fail_max=5, recovery_timeout=0.1, name="provider"
    )
    adapter = MockPaymentProviderAdapter(make_settings(max_retries), cb)
    # убираем задержки между ретраями (по умолчанию exp backoff ждёт секунды)
    n = adapter._max_retries

    def fast_retrying():
        return AsyncRetrying(
            stop=stop_after_attempt(n),
            wait=wait_none(),
            retry=retry_if_exception(_is_retriable_error),
            reraise=True,
        )

    adapter._get_async_retrying = fast_retrying
    return adapter


def make_response(status_code, json_body):
    request = httpx.Request("POST", "http://provider.local/transactions/")
    return httpx.Response(status_code, json=json_body, request=request)


# ---------------------------------------------------------------------------
# Предикат ретраев
# ---------------------------------------------------------------------------

class TestRetriablePredicate:
    def test_network_error_is_retriable(self):
        """
        Проверяем: сетевую ошибку (RequestError) считаем восстановимой.
        Успех: предикат возвращает True.
        Нежелательное поведение: не ретраить временный сетевой сбой.
        """
        exc = httpx.ConnectError("boom", request=httpx.Request("POST", "http://x"))
        assert _is_retriable_error(exc) is True

    def test_5xx_is_retriable(self):
        """
        Проверяем: ответ 5xx считаем восстановимым.
        Успех: предикат возвращает True.
        Нежелательное поведение: сдаться после первой 500 без ретрая.
        """
        resp = make_response(503, {})
        exc = httpx.HTTPStatusError("err", request=resp.request, response=resp)
        assert _is_retriable_error(exc) is True

    def test_4xx_is_not_retriable(self):
        """
        Проверяем: ответ 4xx НЕ восстановим (ошибка запроса, ретрай бесполезен).
        Успех: предикат возвращает False.
        Нежелательное поведение: заваливать провайдер повторами на 400/404.
        """
        resp = make_response(404, {})
        exc = httpx.HTTPStatusError("err", request=resp.request, response=resp)
        assert _is_retriable_error(exc) is False


# ---------------------------------------------------------------------------
# initiate_transaction
# ---------------------------------------------------------------------------

class TestInitiateTransaction:
    @pytest.mark.asyncio
    async def test_success_returns_transaction_id(self):
        """
        Проверяем: провайдер принял платеж.
        Успех: возвращается модель с transaction_id из поля id ответа.
        Нежелательное поведение: потеря transaction_id или падение на валидном ответе.
        """
        adapter = make_adapter()
        adapter._http_client.post = AsyncMock(
            return_value=make_response(200, {"id": "tx-1", "status": "PENDING"})
        )

        result = await adapter.initiate_transaction(
            ProviderTransactionRequest(amount="100.00", currency="RUB")
        )

        assert result.transaction_id == "tx-1"

    @pytest.mark.asyncio
    async def test_retries_on_5xx_then_succeeds(self):
        """
        Проверяем: провайдер отдал 500, затем 200 (ретрай внутри CB).
        Успех: запрос повторён и в итоге успешен; post вызван дважды.
        Нежелательное поведение: сдаться после первой 500 или зациклиться.
        """
        adapter = make_adapter(max_retries=3)
        adapter._http_client.post = AsyncMock(
            side_effect=[
                make_response(500, {}),
                make_response(200, {"id": "tx-2", "status": "PENDING"}),
            ]
        )

        result = await adapter.initiate_transaction(
            ProviderTransactionRequest(amount="100.00", currency="RUB")
        )

        assert result.transaction_id == "tx-2"
        assert adapter._http_client.post.await_count == 2

    @pytest.mark.asyncio
    async def test_no_retry_on_4xx_and_maps_to_integration_error(self):
        """
        Проверяем: провайдер отдал 400 (невосстановимо).
        Успех: ретраев нет (post вызван один раз), ошибка маппится в ProviderIntegrationError.
        Нежелательное поведение: ретраи по 4xx или сырое httpx-исключение наружу.
        """
        adapter = make_adapter(max_retries=3)
        adapter._http_client.post = AsyncMock(return_value=make_response(400, {}))

        with pytest.raises(ProviderIntegrationError):
            await adapter.initiate_transaction(
                ProviderTransactionRequest(amount="100.00", currency="RUB")
            )
        assert adapter._http_client.post.await_count == 1

    @pytest.mark.asyncio
    async def test_exhausted_retries_map_to_integration_error(self):
        """
        Проверяем: все ретраи по 5xx исчерпаны.
        Успех: поднимается ProviderIntegrationError, post вызван max_retries раз.
        Нежелательное поведение: бесконечные ретраи или неверный тип ошибки.
        """
        adapter = make_adapter(max_retries=3)
        adapter._http_client.post = AsyncMock(return_value=make_response(500, {}))

        with pytest.raises(ProviderIntegrationError):
            await adapter.initiate_transaction(
                ProviderTransactionRequest(amount="100.00", currency="RUB")
            )
        assert adapter._http_client.post.await_count == 3

    @pytest.mark.asyncio
    async def test_open_circuit_maps_to_unavailable(self):
        """
        Проверяем: цепь уже открыта (CircuitBreaker OPEN).
        Успех: запрос к провайдеру не уходит, поднимается ProviderUnavailableError.
        Нежелательное поведение: пробитие открытой цепи и обращение к недоступному провайдеру,
                   либо утечка технической CircuitBreakerError наружу.
        """
        # fail_max=1: одна ошибка открывает цепь
        cb = CircuitBreaker(fail_max=1, recovery_timeout=60, name="provider")
        adapter = make_adapter(circuit_breaker=cb, max_retries=1)
        adapter._http_client.post = AsyncMock(return_value=make_response(500, {}))

        # первый вызов роняет цепь
        with pytest.raises(ProviderIntegrationError):
            await adapter.initiate_transaction(
                ProviderTransactionRequest(amount="100.00", currency="RUB")
            )

        # второй вызов уже отбивается открытой цепью
        with pytest.raises(ProviderUnavailableError):
            await adapter.initiate_transaction(
                ProviderTransactionRequest(amount="100.00", currency="RUB")
            )
        # второй запрос до провайдера не дошёл (post так и вызван один раз)
        assert adapter._http_client.post.await_count == 1


# ---------------------------------------------------------------------------
# get_transaction_status
# ---------------------------------------------------------------------------

class TestGetTransactionStatus:
    @pytest.mark.asyncio
    async def test_success_parses_discriminated_status(self):
        """
        Проверяем: разбор статуса транзакции по discriminator=status.
        Успех: ответ со status=PENDING валидируется в соответствующую модель.
        Нежелательное поведение: неверный маппинг варианта union или падение валидации.
        """
        adapter = make_adapter()
        adapter._http_client.get = AsyncMock(
            return_value=make_response(200, {"id": "tx-1", "status": "PENDING"})
        )

        result = await adapter.get_transaction_status("tx-1")

        assert result.status == "PENDING"
        assert result.transaction_id == "tx-1"
