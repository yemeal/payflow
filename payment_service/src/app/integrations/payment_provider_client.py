from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Protocol

import httpx
import structlog
from tenacity import (
    stop_after_attempt,
    wait_exponential_jitter,
    retry_if_exception,
    AsyncRetrying,
)

from app.core.exceptions.payment_provider import (
    ProviderUnavailableError,
    ProviderIntegrationError,
)
from app.core.settings import Settings
from app.utils.circuit_breaker import CircuitBreaker, CircuitBreakerError

logger = structlog.get_logger()


@dataclass
class ProviderPaymentRequest:
    amount: Decimal
    currency: str
    customer_id: str


class PaymentProviderProtocol(Protocol):
    """Интерфейс для работы с внешним провайдером платежей."""

    async def process_payment(
        self, request: ProviderPaymentRequest
    ) -> dict[str, Any]: ...


def _is_retriable_error(exception: BaseException) -> bool:
    """ретраим только сетевые сбои и 5xx ошибки"""

    if isinstance(exception, httpx.RequestError):
        return True
    if isinstance(exception, httpx.HTTPStatusError):
        return exception.response.status_code >= 500
    return False


class PaymentProviderClient:
    def __init__(self, settings: Settings, circuit_breaker: CircuitBreaker) -> None:
        self._base_url = settings.PAYMENT_PROVIDER_URL.rstrip("/")
        self._circuit_breaker = circuit_breaker
        # создаем один раз на весь жизненный цикл
        self._http_client = httpx.AsyncClient(timeout=2.0)

    async def close(self) -> None:
        """Освобождение пула соединений при выключении приложения."""

        await self._http_client.aclose()
        logger.info("payment_provider_client_closed")

    async def process_payment(self, request: ProviderPaymentRequest) -> dict[str, Any]:
        """
        Основной метод отправки платежа.
        Circuit Breaker находится СНАРУЖИ, Retry находится ВНУТРИ.
        """

        async def _do_request() -> dict[str, Any]:
            # настраиваем Retry-политику
            retrying = AsyncRetrying(
                stop=stop_after_attempt(3),
                wait=wait_exponential_jitter(initial=1, max=5, exp_base=2, jitter=1),
                retry=retry_if_exception(_is_retriable_error),
                reraise=True,
            )

            async for attempt in retrying:
                with attempt:
                    logger.info(
                        "provider_request_attempt",
                        attempt_number=attempt.retry_state.attempt_number,
                        amount=request.amount,
                        currency=request.currency,
                        customer_id=request.customer_id,
                    )

                    response = await self._http_client.post(
                        f"{self._base_url}/process-payment",
                        json={
                            "amount": str(request.amount),
                            "currency": request.currency,
                            "customer_id": request.customer_id,
                            "card_number": "1234567812345678",
                        },
                    )
                    response.raise_for_status()
                    return response.json()

        try:
            # оборачиваем внутреннюю логику (которая уже умеет ретраить) в предохранитель
            return await self._circuit_breaker.call(_do_request)

        except CircuitBreakerError as e:
            # Превращаем техническую ошибку CircuitBreaker в доменную
            logger.error("provider_circuit_breaker_open", error=str(e))
            raise ProviderUnavailableError(
                message="Провайдер платежей временно недоступен", details=str(e)
            ) from e

        except httpx.HTTPError as e:
            # Если исчерпаны все попытки ретраев
            logger.error("provider_request_failed", error=str(e))
            raise ProviderIntegrationError(
                message="Ошибка при обращении к внешнему провайдеру", details=str(e)
            ) from e
