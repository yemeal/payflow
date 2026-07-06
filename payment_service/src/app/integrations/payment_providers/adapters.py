import httpx
import structlog
from tenacity import (
    AsyncRetrying,
    stop_after_attempt,
    wait_exponential_jitter,
    retry_if_exception,
)
from aiolimiter import AsyncLimiter

from app.core.exceptions import ProviderUnavailableError, ProviderIntegrationError
from app.core.settings import Settings
from app.integrations.payment_providers.domain.transactions import (
    ProviderTransactionRequest,
    ProviderTransactionInitiated,
    ProviderTransactionStatus,
)
from app.utils.circuit_breaker import CircuitBreaker, CircuitBreakerError

logger = structlog.get_logger()


def _is_retriable_error(exception: BaseException) -> bool:
    """
    функция-предикат, которая обеспечивает,
    что мы ретраим только сетевые сбои и 5xx ошибки
    """

    if isinstance(exception, httpx.RequestError):
        return True
    if isinstance(exception, httpx.HTTPStatusError):
        return exception.response.status_code >= 500
    return False


class MockPaymentProviderAdapter:
    def __init__(
        self,
        settings: Settings,
        circuit_breaker: CircuitBreaker,
    ) -> None:
        self._base_url = settings.PAYMENT_PROVIDER_URL.rstrip("/")
        self._circuit_breaker = circuit_breaker
        # создаем один на весь жизненный цикл
        self._http_client = httpx.AsyncClient(timeout=settings.PAYMENT_PROVIDER_TIMEOUT)
        self._max_retries = settings.PAYMENT_PROVIDER_MAX_RETRIES
        # Ограничиваем RPS (исходящие запросы к провайдеру)
        # Async Limiter работает как Token Bucket.
        # раз в секунду корзина полностью пополняется (1 токен раз в time_period / max_rate)
        self._limiter = AsyncLimiter(
            max_rate=settings.PAYMENT_PROVIDER_MAX_RPS, time_period=1
        )

    async def close(self) -> None:
        """
        Освобождаем пул соединений
        """

        await self._http_client.aclose()
        logger.info("payment provider client closed")

    def _get_async_retrying(self) -> AsyncRetrying:
        # настраиваем политику ретраев
        return AsyncRetrying(
            stop=stop_after_attempt(self._max_retries),
            wait=wait_exponential_jitter(initial=1, max=5, exp_base=2, jitter=1),
            retry=retry_if_exception(_is_retriable_error),
            reraise=True,
        )

    async def _do_initiate_transaction_request(
        self, request: ProviderTransactionRequest
    ) -> ProviderTransactionInitiated:
        url = f"{self._base_url}/transactions/"

        async for attempt in self._get_async_retrying():
            with attempt:
                logger.info(
                    "provider request attempt",
                    attempt_number=attempt.retry_state.attempt_number,
                    amount=request.amount,
                    currency=request.currency,
                    url=url,
                    method="POST",
                )
                payload = request.model_dump(mode="json")
                # httpx через аргумент json сам кодирует объект в json и отправляет как пейлоад
                # каждый ретрай запроса тоже будет потреблять лимит RPS
                async with self._limiter:
                    response = await self._http_client.post(
                        url,
                        json=payload,
                    )
                response.raise_for_status()
        return ProviderTransactionInitiated(**response.json())

    async def _do_get_transaction_status_request(
        self, transaction_id: str
    ) -> ProviderTransactionStatus:
        url = f"{self._base_url}/transactions/{transaction_id}"

        async for attempt in self._get_async_retrying():
            with attempt:
                logger.info(
                    "provider request attempt",
                    attempt_number=attempt.retry_state.attempt_number,
                    transaction_id=transaction_id,
                    method="GET",
                )
                # каждый ретрай запроса тоже будет потреблять лимит RPS
                async with self._limiter:
                    response = await self._http_client.get(url)
                response.raise_for_status()
                
        from pydantic import TypeAdapter
        adapter = TypeAdapter(ProviderTransactionStatus)
        return adapter.validate_python(response.json())

    async def initiate_transaction(
        self,
        request: ProviderTransactionRequest,
    ) -> ProviderTransactionInitiated:
        """
        Основной метод инициации платежа, мы отправляем запрос провайдеру и ждем от него ответ.

        CircuitBreaker находится СНАРУЖИ, Retry находится ВНУТРИ
        """
        try:
            return await self._circuit_breaker.call(
                self._do_initiate_transaction_request,
                request=request,
            )
        except CircuitBreakerError as e:
            logger.error("provider_circuit_breaker_open", error=str(e))
            # превращаем техническую ошибку CircuitBreaker в доменную
            raise ProviderUnavailableError(
                message="Провайдер платежей временно недоступен", details=str(e)
            ) from e
        except httpx.HTTPError as e:
            # исчерпаны попытки ретраев
            logger.error("provider_request_failed", error=str(e))
            raise ProviderIntegrationError(
                message="Ошибка при обращении к внешнему провайдеру", details=str(e)
            ) from e

    async def get_transaction_status(
        self, transaction_id: str
    ) -> ProviderTransactionStatus:
        try:
            return await self._circuit_breaker.call(
                self._do_get_transaction_status_request,
                transaction_id=transaction_id,
            )
        except CircuitBreakerError as e:
            logger.error("provider_circuit_breaker_open", error=str(e))
            raise ProviderUnavailableError(
                message="Провайдер платежей временно недоступен", details=str(e)
            ) from e
        except httpx.HTTPError as e:
            # исчерпаны попытки ретраев
            logger.error("provider_request_failed", error=str(e))
            raise ProviderIntegrationError(
                message="Ошибка при обращении к внешнему провайдеру", details=str(e)
            ) from e
