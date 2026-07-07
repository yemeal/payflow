import structlog
from fastapi import FastAPI, status
from starlette.requests import Request
from starlette.responses import JSONResponse

from app.domain.exceptions.payments import PaymentNotFoundError
from app.application.exceptions.idempotency import (
    IdempotencyKeyAlreadyProcessingError,
    IdempotencyKeyPayloadMismatchError,
)
from app.infrastructure.exceptions.redis import RedisUnavailableError
from app.infrastructure.exceptions.payment_providers import (
    ProviderUnavailableError,
    ProviderIntegrationError,
)
from app.core.settings import get_settings

logger = structlog.get_logger()


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(PaymentNotFoundError)
    async def _(
        request: Request,
        exc: PaymentNotFoundError,
    ) -> JSONResponse:
        logger.info(
            "payment_not_found",
            error=exc.message,
        )
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={"error": exc.message},
        )

    @app.exception_handler(IdempotencyKeyAlreadyProcessingError)
    async def _(
        request: Request,
        exc: IdempotencyKeyAlreadyProcessingError,
    ) -> JSONResponse:
        logger.warning(
            "idempotency_key_locked",
            error=exc.message,
        )
        settings = get_settings()
        return JSONResponse(
            status_code=status.HTTP_423_LOCKED,
            content={
                "error": exc.message,
                "retry_after": settings.IDEMPOTENCY_LOCK_TTL,
            },
            headers={"Retry-After": str(settings.IDEMPOTENCY_LOCK_TTL)},
        )

    @app.exception_handler(IdempotencyKeyPayloadMismatchError)
    async def _(
        request: Request,
        exc: IdempotencyKeyPayloadMismatchError,
    ) -> JSONResponse:
        logger.warning(
            "idempotency_payload_mismatch",
            error=exc.message,
        )
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={"error": exc.message},
        )

    @app.exception_handler(RedisUnavailableError)
    async def _(
        request: Request,
        exc: RedisUnavailableError,
    ) -> JSONResponse:
        logger.error(
            "service_unavailable",
            error=exc.message,
        )
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"error": "Service temporarily unavailable"},
        )

    @app.exception_handler(ProviderUnavailableError)
    async def _(
        request: Request,
        exc: ProviderUnavailableError,
    ) -> JSONResponse:
        logger.error(
            "provider_unavailable",
            error=exc.message,
            details=exc.details,
        )
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"error": "Payment provider is temporarily unavailable. Please try again later."},
        )

    @app.exception_handler(ProviderIntegrationError)
    async def _(
        request: Request,
        exc: ProviderIntegrationError,
    ) -> JSONResponse:
        logger.error(
            "provider_integration_error",
            error=exc.message,
            details=exc.details,
        )
        return JSONResponse(
            status_code=status.HTTP_502_BAD_GATEWAY,
            content={"error": "Payment provider integration error. We are working on it."},
        )
