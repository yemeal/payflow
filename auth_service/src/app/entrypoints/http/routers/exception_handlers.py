"""
Маппинг доменных исключений на публичный HTTP-контракт.

Handler не логирует и не принимает бизнес-решений. Для нескольких внутренних
причин он может вернуть клиенту одну безопасную публичную ошибку.
"""

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from types import MappingProxyType

from fastapi import FastAPI, Request, Response, status
from fastapi.responses import JSONResponse

from app.application.exceptions.idempotency import (
    IdempotencyKeyAlreadyProcessingError,
    IdempotencyKeyPayloadMismatchError,
    IdempotencyStorageUnavailableError,
)
from app.domain.exceptions import (
    InvalidCredentialsError,
    InvalidRefreshTokenError,
    InvalidTokenError,
    UserAlreadyExistsError,
    UserNotFoundError,
)
from app.entrypoints.http.schemas.errors import ApiErrorResponse, AuthApiErrorCode


@dataclass(frozen=True, slots=True)
class HttpErrorSpec:
    status_code: int
    code: AuthApiErrorCode
    detail: str
    headers: tuple[tuple[str, str], ...] = ()


_BEARER_HEADER = (("WWW-Authenticate", "Bearer"),)

_HTTP_ERROR_SPECS: Mapping[type[Exception], HttpErrorSpec] = MappingProxyType(
    {
        UserAlreadyExistsError: HttpErrorSpec(
            status_code=status.HTTP_409_CONFLICT,
            code=AuthApiErrorCode.EMAIL_ALREADY_EXISTS,
            detail="user with this email already exists",
        ),
        InvalidCredentialsError: HttpErrorSpec(
            status_code=status.HTTP_401_UNAUTHORIZED,
            code=AuthApiErrorCode.INVALID_CREDENTIALS,
            detail="invalid email or password",
        ),
        InvalidTokenError: HttpErrorSpec(
            status_code=status.HTTP_401_UNAUTHORIZED,
            code=AuthApiErrorCode.INVALID_TOKEN,
            detail="invalid or expired token",
            headers=_BEARER_HEADER,
        ),
        InvalidRefreshTokenError: HttpErrorSpec(
            status_code=status.HTTP_401_UNAUTHORIZED,
            code=AuthApiErrorCode.INVALID_REFRESH_TOKEN,
            detail="refresh token is invalid",
        ),
        IdempotencyKeyPayloadMismatchError: HttpErrorSpec(
            status_code=status.HTTP_409_CONFLICT,
            code=AuthApiErrorCode.IDEMPOTENCY_KEY_CONFLICT,
            detail="idempotency key was used with another request",
        ),
        IdempotencyKeyAlreadyProcessingError: HttpErrorSpec(
            status_code=status.HTTP_423_LOCKED,
            code=AuthApiErrorCode.IDEMPOTENCY_REQUEST_IN_PROGRESS,
            detail="request with this idempotency key is still processing",
        ),
        IdempotencyStorageUnavailableError: HttpErrorSpec(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            code=AuthApiErrorCode.IDEMPOTENCY_UNAVAILABLE,
            detail="request safety service is temporarily unavailable",
        ),
        UserNotFoundError: HttpErrorSpec(
            status_code=status.HTTP_404_NOT_FOUND,
            code=AuthApiErrorCode.USER_NOT_FOUND,
            detail="user not found",
        ),
    }
)


ExceptionHandler = Callable[[Request, Exception], Awaitable[Response]]


def _create_exception_handler(spec: HttpErrorSpec) -> ExceptionHandler:
    async def handler(_request: Request, _error: Exception) -> Response:
        response = ApiErrorResponse(
            code=spec.code,
            detail=spec.detail,
        )
        return JSONResponse(
            status_code=spec.status_code,
            content=response.model_dump(mode="json"),
            headers=dict(spec.headers),
        )

    return handler


def register_exception_handlers(
    app: FastAPI,
    *,
    idempotency_retry_after_seconds: int = 1,
) -> None:
    for error_type, spec in _HTTP_ERROR_SPECS.items():
        if error_type is IdempotencyKeyAlreadyProcessingError:
            spec = HttpErrorSpec(
                status_code=spec.status_code,
                code=spec.code,
                detail=spec.detail,
                headers=(
                    (
                        "Retry-After",
                        str(idempotency_retry_after_seconds),
                    ),
                ),
            )
        app.add_exception_handler(error_type, _create_exception_handler(spec))
