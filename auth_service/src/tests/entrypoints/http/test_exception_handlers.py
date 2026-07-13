import json

import pytest
from fastapi import FastAPI, Request, status

from app.application.exceptions.idempotency import (
    IdempotencyKeyAlreadyProcessingError,
    IdempotencyKeyPayloadMismatchError,
    IdempotencyStorageUnavailableError,
)
from app.domain.exceptions import DomainError, DomainErrors
from app.entrypoints.http.routers.exception_handlers import register_exception_handlers


class _UnmappedDomainError(DomainError):
    default_message = "Unmapped domain error"


def _request() -> Request:
    return Request(
        {
            "type": "http",
            "http_version": "1.1",
            "method": "GET",
            "scheme": "http",
            "path": "/test",
            "raw_path": b"/test",
            "query_string": b"",
            "headers": [],
            "client": ("test-client", 50000),
            "server": ("test-server", 80),
        }
    )


def _find_handler(app: FastAPI, error: Exception):
    for error_type in type(error).__mro__:
        if handler := app.exception_handlers.get(error_type):
            return handler
    raise AssertionError(f"No exception handler registered for {type(error).__name__}")


class TestDomainExceptionHandlers:
    @pytest.mark.parametrize(
        (
            "error",
            "expected_status",
            "expected_code",
            "expected_detail",
            "requires_bearer",
        ),
        [
            pytest.param(
                DomainErrors.User.EMAIL_ALREADY_EXISTS(),
                status.HTTP_409_CONFLICT,
                "auth.email_already_exists",
                "user with this email already exists",
                False,
                id="email-conflict",
            ),
            pytest.param(
                DomainErrors.Auth.INVALID_CREDENTIALS(),
                status.HTTP_401_UNAUTHORIZED,
                "auth.invalid_credentials",
                "invalid email or password",
                False,
                id="invalid-credentials",
            ),
            pytest.param(
                DomainErrors.Token.EXPIRED(),
                status.HTTP_401_UNAUTHORIZED,
                "auth.invalid_token",
                "invalid or expired token",
                True,
                id="expired-token",
            ),
            pytest.param(
                DomainErrors.Session.REFRESH_TOKEN_REUSED(),
                status.HTTP_401_UNAUTHORIZED,
                "auth.invalid_refresh_token",
                "refresh token is invalid",
                False,
                id="refresh-token-reuse",
            ),
            pytest.param(
                DomainErrors.Session.INACTIVE(),
                status.HTTP_401_UNAUTHORIZED,
                "auth.invalid_refresh_token",
                "refresh token is invalid",
                False,
                id="inactive-session",
            ),
            pytest.param(
                DomainErrors.User.NOT_FOUND(),
                status.HTTP_404_NOT_FOUND,
                "auth.user_not_found",
                "user not found",
                False,
                id="user-not-found",
            ),
            pytest.param(
                IdempotencyKeyPayloadMismatchError(),
                status.HTTP_409_CONFLICT,
                "auth.idempotency_key_conflict",
                "idempotency key was used with another request",
                False,
                id="idempotency-payload-conflict",
            ),
            pytest.param(
                IdempotencyKeyAlreadyProcessingError(),
                status.HTTP_423_LOCKED,
                "auth.idempotency_request_in_progress",
                "request with this idempotency key is still processing",
                False,
                id="idempotency-in-progress",
            ),
            pytest.param(
                IdempotencyStorageUnavailableError(),
                status.HTTP_503_SERVICE_UNAVAILABLE,
                "auth.idempotency_unavailable",
                "request safety service is temporarily unavailable",
                False,
                id="idempotency-storage-unavailable",
            ),
        ],
    )
    async def test_maps_domain_error_without_leaking_internal_data(
        self,
        error: DomainError,
        expected_status: int,
        expected_code: str,
        expected_detail: str,
        requires_bearer: bool,
    ) -> None:
        """
        Проверяем: преобразование доменной ошибки на HTTP-границе.
        Успех: клиент получает стабильный публичный код и безопасное сообщение.
        Нежелательное поведение: наружу уходит внутренний код или context.
        """
        app = FastAPI()
        register_exception_handlers(app)

        handler = _find_handler(app, error)
        response = await handler(_request(), error)

        assert response.status_code == expected_status
        assert json.loads(response.body) == {
            "code": expected_code,
            "detail": expected_detail,
        }
        assert ("WWW-Authenticate" in response.headers) is requires_bearer
        assert b"domain_error_context" not in response.body

    async def test_processing_error_exposes_retry_after(self) -> None:
        app = FastAPI()
        register_exception_handlers(
            app,
            idempotency_retry_after_seconds=17,
        )
        error = IdempotencyKeyAlreadyProcessingError()

        handler = _find_handler(app, error)
        response = await handler(_request(), error)

        assert response.headers["Retry-After"] == "17"

    def test_does_not_register_fallback_for_unknown_domain_error(self) -> None:
        """
        Проверяем: доменную ошибку без явного HTTP-маппинга.
        Успех: она не маскируется под известную клиентскую ошибку.
        Нежелательное поведение: новая ошибка случайно превращается в неверный 4xx.
        """
        app = FastAPI()
        register_exception_handlers(app)

        with pytest.raises(AssertionError, match="No exception handler"):
            _find_handler(app, _UnmappedDomainError())
