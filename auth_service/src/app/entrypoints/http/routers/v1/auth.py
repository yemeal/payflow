"""
Роутеры Auth Service - скелет.

Схемы запросов/ответов и DI-wiring готовы; каждый обработчик отвечает 501,
план реализации оставлен в TODO. Роутеры только принимают и отдают:
бизнес-логика живёт в application/services/auth_service.py.

Роутер собирается фабрикой (bearer-схема живёт в замыкании) -
глобалей уровня модуля нет.
"""

from typing import Annotated

from dishka import FromDishka
from dishka.integrations.fastapi import inject
from fastapi import APIRouter, Depends, Header, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.application.services.auth_service import AuthServiceProtocol
from app.application.services.idempotency import IdempotencyService
from app.entrypoints.http.schemas.auth import (
    LoginRequest,
    MeResponse,
    RefreshRequest,
    RegisterRequest,
    RegisterResponse,
    TokenPairResponse,
)


def _not_implemented() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="auth-service skeleton: endpoint is not implemented yet",
    )


def create_auth_router() -> APIRouter:
    router = APIRouter(prefix="/auth", tags=["auth"])
    bearer_scheme = HTTPBearer()

    @router.post("/register", status_code=status.HTTP_201_CREATED)
    @inject
    async def register(
        payload: RegisterRequest,
        auth_service: FromDishka[AuthServiceProtocol],
    ) -> RegisterResponse:
        # TODO(owner): user = await auth_service.register(payload.email, payload.password)
        #  return RegisterResponse(user_id=user.id)
        #  DomainErrors.User.EMAIL_ALREADY_EXISTS() -> 409
        raise _not_implemented()

    @router.post("/login")
    @inject
    async def login(
        payload: LoginRequest,
        auth_service: FromDishka[AuthServiceProtocol],
    ) -> TokenPairResponse:
        # TODO(owner): pair = await auth_service.login(payload.email, payload.password)
        #  DomainErrors.Auth.INVALID_CREDENTIALS() -> 401 (не 404)
        raise _not_implemented()

    @router.post(
        "/refresh",
        response_model=TokenPairResponse,
        status_code=status.HTTP_200_OK,
    )
    @inject
    async def refresh(
        payload: RefreshRequest,
        idempotency_key: Annotated[
            str,
            Header(
                alias="Idempotency-Key",
                min_length=8,
                max_length=128,
            ),
        ],
        auth_service: FromDishka[AuthServiceProtocol],
        idempotency_service: FromDishka[IdempotencyService],
    ) -> TokenPairResponse:
        """
        HTTP-граница делает одноразовую rotation безопасной для сетевых retry.

        AuthService ничего не знает про Idempotency-Key: guard либо возвращает
        готовую пару, либо разрешает ровно один вызов refresh use case.
        """
        idempotency_payload = payload.model_dump(
            mode="json",
            by_alias=True,
        )
        scoped_key = f"auth:refresh:{idempotency_key}"

        async with idempotency_service(
            scoped_key,
            idempotency_payload,
        ) as guard:
            if guard.has_cached_result:
                return TokenPairResponse.model_validate(
                    guard.cached_response
                )

            pair = await auth_service.refresh(payload.refresh_token)
            response = TokenPairResponse.model_validate(
                pair.model_dump(mode="python")
            )
            guard.set_result(
                status_code=status.HTTP_200_OK,
                response=response.model_dump(
                    mode="json",
                    by_alias=True,
                ),
            )
            return response

    @router.post("/logout", status_code=status.HTTP_204_NO_CONTENT)
    @inject
    async def logout(
        payload: RefreshRequest,
        auth_service: FromDishka[AuthServiceProtocol],
    ) -> None:
        # TODO(owner): await auth_service.logout(payload.refresh_token)
        raise _not_implemented()

    @router.get("/me")
    @inject
    async def me(
        credentials: Annotated[HTTPAuthorizationCredentials, Depends(bearer_scheme)],
        auth_service: FromDishka[AuthServiceProtocol],
    ) -> MeResponse:
        # TODO(owner): user = await auth_service.get_current_user(credentials.credentials)
        #  return MeResponse.model_validate(user)
        raise _not_implemented()

    return router
