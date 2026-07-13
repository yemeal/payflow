"""
Проверка Bearer JWT, выданных auth_service.

Ключевой инвариант безопасности: user_id и role берутся ТОЛЬКО из токена,
никогда из тела запроса - клиент не может действовать от имени другого
пользователя, даже если передаст чужой user_id в payload.

Зависимости собираются фабрикой (create_current_user_dependency):
bearer-схема и настройки живут в замыкании, глобалей уровня модуля нет.
"""

import uuid
from typing import Annotated, Callable

import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel

from app.core.settings import Settings

ADMIN_ROLE = "ADMIN"


class AuthenticatedUser(BaseModel):
    """Идентичность запроса, извлечённая из access-токена"""

    user_id: uuid.UUID
    role: str

    @property
    def is_admin(self) -> bool:
        return self.role == ADMIN_ROLE


def _unauthorized() -> HTTPException:
    # одна ошибка на все случаи (нет подписи, истёк, битые claims):
    # не подсказываем, что именно не так с токеном
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="invalid or expired token",
        headers={"WWW-Authenticate": "Bearer"},
    )


def create_current_user_dependency(
    settings: Settings,
) -> Callable[..., AuthenticatedUser]:
    bearer_scheme = HTTPBearer()

    def get_current_user(
        credentials: Annotated[HTTPAuthorizationCredentials, Depends(bearer_scheme)],
    ) -> AuthenticatedUser:
        try:
            claims = jwt.decode(
                credentials.credentials,
                settings.JWT_SECRET,
                algorithms=[settings.JWT_ALGORITHM],
            )
        except jwt.PyJWTError:
            raise _unauthorized()

        try:
            return AuthenticatedUser(
                user_id=uuid.UUID(str(claims["sub"])),
                role=str(claims.get("role", "USER")),
            )
        except (KeyError, ValueError):
            raise _unauthorized()

    return get_current_user
