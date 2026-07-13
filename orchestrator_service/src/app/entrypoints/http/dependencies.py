"""
Аутентификация Admin API: Bearer JWT, выданный auth_service (общий секрет HS256).

Роль берётся ТОЛЬКО из токена, никогда из тела/query запроса: иначе клиент
объявил бы себя админом сам. Секрет и алгоритм приходят из Settings.

Зависимости собираются фабриками (create_admin_dependency): HTTPBearer и
настройки живут в замыкании, глобалей уровня модуля в entrypoints нет.
"""

from typing import Annotated, Callable

import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel

from app.core.settings import Settings

ADMIN_ROLE = "ADMIN"


class AuthenticatedPrincipal(BaseModel):
    """Идентичность запроса, восстановленная из access-токена"""

    subject: str
    role: str

    @property
    def is_admin(self) -> bool:
        # auth_service отдаёт роль в верхнем регистре (UserRole), но сверяем
        # регистронезависимо: чужой сервис не должен ломаться от смены нотации
        return self.role.upper() == ADMIN_ROLE


def _unauthorized() -> HTTPException:
    # одна ошибка на все случаи (нет заголовка, битая подпись, истёк, нет claims):
    # не подсказываем клиенту, что именно не так с токеном
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="invalid or expired token",
        headers={"WWW-Authenticate": "Bearer"},
    )


def create_current_principal_dependency(
    settings: Settings,
) -> Callable[..., AuthenticatedPrincipal]:
    """Декодирует Bearer-токен -> AuthenticatedPrincipal (401 при любой проблеме)"""

    # auto_error=False: сами формируем 401, не полагаясь на код статуса,
    # который HTTPBearer выбирает для отсутствующего заголовка
    bearer_scheme = HTTPBearer(auto_error=False)

    def get_current_principal(
        credentials: Annotated[
            HTTPAuthorizationCredentials | None, Depends(bearer_scheme)
        ],
    ) -> AuthenticatedPrincipal:
        if credentials is None:
            raise _unauthorized()

        try:
            claims = jwt.decode(
                credentials.credentials,
                settings.JWT_SECRET,
                algorithms=[settings.JWT_ALGORITHM],
            )
        except jwt.PyJWTError:
            raise _unauthorized()

        subject = claims.get("sub")
        role = claims.get("role")
        if not subject or not role:
            # токен подписан нашим секретом, но без claims работать нельзя
            raise _unauthorized()

        return AuthenticatedPrincipal(subject=str(subject), role=str(role))

    return get_current_principal


def create_admin_dependency(
    settings: Settings,
) -> Callable[..., AuthenticatedPrincipal]:
    """Пропускает только роль admin: 401 - токен невалиден, 403 - роль не та"""

    current_principal = create_current_principal_dependency(settings)

    def require_admin(
        principal: Annotated[AuthenticatedPrincipal, Depends(current_principal)],
    ) -> AuthenticatedPrincipal:
        if not principal.is_admin:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="admin role required",
            )
        return principal

    return require_admin
