from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Protocol

import structlog
from pydantic import BaseModel

from app.application.ports.dto import AccessPrincipal
from app.application.ports.repositories import (
    AuthSessionRepositoryProtocol,
    RefreshTokenRepositoryProtocol,
    UserRepositoryProtocol,
)
from app.application.ports.security import (
    AccessTokenIssuerProtocol,
    AccessTokenVerifierProtocol,
    OpaqueRefreshTokenCodecProtocol,
    PasswordHasherProtocol,
)
from app.application.ports.uow import AsyncUOWProtocol
from app.core.settings import Settings
from app.domain.auth_sessions import AuthSession
from app.domain.base import utc_now
from app.domain.exceptions import (
    DomainError,
    DomainErrors,
    InvalidCredentialsError,
    InvalidRefreshTokenError,
)
from app.domain.refresh_tokens import RefreshToken
from app.domain.users import User
from app.domain.value_objects.email import NormalizedEmail

logger = structlog.get_logger()


class TokenPair(BaseModel):
    """Результат login/refresh: пара токенов"""

    access_token: str
    refresh_token: str
    token_type: str = "bearer"


class AuthServiceProtocol(Protocol):
    async def register(self, email: NormalizedEmail, password: str) -> User: ...

    async def login(self, email: NormalizedEmail, password: str) -> TokenPair: ...

    async def refresh(self, refresh_token: str) -> TokenPair: ...

    async def logout(self, refresh_token: str) -> None: ...

    async def get_current_user(self, access_token: str) -> User: ...


class AuthService:
    def __init__(
        self,
        user_repo: UserRepositoryProtocol,
        refresh_token_repo: RefreshTokenRepositoryProtocol,
        auth_session_repo: AuthSessionRepositoryProtocol,
        uow: AsyncUOWProtocol,
        password_hasher: PasswordHasherProtocol,
        access_token_issuer: AccessTokenIssuerProtocol,
        access_token_verifier: AccessTokenVerifierProtocol,
        refresh_token_codec: OpaqueRefreshTokenCodecProtocol,
        settings: Settings,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self._user_repo = user_repo
        self._refresh_token_repo = refresh_token_repo
        self._auth_session_repo = auth_session_repo
        self._uow = uow
        self._password_hasher = password_hasher
        self._access_token_issuer = access_token_issuer
        self._access_token_verifier = access_token_verifier
        self._refresh_token_codec = refresh_token_codec
        self._settings = settings
        self._clock = clock

    async def register(self, email: NormalizedEmail, password: str) -> User:
        """
        Флоу таков:
            - хешируем пароль в отдельном потоке, не блокируя event loop
            - создаем пользователя через INSERT ON CONFLICT (email) DO NOTHING
                - если репозиторий не вернул юзера -> такой юзер уже есть -> рейзим ошибку
                - если репозиторий вернул юзера -> юзер успешно создан -> передаем созданного юзера дальше
        """
        password_hash = await self._password_hasher.hash(password)
        user_to_create = User(email=email, password_hash=password_hash)

        async with self._uow:
            created_user = await self._user_repo.create_if_absent(user_to_create)
            if created_user is None:
                logger.info("user registration conflict", email=email)
                raise DomainErrors.User.EMAIL_ALREADY_EXISTS()

        logger.info("user successfully created", user_id=str(created_user.id))
        return created_user

    async def login(self, email: NormalizedEmail, password: str) -> TokenPair:
        login_log = logger.bind(operation="login")
        login_log.debug("login started")

        stage = "user lookup"
        try:
            # Read-транзакция завершается до Argon2. Иначе запросы, ожидающие
            # semaphore хешера, удерживали бы соединения из DB-пула.
            async with self._uow:
                candidate_user = await self._user_repo.get_by_email(email)

            password_hash = (
                candidate_user.password_hash
                if candidate_user is not None and candidate_user.can_authenticate
                else None
            )

            stage = "password verification"
            is_password_correct = await self._password_hasher.verify_or_dummy(
                password,
                password_hash,
            )
            if (
                candidate_user is None
                or password_hash is None
                or not is_password_correct
            ):
                reason = (
                    "user unavailable"
                    if password_hash is None
                    else "incorrect password"
                )
                login_log.warning(
                    "login rejected",
                    stage=stage,
                    reason=reason,
                )
                raise DomainErrors.Auth.INVALID_CREDENTIALS()

            login_log = login_log.bind(user_id=str(candidate_user.id))

            # Между lookup и окончанием Argon2 user мог быть отключен, удален
            # или получить новый password hash. Повторная проверка под lock
            # задает короткую и однозначную границу успешного login.
            async with self._uow:
                stage = "user revalidation"
                locked_user = await self._user_repo.get_for_update(candidate_user.id)
                if (
                    locked_user is None
                    or not locked_user.can_authenticate
                    or locked_user.password_hash != password_hash
                ):
                    login_log.warning(
                        "login rejected",
                        stage=stage,
                        reason="user state changed",
                    )
                    raise DomainErrors.Auth.INVALID_CREDENTIALS()

                login_log.debug("login user locked")
                now = self._clock()
                access_principal = AccessPrincipal(
                    user_id=locked_user.id,
                    role=locked_user.role,
                )

                stage = "access token issuance"
                new_access_token = self._access_token_issuer.issue(
                    access_principal,
                    now=now,
                )
                login_log.debug("access token issued")

                stage = "refresh token issuance"
                new_refresh_token = self._refresh_token_codec.issue()
                login_log.debug("new refresh token issued")

                stage = "auth session storage"
                idle_expires_at = now + timedelta(
                    seconds=self._settings.AUTH_SESSION_IDLE_TTL_SECONDS
                )
                created_auth_session = await self._auth_session_repo.create(
                    AuthSession(
                        user_id=locked_user.id,
                        idle_expires_at=idle_expires_at,
                    )
                )
                login_log = login_log.bind(auth_session_id=str(created_auth_session.id))
                login_log.debug(
                    "auth session stored",
                    idle_expires_at=created_auth_session.idle_expires_at,
                )

                stage = "refresh token storage"
                refresh_token_to_create = RefreshToken(
                    session_id=created_auth_session.id,
                    token_hash=new_refresh_token.digest,
                )
                created_refresh_token = await self._refresh_token_repo.create(
                    refresh_token_to_create
                )
                login_log.debug(
                    "new refresh token stored",
                    new_refresh_token_id=str(created_refresh_token.id),
                )

                token_pair = TokenPair(
                    access_token=new_access_token,
                    refresh_token=new_refresh_token.value,
                )

            login_log.debug("login transaction committed")
            login_log.info("login succeeded")
            return token_pair
        except InvalidCredentialsError:
            raise
        except Exception:
            login_log.exception("login failed", stage=stage)
            raise

    async def refresh(self, refresh_token: str) -> TokenPair:
        """
        Ротируем одноразовый refresh-токен внутри одной транзакции.

        Сырой токен нужен только клиенту. Здесь мы вычисляем его digest, а в БД
        ищем и сохраняем исключительно digest. Ни токен, ни digest в логи не попадают.

        Важно: этот метод сам ничего не знает про идемпотентность, потому что это
        ответственность entrypoint-слоя. Но вызывать его напрямую нельзя:
        каждый refresh-запрос должен быть обёрнут в idempotency guard.

        Без такой обёртки обычный повтор запроса после потерянного ответа будет
        выглядеть как reuse уже использованного refresh-токена. В результате мы
        примем безопасный повтор клиента за кражу токена и отзовём всю token family.

        Контракт entrypoint-слоя:
            - тот же refresh-токен + тот же idempotency key -> вернуть ту же TokenPair,
              не вызывая этот метод повторно;
            - тот же refresh-токен + другой key -> обычный reuse и отзыв связанной
              token family;
            - повтор после окончания idempotency window -> обычный reuse;
            - конкурентные запросы с одним key -> этот метод вызывается ровно один
              раз, остальные запросы получают сохранённый результат или ожидают его.

        Таким образом, защита от reuse остаётся внутри refresh-флоу, а различать
        повтор entrypoint-запроса и настоящее повторное использование обязан entrypoint.
        """
        refresh_log = logger.bind(operation="refresh")
        refresh_log.debug("refresh started")

        deferred_error: InvalidRefreshTokenError | None = None
        token_pair: TokenPair | None = None

        try:
            old_digest = self._refresh_token_codec.digest(refresh_token)
            refresh_log.debug("refresh digest calculated")

            async with self._uow:
                refresh_log.debug("refresh token lock started")
                old_refresh_token = (
                    await self._refresh_token_repo.get_by_hash_for_update(old_digest)
                )
                if old_refresh_token is None:
                    refresh_log.warning(
                        "refresh rejected",
                        stage="refresh token lookup",
                        reason="token not found",
                    )
                    raise DomainErrors.Token.INVALID_REFRESH()

                refresh_log = refresh_log.bind(
                    refresh_token_id=str(old_refresh_token.id),
                    auth_session_id=str(old_refresh_token.session_id),
                )
                refresh_log.debug(
                    "refresh token locked",
                    is_used=old_refresh_token.is_used,
                )

                # сессию блокируем той же транзакцией: так параллельные refresh/logout
                # не смогут одновременно менять одну token family.
                refresh_log.debug("auth session lock started")
                auth_session = await self._auth_session_repo.get_for_update(
                    old_refresh_token.session_id
                )
                if auth_session is None:
                    refresh_log.warning(
                        "refresh rejected",
                        stage="auth session lookup",
                        reason="session not found",
                    )
                    raise DomainErrors.Token.INVALID_REFRESH()

                now = self._clock()
                refresh_log = refresh_log.bind(user_id=str(auth_session.user_id))
                refresh_log.debug(
                    "auth session locked",
                    is_active=auth_session.is_active(now),
                )

                if old_refresh_token.is_used:
                    # reuse означает возможную кражу токена. отзываем только
                    # связанную с ним family, а не все сессии пользователя.
                    refresh_log.warning("refresh token reuse detected")
                    auth_session.revoke(now)
                    await self._auth_session_repo.update(auth_session)
                    refresh_log.warning(
                        "auth session revoked",
                        reason="refresh token reuse",
                        revoked_at=auth_session.revoked_at,
                    )
                    # исключение поднимем после выхода из UOW. Иначе 401 откатит
                    # отзыв сессии вместе со всей транзакцией.
                    deferred_error = DomainErrors.Session.REFRESH_TOKEN_REUSED()
                elif not auth_session.is_active(now):
                    reason = (
                        "revoked"
                        if auth_session.revoked_at is not None
                        else "idle expired"
                    )
                    refresh_log.info(
                        "refresh rejected",
                        stage="auth session validation",
                        reason=reason,
                        idle_expires_at=auth_session.idle_expires_at,
                    )
                    raise DomainErrors.Session.INACTIVE()
                else:
                    refresh_log.debug("auth session validated")
                    refresh_log.debug("refresh user lookup started")
                    user = await self._user_repo.get(auth_session.user_id)

                    if user is None or not user.can_authenticate:
                        reason = (
                            "user not found"
                            if user is None
                            else "user cannot authenticate"
                        )
                        refresh_log.warning(
                            "refresh rejected",
                            stage="user validation",
                            reason=reason,
                        )

                        # такая сессия больше не должна продолжаться. Снаружи
                        # возвращаем единый INVALID_REFRESH без утечки состояния user.
                        auth_session.revoke(now)
                        await self._auth_session_repo.update(auth_session)
                        refresh_log.info(
                            "auth session revoked",
                            reason=reason,
                            revoked_at=auth_session.revoked_at,
                        )
                        deferred_error = DomainErrors.Token.INVALID_REFRESH()
                    else:
                        refresh_log.debug("refresh user validated")
                        access_principal = AccessPrincipal(
                            user_id=user.id,
                            role=user.role,
                        )
                        new_access_token = self._access_token_issuer.issue(
                            access_principal,
                            now=now,
                        )
                        refresh_log.debug("access token issued")

                        # новый секрет создаём только после всех проверок. Поэтому
                        # из двух конкурентных запросов issue() вызовет лишь победитель.
                        new_refresh_token = self._refresh_token_codec.issue()
                        refresh_log.debug("new refresh token issued")

                        old_refresh_token.consume(now=now)
                        await self._refresh_token_repo.update(old_refresh_token)
                        refresh_log.debug(
                            "old refresh token consumed",
                            used_at=old_refresh_token.used_at,
                        )

                        auth_session.extend_idle(
                            now=now,
                            idle_ttl=timedelta(
                                seconds=self._settings.AUTH_SESSION_IDLE_TTL_SECONDS
                            ),
                        )
                        await self._auth_session_repo.update(auth_session)
                        refresh_log.debug(
                            "auth session idle extended",
                            idle_expires_at=auth_session.idle_expires_at,
                        )

                        refresh_token_to_create = RefreshToken(
                            session_id=auth_session.id,
                            token_hash=new_refresh_token.digest,
                        )
                        await self._refresh_token_repo.create(refresh_token_to_create)
                        refresh_log.debug(
                            "new refresh token stored",
                            new_refresh_token_id=str(refresh_token_to_create.id),
                        )

                        token_pair = TokenPair(
                            access_token=new_access_token,
                            refresh_token=new_refresh_token.value,
                        )

            refresh_log.debug("refresh transaction committed")

            if deferred_error is not None:
                refresh_log.warning(
                    "refresh finished with error",
                    error_type=type(deferred_error).__name__,
                )
                raise deferred_error

            if token_pair is None:
                refresh_log.error("refresh finished without result")
                raise DomainErrors.Token.INVALID_REFRESH()

            refresh_log.info("refresh succeeded")
            return token_pair
        except DomainError:
            raise
        except Exception:
            refresh_log.exception("refresh failed")
            raise

    async def logout(self, refresh_token: str) -> None:
        # TODO: отозвать связанную AuthSession. Access-токен сервер "забыть"
        #  не может - он живет до своего exp и удаляется клиентом
        raise NotImplementedError("auth skeleton: logout")

    async def get_current_user(self, access_token: str) -> User:
        # TODO: claims = self._access_token_verifier.verify(access_token);
        #  self._users.get(claims.user_id); нет пользователя -> NOT_FOUND
        raise NotImplementedError("auth skeleton: get_current_user")
