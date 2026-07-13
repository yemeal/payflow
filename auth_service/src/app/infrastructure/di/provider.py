from datetime import timedelta
from typing import AsyncIterator

from dishka import Provider, Scope, provide
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

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
from app.application.services.auth_service import AuthService, AuthServiceProtocol
from app.application.services.idempotency import (
    IdempotencyService,
    IdempotencyStorageProtocol,
)
from app.core.settings import Settings
from app.infrastructure.database.repositories.auth_session_repository import (
    AuthSessionRepository,
)
from app.infrastructure.database.repositories.refresh_token_repository import (
    RefreshTokenRepository,
)
from app.infrastructure.database.repositories.user_repository import UserRepository
from app.infrastructure.database.uow import SQLAlchemyAsyncUOW
from app.infrastructure.idempotency import RedisIdempotencyStorage
from app.infrastructure.security.access_token_issuer import PyJWTAccessTokenIssuer
from app.infrastructure.security.access_token_verifier import (
    PyJWTAccessTokenVerifier,
)
from app.infrastructure.security.opaque_refresh_token_codec import (
    SHA256OpaqueRefreshTokenCodec,
)
from app.infrastructure.security.password_hasher import Argon2PasswordHasher
from app.infrastructure.security.rsa_keys import RSAKeyPair, load_rsa_key_pair


class SettingsProvider(Provider):
    """Settings передаются фабрикой контейнера - без обращения к глобалям"""

    def __init__(self, settings: Settings) -> None:
        super().__init__()
        self._settings = settings

    @provide(scope=Scope.APP)
    def get_settings(self) -> Settings:
        return self._settings


class DatabaseProvider(Provider):
    @provide(scope=Scope.APP)
    def get_engine(self, settings: Settings) -> AsyncEngine:
        return create_async_engine(settings.DATABASE_URL)

    @provide(scope=Scope.APP)
    def get_sessionmaker(
        self, engine: AsyncEngine
    ) -> async_sessionmaker[AsyncSession]:
        return async_sessionmaker(engine, expire_on_commit=False)

    @provide(scope=Scope.REQUEST)
    async def get_session(
        self, sessionmaker: async_sessionmaker[AsyncSession]
    ) -> AsyncIterator[AsyncSession]:
        async with sessionmaker() as session:
            yield session


class RedisProvider(Provider):
    @provide(scope=Scope.APP)
    async def get_redis(
        self,
        settings: Settings,
    ) -> AsyncIterator[Redis]:
        redis_client = Redis.from_url(
            settings.REDIS_URL,
            decode_responses=True,
        )
        try:
            yield redis_client
        finally:
            await redis_client.aclose()

    @provide(scope=Scope.APP)
    def get_idempotency_storage(
        self,
        redis_client: Redis,
    ) -> IdempotencyStorageProtocol:
        return RedisIdempotencyStorage(redis_client)

    @provide(scope=Scope.REQUEST)
    def get_idempotency_service(
        self,
        storage: IdempotencyStorageProtocol,
        settings: Settings,
    ) -> IdempotencyService:
        return IdempotencyService(storage, settings)


class SecurityProvider(Provider):
    """Криптографические адаптеры: без состояния, живут весь срок приложения"""

    @provide(scope=Scope.APP)
    def get_password_hasher(self) -> PasswordHasherProtocol:
        return Argon2PasswordHasher()

    @provide(scope=Scope.APP)
    def get_access_token_key_pair(
        self,
        settings: Settings,
    ) -> RSAKeyPair:
        return load_rsa_key_pair(
            private_key_path=settings.JWT_PRIVATE_KEY_PATH,
            public_key_path=settings.JWT_PUBLIC_KEY_PATH,
        )

    @provide(scope=Scope.APP)
    def get_access_token_issuer(
        self,
        settings: Settings,
        key_pair: RSAKeyPair,
    ) -> AccessTokenIssuerProtocol:
        return PyJWTAccessTokenIssuer(
            private_key=key_pair.private_key,
            key_id=settings.JWT_ACTIVE_KEY_ID,
            issuer=settings.JWT_ISSUER,
            audiences=settings.JWT_AUDIENCES,
            access_token_ttl=timedelta(
                seconds=settings.ACCESS_TOKEN_TTL_SECONDS
            ),
        )

    @provide(scope=Scope.APP)
    def get_access_token_verifier(
        self,
        settings: Settings,
        key_pair: RSAKeyPair,
    ) -> AccessTokenVerifierProtocol:
        return PyJWTAccessTokenVerifier(
            public_keys={
                settings.JWT_ACTIVE_KEY_ID: key_pair.public_key,
            },
            issuer=settings.JWT_ISSUER,
            audience=settings.JWT_SERVICE_AUDIENCE,
            leeway=timedelta(seconds=settings.JWT_CLOCK_SKEW_SECONDS),
        )

    @provide(scope=Scope.APP)
    def get_refresh_token_codec(self) -> OpaqueRefreshTokenCodecProtocol:
        return SHA256OpaqueRefreshTokenCodec()


class ServiceProvider(Provider):
    scope = Scope.REQUEST

    @provide
    def get_uow(self, session: AsyncSession) -> AsyncUOWProtocol:
        return SQLAlchemyAsyncUOW(session)

    @provide
    def get_user_repository(self, session: AsyncSession) -> UserRepositoryProtocol:
        return UserRepository(session)

    @provide
    def get_refresh_token_repository(
        self, session: AsyncSession
    ) -> RefreshTokenRepositoryProtocol:
        return RefreshTokenRepository(session)

    @provide
    def get_auth_session_repository(
        self, session: AsyncSession
    ) -> AuthSessionRepositoryProtocol:
        return AuthSessionRepository(session)

    @provide
    def get_auth_service(
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
    ) -> AuthServiceProtocol:
        return AuthService(
            user_repo=user_repo,
            refresh_token_repo=refresh_token_repo,
            auth_session_repo=auth_session_repo,
            uow=uow,
            password_hasher=password_hasher,
            access_token_issuer=access_token_issuer,
            access_token_verifier=access_token_verifier,
            refresh_token_codec=refresh_token_codec,
            settings=settings,
        )
