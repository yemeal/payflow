from typing import AsyncGenerator

import structlog
from redis.asyncio import Redis
from dishka import Provider, Scope, provide
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    async_sessionmaker,
    AsyncSession,
    create_async_engine,
)

from app.core.settings import Settings, get_settings
from app.models import Payment
from app.repositories.payment_repository import (
    PaymentRepository,
    PaymentRepositoryProtocol,
)
from app.services.idempotency import IdempotencyService
from app.services.payment_service import PaymentService, PaymentServiceProtocol
from app.utils.unit_of_work import SQLAlchemyAsyncUOW, AsyncUOWProtocol

logger = structlog.get_logger()


class SettingsProvider(Provider):
    @provide(scope=Scope.APP)
    def provide_settings(self) -> Settings:
        return get_settings()


class DatabaseProvider(Provider):
    @provide(scope=Scope.APP)
    def provide_async_engine(self, settings: Settings) -> AsyncEngine:
        return create_async_engine(
            url=settings.DATABASE_URL,
            pool_size=20,  # постоянно открытые соединения с БД
            max_overflow=30,  # если основные соединения заняты, движок разрешит создать до 30 дополнительных (в сумме 50)
            pool_pre_ping=True,  # перед тем, как отдать соединение мы делаем healthcheck
            pool_recycle=3600,  # раз в час обновляет (закрывает и снова открывает) старые соединения
            connect_args={
                "command_timeout": 60,  # вопрос: корректно ли использовать такой таймаут в проде, дабы не морозить всю аппку?
            },
        )

    @provide(scope=Scope.APP)
    def provide_async_sessionmaker(
        self, engine: AsyncEngine
    ) -> async_sessionmaker[AsyncSession]:
        return async_sessionmaker(
            bind=engine,
            expire_on_commit=False,
        )

    @provide(scope=Scope.REQUEST)
    async def provide_session(
        self, sessionmaker: async_sessionmaker[AsyncSession]
    ) -> AsyncGenerator[AsyncSession, None]:
        async with sessionmaker() as session:
            yield session


class RedisProvider(Provider):
    @provide(scope=Scope.APP)
    async def provide_redis(self, settings: Settings) -> AsyncGenerator[Redis, None]:
        redis_client = None
        try:
            redis_client = Redis.from_url(settings.REDIS_URL, decode_responses=True)
            logger.info("redis_client_created", url=settings.REDIS_URL)
            yield redis_client
        finally:
            if redis_client:
                await redis_client.aclose()
                logger.info(f"redis_client_closed")


class ServiceProvider(Provider):
    @provide(scope=Scope.REQUEST)
    async def provide_uow(self, session: AsyncSession) -> AsyncUOWProtocol:
        return SQLAlchemyAsyncUOW(session)

    @provide(scope=Scope.REQUEST)
    def provide_payment_repository(
        self, session: AsyncSession
    ) -> PaymentRepositoryProtocol:
        return PaymentRepository(session, Payment)

    @provide(scope=Scope.REQUEST)
    def provide_payment_service(
        self,
        repo: PaymentRepositoryProtocol,
        uow: AsyncUOWProtocol,
    ) -> PaymentServiceProtocol:
        return PaymentService(repo, uow)

    @provide(scope=Scope.REQUEST)
    def provide_idempotency_service(
        self, redis: Redis, settings: Settings
    ) -> IdempotencyService:
        return IdempotencyService(redis, settings)
