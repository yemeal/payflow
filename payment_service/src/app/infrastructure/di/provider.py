from typing import AsyncGenerator

import structlog
from redis.asyncio import Redis
from dishka import Provider, Scope, provide, AsyncContainer
from aiokafka import AIOKafkaProducer
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    async_sessionmaker,
    AsyncSession,
    create_async_engine,
)

from app.core.settings import Settings, get_settings
from app.application.ports.payment_provider import PaymentProviderProtocol
from app.infrastructure.payment_providers.adapters import MockPaymentProviderAdapter
from app.domain.payments import Payment
from app.domain.outbox import OutboxEvent
from app.application.ports.repositories import (
    OutboxRepositoryProtocol,
    PaymentRepositoryProtocol,
)
from app.infrastructure.database.repositories.outbox_repository import OutboxRepository
from app.infrastructure.database.repositories.payment_repository import (
    PaymentRepository,
)
from app.application.services.idempotency import (
    IdempotencyService,
    IdempotencyStorageProtocol,
    RedisIdempotencyStorage,
)
from app.application.services.payment_service import (
    PaymentService,
    PaymentServiceProtocol,
)
from app.application.services.outbox_relay import OutboxRelayService
from app.application.ports.outbox_publisher import OutboxPublisherProtocol, OutboxScopeFactory
from app.infrastructure.brokers.adapters import KafkaOutboxPublisher
from app.infrastructure.di.outbox_scope import DishkaOutboxScopeFactory
from app.infrastructure.resilience.circuit_breaker import CircuitBreaker
from app.application.ports.uow import AsyncUOWProtocol
from app.infrastructure.database.uow import SQLAlchemyAsyncUOW

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
        return PaymentRepository(session)

    @provide(scope=Scope.REQUEST)
    def provide_outbox_repository(
        self, session: AsyncSession
    ) -> OutboxRepositoryProtocol:
        return OutboxRepository(session)

    @provide(scope=Scope.REQUEST)
    def provide_payment_service(
        self,
        repo: PaymentRepositoryProtocol,
        uow: AsyncUOWProtocol,
        payment_provider: PaymentProviderProtocol,
        outbox_repo: OutboxRepositoryProtocol,
    ) -> PaymentServiceProtocol:
        return PaymentService(repo, uow, payment_provider, outbox_repo)

    @provide(scope=Scope.APP)
    def provide_idempotency_storage(self, redis: Redis) -> IdempotencyStorageProtocol:
        return RedisIdempotencyStorage(redis)

    @provide(scope=Scope.REQUEST)
    def provide_idempotency_service(
        self, storage: IdempotencyStorageProtocol, settings: Settings
    ) -> IdempotencyService:
        return IdempotencyService(storage, settings)


class IntegrationsProvider(Provider):
    @provide(scope=Scope.APP)
    def provide_circuit_breaker(self, settings: Settings) -> CircuitBreaker:
        from app.infrastructure.payment_providers.adapters import _is_retriable_error

        return CircuitBreaker(
            fail_max=settings.CIRCUIT_BREAKER_MAX_ATTEMPTS,
            recovery_timeout=settings.CIRCUIT_BREAKER_RECOVERY_TIMEOUT,
            name="payment-provider-mock",
            is_failure=_is_retriable_error,
        )

    @provide(scope=Scope.APP)
    async def provide_payment_provider(
        self, settings: Settings, cb: CircuitBreaker
    ) -> AsyncGenerator[PaymentProviderProtocol, None]:
        client = MockPaymentProviderAdapter(settings, circuit_breaker=cb)
        try:
            yield client
        finally:
            await client.close()


class KafkaProvider(Provider):
    @provide(scope=Scope.APP)
    async def provide_kafka_producer(
        self, settings: Settings
    ) -> AsyncGenerator[AIOKafkaProducer, None]:
        producer = AIOKafkaProducer(bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS)
        await producer.start()
        logger.info(
            "kafka_producer_started", bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS
        )
        try:
            yield producer
        finally:
            await producer.stop()
            logger.info("kafka_producer_stopped")

    @provide(scope=Scope.APP)
    def provide_outbox_publisher(
        self, producer: AIOKafkaProducer, settings: Settings
    ) -> OutboxPublisherProtocol:
        return KafkaOutboxPublisher(producer, topic=settings.KAFKA_EVENTS_TOPIC)

    @provide(scope=Scope.APP)
    def provide_outbox_scope_factory(
        self, container: AsyncContainer
    ) -> OutboxScopeFactory:
        return DishkaOutboxScopeFactory(container)

    @provide(scope=Scope.APP)
    def provide_outbox_relay(
        self,
        publisher: OutboxPublisherProtocol,
        scope_factory: OutboxScopeFactory,
        settings: Settings,
    ) -> OutboxRelayService:
        return OutboxRelayService(
            publisher,
            scope_factory,
            max_publish_attempts=settings.OUTBOX_MAX_PUBLISH_ATTEMPTS,
        )
