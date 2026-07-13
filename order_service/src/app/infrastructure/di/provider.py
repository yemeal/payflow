from typing import AsyncIterator

from aiokafka import AIOKafkaProducer
from dishka import AsyncContainer, Provider, Scope, provide
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.application.ports.cache import OrderCacheProtocol
from app.application.ports.outbox_publisher import (
    OutboxPublisherProtocol,
    OutboxScopeFactory,
)
from app.application.ports.repositories import (
    OrderRepositoryProtocol,
    OutboxRepositoryProtocol,
    ProcessedEventRepositoryProtocol,
)
from app.application.ports.uow import AsyncUOWProtocol
from app.application.services.order_service import OrderService, OrderServiceProtocol
from app.application.services.outbox_relay import OutboxRelayService
from app.application.services.saga_events import SagaEventsHandlerService
from app.core.settings import Settings
from app.infrastructure.brokers.adapters import KafkaOutboxPublisher
from app.infrastructure.cache.redis_order_cache import RedisOrderCache
from app.infrastructure.database.repositories.order_repository import OrderRepository
from app.infrastructure.database.repositories.outbox_repository import OutboxRepository
from app.infrastructure.database.repositories.processed_events_repository import (
    ProcessedEventRepository,
)
from app.infrastructure.database.uow import SQLAlchemyAsyncUOW
from app.infrastructure.di.outbox_scope import DishkaOutboxScopeFactory


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
    async def get_redis(self, settings: Settings) -> AsyncIterator[Redis]:
        client = Redis.from_url(settings.REDIS_URL, decode_responses=True)
        yield client
        await client.aclose()

    @provide(scope=Scope.APP)
    def get_order_cache(self, redis: Redis, settings: Settings) -> OrderCacheProtocol:
        return RedisOrderCache(
            redis=redis,
            ttl_seconds=settings.ORDER_CACHE_TTL_SECONDS,
        )


class KafkaProvider(Provider):
    @provide(scope=Scope.APP)
    async def get_kafka_producer(
        self, settings: Settings
    ) -> AsyncIterator[AIOKafkaProducer]:
        producer = AIOKafkaProducer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        )
        await producer.start()
        yield producer
        await producer.stop()

    @provide(scope=Scope.APP)
    def get_outbox_publisher(
        self, producer: AIOKafkaProducer
    ) -> OutboxPublisherProtocol:
        # топик и ключ берутся из самой outbox-записи (единый outbox, ADR-006)
        return KafkaOutboxPublisher(producer=producer)


class OutboxRelayProvider(Provider):
    """Зависимости фонового relay-процесса (entrypoints/messaging/relay.py)"""

    @provide(scope=Scope.APP)
    def get_outbox_scope_factory(
        self, container: AsyncContainer
    ) -> OutboxScopeFactory:
        # relay зависит от абстрактной фабрики, а не от Dishka напрямую
        return DishkaOutboxScopeFactory(container)

    @provide(scope=Scope.APP)
    def get_outbox_relay(
        self,
        publisher: OutboxPublisherProtocol,
        scope_factory: OutboxScopeFactory,
        settings: Settings,
    ) -> OutboxRelayService:
        return OutboxRelayService(
            publisher=publisher,
            scope_factory=scope_factory,
            max_publish_attempts=settings.OUTBOX_MAX_PUBLISH_ATTEMPTS,
        )


class ServiceProvider(Provider):
    scope = Scope.REQUEST

    @provide
    def get_uow(self, session: AsyncSession) -> AsyncUOWProtocol:
        return SQLAlchemyAsyncUOW(session)

    @provide
    def get_order_repository(self, session: AsyncSession) -> OrderRepositoryProtocol:
        return OrderRepository(session)

    @provide
    def get_outbox_repository(self, session: AsyncSession) -> OutboxRepositoryProtocol:
        return OutboxRepository(session)

    @provide
    def get_processed_events_repository(
        self, session: AsyncSession
    ) -> ProcessedEventRepositoryProtocol:
        return ProcessedEventRepository(session)

    @provide
    def get_order_service(
        self,
        orders: OrderRepositoryProtocol,
        outbox: OutboxRepositoryProtocol,
        uow: AsyncUOWProtocol,
        cache: OrderCacheProtocol,
        settings: Settings,
    ) -> OrderServiceProtocol:
        return OrderService(
            orders=orders,
            outbox=outbox,
            uow=uow,
            cache=cache,
            settings=settings,
        )

    @provide
    def get_saga_events_handler(
        self,
        orders: OrderRepositoryProtocol,
        processed_events: ProcessedEventRepositoryProtocol,
        cache: OrderCacheProtocol,
        uow: AsyncUOWProtocol,
    ) -> SagaEventsHandlerService:
        return SagaEventsHandlerService(
            orders=orders,
            processed_events=processed_events,
            cache=cache,
            uow=uow,
        )
