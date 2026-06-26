from typing import AsyncGenerator, AsyncIterable

import structlog
from redis.asyncio import Redis
from dishka import Provider, Scope, provide, AsyncContainer
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    async_sessionmaker,
    AsyncSession,
    create_async_engine,
)
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from app.core.settings import Settings, get_settings
from app.consumer.kafka_consumer import AnalyticsConsumerRunner

from app.utils.unit_of_work import AsyncUOWProtocol, SQLAlchemyAsyncUOW
from app.repositories.payments import PaymentRepositoryProtocol, PaymentRepository
from app.repositories.processed_events import (
    ProcessedEventRepositoryProtocol,
    ProcessedEventRepository,
)

from app.services.deduplication import (
    EventDeduplicationServiceProtocol,
    EventDeduplicationService,
)
from app.services.payment_projection import (
    PaymentProjectionServiceProtocol,
    PaymentProjectionService,
)
from app.services.event_handler import PaymentEventHandlerProtocol, PaymentEventHandler
from app.models.payments import Payment
from app.models.processed_events import ProcessedEvent
from app.services.analytics import AnalyticsServiceProtocol, AnalyticsService

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
            pool_size=20,
            max_overflow=30,
            pool_pre_ping=True,
            pool_recycle=3600,
            connect_args={
                "command_timeout": 60,
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
                logger.info("redis_client_closed")


class RepositoriesProvider(Provider):
    @provide(scope=Scope.REQUEST)
    def provide_uow(self, session: AsyncSession) -> AsyncUOWProtocol:
        return SQLAlchemyAsyncUOW(session=session)

    @provide(scope=Scope.REQUEST)
    def provide_payment_repo(self, session: AsyncSession) -> PaymentRepositoryProtocol:
        return PaymentRepository(session=session, model=Payment)

    @provide(scope=Scope.REQUEST)
    def provide_processed_event_repo(
        self, session: AsyncSession
    ) -> ProcessedEventRepositoryProtocol:
        return ProcessedEventRepository(session=session, model=ProcessedEvent)


from app.services.cache import CacheServiceProtocol, RedisCacheService

class ServiceProvider(Provider):
    @provide(scope=Scope.APP)
    def provide_cache_service(self, redis: Redis) -> CacheServiceProtocol:
        return RedisCacheService(redis=redis)

    @provide(scope=Scope.REQUEST)
    def provide_analytics_service(
        self, 
        repo: PaymentRepositoryProtocol,
        cache: CacheServiceProtocol,
        settings: Settings,
    ) -> AnalyticsServiceProtocol:
        return AnalyticsService(repo=repo, cache=cache, settings=settings)

    @provide(scope=Scope.REQUEST)
    def provide_deduplication_service(
        self, repo: ProcessedEventRepositoryProtocol
    ) -> EventDeduplicationServiceProtocol:
        return EventDeduplicationService(repo=repo)

    @provide(scope=Scope.REQUEST)
    def provide_payment_projection_service(
        self, repo: PaymentRepositoryProtocol
    ) -> PaymentProjectionServiceProtocol:
        return PaymentProjectionService(payment_repo=repo)

    @provide(scope=Scope.REQUEST)
    def provide_event_handler(
        self,
        uow: AsyncUOWProtocol,
        deduplication_service: EventDeduplicationServiceProtocol,
        projection_service: PaymentProjectionServiceProtocol,
        cache: CacheServiceProtocol,
    ) -> PaymentEventHandlerProtocol:
        return PaymentEventHandler(
            uow=uow,
            deduplication_service=deduplication_service,
            projection_service=projection_service,
            cache=cache,
        )


class KafkaProvider(Provider):
    @provide(scope=Scope.APP)
    async def provide_kafka_consumer(
        self, settings: Settings
    ) -> AsyncIterable[AIOKafkaConsumer]:
        consumer = AIOKafkaConsumer(
            settings.KAFKA_TOPIC,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            group_id=settings.KAFKA_CONSUMER_GROUP,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        await consumer.start()
        logger.info(
            "kafka_consumer_started",
            topic=settings.KAFKA_TOPIC,
            group=settings.KAFKA_CONSUMER_GROUP,
        )
        try:
            yield consumer
        finally:
            await consumer.stop()
            logger.info("kafka_producer_stopped")

    @provide(scope=Scope.APP)
    async def provide_kafka_producer(
        self, settings: Settings
    ) -> AsyncIterable[AIOKafkaProducer]:
        producer = AIOKafkaProducer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS, acks="all"
        )
        await producer.start()
        logger.info("kafka_producer_started")
        try:
            yield producer
        finally:
            await producer.stop()
            logger.info("kafka_producer_stopped")

    @provide(scope=Scope.APP)
    def provide_consumer_runner(
        self,
        container: AsyncContainer,
        consumer: AIOKafkaConsumer,
        producer: AIOKafkaProducer,
        settings: Settings,
    ) -> AnalyticsConsumerRunner:
        return AnalyticsConsumerRunner(
            consumer=consumer,
            producer=producer,
            container=container,
            dlq_topic=f"{settings.KAFKA_TOPIC}.dlq",
        )
