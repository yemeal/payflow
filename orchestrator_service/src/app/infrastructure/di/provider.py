from typing import AsyncIterator

from aiokafka import AIOKafkaProducer
from dishka import AsyncContainer, Provider, Scope, provide
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.application.ports.outbox_publisher import OutboxPublisherProtocol
from app.application.ports.repositories import (
    OutboxRepositoryProtocol,
    ProcessedEventRepositoryProtocol,
    SagaRepositoryProtocol,
    SagaTransitionRepositoryProtocol,
)
from app.application.ports.uow import AsyncUOWProtocol
from app.application.sagas.order_fulfillment import create_saga_registry
from app.application.services.outbox_relay import OutboxRelayService
from app.application.services.saga_executor import SagaExecutorService
from app.application.services.saga_poller import SagaPollerService
from app.core.settings import Settings
from app.domain.definitions import SagaRegistry
from app.infrastructure.brokers.adapters import DlqPublisher, KafkaOutboxPublisher
from app.infrastructure.database.repositories.outbox_repository import OutboxRepository
from app.infrastructure.database.repositories.processed_events_repository import (
    ProcessedEventRepository,
)
from app.infrastructure.database.repositories.saga_repository import SagaRepository
from app.infrastructure.database.repositories.saga_transition_repository import (
    SagaTransitionRepository,
)
from app.infrastructure.database.uow import SQLAlchemyAsyncUOW
from app.infrastructure.di.outbox_scope import DishkaOutboxScopeFactory
from app.infrastructure.di.poller_scope import DishkaSagaPollerScopeFactory


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
        return KafkaOutboxPublisher(producer=producer)

    @provide(scope=Scope.APP)
    def get_dlq_publisher(
        self, producer: AIOKafkaProducer, settings: Settings
    ) -> DlqPublisher:
        # тот же продюсер, что и у релея: ядовитые события уходят в DLQ напрямую,
        # без outbox (перехода саги, с которым их надо коммитить, нет)
        return DlqPublisher(
            producer=producer, consumer_group=settings.KAFKA_CONSUMER_GROUP
        )


class SagaDefinitionsProvider(Provider):
    """Реестр определений саг - замена глобального TRANSITIONS-словаря (ADR-006)"""

    @provide(scope=Scope.APP)
    def get_registry(self, settings: Settings) -> SagaRegistry:
        # fail-fast: инварианты определений (уникальность шагов, TTL резерва
        # против дедлайна оплаты и т.д.) проверяются при сборке контейнера
        return create_saga_registry(settings)


class ServiceProvider(Provider):
    scope = Scope.REQUEST

    @provide
    def get_uow(self, session: AsyncSession) -> AsyncUOWProtocol:
        return SQLAlchemyAsyncUOW(session)

    @provide
    def get_saga_repository(self, session: AsyncSession) -> SagaRepositoryProtocol:
        return SagaRepository(session)

    @provide
    def get_saga_transition_repository(
        self, session: AsyncSession
    ) -> SagaTransitionRepositoryProtocol:
        return SagaTransitionRepository(session)

    @provide
    def get_processed_events_repository(
        self, session: AsyncSession
    ) -> ProcessedEventRepositoryProtocol:
        return ProcessedEventRepository(session)

    @provide
    def get_outbox_repository(self, session: AsyncSession) -> OutboxRepositoryProtocol:
        return OutboxRepository(session)

    @provide
    def get_saga_executor(
        self,
        registry: SagaRegistry,
        sagas: SagaRepositoryProtocol,
        transitions: SagaTransitionRepositoryProtocol,
        processed_events: ProcessedEventRepositoryProtocol,
        outbox: OutboxRepositoryProtocol,
        uow: AsyncUOWProtocol,
        settings: Settings,
    ) -> SagaExecutorService:
        return SagaExecutorService(
            registry=registry,
            sagas=sagas,
            transitions=transitions,
            processed_events=processed_events,
            outbox=outbox,
            uow=uow,
            settings=settings,
        )


class BackgroundProvider(Provider):
    """Долгоживущие фоновые сервисы: relay и поллер (APP scope)"""

    @provide(scope=Scope.APP)
    def get_outbox_relay(
        self,
        container: AsyncContainer,
        publisher: OutboxPublisherProtocol,
        settings: Settings,
    ) -> OutboxRelayService:
        return OutboxRelayService(
            publisher=publisher,
            scope_factory=DishkaOutboxScopeFactory(container),
            max_publish_attempts=settings.OUTBOX_MAX_PUBLISH_ATTEMPTS,
        )

    @provide(scope=Scope.APP)
    def get_saga_poller(
        self, container: AsyncContainer, settings: Settings
    ) -> SagaPollerService:
        return SagaPollerService(
            scope_factory=DishkaSagaPollerScopeFactory(container),
            interval_seconds=settings.SAGA_POLLER_INTERVAL_SECONDS,
        )
