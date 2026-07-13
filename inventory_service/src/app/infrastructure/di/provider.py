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
    ProcessedCommandRepositoryProtocol,
    ReservationRepositoryProtocol,
    StockRepositoryProtocol,
)
from app.application.ports.uow import AsyncUOWProtocol
from app.application.services.inventory_service import (
    InventoryService,
    InventoryServiceProtocol,
)
from app.application.services.outbox_relay import OutboxRelayService
from app.application.services.reservation_expiry import ReservationExpiryService
from app.core.settings import Settings
from app.infrastructure.brokers.adapters import KafkaOutboxPublisher
from app.infrastructure.database.repositories.outbox_repository import OutboxRepository
from app.infrastructure.database.repositories.processed_commands_repository import (
    ProcessedCommandRepository,
)
from app.infrastructure.database.repositories.reservation_repository import (
    ReservationRepository,
)
from app.infrastructure.database.repositories.stock_repository import StockRepository
from app.infrastructure.database.uow import SQLAlchemyAsyncUOW
from app.infrastructure.di.expiry_scope import DishkaExpiryScopeFactory
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
    def get_sessionmaker(self, engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
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


class ServiceProvider(Provider):
    """Всё, что живёт в границах одной транзакции (REQUEST scope)"""

    scope = Scope.REQUEST

    @provide
    def get_uow(self, session: AsyncSession) -> AsyncUOWProtocol:
        return SQLAlchemyAsyncUOW(session)

    @provide
    def get_stock_repository(self, session: AsyncSession) -> StockRepositoryProtocol:
        return StockRepository(session)

    @provide
    def get_reservation_repository(
        self, session: AsyncSession
    ) -> ReservationRepositoryProtocol:
        return ReservationRepository(session)

    @provide
    def get_processed_commands_repository(
        self, session: AsyncSession
    ) -> ProcessedCommandRepositoryProtocol:
        return ProcessedCommandRepository(session)

    @provide
    def get_outbox_repository(self, session: AsyncSession) -> OutboxRepositoryProtocol:
        return OutboxRepository(session)

    @provide
    def get_inventory_service(
        self,
        stock: StockRepositoryProtocol,
        reservations: ReservationRepositoryProtocol,
        processed_commands: ProcessedCommandRepositoryProtocol,
        outbox: OutboxRepositoryProtocol,
        uow: AsyncUOWProtocol,
        settings: Settings,
    ) -> InventoryServiceProtocol:
        return InventoryService(
            stock=stock,
            reservations=reservations,
            processed_commands=processed_commands,
            outbox=outbox,
            uow=uow,
            settings=settings,
        )


class BackgroundProvider(Provider):
    """Долгоживущие фоновые сервисы: relay и поллер автоистечения (APP scope)"""

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
    def get_reservation_expiry(
        self, container: AsyncContainer, settings: Settings
    ) -> ReservationExpiryService:
        return ReservationExpiryService(
            scope_factory=DishkaExpiryScopeFactory(container),
            interval_seconds=settings.EXPIRY_POLLER_INTERVAL_SECONDS,
            batch_size=settings.EXPIRY_POLLER_BATCH_SIZE,
        )
