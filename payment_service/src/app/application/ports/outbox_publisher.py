from typing import Protocol, AsyncContextManager
from app.application.ports.dto.events import EventEnvelope
from app.application.ports.repositories import OutboxRepositoryProtocol
from app.application.ports.uow import AsyncUOWProtocol


# порт для публикации событий в брокер
# application-слой не знает куда отправляются события (Kafka, RabbitMQ, etc.)
# конкретная реализация в infrastructure/brokers/
class OutboxPublisherProtocol(Protocol):
    async def publish(self, envelope: EventEnvelope) -> None:
        """Опубликовать событие в брокер сообщений"""
        ...


# фабрика per-batch зависимостей (замена DI-контейнера)
# OutboxRelayService - long-running процесс (APP scope),
# но ему нужны per-batch зависимости (REQUEST scope): новая сессия/UOW/репозиторий.
# Вместо прямой зависимости от Dishka AsyncContainer - абстрактная фабрика,
# чей адаптер (DishkaOutboxScopeFactory) живёт в infrastructure/di/
class OutboxScope(Protocol):
    """Per-batch набор зависимостей для обработки пачки событий"""

    @property
    def uow(self) -> AsyncUOWProtocol: ...

    @property
    def outbox_repo(self) -> OutboxRepositoryProtocol: ...


class OutboxScopeFactory(Protocol):
    """Фабрика, создающая новый scope на каждый batch"""

    def __call__(self) -> AsyncContextManager[OutboxScope]: ...
