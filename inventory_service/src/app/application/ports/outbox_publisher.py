from contextlib import AbstractAsyncContextManager
from typing import Protocol

from app.application.ports.repositories import OutboxRepositoryProtocol
from app.application.ports.uow import AsyncUOWProtocol
from app.domain.outbox import OutboxMessage


class OutboxPublisherProtocol(Protocol):
    """Публикация outbox-записи в брокер. Топик и ключ берутся из самой записи."""

    async def publish(self, message: OutboxMessage) -> None: ...


class OutboxScope(Protocol):
    """Per-batch зависимости релея: свежая транзакция на каждую пачку"""

    uow: AsyncUOWProtocol
    outbox_repo: OutboxRepositoryProtocol


class OutboxScopeFactory(Protocol):
    """Фабрика scope'ов; абстрагирует DI-фреймворк от relay-сервиса"""

    def __call__(self) -> AbstractAsyncContextManager[OutboxScope]: ...
