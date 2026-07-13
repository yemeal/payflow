import uuid
from datetime import datetime
from typing import Protocol

from app.domain.outbox import OutboxMessage
from app.domain.processed_events import ProcessedEvent
from app.domain.saga import Saga, SagaTransition


class SagaRepositoryProtocol(Protocol):
    """Протокол репозитория саг (generic-ядро, ADR-006)"""

    async def create_if_absent(self, saga: Saga) -> bool:
        """INSERT ... ON CONFLICT (saga_type, business_key) DO NOTHING.

        True - сага создана; False - уже существует (дубль стартового события).
        Атомарность вместо SELECT-затем-INSERT - иначе race двух консюмеров."""
        ...

    async def get(self, saga_id: uuid.UUID) -> Saga | None: ...

    async def get_for_update(self, saga_id: uuid.UUID) -> Saga | None:
        """SELECT FOR UPDATE: переход делается под блокировкой строки"""
        ...

    async def get_by_business_key_for_update(
        self, saga_type: str, business_key: str
    ) -> Saga | None: ...

    async def update(self, saga: Saga) -> Saga: ...

    async def find_retry_due(self, now: datetime, limit: int) -> list[Saga]:
        """Саги с наступившим retry_after (вне терминальных статусов),
        FOR UPDATE SKIP LOCKED - несколько инстансов поллера не конфликтуют"""
        ...

    async def find_deadline_due(self, now: datetime, limit: int) -> list[Saga]:
        """Саги, чей дедлайн ответа участника истёк (retry не запланирован),
        FOR UPDATE SKIP LOCKED"""
        ...

    async def list_sagas(
        self,
        saga_type: str | None,
        status: str | None,
        limit: int,
        offset: int,
    ) -> list[Saga]: ...

    async def list_stuck(self, older_than: datetime, limit: int) -> list[Saga]:
        """Нетерминальные саги, не обновлявшиеся дольше порога (Admin API)"""
        ...


class SagaTransitionRepositoryProtocol(Protocol):
    """Append-only история переходов"""

    async def add(self, transition: SagaTransition) -> SagaTransition: ...

    async def list_for_saga(self, saga_id: uuid.UUID) -> list[SagaTransition]: ...


class ProcessedEventRepositoryProtocol(Protocol):
    """Реестр обработанных событий (Idempotent Consumer)"""

    async def try_mark_processed(self, event: ProcessedEvent) -> bool:
        """INSERT ... ON CONFLICT DO NOTHING, атомарно.

        True - событие новое, можно обрабатывать; False - дубль, пропустить.
        Вызывается строго в ОДНОЙ транзакции с бизнес-обработкой события."""
        ...


class OutboxRepositoryProtocol(Protocol):
    """Единая outbox-таблица команд и событий (ADR-006)"""

    async def add(self, message: OutboxMessage) -> OutboxMessage: ...

    async def update(self, message: OutboxMessage) -> OutboxMessage: ...

    async def get_unpublished(self, limit: int) -> list[OutboxMessage]:
        """PENDING в порядке создания, FOR UPDATE SKIP LOCKED"""
        ...
