from datetime import datetime
from typing import Protocol, Sequence
from uuid import UUID

from app.domain.outbox import OutboxMessage
from app.domain.processed_commands import ProcessedCommand
from app.domain.reservations import Reservation
from app.domain.stock import StockItem


class StockRepositoryProtocol(Protocol):
    """Остатки склада"""

    async def get_for_update(self, product_ids: Sequence[str]) -> list[StockItem]:
        """SELECT ... FOR UPDATE по списку товаров.

        Блокировка обязательна: проверка available >= qty и уменьшение остатка
        без неё - классическая гонка (два заказа резервируют последний товар).
        Строки берутся в порядке product_id: одинаковый порядок захвата
        блокировок у всех транзакций исключает взаимный дедлок."""
        ...

    async def update(self, item: StockItem) -> StockItem: ...


class ReservationRepositoryProtocol(Protocol):
    """Резервы с TTL"""

    async def add(self, reservation: Reservation) -> Reservation: ...

    async def update(self, reservation: Reservation) -> Reservation: ...

    async def get_by_order_id(self, order_id: UUID) -> Reservation | None: ...

    async def get_by_order_id_for_update(
        self, order_id: UUID
    ) -> Reservation | None:
        """SELECT FOR UPDATE: commit/cancel меняют резерв и остатки атомарно"""
        ...

    async def find_expired_active(
        self, now: datetime, limit: int
    ) -> list[Reservation]:
        """ACTIVE-резервы с expires_at <= now, FOR UPDATE SKIP LOCKED:
        несколько инстансов поллера разгребают их параллельно, не конфликтуя"""
        ...


class ProcessedCommandRepositoryProtocol(Protocol):
    """Журнал идемпотентности участника: command_id -> результат"""

    async def get(self, command_id: str) -> ProcessedCommand | None: ...

    async def add_if_absent(self, command: ProcessedCommand) -> bool:
        """INSERT ... ON CONFLICT DO NOTHING, атомарно.

        True - команда записана в журнал (выполняется впервые);
        False - её уже записал кто-то другой (гонка двух консюмеров).
        Вызывается строго в ОДНОЙ транзакции с бизнес-эффектом и outbox."""
        ...


class OutboxRepositoryProtocol(Protocol):
    """Единая outbox-таблица (у склада - только события)"""

    async def add(self, message: OutboxMessage) -> OutboxMessage: ...

    async def update(self, message: OutboxMessage) -> OutboxMessage: ...

    async def get_unpublished(self, limit: int) -> list[OutboxMessage]:
        """PENDING в порядке создания, FOR UPDATE SKIP LOCKED"""
        ...
