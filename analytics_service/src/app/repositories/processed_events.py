from typing import Protocol

from sqlalchemy.dialects.postgresql import insert

from app.models import ProcessedEvent
from app.repositories.base_repository import (
    SQLAlchemyAsyncRepository,
    AsyncRepositoryProtocol,
)


class ProcessedEventRepositoryProtocol(
    AsyncRepositoryProtocol[ProcessedEvent], Protocol
):
    async def save_if_not_exists(self, event_id: str) -> bool:
        """
        INSERT ... ON CONFLICT (event_id) DO NOTHING
        """
        ...


class ProcessedEventRepository(SQLAlchemyAsyncRepository[ProcessedEvent]):
    async def save_if_not_exists(self, event_id: str) -> bool:
        stmt = insert(self._model).values(event_id=event_id)
        # Если конфликт по PK (event_id) - ничего не делаем
        stmt = stmt.on_conflict_do_nothing(index_elements=["event_id"])

        result = await self._session.execute(stmt)
        # Если rowcount > 0, значит запись успешно вставлена (событие новое).
        # Если 0 - это дубликат.
        return result.rowcount > 0
