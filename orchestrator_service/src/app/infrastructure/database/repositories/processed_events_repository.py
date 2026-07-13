from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.processed_events import ProcessedEvent
from app.infrastructure.database.models.processed_events import ProcessedEventORM


class ProcessedEventRepository:
    """Реестр обработанных событий (Idempotent Consumer)"""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def try_mark_processed(self, event: ProcessedEvent) -> bool:
        # INSERT ... ON CONFLICT DO NOTHING - атомарная операция.
        # SELECT + INSERT двумя запросами - классическая ошибка идемпотентности:
        # два консьюмера успевают оба пройти SELECT и оба обработать событие.
        stmt = (
            pg_insert(ProcessedEventORM)
            .values(
                event_id=event.event_id,
                saga_id=event.saga_id,
                event_type=event.event_type,
            )
            .on_conflict_do_nothing(index_elements=["event_id"])
        )
        result = await self._session.execute(stmt)
        # rowcount == 0 -> сработал ON CONFLICT: событие уже обработано (дубль)
        return bool(result.rowcount)
