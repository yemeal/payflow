from typing import Protocol

import structlog

from app.repositories.processed_events import ProcessedEventRepositoryProtocol

logger = structlog.getLogger()


class EventDeduplicationServiceProtocol(Protocol):
    async def register_event(self, event_id: str) -> bool: ...


class EventDeduplicationService:
    """
    Сервис "идемпотентности" (вернее дедупликации).
    его единственная ответственность — гарантировать, что каждое
    событие обрабатывается строго один раз (Exactly-Once).
    """

    def __init__(self, repo: ProcessedEventRepositoryProtocol) -> None:
        self._repo = repo

    async def register_event(self, event_id: str) -> bool:
        """
        Возвращает `True`, если такое событие еще не было обработано
        и `False` если уже было обработано (есть в таблице `processed_events`)
        """
        logger.debug(
            "attempting_event_registration",
            event_id=event_id,
        )

        is_new = await self._repo.save_if_not_exists(event_id)

        if not is_new:
            logger.warning(
                "duplicate_event_detected",
                event_id=event_id,
            )

        return is_new
