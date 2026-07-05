import structlog
from typing import Protocol

from app.schemas.events import PaymentEvent
from app.utils.unit_of_work import AsyncUOWProtocol
from app.services.deduplication import EventDeduplicationServiceProtocol
from app.services.payment_projection import PaymentProjectionServiceProtocol
from app.services.cache import CacheServiceProtocol

logger = structlog.get_logger()


class PaymentEventHandlerProtocol(Protocol):
    async def handle(self, event: PaymentEvent) -> bool: ...


class PaymentEventHandler:
    """
    Фасад бизнес-логики.
    управляет границами транзакции (UoW) и оркестрирует процесс обработки события,
    но сам не реализует ни дедупликацию, ни обновление аналитики.
    """

    def __init__(
        self,
        uow: AsyncUOWProtocol,
        deduplication_service: EventDeduplicationServiceProtocol,
        projection_service: PaymentProjectionServiceProtocol,
        cache: CacheServiceProtocol,
    ) -> None:
        self._uow = uow
        self._deduplication_service = deduplication_service
        self._projection_service = projection_service
        self._cache = cache

    async def handle(self, event: PaymentEvent) -> bool:
        logger.info(
            "processing_payment_event_started",
            event_id=str(event.metadata.event_id),
        )

        async with self._uow:
            # 1. проверяем на дубликаты (идемпотентность)
            # Благодаря on_conflict_do_nothing под капотом, транзакция БД остается живой
            if not await self._deduplication_service.register_event(str(event.metadata.event_id)):
                logger.info(
                    "processing_payment_event_skipped",
                    reason="duplicate",
                    event_id=str(event.metadata.event_id),
                )
                return False

            # 2. применяем проекцию к Read-модели
            await self._projection_service.project_payment(event.data)

        # 3. UoW автоматически коммитит изменения при успешном выходе из блока.
        logger.info(
            "processing_payment_event_success",
            event_id=str(event.metadata.event_id),
        )

        # 4. Сбрасываем кэш аналитики, так как данные обновились
        await self._cache.delete_by_pattern("analytics:summary:*")
        logger.info("analytics_cache_invalidated")

        return True
