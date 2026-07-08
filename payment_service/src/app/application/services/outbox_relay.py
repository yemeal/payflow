import asyncio

import structlog

from app.domain.outbox import OutboxStatus
from app.application.ports.outbox_publisher import (
    OutboxPublisherProtocol,
    OutboxScopeFactory,
    EventEnvelope,
)

logger = structlog.get_logger()

# сколько символов ошибки сохраняем в outbox_events.last_error
_LAST_ERROR_MAX_LEN = 1000


class OutboxRelayService:
    def __init__(
        self,
        publisher: OutboxPublisherProtocol,
        scope_factory: OutboxScopeFactory,
        max_publish_attempts: int = 5,
    ) -> None:
        self._publisher = publisher
        self._scope_factory = scope_factory
        self._max_publish_attempts = max_publish_attempts
        self._is_running = False

    async def run(self, poll_interval: float = 2.0, batch_size: int = 50) -> None:
        self._is_running = True
        logger.info("outbox_relay_started")

        while self._is_running:
            try:
                await self._process_batch(batch_size)
            except Exception:
                logger.exception("outbox_relay_error")

            if self._is_running:
                await asyncio.sleep(poll_interval)

    async def _process_batch(self, batch_size: int) -> None:
        # создаем новый scope для каждой пачки, чтобы получить свежую транзакцию
        async with self._scope_factory() as scope:
            async with scope.uow:
                events = await scope.outbox_repo.get_unpublished_events(limit=batch_size)

                if not events:
                    return

                published = 0
                for event in events:
                    # конвертируем доменный OutboxEvent в типизированный EventEnvelope
                    envelope = EventEnvelope.from_outbox_event(event)
                    try:
                        await self._publisher.publish(envelope)
                    except Exception as e:
                        # фиксируем неудачную попытку; после max_publish_attempts
                        # событие считается "ядовитым" и помечается FAILED,
                        # чтобы не блокировать очередь бесконечными ретраями
                        event.attempts += 1
                        event.last_error = str(e)[:_LAST_ERROR_MAX_LEN]

                        if event.attempts >= self._max_publish_attempts:
                            event.status = OutboxStatus.FAILED
                            logger.error(
                                "outbox_event_failed_permanently",
                                event_id=str(event.id),
                                event_type=event.event_type,
                                attempts=event.attempts,
                                error=event.last_error,
                            )
                        else:
                            logger.warning(
                                "outbox_event_publish_failed",
                                event_id=str(event.id),
                                event_type=event.event_type,
                                attempts=event.attempts,
                                error=event.last_error,
                            )

                        await scope.outbox_repo.update(event)
                        # прерываем батч: более новые события не должны
                        # обгонять упавшее (сохранение порядка публикации).
                        # attempts/FAILED закоммитятся вместе с уже опубликованными.
                        break

                    # Помечаем как отправленное
                    event.status = OutboxStatus.SUCCESS
                    await scope.outbox_repo.update(event)
                    published += 1

                # UOW коммитит изменения при успешном выходе из блока
                if published:
                    logger.info("outbox_relay_batch_processed", count=published)

    def stop(self) -> None:
        self._is_running = False
        logger.info("outbox_relay_stopping")
