import asyncio
import json

import structlog
from aiokafka import AIOKafkaProducer
from dishka import AsyncContainer

from app.repositories.outbox_repository import OutboxRepositoryProtocol
from app.utils.unit_of_work import AsyncUOWProtocol

logger = structlog.get_logger()


class OutboxRelayService:
    def __init__(self, container: AsyncContainer, producer: AIOKafkaProducer) -> None:
        self._container = container
        self._producer = producer
        self._is_running = False

    async def run(self, poll_interval: float = 2.0, batch_size: int = 50) -> None:
        self._is_running = True
        logger.info("outbox_relay_started")

        while self._is_running:
            try:
                await self._process_batch(batch_size)
            except Exception as e:
                logger.error("outbox_relay_error", error=str(e))
                
            if self._is_running:
                await asyncio.sleep(poll_interval)

    async def _process_batch(self, batch_size: int) -> None:
        # Создаем новый Scope.REQUEST для каждой пачки, чтобы получить новую транзакцию
        async with self._container() as request_container:
            uow = await request_container.get(AsyncUOWProtocol)
            outbox_repo = await request_container.get(OutboxRepositoryProtocol)

            async with uow:
                events = await outbox_repo.get_unpublished_events(limit=batch_size)
                
                if not events:
                    return

                for event in events:
                    # Отправляем в Kafka
                    await self._producer.send_and_wait(
                        topic="payments",
                        key=str(event.payload.get("payment_id")).encode("utf-8"),
                        value=json.dumps(event.payload).encode("utf-8"),
                    )
                    
                    # Помечаем как отправленное
                    event.published = True
                
                # UOW коммитит изменения (published=True) при успешном выходе из блока
                logger.info("outbox_relay_batch_processed", count=len(events))

    def stop(self) -> None:
        self._is_running = False
        logger.info("outbox_relay_stopping")
