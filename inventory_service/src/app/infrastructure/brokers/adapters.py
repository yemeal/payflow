import json

import structlog
from aiokafka import AIOKafkaProducer

from app.domain.outbox import OutboxMessage

logger = structlog.get_logger()


class KafkaOutboxPublisher:
    """
    Адаптер OutboxPublisherProtocol: публикует outbox-запись в её topic.

    payload записи - уже готовый конверт {"metadata": ..., "data": ...},
    собранный в application-слое: транспорт ничего в нём не меняет и не
    интерпретирует (в частности, echo-блок correlation).
    key = business_key (order_id): все события одного заказа в одной партиции.
    """

    def __init__(self, producer: AIOKafkaProducer) -> None:
        self._producer = producer

    async def publish(self, message: OutboxMessage) -> None:
        await self._producer.send_and_wait(
            topic=message.topic,
            key=message.key.encode("utf-8"),
            value=json.dumps(message.payload).encode("utf-8"),
        )
        logger.info(
            "outbox_message_published",
            topic=message.topic,
            type=message.type,
            message_id=str(message.id),
            key=message.key,
        )
