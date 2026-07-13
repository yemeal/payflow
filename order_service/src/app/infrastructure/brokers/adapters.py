import json

import structlog
from aiokafka import AIOKafkaProducer

from app.domain.outbox import OutboxMessage

logger = structlog.get_logger()


class KafkaOutboxPublisher:
    """
    Адаптер OutboxPublisherProtocol: публикует outbox-запись в её topic.

    key = business_key (для заказа - order_id): все сообщения одной саги
    попадают в одну партицию, Kafka сохраняет их порядок.
    payload записи - уже готовый конверт {"metadata": ..., "data": ...}.
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
            kind=message.kind.value,
            type=message.type,
            message_id=str(message.id),
            key=message.key,
        )
