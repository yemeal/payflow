import json

import structlog
from aiokafka import AIOKafkaProducer

from app.application.ports.outbox_publisher import (
    OutboxPublisherProtocol,
    EventEnvelope,
)

logger = structlog.get_logger()


class KafkaOutboxPublisher:
    """
    Адаптер OutboxPublisherProtocol для Kafka.
    Получает типизированный EventEnvelope, сериализует в JSON и отправляет в Kafka.
    Топик настраивается извне (из Settings через DI)
    """

    def __init__(self, producer: AIOKafkaProducer, topic: str) -> None:
        self._producer = producer
        self._topic = topic

    async def publish(self, envelope: EventEnvelope) -> None:
        # key - payment ID для партиционирования:
        # все события одного платежа попадают в один partition -> гарантируем порядок
        key = str(envelope.data.get("id", "")).encode("utf-8")
        value = json.dumps(
            envelope.model_dump(mode="json"),
        ).encode("utf-8")

        await self._producer.send_and_wait(
            topic=self._topic,
            key=key,
            value=value,
        )
