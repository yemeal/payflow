import json

import structlog
from aiokafka import AIOKafkaProducer

from app.application.ports.correlation import CommandCorrelationStoreProtocol
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
            # exclude_none: событие вне саги уходит без пустого поля correlation
            envelope.model_dump(mode="json", exclude_none=True),
        ).encode("utf-8")

        await self._producer.send_and_wait(
            topic=self._topic,
            key=key,
            value=value,
        )


class CorrelationEnrichingPublisher:
    """
    Декоратор паблишера: проставляет metadata.correlation исходящему событию.

    Корреляция - транспортная метадата (contracts/README, правило 1), поэтому она
    и подставляется на транспортном уровне: ни домен, ни application-слой о ней
    не знают. Источник - журнал команд (payment.id -> idempotency_key -> correlation).

    Платёж вне саги (HTTP API) корреляции не имеет: событие уходит без блока,
    оркестратор такие события игнорирует.
    """

    def __init__(
        self,
        inner: OutboxPublisherProtocol,
        correlations: CommandCorrelationStoreProtocol,
    ) -> None:
        self._inner = inner
        self._correlations = correlations

    async def publish(self, envelope: EventEnvelope) -> None:
        payment_id = envelope.data.get("id")
        if payment_id:
            correlation = await self._correlations.resolve_for_payment(str(payment_id))
            if correlation:
                envelope.metadata.correlation = correlation
                logger.debug(
                    "event_correlation_attached",
                    event_id=str(envelope.metadata.event_id),
                    saga_id=correlation.get("sagaId"),
                )
        await self._inner.publish(envelope)
