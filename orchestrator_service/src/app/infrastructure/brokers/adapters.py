import json
from typing import Any

import structlog
from aiokafka import AIOKafkaProducer

from app.domain.outbox import OutboxMessage
from app.domain.saga import utc_now

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
            message_type=message.type,
            message_id=str(message.id),
            key=message.key,
        )


class DlqPublisher:
    """
    Публикация ядовитого ВХОДЯЩЕГО события в парный <топик>.dlq
    (contracts/envelope/dlq-envelope.v1) напрямую продюсером, мимо outbox.

    Почему мимо outbox: outbox гарантирует "сообщение уходит тогда и только тогда,
    когда закоммичен переход саги". Здесь переходить нечему: событие нарушает
    контракт, состояние саги не менялось, своей транзакции у консюмера нет.
    Если публикация упадёт, исключение уйдёт наверх, offset не закоммитится
    (NACK_ON_ERROR) и Kafka переиграет событие - потери нет.

    Команды, исчерпавшие ретраи, - другой случай: их в DLQ кладёт SagaExecutorService
    ЧЕРЕЗ outbox, потому что там DLQ-запись идёт в одной транзакции с переходом саги.
    """

    def __init__(self, producer: AIOKafkaProducer, consumer_group: str) -> None:
        self._producer = producer
        self._consumer_group = consumer_group

    async def publish_poison(
        self,
        source_topic: str,
        message: dict[str, Any],
        error_message: str,
        key: bytes | None = None,
        partition: int | None = None,
        offset: int | None = None,
        error_class: str = "PoisonMessage",
    ) -> None:
        dlq_topic = f"{source_topic}.dlq"
        dlq_meta: dict[str, Any] = {
            "sourceTopic": source_topic,
            "consumerGroup": self._consumer_group,
            "errorClass": error_class,
            "errorMessage": error_message,
            # ретраев не было: ядовитое сообщение переигрывать бессмысленно
            "retryCount": 0,
            "redriveCount": 0,
            "failedAt": utc_now().isoformat(),
        }
        # partition/offset по схеме опциональны, но именно они дают найти
        # исходную запись в топике при разборе инцидента
        if partition is not None:
            dlq_meta["partition"] = partition
        if offset is not None:
            dlq_meta["offset"] = offset

        envelope = {"original": message, "dlqMeta": dlq_meta}
        # ключ исходного сообщения сохраняем как есть: DLQ партиционируется
        # так же, как исходный топик, и порядок по бизнес-ключу не ломается
        await self._producer.send_and_wait(
            topic=dlq_topic,
            key=key,
            value=json.dumps(envelope).encode("utf-8"),
        )
        logger.error(
            "poison_message_sent_to_dlq",
            source_topic=source_topic,
            dlq_topic=dlq_topic,
            error_class=error_class,
            error_message=error_message,
        )
