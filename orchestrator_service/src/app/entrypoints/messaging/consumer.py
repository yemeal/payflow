"""
Консюмер оркестратора: слушает orders.events и payments.events и скармливает
события generic-исполнителю саг (docs/saga-design.md, 9.3).

Только консюмер: relay единого outbox и поллер retry/deadline вынесены в
отдельные процессы-энтрипоинты (relay.py, poller.py), чтобы compose масштабировал
их независимо. Консюмеров поднимают по числу партиций, а поллер держат в одном-двух
экземплярах (его выборки и так защищены FOR UPDATE SKIP LOCKED).

Инварианты подписчиков выставлены и менять их нельзя: group_id (иначе offset'ы
не коммитятся и рестарт теряет события), auto_offset_reset="earliest",
ack_policy=NACK_ON_ERROR (at-least-once; дубли гасит processed_events).

Всё состояние собирается фабриками (create_broker / create_app) - глобалей
уровня модуля нет.
"""

import asyncio
from typing import Any

import structlog
from dishka_faststream import FromDishka, setup_dishka
from faststream import AckPolicy, FastStream
from faststream.kafka import KafkaBroker, KafkaMessage
from prometheus_client import CollectorRegistry, Counter, start_http_server

from app.application.services.saga_executor import SagaExecutorService
from app.core.logging import setup_logging
from app.core.settings import Settings, get_settings
from app.infrastructure.brokers.adapters import DlqPublisher
from app.infrastructure.di import create_container

logger = structlog.get_logger(__name__)


def create_broker(settings: Settings) -> KafkaBroker:
    return KafkaBroker(settings.KAFKA_BOOTSTRAP_SERVERS)


def _source_coordinates(
    msg: KafkaMessage,
) -> tuple[bytes | None, int | None, int | None]:
    """Ключ, партиция и offset исходной записи для dlqMeta.

    Достаём из сырой записи защитно: у другого брокера (или в тестах) атрибутов
    может не быть, а падать из-за необязательных метаданных DLQ консюмер не должен.
    """
    raw = getattr(msg, "raw_message", None)
    key = getattr(raw, "key", None)
    partition = getattr(raw, "partition", None)
    offset = getattr(raw, "offset", None)
    return key, partition, offset


def register_subscribers(
    broker: KafkaBroker,
    settings: Settings,
    handled_events: Counter,
) -> None:
    """Подписчики объявляются в замыкании фабрики: никакого модульного состояния"""

    async def process(
        source_topic: str,
        message: dict[str, Any],
        msg: KafkaMessage,
        executor: SagaExecutorService,
        dlq: DlqPublisher,
    ) -> None:
        # прочие исключения НЕ ловим: временный сбой (БД, сеть) - это NACK,
        # Kafka переиграет событие, дубль погасит processed_events
        report = await executor.handle_event(source_topic, message)
        handled_events.labels(topic=source_topic, action=report.action).inc()

        if report.action != "poison":
            return

        # контрактный брак: ретрай бессмыслен и заблокировал бы партицию.
        # Конверт уходит в парный DLQ, исходное событие ACK'ается (дедуп уже
        # зафиксирован исполнителем, повторной обработки не будет)
        key, partition, offset = _source_coordinates(msg)
        await dlq.publish_poison(
            source_topic=source_topic,
            message=message,
            error_message=report.detail or "poison message",
            key=key,
            partition=partition,
            offset=offset,
        )

    @broker.subscriber(
        settings.KAFKA_ORDERS_EVENTS_TOPIC,
        group_id=settings.KAFKA_CONSUMER_GROUP,
        auto_offset_reset="earliest",
        ack_policy=AckPolicy.NACK_ON_ERROR,
    )
    async def handle_orders_events(
        message: dict,
        msg: KafkaMessage,
        executor: FromDishka[SagaExecutorService],
        dlq: FromDishka[DlqPublisher],
    ) -> None:
        await process(settings.KAFKA_ORDERS_EVENTS_TOPIC, message, msg, executor, dlq)

    @broker.subscriber(
        settings.KAFKA_PAYMENTS_EVENTS_TOPIC,
        group_id=settings.KAFKA_CONSUMER_GROUP,
        auto_offset_reset="earliest",
        ack_policy=AckPolicy.NACK_ON_ERROR,
    )
    async def handle_payments_events(
        message: dict,
        msg: KafkaMessage,
        executor: FromDishka[SagaExecutorService],
        dlq: FromDishka[DlqPublisher],
    ) -> None:
        await process(settings.KAFKA_PAYMENTS_EVENTS_TOPIC, message, msg, executor, dlq)


def create_app() -> FastStream:
    setup_logging()
    settings = get_settings()
    container = create_container(settings)

    # свой registry вместо глобального REGISTRY prometheus_client:
    # повторный create_app (тесты) не роняет процесс дублями коллекторов
    metrics_registry = CollectorRegistry()
    handled_events = Counter(
        "orchestrator_handled_events_total",
        "События, обработанные консюмером оркестратора",
        labelnames=("topic", "action"),
        registry=metrics_registry,
    )

    broker = create_broker(settings)
    setup_dishka(container=container, broker=broker, auto_inject=True)
    register_subscribers(broker, settings, handled_events)

    app = FastStream(broker)

    @app.on_startup
    async def start_metrics() -> None:
        start_http_server(settings.METRICS_PORT, registry=metrics_registry)
        logger.info(
            "orchestrator_consumer_started",
            metrics_port=settings.METRICS_PORT,
            group=settings.KAFKA_CONSUMER_GROUP,
        )

    @app.after_shutdown
    async def close_container() -> None:
        # контейнер закрывается последним: в нём продюсер DLQ и пул соединений БД
        await container.close()
        logger.info("orchestrator_consumer_stopped")

    return app


async def main() -> None:
    await create_app().run()


if __name__ == "__main__":
    asyncio.run(main())
