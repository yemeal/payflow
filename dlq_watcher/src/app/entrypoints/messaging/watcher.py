"""
DLQ-watcher: единая точка наблюдения за мёртвыми сообщениями всей платформы.

Подписывается по regex на ВСЕ топики, оканчивающиеся на .dlq (конвенция
contracts/README: у каждого рабочего топика есть парный <топик>.dlq), логирует
каждое сообщение уровнем ERROR, инкрементирует dlq_messages_total{topic} и шлёт
мок-алерт. Никакой обработки: watcher только даёт видимость (saga-design, 9.10).

ПОЧЕМУ pattern-подписка средствами FastStream, а не свой цикл на aiokafka
(проверено по faststream 0.7.2 в .venv, а не по памяти):
  1. KafkaBroker.subscriber(pattern=...) существует и доезжает до брокера:
     kafka/subscriber/usecase.py вызывает consumer.subscribe(pattern=self.pattern),
     то есть regex уходит прямо в aiokafka - ровно то, что мы написали бы руками.
  2. Единственное, что FastStream делает с паттерном по дороге - прогоняет через
     compile_path(), а тот трогает строку ТОЛЬКО при наличии плейсхолдеров вида
     {param}. В ".*\\.dlq$" их нет -> regex доезжает до aiokafka байт в байт.
Свой asyncio-цикл с ручным коммитом дал бы то же самое, но ценой повторной реализации
коммитов, ребаланса и graceful shutdown. Он остался бы оправдан, не поддержи FastStream
pattern; поддержка есть, поэтому берём штатный путь.

Почему подписчик принимает только KafkaMessage и читает msg.body:
декодер FastStream ленивый (message.decode() зовётся, лишь когда хендлеру нужен
разобранный body). Не прося декодированное тело, мы гарантируем, что не-JSON мусор
не уронит фреймворк ДО нашего хендлера. JSON разбираем сами - тотальным парсером.

Почему хендлер глотает исключения при ack_policy=NACK_ON_ERROR (инвариант консюмеров
менять нельзя): сообщение уже мёртвое, переигрывать его в DLQ некуда. Всплывшее
исключение дало бы NACK -> тот же offset -> вечный цикл на одном сообщении, и watcher
ослеп бы для всех остальных. Инвариант сохранён, а до NACK дело не доходит.

Все зависимости собираются фабриками (create_broker / create_container / create_app):
глобалей уровня модуля нет.
"""

import asyncio

import structlog
from dishka_faststream import FromDishka, setup_dishka
from faststream import AckPolicy, FastStream
from faststream.kafka import KafkaBroker, KafkaMessage
from prometheus_client import CollectorRegistry

from app.application.services.dlq_service import DlqService
from app.core.logging import setup_logging
from app.core.settings import Settings, get_settings
from app.infrastructure.di import create_container
from app.infrastructure.observability.metrics import start_metrics_server

logger = structlog.get_logger(__name__)


def create_broker(settings: Settings) -> KafkaBroker:
    return KafkaBroker(settings.KAFKA_BOOTSTRAP_SERVERS)


def register_subscribers(broker: KafkaBroker, settings: Settings) -> None:
    # group_id обязателен: без него offset'ы не коммитятся и рестарт контейнера
    # заново перечитает всю DLQ (шквал ложных алертов).
    # auto_offset_reset=earliest: watcher, поднятый после аварии, обязан увидеть
    # сообщения, которые умерли, пока его не было.
    @broker.subscriber(
        pattern=settings.KAFKA_DLQ_PATTERN,
        group_id=settings.KAFKA_CONSUMER_GROUP,
        auto_offset_reset="earliest",
        ack_policy=AckPolicy.NACK_ON_ERROR,
    )
    async def handle_dlq_message(
        msg: KafkaMessage,
        dlq_service: FromDishka[DlqService],
    ) -> None:
        topic = str(getattr(msg.raw_message, "topic", "unknown"))
        try:
            await dlq_service.handle(topic=topic, body=msg.body)
        except Exception:
            # последний рубеж: сюда попадёт только сбой самого наблюдения
            # (например, прилёг алерт-канал). Гасим - см. docstring модуля
            logger.exception("dlq_watcher_handler_failed", topic=topic)


def create_app() -> FastStream:
    setup_logging()
    settings = get_settings()

    broker = create_broker(settings)
    register_subscribers(broker, settings)

    app = FastStream(broker)

    container = create_container()
    setup_dishka(container=container, broker=broker, auto_inject=True)

    # держим сервер метрик в замыкании фабрики, а не в глобали и не в app.state:
    # у FastStream нет контракта на пользовательские атрибуты приложения
    metrics_server = None

    @app.on_startup
    async def start_metrics() -> None:
        nonlocal metrics_server
        # registry берём из контейнера, а не создаём здесь: Counter и сервер метрик
        # обязаны смотреть в ОДИН registry, иначе /metrics отдаст пустоту
        registry = await container.get(CollectorRegistry)
        metrics_server, _ = start_metrics_server(settings.METRICS_PORT, registry)
        logger.info(
            "dlq_watcher_started",
            pattern=settings.KAFKA_DLQ_PATTERN,
            consumer_group=settings.KAFKA_CONSUMER_GROUP,
        )

    @app.on_shutdown
    async def stop_watcher() -> None:
        # graceful shutdown: гасим http-сервер метрик и закрываем контейнер.
        # FastStream к этому моменту уже остановил консюмер и закоммитил offset'ы
        if metrics_server is not None:
            metrics_server.shutdown()
        await container.close()
        logger.info("dlq_watcher_stopped")

    return app


if __name__ == "__main__":
    asyncio.run(create_app().run())
