"""
Мини-экспортер Prometheus для Kafka-only сервиса (docs/saga-design.md, 9.9).

HTTP-приложения отдают /metrics своим же сервером, у watcher'а его нет, поэтому
поднимаем отдельный http-сервер prometheus_client на METRICS_PORT.
"""

import structlog
from prometheus_client import CollectorRegistry, Counter, start_http_server

logger = structlog.get_logger(__name__)


class PrometheusDlqMetrics:
    """
    Счётчик dlq_messages_total{topic}.

    Registry передаётся снаружи (а не берётся глобальный REGISTRY по умолчанию):
    иначе повторное создание сервиса в одном процессе - а это ровно то, что делают
    тесты - падало бы на Duplicated timeseries in CollectorRegistry.
    """

    def __init__(self, registry: CollectorRegistry) -> None:
        self._counter = Counter(
            "dlq_messages_total",
            "Сообщения, прочитанные из DLQ-топиков",
            labelnames=("topic",),
            registry=registry,
        )

    def observe_dlq_message(self, topic: str) -> None:
        self._counter.labels(topic=topic).inc()


def start_metrics_server(port: int, registry: CollectorRegistry):
    """Поднимает /metrics в фоновом daemon-потоке. Возвращает (server, thread)."""
    server, thread = start_http_server(port, registry=registry)
    logger.info("metrics_server_started", port=port)
    return server, thread
