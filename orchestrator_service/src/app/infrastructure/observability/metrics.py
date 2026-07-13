"""
Prometheus-метрики оркестратора (docs/saga-design.md, 9.9).

Реестр создаётся фабрикой, а не берётся из глобального REGISTRY prometheus_client:
процесс может собрать приложение дважды (тесты, uvicorn --factory с reload), а
повторная регистрация коллектора в общем реестре роняет процесс ошибкой
"Duplicated timeseries in CollectorRegistry".

Сами коллекторы спрятаны внутри SagaMetrics: вызывающий код дергает inc_*/set_*
и ничего не знает про prometheus_client. Это позволяет подменить метрики
заглушкой в тестах и не тащить лейблы по всему коду.

Экспозиция:
  - HTTP-процесс (Admin API): роут GET /metrics поверх render_latest();
  - Kafka-only процессы (консюмер, relay, поллер): start_metrics_server(port, metrics).
"""

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    generate_latest,
)
from prometheus_client import start_http_server as _start_http_server


class SagaMetrics:
    """Базовый набор метрик саги. Реестр свой, ничего не пишется в глобальный."""

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        # свой CollectorRegistry по умолчанию: см. модульный docstring
        self.registry = registry if registry is not None else CollectorRegistry()

        self._sagas_started = Counter(
            "sagas_started_total",
            "Запущенные саги",
            labelnames=("saga_type",),
            registry=self.registry,
        )
        self._sagas_finished = Counter(
            "sagas_finished_total",
            "Саги, дошедшие до терминального статуса",
            labelnames=("saga_type", "status"),
            registry=self.registry,
        )
        self._step_retries = Counter(
            "saga_step_retries_total",
            "Переотправки команды шага саги (retry с backoff)",
            labelnames=("saga_type", "step"),
            registry=self.registry,
        )
        self._compensations = Counter(
            "saga_compensations_total",
            "Саги, ушедшие в компенсацию",
            labelnames=("saga_type",),
            registry=self.registry,
        )
        self._dlq_messages = Counter(
            "saga_dlq_messages_total",
            "Сообщения, отправленные в DLQ",
            labelnames=("topic",),
            registry=self.registry,
        )
        # Gauge, а не Counter: интересует текущий размер очереди публикации,
        # рост этого числа - сигнал, что relay встал
        self._outbox_pending = Gauge(
            "outbox_pending",
            "Записи outbox в статусе PENDING",
            registry=self.registry,
        )

    def inc_saga_started(self, saga_type: str) -> None:
        self._sagas_started.labels(saga_type=saga_type).inc()

    def inc_saga_finished(self, saga_type: str, status: str) -> None:
        self._sagas_finished.labels(saga_type=saga_type, status=status).inc()

    def inc_step_retry(self, saga_type: str, step: str) -> None:
        self._step_retries.labels(saga_type=saga_type, step=step).inc()

    def inc_compensation(self, saga_type: str) -> None:
        self._compensations.labels(saga_type=saga_type).inc()

    def inc_dlq_message(self, topic: str) -> None:
        self._dlq_messages.labels(topic=topic).inc()

    def set_outbox_pending(self, value: int) -> None:
        self._outbox_pending.set(value)


def render_latest(metrics: SagaMetrics) -> tuple[bytes, str]:
    """
    Экспозиция реестра для HTTP-процессов: тело и content-type.

    Сознательно не используем make_asgi_app + app.mount("/metrics"): Mount в
    Starlette матчит только "/metrics/...", а на точный "/metrics" (именно его
    скрейпит Prometheus) роутер отвечает редиректом 307. Скрейперы, не идущие
    по редиректам, молча теряли бы метрики - обычный роут отдаёт 200 сразу.
    """
    return generate_latest(metrics.registry), CONTENT_TYPE_LATEST


def start_metrics_server(port: int, metrics: SagaMetrics | None = None) -> SagaMetrics:
    """
    Мини-экспортер для Kafka-only процессов (консюмер, relay, поллер): у них нет
    своего HTTP-сервера, поэтому prometheus_client поднимает отдельный порт.

    Возвращает SagaMetrics (созданный или переданный), чтобы вызывающая фабрика
    отдала его сервисам и не держала метрики в глобали.
    """
    saga_metrics = metrics if metrics is not None else SagaMetrics()
    _start_http_server(port, registry=saga_metrics.registry)
    return saga_metrics
