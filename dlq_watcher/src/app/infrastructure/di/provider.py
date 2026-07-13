from dishka import Provider, Scope, provide
from prometheus_client import CollectorRegistry

from app.application.ports.alert_sink import AlertSinkProtocol
from app.application.ports.metrics import DlqMetricsProtocol
from app.application.services.dlq_service import DlqService
from app.core.settings import Settings, get_settings
from app.infrastructure.observability.alert_sink import LoggingAlertSink
from app.infrastructure.observability.metrics import PrometheusDlqMetrics


class SettingsProvider(Provider):
    @provide(scope=Scope.APP)
    def provide_settings(self) -> Settings:
        return get_settings()


class ObservabilityProvider(Provider):
    # APP-scope обязателен для всех трёх: Counter должен быть ОДИН на процесс.
    # Дай его в REQUEST-scope - и каждое сообщение регистрировало бы новый
    # коллектор в registry (Duplicated timeseries), а счётчик обнулялся бы
    @provide(scope=Scope.APP)
    def provide_registry(self) -> CollectorRegistry:
        return CollectorRegistry()

    @provide(scope=Scope.APP)
    def provide_metrics(self, registry: CollectorRegistry) -> DlqMetricsProtocol:
        return PrometheusDlqMetrics(registry)

    @provide(scope=Scope.APP)
    def provide_alert_sink(self) -> AlertSinkProtocol:
        return LoggingAlertSink()


class ServiceProvider(Provider):
    # сервис без состояния: держать по экземпляру на сообщение незачем
    @provide(scope=Scope.APP)
    def provide_dlq_service(
        self,
        alert_sink: AlertSinkProtocol,
        metrics: DlqMetricsProtocol,
    ) -> DlqService:
        return DlqService(alert_sink=alert_sink, metrics=metrics)
