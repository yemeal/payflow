"""Фейки портов: тесты сервиса не поднимают ни Kafka, ни Prometheus, ни контейнер."""

import json
from typing import Any

import pytest

from app.application.ports.dto import DlqRecord
from app.application.services.dlq_service import DlqService


class FakeAlertSink:
    """Пишет вызовы в списки вместо PagerDuty. Реализует AlertSinkProtocol."""

    def __init__(self) -> None:
        self.alerts: list[DlqRecord] = []
        self.invalid_alerts: list[dict[str, str]] = []

    async def alert(self, record: DlqRecord) -> None:
        self.alerts.append(record)

    async def alert_invalid(self, topic: str, reason: str, body_preview: str) -> None:
        self.invalid_alerts.append(
            {"topic": topic, "reason": reason, "body_preview": body_preview}
        )


class FakeMetrics:
    """Копит инкременты вместо prometheus_client. Реализует DlqMetricsProtocol."""

    def __init__(self) -> None:
        self.observed: list[str] = []

    def observe_dlq_message(self, topic: str) -> None:
        self.observed.append(topic)


@pytest.fixture
def alert_sink() -> FakeAlertSink:
    return FakeAlertSink()


@pytest.fixture
def metrics() -> FakeMetrics:
    return FakeMetrics()


@pytest.fixture
def dlq_service(alert_sink: FakeAlertSink, metrics: FakeMetrics) -> DlqService:
    return DlqService(alert_sink=alert_sink, metrics=metrics)


@pytest.fixture
def valid_envelope() -> dict[str, Any]:
    """
    Корректный конверт по contracts/envelope/dlq-envelope.v1.schema.json.

    original - конверт события (metadata snake_case, корреляция вложена
    в metadata.correlation), как их шлёт участник саги.
    """
    return {
        "original": {
            "metadata": {
                "event_id": "0f1b7f3c-2f47-4a2e-9c6a-2b6b2a1f0e11",
                "event_type": "inventory.reserve.failed",
                "version": 1,
                "timestamp": "2026-07-15T10:00:00Z",
                "source": "inventory_service",
                "correlation": {
                    "sagaId": "8a2f1d64-0e35-4a9f-9e5c-77e1c0b2d3a4",
                    "businessKey": "order-42",
                    "commandId": "c1d2e3f4-a5b6-47c8-99d0-1e2f3a4b5c6d",
                },
            },
            "data": {"orderId": "order-42"},
        },
        "dlqMeta": {
            "sourceTopic": "inventory.commands",
            "partition": 3,
            "offset": 1042,
            "consumerGroup": "inventory-service-commands",
            "errorClass": "ValidationError",
            "errorMessage": "items must not be empty",
            "retryCount": 3,
            "redriveCount": 1,
            "failedAt": "2026-07-15T10:00:01Z",
        },
    }


@pytest.fixture
def valid_body(valid_envelope: dict[str, Any]) -> bytes:
    """Тело ровно в том виде, в каком его отдаёт Kafka: сырые байты."""
    return json.dumps(valid_envelope).encode()
