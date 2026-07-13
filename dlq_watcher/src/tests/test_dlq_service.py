"""Тесты DlqService: корректный конверт -> алерт, битый конверт -> лог и никакого падения."""

import json
from typing import Any

import pytest
from structlog.testing import capture_logs

from app.application.services.dlq_service import DlqService

from .conftest import FakeAlertSink, FakeMetrics

DLQ_TOPIC = "inventory.commands.dlq"


async def test_valid_envelope_triggers_alert_with_parsed_fields(
    dlq_service: DlqService,
    alert_sink: FakeAlertSink,
    valid_body: bytes,
) -> None:
    """
    Проверяем: корректный dlq-конверт разбирается и уходит в AlertSinkProtocol.alert.
    Успех: алерт вызван один раз, в нём sourceTopic, errorClass/errorMessage, счётчики
    retry/redrive, координаты partition/offset и корреляция саги из original.metadata.
    Нежелательное поведение: алерт не вызван, поля потеряны или подменены заглушками.
    """
    await dlq_service.handle(topic=DLQ_TOPIC, body=valid_body)

    assert len(alert_sink.alerts) == 1
    record = alert_sink.alerts[0]

    assert record.dlq_topic == DLQ_TOPIC
    assert record.source_topic == "inventory.commands"
    assert record.error_class == "ValidationError"
    assert record.error_message == "items must not be empty"
    assert record.retry_count == 3
    assert record.redrive_count == 1
    assert record.partition == 3
    assert record.offset == 1042
    assert record.consumer_group == "inventory-service-commands"
    assert record.failed_at == "2026-07-15T10:00:01Z"

    # корреляция саги обязана доехать: без неё дежурный не свяжет труп с заказом
    assert record.saga_id == "8a2f1d64-0e35-4a9f-9e5c-77e1c0b2d3a4"
    assert record.business_key == "order-42"

    assert alert_sink.invalid_alerts == []


async def test_valid_envelope_increments_metric_for_its_topic(
    dlq_service: DlqService,
    metrics: FakeMetrics,
    valid_body: bytes,
) -> None:
    """
    Проверяем: каждое принятое сообщение инкрементирует dlq_messages_total{topic}.
    Успех: метрика отмечена ровно один раз и с меткой того .dlq-топика, откуда читали.
    Нежелательное поведение: метка sourceTopic вместо топика чтения, двойной инкремент.
    """
    await dlq_service.handle(topic=DLQ_TOPIC, body=valid_body)

    assert metrics.observed == [DLQ_TOPIC]


async def test_command_envelope_correlation_is_read_from_metadata_root(
    dlq_service: DlqService,
    alert_sink: FakeAlertSink,
    valid_envelope: dict[str, Any],
) -> None:
    """
    Проверяем: у конверта КОМАНДЫ корреляция лежит в metadata напрямую, не в correlation.
    Успех: sagaId/businessKey извлечены из original.metadata (camelCase, без вложенности).
    Нежелательное поведение: watcher понимает только события и теряет сагу у команд.
    """
    valid_envelope["original"]["metadata"] = {
        "commandId": "c1d2e3f4-a5b6-47c8-99d0-1e2f3a4b5c6d",
        "commandType": "inventory.reserve",
        "version": 1,
        "timestamp": "2026-07-15T10:00:00Z",
        "source": "orchestrator_service",
        "sagaId": "8a2f1d64-0e35-4a9f-9e5c-77e1c0b2d3a4",
        "businessKey": "order-42",
    }

    await dlq_service.handle(
        topic=DLQ_TOPIC, body=json.dumps(valid_envelope).encode()
    )

    record = alert_sink.alerts[0]
    assert record.saga_id == "8a2f1d64-0e35-4a9f-9e5c-77e1c0b2d3a4"
    assert record.business_key == "order-42"


@pytest.mark.parametrize(
    ("case", "body"),
    [
        ("not_json", b"{ this is not json"),
        ("not_utf8", b"\xff\xfe\x00broken"),
        ("json_but_not_object", b"[1, 2, 3]"),
        ("no_dlq_meta", b'{"original": {"data": {}}}'),
        ("dlq_meta_not_object", b'{"original": {}, "dlqMeta": "oops"}'),
        ("no_source_topic", b'{"original": {}, "dlqMeta": {"errorClass": "X"}}'),
        ("empty_body", b""),
    ],
)
async def test_broken_envelope_is_logged_and_does_not_raise(
    dlq_service: DlqService,
    alert_sink: FakeAlertSink,
    metrics: FakeMetrics,
    case: str,
    body: bytes,
) -> None:
    """
    Проверяем: битый конверт (не JSON / нет dlqMeta / нет sourceTopic) не роняет обработку.
    Успех: handle отрабатывает без исключения, обычный alert НЕ вызывается, зато вызван
    alert_invalid с причиной, а метрика всё равно инкрементирована (сообщение-то умерло).
    Нежелательное поведение: исключение наружу -> NACK -> вечный цикл на мёртвом offset.
    """
    # исключения быть не должно: любое исключение здесь и есть тот самый вечный цикл
    await dlq_service.handle(topic=DLQ_TOPIC, body=body)

    assert alert_sink.alerts == [], f"{case}: битый конверт не должен идти в обычный alert"
    assert len(alert_sink.invalid_alerts) == 1, f"{case}: ожидали ровно один alert_invalid"
    assert alert_sink.invalid_alerts[0]["topic"] == DLQ_TOPIC
    assert alert_sink.invalid_alerts[0]["reason"], f"{case}: причина обязана быть указана"

    # факт попадания в DLQ виден на графике даже у нечитаемого сообщения
    assert metrics.observed == [DLQ_TOPIC]


async def test_valid_envelope_emits_dlq_message_received_log(
    dlq_service: DlqService,
    valid_body: bytes,
) -> None:
    """
    Проверяем: принятое сообщение пишет ERROR-лог dlq_message_received с полями разбора.
    Успех: одна запись уровня error, в ней topic, source_topic, error_class/error_message,
    retry/redrive-счётчики и корреляция саги.
    Нежелательное поведение: уровень ниже ERROR или потерянные поля - дежурный не увидит.
    """
    with capture_logs() as logs:
        await dlq_service.handle(topic=DLQ_TOPIC, body=valid_body)

    received = [entry for entry in logs if entry["event"] == "dlq_message_received"]

    assert len(received) == 1
    entry = received[0]
    assert entry["log_level"] == "error"
    assert entry["topic"] == DLQ_TOPIC
    assert entry["source_topic"] == "inventory.commands"
    assert entry["error_class"] == "ValidationError"
    assert entry["error_message"] == "items must not be empty"
    assert entry["retry_count"] == 3
    assert entry["redrive_count"] == 1
    assert entry["saga_id"] == "8a2f1d64-0e35-4a9f-9e5c-77e1c0b2d3a4"
    assert entry["business_key"] == "order-42"


async def test_broken_envelope_emits_dlq_envelope_invalid_log(
    dlq_service: DlqService,
) -> None:
    """
    Проверяем: битый конверт пишет ERROR-лог с событием dlq_envelope_invalid.
    Успех: в логах есть запись уровня error с event=dlq_envelope_invalid, топиком и причиной.
    Нежелательное поведение: сообщение молча проглочено и никто о нём не узнал.
    """
    # ловим именно structlog, а не stdlib-логи: caplog здесь бесполезен, потому что
    # без setup_logging() structlog пишет мимо stdlib logging и caplog.records пуст
    with capture_logs() as logs:
        await dlq_service.handle(topic=DLQ_TOPIC, body=b"{ not json")

    invalid_logs = [entry for entry in logs if entry["event"] == "dlq_envelope_invalid"]

    assert len(invalid_logs) == 1
    assert invalid_logs[0]["log_level"] == "error"
    assert invalid_logs[0]["topic"] == DLQ_TOPIC
    assert invalid_logs[0]["reason"]


async def test_missing_optional_fields_do_not_break_parsing(
    dlq_service: DlqService,
    alert_sink: FakeAlertSink,
) -> None:
    """
    Проверяем: конверт с минимально необходимым dlqMeta (только sourceTopic) разбирается.
    Успех: алерт вызван, счётчики по умолчанию нулевые, отсутствующие поля равны None.
    Нежелательное поведение: падение или пропуск сообщения из-за необязательных полей.
    """
    body = json.dumps(
        {"original": {}, "dlqMeta": {"sourceTopic": "orders.events"}}
    ).encode()

    await dlq_service.handle(topic="orders.events.dlq", body=body)

    record = alert_sink.alerts[0]
    assert record.source_topic == "orders.events"
    assert record.error_class == "unknown"
    assert record.retry_count == 0
    assert record.redrive_count == 0
    assert record.saga_id is None
    assert record.business_key is None
