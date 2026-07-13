"""
Разбор DLQ-конверта и реакция на него: метрика + структурный лог + алерт.

Watcher НИЧЕГО не переигрывает и не чинит: его задача - видимость
(docs/saga-design.md, 9.10). Re-drive остаётся ручной операцией, см. README.

Ключевой инвариант модуля: обработка мёртвого сообщения не имеет права упасть.
Сообщение уже в DLQ, ретраить его тут некуда, а исключение наружу означало бы
NACK -> Kafka переиграет то же самое сообщение -> вечный цикл на одном offset,
и watcher перестанет видеть всё остальное. Поэтому парсер тотальный: любой мусор
превращается в DlqEnvelopeError, а не в падение.
"""

import json
from typing import Any

import structlog

from app.application.ports.alert_sink import AlertSinkProtocol
from app.application.ports.dto import DlqRecord
from app.application.ports.metrics import DlqMetricsProtocol

logger = structlog.get_logger(__name__)

# сырое тело режем: в DLQ может лежать мегабайтный мусор, и он не должен раздуть лог
_PREVIEW_MAX_LEN = 512


class DlqEnvelopeError(ValueError):
    """Тело не соответствует contracts/envelope/dlq-envelope.v1.schema.json."""


def _preview(body: Any) -> str:
    return repr(body)[:_PREVIEW_MAX_LEN]


def _as_str(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _as_int(value: Any) -> int | None:
    # tolerant reader: поле может отсутствовать или приехать строкой.
    # bool отсекаем явно, потому что в Python bool - подкласс int
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().lstrip("-").isdigit():
        return int(value)
    return None


def _first_str(source: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        if (found := _as_str(source.get(key))) is not None:
            return found
    return None


def _extract_correlation(original: Any) -> tuple[str | None, str | None]:
    """
    Достаёт sagaId/businessKey из original.metadata.

    Конверты у команды и события разные (contracts/README): у команды корреляция
    лежит прямо в metadata, у события - вложена в metadata.correlation. Читаем оба
    варианта и оба стиля именования, потому что watcher обязан работать с ЛЮБЫМ
    мёртвым сообщением, включая пришедшее от сервиса, который конверт и нарушил.
    Корреляции может не быть вовсе - это не ошибка, просто не свяжем с сагой.
    """
    if not isinstance(original, dict):
        return None, None

    metadata = original.get("metadata")
    if not isinstance(metadata, dict):
        return None, None

    correlation = metadata.get("correlation")
    source = correlation if isinstance(correlation, dict) else metadata

    saga_id = _first_str(source, "sagaId", "saga_id")
    business_key = _first_str(source, "businessKey", "business_key")
    return saga_id, business_key


def parse_dlq_envelope(topic: str, body: Any) -> DlqRecord:
    """Собирает DlqRecord из сырого тела. На любом мусоре бросает DlqEnvelopeError."""
    if isinstance(body, bytes | bytearray | str):
        try:
            payload = json.loads(body)
        except (ValueError, UnicodeDecodeError) as error:
            raise DlqEnvelopeError(f"body is not valid JSON: {error}") from error
    else:
        payload = body

    if not isinstance(payload, dict):
        raise DlqEnvelopeError("envelope is not a JSON object")

    dlq_meta = payload.get("dlqMeta")
    if not isinstance(dlq_meta, dict):
        raise DlqEnvelopeError("dlqMeta is missing or is not an object")

    # sourceTopic обязателен по схеме: без него неизвестно, куда переигрывать,
    # то есть конверт бесполезен для единственной операции, ради которой он нужен
    source_topic = _as_str(dlq_meta.get("sourceTopic"))
    if source_topic is None:
        raise DlqEnvelopeError("dlqMeta.sourceTopic is missing")

    saga_id, business_key = _extract_correlation(payload.get("original"))

    # остальные поля по схеме тоже required, но их отсутствие не мешает
    # показать сообщение дежурному -> подставляем заглушки, а не падаем
    return DlqRecord(
        dlq_topic=topic,
        source_topic=source_topic,
        error_class=_as_str(dlq_meta.get("errorClass")) or "unknown",
        error_message=_as_str(dlq_meta.get("errorMessage")) or "",
        retry_count=_as_int(dlq_meta.get("retryCount")) or 0,
        redrive_count=_as_int(dlq_meta.get("redriveCount")) or 0,
        failed_at=_as_str(dlq_meta.get("failedAt")),
        partition=_as_int(dlq_meta.get("partition")),
        offset=_as_int(dlq_meta.get("offset")),
        consumer_group=_as_str(dlq_meta.get("consumerGroup")),
        saga_id=saga_id,
        business_key=business_key,
    )


class DlqService:
    def __init__(
        self,
        alert_sink: AlertSinkProtocol,
        metrics: DlqMetricsProtocol,
    ) -> None:
        self._alert_sink = alert_sink
        self._metrics = metrics

    async def handle(self, topic: str, body: Any) -> None:
        """
        Обрабатывает одно сообщение из <топик>.dlq: метрика -> лог -> алерт.

        Битый конверт не пробрасывается наружу (см. docstring модуля): он логируется
        как dlq_envelope_invalid и уходит в алерт отдельным каналом.
        """
        # метрику инкрементируем ДО разбора: факт попадания сообщения в DLQ ценен
        # сам по себе, даже если конверт нечитаем. Иначе именно самые сломанные
        # сообщения не попадут в dlq_messages_total и график покажет ложный ноль
        self._metrics.observe_dlq_message(topic)

        try:
            record = parse_dlq_envelope(topic, body)
        except DlqEnvelopeError as error:
            logger.error(
                "dlq_envelope_invalid",
                topic=topic,
                reason=str(error),
                body_preview=_preview(body),
            )
            await self._alert_sink.alert_invalid(
                topic=topic,
                reason=str(error),
                body_preview=_preview(body),
            )
            return

        # ERROR, а не WARNING: любое сообщение в DLQ - это невыполненный шаг саги
        logger.error(
            "dlq_message_received",
            topic=record.dlq_topic,
            source_topic=record.source_topic,
            error_class=record.error_class,
            error_message=record.error_message,
            retry_count=record.retry_count,
            redrive_count=record.redrive_count,
            saga_id=record.saga_id,
            business_key=record.business_key,
            partition=record.partition,
            offset=record.offset,
            consumer_group=record.consumer_group,
            failed_at=record.failed_at,
        )
        await self._alert_sink.alert(record)
