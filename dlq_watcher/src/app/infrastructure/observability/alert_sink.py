"""
Мок алертинга: ERROR-лог с префиксом ALERT (docs/saga-design.md, 9.10).

Реального PagerDuty/Sentry в MVP нет, и это осознанно: канал доставки алерта
подменяется реализацией порта, а не переписыванием сервиса. Префикс ALERT в
message выбран так, чтобы на него вешалось лог-правило (grep/Loki/CloudWatch)
до появления настоящей интеграции.
"""

import structlog

from app.application.ports.dto import DlqRecord

logger = structlog.get_logger(__name__)


class LoggingAlertSink:
    async def alert(self, record: DlqRecord) -> None:
        logger.error(
            "ALERT dlq_message",
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

    async def alert_invalid(self, topic: str, reason: str, body_preview: str) -> None:
        logger.error(
            "ALERT dlq_envelope_invalid",
            topic=topic,
            reason=reason,
            body_preview=body_preview,
        )
