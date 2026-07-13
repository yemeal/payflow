from typing import Protocol

from app.application.ports.dto import DlqRecord


class AlertSinkProtocol(Protocol):
    """
    Канал доставки алерта дежурному.

    В MVP реализация одна - LoggingAlertSink (ERROR-лог с префиксом ALERT).
    Порт существует ради подмены: PagerDuty/Sentry/Slack встанут сюда же,
    не трогая ни сервис, ни entrypoint.
    """

    async def alert(self, record: DlqRecord) -> None:
        """Сообщение легло в DLQ: конверт разобран, известно что и почему умерло."""
        ...

    async def alert_invalid(self, topic: str, reason: str, body_preview: str) -> None:
        """
        Конверт нечитаем (не JSON / нет dlqMeta).

        Отдельный метод, а не DlqRecord с заглушками: это не "сообщение умерло",
        а "сервис-отправитель нарушил contracts/" - другой класс проблемы и другой
        адресат разбора. Молчать про такое нельзя: иначе именно самые сломанные
        сообщения окажутся невидимыми, а ради видимости watcher и существует.
        """
        ...
