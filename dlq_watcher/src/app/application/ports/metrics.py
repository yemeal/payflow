from typing import Protocol


class DlqMetricsProtocol(Protocol):
    """
    Счётчики watcher'а.

    Порт нужен, чтобы сервис не зависел от prometheus_client: в тестах вместо
    реального Counter подставляется список-накопитель.
    """

    def observe_dlq_message(self, topic: str) -> None:
        """Инкремент dlq_messages_total{topic} - по факту получения, до разбора конверта."""
        ...
