from typing import Any, Protocol


class CommandCorrelationStoreProtocol(Protocol):
    """
    Транспортная корреляция команд саги (contracts/README, правило 1).

    Это НЕ доменное знание платежа: домену безразлично, кто и в рамках какого
    процесса его инициировал. Поэтому correlation не живёт ни в Payment,
    ни в OutboxEvent - только в отдельном инфраструктурном журнале сообщений.

    Зачем журнал вообще нужен: payment.completed / payment.failed рождаются не
    в обработке команды, а позже - в reconciliation-цикле (sync_payment_with_provider),
    где контекста входящего сообщения уже нет. Correlation обязана пережить это время.

    Ключ - command_id (он же idempotency_key платежа): запись делается ДО создания
    платежа, поэтому ни одно событие о платеже не может быть опубликовано раньше,
    чем его correlation станет доступна.
    """

    async def remember(self, command_id: str, correlation: dict[str, Any]) -> None:
        """Идемпотентно (ON CONFLICT DO NOTHING): переигранная команда не ломает запись"""
        ...

    async def resolve_for_payment(self, payment_id: str) -> dict[str, Any] | None:
        """Correlation платежа: payment.id -> payment.idempotency_key (= command_id) -> correlation.

        None - платёж создан вне саги (HTTP API), события идут без correlation."""
        ...
