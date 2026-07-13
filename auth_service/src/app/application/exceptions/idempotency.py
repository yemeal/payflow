from typing import ClassVar


class IdempotencyError(Exception):
    """Базовая ошибка HTTP-механизма идемпотентности."""

    default_message: ClassVar[str] = "Idempotency request failed"

    def __init__(self) -> None:
        super().__init__(self.default_message)


class IdempotencyKeyAlreadyProcessingError(IdempotencyError):
    """Запрос с таким ключом уже выполняется."""

    default_message = "Request with this idempotency key is already processing"


class IdempotencyKeyPayloadMismatchError(IdempotencyError):
    """Один ключ использовали для разных payload."""

    default_message = "Idempotency key was used with another payload"


class IdempotencyStateInconsistencyError(IdempotencyError):
    """Storage вернул состояние, которое нарушает контракт guard."""

    default_message = "Idempotency state is inconsistent"


class IdempotencyStorageUnavailableError(IdempotencyError):
    """Хранилище идемпотентности недоступно."""

    default_message = "Idempotency storage is unavailable"
