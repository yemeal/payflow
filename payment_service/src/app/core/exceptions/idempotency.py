from app.core.exceptions.base import AppError


class IdempotencyError(AppError):
    """Базовый класс для ошибок идемпотентости"""

    ...


class IdempotencyKeyAlreadyProcessingError(IdempotencyError):
    """Запрос с таким ключом уже обрабатывается (стоит лок в редисе)"""

    def __init__(self, message: str = "Запрос с этим ключом уже обрабатывается"):
        super().__init__(message)


class IdempotencyKeyPayloadMismatchError(IdempotencyError):
    """Ключ переиспользован с другим пейлоадом (конрктная привязка ключа к пейлоаду)"""

    def __init__(
        self, message: str = "Ключ идемпотентности использован с другим payload"
    ):
        super().__init__(message)


class IdempotencyStateInconsistencyError(IdempotencyError):
    """Внутренняя ошибка сервиса идемпотентности: несогласованность данных/состояний"""

    def __init__(self, message: str = "Нарушена консистентность состояния идемпотентности"):
        super().__init__(message)
