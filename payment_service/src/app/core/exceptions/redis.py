from app.core.exceptions.base import AppError


class RedisError(AppError):
    """Базовый класс для ошибок редиса"""

    ...


class RedisUnavailableError(RedisError):
    """Редис не доступен"""

    def __init__(self, message: str = "Redis недоступен"):
        super().__init__(message)
