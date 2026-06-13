class AppError(Exception):
    """Базовое исключение приложения"""

    def __init__(self, message: str | None, details: str | None = None) -> None:
        self.message = message
        self.details = details
        super().__init__(message)
