from app.domain.exceptions.base import AppError


class ProviderIntegrationError(AppError):
    """Базовая ошибка интеграции с платежным провайдером."""

    pass


class ProviderUnavailableError(ProviderIntegrationError):
    """Провайдер недоступен (Circuit Breaker находится в состоянии OPEN)."""

    pass
