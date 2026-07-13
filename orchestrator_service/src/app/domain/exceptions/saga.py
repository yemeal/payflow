from app.domain.exceptions.base import AppError


class SagaError(AppError):
    """Базовая ошибка домена саги"""


class SagaNotFoundError(SagaError):
    """Событие ссылается на сагу, которой нет в saga_state"""


class InvalidSagaTransitionError(SagaError):
    """Запрошен переход, недопустимый из текущего статуса саги"""
