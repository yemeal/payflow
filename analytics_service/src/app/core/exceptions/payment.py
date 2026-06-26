from app.core.exceptions.base import AppError

class PaymentNotFoundError(AppError):
    """Платеж не найден"""
    ...
