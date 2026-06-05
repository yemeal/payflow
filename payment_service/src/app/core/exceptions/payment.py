from app.core.exceptions.base import AppError

class PaymentError(AppError):
    """Базовый класс для ошибок, связанных с платежом"""

class PaymentNotFoundError(PaymentError):
    """Платеж не найден"""
    ...
