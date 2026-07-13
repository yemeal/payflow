from app.domain.exceptions.base import AppError


class OrderError(AppError):
    """Базовая ошибка домена заказов"""


class OrderNotFoundError(OrderError):
    """
    Заказ не найден (HTTP 404).
    Бросается и на чужой заказ: 404 вместо 403, чтобы не раскрывать
    существование ресурса другому пользователю.
    """


class OrderCancellationNotAllowedError(OrderError):
    """Отмена возможна только из статуса PENDING (HTTP 409)"""
