import uuid
from typing import Any, Protocol


class OrderCacheProtocol(Protocol):
    """
    Порт кэша заказов (Cache-Aside). Реализация - infrastructure/cache/.

    Кэш - оптимизация, а не источник правды (источник правды - БД).
    Контракт обязывает реализацию к graceful degradation: недоступный кэш
    не роняет запрос, get при ошибке просто возвращает None.
    """

    async def get(self, order_id: uuid.UUID) -> dict[str, Any] | None: ...

    async def set(self, order_id: uuid.UUID, payload: dict[str, Any]) -> None: ...

    async def invalidate(self, order_id: uuid.UUID) -> None:
        """Вызывается при смене статуса заказа (события saga.completed/cancelled)"""
        ...
