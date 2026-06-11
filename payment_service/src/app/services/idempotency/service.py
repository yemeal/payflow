from typing import Awaitable, Callable

from redis.asyncio import Redis

from app.core.settings import Settings
from app.services.idempotency import IdempotencyCachedResult, IdempotencyGuard


class IdempotencyService:
    """
    Фабрика IdempotencyGuard объектов, инжектится через DI.
    Не знает про конкретные сузности
    """

    def __init__(
        self,
        redis: Redis,
        settings: Settings,
    ) -> None:
        self._redis: Redis = redis
        self._settings: Settings = settings

    def __call__(
        self,
        idempotency_key: str,
        payload: dict,
        db_lookup: (
            Callable[[str], Awaitable[IdempotencyCachedResult | None]] | None
        ) = None,
    ) -> IdempotencyGuard:
        return IdempotencyGuard(
            redis=self._redis,
            settings=self._settings,
            idempotency_key=idempotency_key,
            payload=payload,
            db_lookup=db_lookup,
        )
