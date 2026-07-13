import json
import uuid
from typing import Any

import structlog
from redis.asyncio import Redis

logger = structlog.get_logger()

_KEY_PREFIX = "order:"


class RedisOrderCache:
    """
    Адаптер OrderCacheProtocol на Redis (Cache-Aside).

    Graceful degradation: любая ошибка Redis логируется warning'ом и глотается.
    Кэш - оптимизация; при недоступном Redis запрос обслуживается из БД,
    API не должно отвечать 500 из-за кэша.

    В кэше лежит JSON: Decimal и datetime сериализуются строками ещё до set
    (клиент кладёт model_dump(mode="json")).
    """

    def __init__(self, redis: Redis, ttl_seconds: int) -> None:
        self._redis = redis
        # TTL - страховка на случай несработавшей инвалидации
        self._ttl = ttl_seconds

    @staticmethod
    def _key(order_id: uuid.UUID) -> str:
        return f"{_KEY_PREFIX}{order_id}"

    async def get(self, order_id: uuid.UUID) -> dict[str, Any] | None:
        try:
            raw = await self._redis.get(self._key(order_id))
        except Exception:
            logger.warning(
                "order_cache_get_failed", order_id=str(order_id), exc_info=True
            )
            return None

        if raw is None:
            return None

        try:
            return json.loads(raw)
        except (TypeError, ValueError):
            logger.warning("order_cache_corrupted_entry", order_id=str(order_id))
            return None

    async def set(self, order_id: uuid.UUID, payload: dict[str, Any]) -> None:
        try:
            await self._redis.set(
                self._key(order_id), json.dumps(payload), ex=self._ttl
            )
        except Exception:
            # ответ уже сформирован из БД, поэтому просто предупреждаем
            logger.warning(
                "order_cache_set_failed", order_id=str(order_id), exc_info=True
            )

    async def invalidate(self, order_id: uuid.UUID) -> None:
        try:
            await self._redis.delete(self._key(order_id))
        except Exception:
            # инвалидация не удалась - устаревшую запись добьёт TTL
            logger.warning(
                "order_cache_invalidate_failed", order_id=str(order_id), exc_info=True
            )
