import structlog
from typing import Protocol
from redis.asyncio import Redis

logger = structlog.get_logger()


class CacheServiceProtocol(Protocol):
    async def get(self, key: str) -> str | None: ...
    async def set(self, key: str, value: str, expire: int) -> None: ...
    async def delete_by_pattern(self, pattern: str) -> None: ...


class RedisCacheService:
    def __init__(self, redis: Redis):
        self._redis = redis

    async def get(self, key: str) -> str | None:
        try:
            return await self._redis.get(key)
        except Exception as e:
            logger.error("redis_get_failed", key=key, error=str(e))
            return None

    async def set(self, key: str, value: str, expire: int) -> None:
        try:
            await self._redis.set(key, value, ex=expire)
        except Exception as e:
            logger.error("redis_set_failed", key=key, error=str(e))

    async def delete_by_pattern(self, pattern: str) -> None:
        try:
            # SCAN for keys matching the pattern
            cursor = b"0"
            while cursor:
                cursor, keys = await self._redis.scan(
                    cursor=cursor, match=pattern, count=100
                )
                if keys:
                    await self._redis.delete(*keys)
        except Exception as e:
            logger.error(
                "redis_delete_by_pattern_failed", pattern=pattern, error=str(e)
            )
