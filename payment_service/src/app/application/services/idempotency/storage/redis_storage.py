import structlog
from redis.asyncio import Redis
import redis.exceptions

from app.infrastructure.exceptions.redis import RedisUnavailableError
from app.application.services.idempotency.domain import AcquireLockResult, IdempotencyEntry
from app.application.services.idempotency.enums import LockAcquireStatus
from app.application.services.idempotency.protocols import IdempotencyStorageProtocol

logger = structlog.get_logger()


class RedisIdempotencyStorage(IdempotencyStorageProtocol):
    """
    Реализация IdempotencyStorageProtocol через Redis + Lua
    """

    _ACQUIRE_LOCK_SCRIPT = """
local current = redis.call('GET', KEYS[1])
if current then
    return {2, current}
end
redis.call('SET', KEYS[1], ARGV[1], 'EX', tonumber(ARGV[2]))
return {1}
"""

    # Удаляет ключ ТОЛЬКО если текущее значение содержит status="processing"
    # Это защита: не удалить чужой result, если lock уже истёк и другой запрос записал результат
    _RELEASE_LOCK_SCRIPT = """
local current = redis.call('GET', KEYS[1])
if not current then
    return 0
end
if current == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""

    def __init__(self, redis_client: Redis) -> None:
        self._redis = redis_client

    async def acquire_lock(
        self, key: str, lock_value: str, ttl: int
    ) -> AcquireLockResult:
        try:
            raw = await self._redis.eval(
                self._ACQUIRE_LOCK_SCRIPT, 1, key, lock_value, str(ttl)
            )
            return self._parse_acquire_result(raw)
        except redis.exceptions.RedisError as e:
            logger.error(
                "redis_unavailable_when_acquiring_lock",
                key=key,
                error=str(e),
            )
            raise RedisUnavailableError() from e

    async def release_lock(self, key: str, expected_value: str) -> bool:
        try:
            result = await self._redis.eval(
                self._RELEASE_LOCK_SCRIPT, 1, key, expected_value
            )
            return bool(result)
        except redis.exceptions.RedisError as e:
            logger.warning(
                "redis_unavailable_when_releasing_lock",
                key=key,
                error=str(e),
            )
            raise RedisUnavailableError() from e

    async def save_result(self, key: str, entry: IdempotencyEntry, ttl: int) -> None:
        try:
            await self._redis.set(key, entry.model_dump_json(), ex=ttl)
            logger.info(
                "idempotency_result_cached",
                key=key,
                ttl=ttl,
            )
        except redis.exceptions.RedisError as e:
            logger.warning(
                "unsuccessful_idempotency_result_cache_write",
                error=str(e),
                key=key,
                status_code=entry.status_code,
            )
            # Мы не кидаем ошибку здесь, чтобы не сломать успешную операцию

    @staticmethod
    def _parse_acquire_result(raw: list) -> AcquireLockResult:
        """
        парсит сырой ответ от Redis (Lua) в доменную модель AcquireLockResult.
        """
        status_code = int(raw[0])
        status = LockAcquireStatus(status_code)

        if status == LockAcquireStatus.ENTRY_EXISTS:
            entry = IdempotencyEntry.model_validate_json(raw[1])
            return AcquireLockResult(status=status, existing_entry=entry)

        return AcquireLockResult(status=status)
