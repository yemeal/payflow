import redis.exceptions
import structlog
from redis.asyncio import Redis

from app.application.exceptions.idempotency import (
    IdempotencyStorageUnavailableError,
)
from app.application.services.idempotency.domain import (
    AcquireLockResult,
    IdempotencyEntry,
)
from app.application.services.idempotency.enums import LockAcquireStatus
from app.application.services.idempotency.protocols import (
    IdempotencyStorageProtocol,
)

logger = structlog.get_logger()


class RedisIdempotencyStorage(IdempotencyStorageProtocol):
    """Redis + Lua adapter из payment_service."""

    _ACQUIRE_LOCK_SCRIPT = """
local current = redis.call('GET', KEYS[1])
if current then
    return {2, current}
end
redis.call('SET', KEYS[1], ARGV[1], 'EX', tonumber(ARGV[2]))
return {1}
"""

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
        self,
        key: str,
        lock_value: str,
        ttl: int,
    ) -> AcquireLockResult:
        try:
            raw = await self._redis.eval(
                self._ACQUIRE_LOCK_SCRIPT,
                1,
                key,
                lock_value,
                str(ttl),
            )
            return self._parse_acquire_result(raw)
        except redis.exceptions.RedisError as error:
            logger.error(
                "idempotency storage unavailable while acquiring lock",
                storage_key=key,
                error_type=type(error).__name__,
            )
            raise IdempotencyStorageUnavailableError() from error

    async def release_lock(
        self,
        key: str,
        expected_value: str,
    ) -> bool:
        try:
            result = await self._redis.eval(
                self._RELEASE_LOCK_SCRIPT,
                1,
                key,
                expected_value,
            )
            return bool(result)
        except redis.exceptions.RedisError as error:
            logger.error(
                "idempotency storage unavailable while releasing lock",
                storage_key=key,
                error_type=type(error).__name__,
            )
            raise IdempotencyStorageUnavailableError() from error

    async def save_result(
        self,
        key: str,
        entry: IdempotencyEntry,
        ttl: int,
    ) -> None:
        try:
            await self._redis.set(
                key,
                entry.model_dump_json(),
                ex=ttl,
            )
            logger.debug(
                "idempotency result cached",
                storage_key=key,
                ttl=ttl,
            )
        except redis.exceptions.RedisError as error:
            # Бизнес-транзакция refresh уже могла закоммититься. Поэтому нельзя
            # превращать успешно выпущенную пару в 503 только из-за cache write.
            # Это тот же best-effort контракт, который используется в payment_service.
            logger.warning(
                "idempotency storage unavailable while saving result",
                storage_key=key,
                error_type=type(error).__name__,
            )

    @staticmethod
    def _parse_acquire_result(raw: list[object]) -> AcquireLockResult:
        status = LockAcquireStatus(int(raw[0]))
        if status is LockAcquireStatus.ENTRY_EXISTS:
            entry = IdempotencyEntry.model_validate_json(raw[1])
            return AcquireLockResult(
                status=status,
                existing_entry=entry,
            )
        return AcquireLockResult(status=status)
