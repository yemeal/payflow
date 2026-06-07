from typing import Callable, Awaitable

import structlog
from redis.asyncio import Redis

from app.core.exceptions import RedisUnavailableError, IdempotencyKeyPayloadMismatchError, \
    IdempotencyKeyAlreadyProcessingError
from app.core.settings import Settings
from app.services.idempotency.enums import IdempotencyKeyStatus, LockStatus
from app.services.idempotency.schemas import IdempotencyCachedResult, IdempotencyEntry
from app.utils.compute_payload_hash import compute_payload_hash

logger = structlog.get_logger()

_ACQUIRE_LOCK_SCRIPT = """
local current = redis.call('GET', KEYS[1])
if current then
    return {'EXISTS', current}
end
redis.call('SET', KEYS[1], ARGV[1], 'EX', tonumber(ARGV[2]))
return {'LOCKED'}
"""

# Удаляет ключ ТОЛЬКО если текущее значение содержит status="processing"
# Это защита: не удалить чужой result, если lock уже истёк и другой запрос записал результат
_RELEASE_LOCK_SCRIPT = """
local current = redis.call('GET', KEYS[1])
if not current then
    return 0
end
-- проверяем что текущий payload_hash совпадает с нашим (мы удаляем именно свой lock)
if current == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""

class IdempotencyGuard:
    """
    Контекстный менеджер для идемпотентной обработки запросов

    Пример применения:
    ```python
        async with IdempotencyService() as service:
            if service.has_cached_result:
                return service.cached_response
            result = await business_logic()
            service.set_result(status_code=201, response=result)
            return result
    ```

    db_lookup - функция, которая принимает на вход ключ идемпотентности, возвращает dict или None
    """
    def __init__(
            self,
            settings: Settings,
            redis: Redis,
            idempotency_key: str,
            payload: dict,
            db_lookup: Callable[[str], Awaitable[IdempotencyCachedResult | None]] | None = None,
    ) -> None:
        self._payload_hash = compute_payload_hash(payload)
        self._redis = redis
        self._idempotency_key = idempotency_key

        self._cached_response: dict | None = None
        self._cached_status_code: int | None = None
        self._result_set: bool = False
        self._lock_acquired: bool = False
        self._lock_entry: IdempotencyEntry = IdempotencyEntry(
            status=IdempotencyKeyStatus.PROCESSING,
            payload_hash=self._payload_hash
        )
        self._lock_value: str = self._lock_entry.model_dump_json()
        self._lock_ttl = settings.IDEMPOTENCY_LOCK_TTL
        self._result_ttl = settings.IDEMPOTENCY_RESULT_TTL

        self._db_lookup = db_lookup

    @property
    def redis_idempotency_key(self) -> str:
        return f'idempotency:{self._idempotency_key}'

    @property
    def has_cached_result(self) -> bool:
        """Есть ли кешированный результат (из редиса или бд)"""
        return self._cached_response is not None

    @property
    def cached_response(self) -> dict | None:
        """Кешированный response (JSON-serializable dict)"""
        return self._cached_response

    @property
    def cached_status_code(self) -> int | None:
        """HTTP-статус кешированного ответа"""
        return self._cached_status_code

    def set_result(self, status_code: int, response: dict) -> None:
        """Вызывается после успешной бизнес-логики"""
        self._result_set = True
        self._cached_status_code = status_code
        self._cached_response = response


    async def _acquire_lock(self) -> str:
        result = await self._redis.eval(
            _ACQUIRE_LOCK_SCRIPT,
            1,
            self.redis_idempotency_key,  # KEYS[1]
            self._lock_value,  # ARGV[1]
            str(self._lock_ttl),  # ARGV[2]
        )
        return result

    async def _release_lock(self) -> str:
        result = await self._redis.eval(
            _RELEASE_LOCK_SCRIPT,
            1,
            self.redis_idempotency_key,
            self._lock_value,
        )
        return result

    async def __aenter__(self) -> 'IdempotencyGuard':
        try:
            lock_acquire_result = await self._acquire_lock()
        except Exception as e:
            logger.error(
                "redis_unavailable_when_acquiring_lock",
                idempotency_key=self._idempotency_key,
                error=str(e),
            )
            raise RedisUnavailableError() from e

        # разве вот так хардкодить нормально, можно ли это сделать как-то элегантнее?
        match lock_acquire_result[0]:
            case LockStatus.LOCKED.value:
                self._lock_acquired = True

                # Идем в БД проверять наличие готового рез-та с таким же ключом идемпотентности
                # в случае, если нам была передана подходящая функция
                if self._db_lookup is not None:
                    existing = await self._db_lookup(self._idempotency_key)
                    if existing is not None:
                        self._cached_response = existing.response
                        self._cached_status_code = existing.status_code
                        logger.info(
                            "idempotency_cache_miss_found_in_db",
                            idempotency_key=self._idempotency_key
                        )
                    else:
                        logger.info(
                            "idempotency_lock_acquired_not_found_in_db",
                            idempotency_key=self._idempotency_key,
                        )
                else:
                    logger.info(
                        "idempotency_lock_acquired_without_db_lookup",
                        idempotency_key=self._idempotency_key,
                    )
            case LockStatus.EXISTS.value:
                # lock_acquire_result[1] - JSON-строка из Редиса
                entry = IdempotencyEntry.model_validate_json(lock_acquire_result[1])

                if entry.status == IdempotencyKeyStatus.PROCESSING:
                    raise IdempotencyKeyAlreadyProcessingError

                if entry.status == IdempotencyKeyStatus.DONE:
                    if entry.payload_hash == self._payload_hash:
                        self._cached_response = entry.response
                        self._cached_status_code = entry.status_code
                        logger.info(
                            "idempotency_cache_hit",
                            idempotency_key=self._idempotency_key
                        )
                    else:
                        raise IdempotencyKeyPayloadMismatchError
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        # в случае ошибки освобождаем блокировку
        if exc_type is not None and self._lock_acquired:
            try:
                await self._release_lock()
                logger.warning(
                    "idempotency_lock_released_due_to_error",
                    idempotency_key=self._idempotency_key,
                    error_type=exc_type.__name__ if exc_type else None,
                    error=str(exc_val),
                )
                return
            except Exception as e:
                logger.warning(
                    "redis_unavailable_when_releasing_lock",
                    idempotency_key=self._idempotency_key,
                    error=str(e),
                )

        # при успехе и полученном результате - кешируем его
        if self._result_set and self._lock_acquired:
            result_value: str = IdempotencyEntry(
                status=IdempotencyKeyStatus.DONE,
                payload_hash=self._payload_hash,
                status_code=self._cached_status_code,
                response=self._cached_response,
            ).model_dump_json()

            try:
                await self._redis.set(
                    self.redis_idempotency_key,
                    result_value,
                    ex=self._result_ttl
                )
                logger.info(
                    "idempotency_result_cached",
                    idempotency_key=self._idempotency_key,
                    ttl=self._result_ttl,
                )
            except Exception as e:
                # Redis упал при попытке записать в кэш, но результат уже создан в БД
                # и при повторном обращении возьмет уже готовый и снова попробует его закешировать.
                # Поэтому идемпотентность будет соблюдена, даже без записи кэша
                logger.warning(
                    "unsuccessful_idempotency_result_cache_write",
                    error=str(e),
                    idempotency_key=self._idempotency_key,
                    status_code=self._cached_status_code,
                )