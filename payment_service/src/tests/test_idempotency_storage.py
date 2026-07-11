"""
Тесты RedisIdempotencyStorage - адаптер уровня 1 (Redis+Lua) для идемпотентности.

Redis не поднимаем: подменяем клиента AsyncMock и проверяем контракт адаптера -
разбор ответа Lua-скрипта в доменную модель и корректную обработку недоступности Redis.

Ключевые инварианты:
  - Lua возвращает числовые коды (1 = lock acquired, 2 = entry exists),
    адаптер превращает их в AcquireLockResult;
  - недоступность Redis при acquire/release -> RedisUnavailableError (наверх, 503);
  - сбой при save_result НЕ роняет операцию (результат уже отдан клиенту).

Формат документации: Проверяем / Успех / Нежелательное поведение.
"""

import pytest
from unittest.mock import AsyncMock

import redis.exceptions

from app.application.services.idempotency.storage.redis_storage import (
    RedisIdempotencyStorage,
)
from app.application.services.idempotency.domain import IdempotencyEntry
from app.application.services.idempotency.enums import (
    LockAcquireStatus,
    IdempotencyKeyStatus,
)
from app.infrastructure.exceptions.redis import RedisUnavailableError


def make_storage(eval_return=None, eval_side_effect=None, set_side_effect=None):
    redis_client = AsyncMock()
    if eval_side_effect is not None:
        redis_client.eval.side_effect = eval_side_effect
    else:
        redis_client.eval.return_value = eval_return
    if set_side_effect is not None:
        redis_client.set.side_effect = set_side_effect
    return RedisIdempotencyStorage(redis_client), redis_client


# ---------------------------------------------------------------------------
# acquire_lock
# ---------------------------------------------------------------------------

class TestAcquireLock:
    @pytest.mark.asyncio
    async def test_lock_acquired(self):
        """
        Проверяем: Lua вернул код 1 (лок захвачен).
        Успех: статус LOCK_ACQUIRED, existing_entry отсутствует.
        Нежелательное поведение: неверный разбор кода, ложный ENTRY_EXISTS.
        """
        storage, _ = make_storage(eval_return=[1])

        result = await storage.acquire_lock("k", "lock-value", ttl=60)

        assert result.status == LockAcquireStatus.LOCK_ACQUIRED
        assert result.existing_entry is None

    @pytest.mark.asyncio
    async def test_entry_exists_parses_existing_entry(self):
        """
        Проверяем: Lua вернул код 2 и текущее значение ключа (JSON записи).
        Успех: статус ENTRY_EXISTS, existing_entry распарсен в IdempotencyEntry.
        Нежелательное поведение: потеря существующей записи (guard не отличит
                   PROCESSING от DONE и сломает идемпотентность).
        """
        existing = IdempotencyEntry(
            status=IdempotencyKeyStatus.DONE,
            payload_hash="hash-1",
            status_code=201,
            response={"id": "p-1"},
        )
        storage, _ = make_storage(eval_return=[2, existing.model_dump_json()])

        result = await storage.acquire_lock("k", "lock-value", ttl=60)

        assert result.status == LockAcquireStatus.ENTRY_EXISTS
        assert result.existing_entry.status == IdempotencyKeyStatus.DONE
        assert result.existing_entry.response == {"id": "p-1"}

    @pytest.mark.asyncio
    async def test_redis_error_raises_unavailable(self):
        """
        Проверяем: Redis недоступен при захвате лока.
        Успех: техническая ошибка Redis превращается в доменную RedisUnavailableError
               (обработчик отдаст 503).
        Нежелательное поведение: сырое исключение redis наружу или молчаливый проход
                   без лока (риск двойной обработки).
        """
        storage, _ = make_storage(
            eval_side_effect=redis.exceptions.ConnectionError("down")
        )

        with pytest.raises(RedisUnavailableError):
            await storage.acquire_lock("k", "lock-value", ttl=60)


# ---------------------------------------------------------------------------
# release_lock
# ---------------------------------------------------------------------------

class TestReleaseLock:
    @pytest.mark.asyncio
    async def test_released_returns_true(self):
        """
        Проверяем: Lua-скрипт удалил наш лок (вернул 1).
        Успех: release_lock возвращает True.
        Нежелательное поведение: неверная интерпретация ответа Lua.
        """
        storage, _ = make_storage(eval_return=1)
        assert await storage.release_lock("k", "lock-value") is True

    @pytest.mark.asyncio
    async def test_not_owned_returns_false(self):
        """
        Проверяем: лок принадлежит уже не нам (Lua вернул 0, compare-and-delete не сработал).
        Успех: release_lock возвращает False, чужой результат не затирается.
        Нежелательное поведение: удаление чужого значения (гонка при протухшем TTL).
        """
        storage, _ = make_storage(eval_return=0)
        assert await storage.release_lock("k", "lock-value") is False

    @pytest.mark.asyncio
    async def test_redis_error_raises_unavailable(self):
        """
        Проверяем: Redis недоступен при освобождении лока.
        Успех: поднимается RedisUnavailableError.
        Нежелательное поведение: сырое исключение redis наружу.
        """
        storage, _ = make_storage(
            eval_side_effect=redis.exceptions.ConnectionError("down")
        )
        with pytest.raises(RedisUnavailableError):
            await storage.release_lock("k", "lock-value")


# ---------------------------------------------------------------------------
# save_result
# ---------------------------------------------------------------------------

class TestSaveResult:
    @pytest.mark.asyncio
    async def test_saves_entry_with_ttl(self):
        """
        Проверяем: успешное сохранение результата.
        Успех: redis.set вызван с сериализованной записью и переданным TTL.
        Нежелательное поведение: сохранение без TTL (кэш пухнет вечно) или потеря записи.
        """
        entry = IdempotencyEntry(
            status=IdempotencyKeyStatus.DONE, payload_hash="h", status_code=201,
            response={"id": "p"},
        )
        storage, redis_client = make_storage()

        await storage.save_result("k", entry, ttl=3600)

        redis_client.set.assert_awaited_once()
        _, kwargs = redis_client.set.call_args
        assert kwargs.get("ex") == 3600

    @pytest.mark.asyncio
    async def test_redis_error_is_swallowed(self):
        """
        Проверяем: Redis упал при сохранении результата.
        Успех: исключение НЕ пробрасывается - основная операция уже успешна,
               кэш восстановится позже (или подхватится через db_lookup).
        Нежелательное поведение: падение успешно обработанного запроса из-за проблем кэша.
        """
        entry = IdempotencyEntry(
            status=IdempotencyKeyStatus.DONE, payload_hash="h", status_code=201,
            response={"id": "p"},
        )
        storage, _ = make_storage(
            set_side_effect=redis.exceptions.ConnectionError("down")
        )

        # не должно поднять исключение
        await storage.save_result("k", entry, ttl=3600)
