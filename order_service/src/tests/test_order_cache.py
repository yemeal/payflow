"""
Юнит-тесты RedisOrderCache: Cache-Aside поверх Redis с graceful degradation.

Ключевой инвариант адаптера: кэш - оптимизация, а не источник правды. Любая
ошибка Redis логируется и глотается; get при сбое возвращает None (запрос
обслужится из БД), set/invalidate при сбое не роняют вызывающий код.

Формат docstring: Проверяем / Успех / Нежелательное поведение.
"""

from __future__ import annotations

import uuid

from app.infrastructure.cache.redis_order_cache import RedisOrderCache


class DictRedis:
    """Рабочий stub Redis: get/set/delete поверх словаря (ex игнорируем)."""

    def __init__(self) -> None:
        self.store: dict[str, str] = {}

    async def get(self, key: str) -> str | None:
        return self.store.get(key)

    async def set(self, key: str, value: str, ex: int | None = None) -> None:
        self.store[key] = value

    async def delete(self, key: str) -> None:
        self.store.pop(key, None)


class RaisingRedis:
    """Недоступный Redis: любая операция бросает исключение."""

    async def get(self, key: str) -> str | None:
        raise ConnectionError("redis down")

    async def set(self, key: str, value: str, ex: int | None = None) -> None:
        raise ConnectionError("redis down")

    async def delete(self, key: str) -> None:
        raise ConnectionError("redis down")


class TestRoundTrip:
    async def test_set_then_get_returns_payload(self):
        """
        Проверяем: положенное в кэш значение затем читается обратно.
        Успех: get возвращает тот же payload, что был передан в set.
        Нежелательное поведение: потеря или искажение записи при сериализации.
        """
        cache = RedisOrderCache(DictRedis(), ttl_seconds=60)
        order_id = uuid.uuid4()
        payload = {"id": str(order_id), "status": "PENDING"}

        await cache.set(order_id, payload)
        result = await cache.get(order_id)

        assert result == payload

    async def test_miss_returns_none(self):
        """
        Проверяем: чтение отсутствующего ключа.
        Успех: get возвращает None (промах, вызывающий пойдёт в БД).
        Нежелательное поведение: исключение или пустой объект вместо None.
        """
        cache = RedisOrderCache(DictRedis(), ttl_seconds=60)

        assert await cache.get(uuid.uuid4()) is None

    async def test_invalidate_removes_entry(self):
        """
        Проверяем: инвалидация удаляет запись из кэша.
        Успех: после invalidate get по тому же ключу даёт None.
        Нежелательное поведение: устаревший статус продолжает отдаваться из кэша.
        """
        cache = RedisOrderCache(DictRedis(), ttl_seconds=60)
        order_id = uuid.uuid4()
        await cache.set(order_id, {"status": "PENDING"})

        await cache.invalidate(order_id)

        assert await cache.get(order_id) is None

    async def test_corrupted_entry_returns_none(self):
        """
        Проверяем: в кэше лежит не-JSON мусор.
        Успех: get возвращает None (битую запись добьёт TTL), без исключения.
        Нежелательное поведение: падение сериализации роняет запрос.
        """
        redis = DictRedis()
        cache = RedisOrderCache(redis, ttl_seconds=60)
        order_id = uuid.uuid4()
        redis.store[f"order:{order_id}"] = "{not-json"

        assert await cache.get(order_id) is None


class TestGracefulDegradation:
    async def test_get_on_redis_failure_returns_none(self):
        """
        Проверяем: недоступный Redis на чтении.
        Успех: get возвращает None вместо проброса ошибки (запрос уйдёт в БД).
        Нежелательное поведение: 500 у API из-за упавшего кэша.
        """
        cache = RedisOrderCache(RaisingRedis(), ttl_seconds=60)

        assert await cache.get(uuid.uuid4()) is None

    async def test_set_on_redis_failure_is_swallowed(self):
        """
        Проверяем: недоступный Redis на записи.
        Успех: set не бросает - ответ уже сформирован из БД.
        Нежелательное поведение: сбой прогрева кэша срывает успешный запрос.
        """
        cache = RedisOrderCache(RaisingRedis(), ttl_seconds=60)

        await cache.set(uuid.uuid4(), {"status": "PENDING"})  # не должно бросить

    async def test_invalidate_on_redis_failure_is_swallowed(self):
        """
        Проверяем: недоступный Redis на инвалидации.
        Успех: invalidate не бросает - устаревшую запись позже добьёт TTL.
        Нежелательное поведение: сбой инвалидации откатывает уже закоммиченную смену статуса.
        """
        cache = RedisOrderCache(RaisingRedis(), ttl_seconds=60)

        await cache.invalidate(uuid.uuid4())  # не должно бросить
