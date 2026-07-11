"""
Тесты RedisCacheService - кэш сводной аналитики.

Redis не поднимаем: клиент - AsyncMock. Проверяем базовые операции и, главное,
отказоустойчивость: проблемы с кэшем НЕ должны валить запрос (кэш - ускоритель,
а не источник истины). При недоступности Redis get возвращает None, set/delete молчат.

Формат документации: Проверяем / Успех / Нежелательное поведение.
"""

import pytest
from unittest.mock import AsyncMock

from app.services.cache import RedisCacheService


@pytest.mark.asyncio
async def test_get_returns_value():
    """
    Проверяем: чтение существующего ключа.
    Успех: возвращается значение из Redis.
    Нежелательное поведение: подмена или проглатывание валидного значения.
    """
    redis = AsyncMock()
    redis.get.return_value = "cached-value"
    cache = RedisCacheService(redis)

    assert await cache.get("key") == "cached-value"


@pytest.mark.asyncio
async def test_get_on_error_returns_none():
    """
    Проверяем: Redis упал при чтении.
    Успех: get возвращает None (вызывающий код пойдёт в БД), исключение не летит.
    Нежелательное поведение: падение запроса аналитики из-за недоступности кэша.
    """
    redis = AsyncMock()
    redis.get.side_effect = Exception("redis down")
    cache = RedisCacheService(redis)

    assert await cache.get("key") is None


@pytest.mark.asyncio
async def test_set_with_ttl():
    """
    Проверяем: запись значения с TTL.
    Успех: redis.set вызван с переданным ключом, значением и ex=TTL.
    Нежелательное поведение: запись без TTL (кэш не протухает) или потеря значения.
    """
    redis = AsyncMock()
    cache = RedisCacheService(redis)

    await cache.set("key", "value", expire=60)

    redis.set.assert_awaited_once_with("key", "value", ex=60)


@pytest.mark.asyncio
async def test_set_on_error_is_swallowed():
    """
    Проверяем: Redis упал при записи.
    Успех: исключение не пробрасывается (основная операция уже успешна).
    Нежелательное поведение: падение из-за проблем записи в кэш.
    """
    redis = AsyncMock()
    redis.set.side_effect = Exception("redis down")
    cache = RedisCacheService(redis)

    await cache.set("key", "value", expire=60)  # не должно упасть


@pytest.mark.asyncio
async def test_delete_by_pattern_scans_and_deletes():
    """
    Проверяем: инвалидация ключей по паттерну (SCAN + DELETE).
    Успех: найденные ключи удаляются пачкой.
    Нежелательное поведение: удаление не тех ключей или пропуск инвалидации.
    """
    redis = AsyncMock()
    # первый SCAN возвращает курсор 0 (обход завершён) и один ключ
    redis.scan.return_value = (0, [b"analytics:summary:x"])
    cache = RedisCacheService(redis)

    await cache.delete_by_pattern("analytics:summary:*")

    redis.delete.assert_awaited_once_with(b"analytics:summary:x")


@pytest.mark.asyncio
async def test_delete_by_pattern_on_error_is_swallowed():
    """
    Проверяем: Redis упал во время инвалидации.
    Успех: исключение не пробрасывается.
    Нежелательное поведение: падение обработчика события из-за проблем сброса кэша.
    """
    redis = AsyncMock()
    redis.scan.side_effect = Exception("redis down")
    cache = RedisCacheService(redis)

    await cache.delete_by_pattern("analytics:summary:*")  # не должно упасть
