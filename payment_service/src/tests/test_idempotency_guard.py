"""
Тесты IdempotencyGuard - сердце Two-Level Idempotency (см. AGENTS.md).

Guard - контекстный менеджер с FSM. Два уровня защиты от дублей:
  Уровень 1 (Redis): атомарный acquire_lock; повторный ключ ловится тут же.
  Уровень 2 (БД):    db_lookup при первом заходе - если запись уже создана
                     (лок в Redis протух, а платеж есть), результат поднимается из БД.

Проверяем все переходы автомата и сайд-эффекты на выходе (release_lock / save_result).
Хранилище - in_memory_storage из conftest (семантика повторяет Redis+Lua адаптер).

Формат документации: Проверяем / Успех / Нежелательное поведение.
"""

import pytest

from app.application.services.idempotency.guard import IdempotencyGuard
from app.application.services.idempotency.domain import (
    IdempotencyCachedResult,
    IdempotencyEntry,
)
from app.application.services.idempotency.enums import (
    IdempotencyKeyStatus,
    GuardState,
)
from app.application.exceptions.idempotency import (
    IdempotencyKeyAlreadyProcessingError,
    IdempotencyKeyPayloadMismatchError,
)
from app.application.utils.compute_payload_hash import compute_payload_hash


PAYLOAD = {"amount": "100.00", "currency": "RUB"}
KEY = "idem-key-1"


def build_guard(settings, storage, key=KEY, payload=None, db_lookup=None):
    return IdempotencyGuard(
        settings=settings,
        storage=storage,
        idempotency_key=key,
        payload=payload if payload is not None else PAYLOAD,
        db_lookup=db_lookup,
    )


def seed_entry(storage, key, entry: IdempotencyEntry):
    """Кладём готовую запись в хранилище под реальным префиксом ключа."""
    storage._data[f"idempotency:{key}"] = entry.model_dump_json()


# ---------------------------------------------------------------------------
# Уровень 1: захват лока и обычная обработка
# ---------------------------------------------------------------------------

class TestLockAndProcessing:
    @pytest.mark.asyncio
    async def test_first_request_acquires_lock_and_processes(self, idempotency_settings, in_memory_storage):
        """
        Проверяем: первый запрос с новым ключом, db_lookup не задан.
        Успех: лок захвачен, кэшированного результата нет, состояние PROCESSING
               (guard готов выполнять бизнес-логику).
        Нежелательное поведение: ложный кэш-хит на первом запросе.
        """
        guard = build_guard(idempotency_settings, in_memory_storage)

        async with guard as g:
            assert g.has_cached_result is False
            assert g._state == GuardState.PROCESSING

        assert in_memory_storage.acquire_calls == 1

    @pytest.mark.asyncio
    async def test_completed_result_is_saved_on_exit(self, idempotency_settings, in_memory_storage):
        """
        Проверяем: успешная обработка с set_result и штатным выходом.
        Успех: на выходе результат кэшируется в хранилище как DONE с тем же payload_hash,
               кодом и телом ответа.
        Нежелательное поведение: результат не сохранён - повтор снова уйдёт в бизнес-логику.
        """
        guard = build_guard(idempotency_settings, in_memory_storage)

        async with guard as g:
            g.set_result(status_code=201, response={"id": "p-1"})

        entry = in_memory_storage.entry(f"idempotency:{KEY}")
        assert entry.status == IdempotencyKeyStatus.DONE
        assert entry.status_code == 201
        assert entry.response == {"id": "p-1"}
        assert entry.payload_hash == compute_payload_hash(PAYLOAD)

    @pytest.mark.asyncio
    async def test_exception_releases_lock(self, idempotency_settings, in_memory_storage):
        """
        Проверяем: во время обработки (state PROCESSING) выброшено исключение.
        Успех: лок освобождается (release_lock), ключ удалён - следующий запрос сможет
               повторить обработку; исходное исключение пробрасывается наружу.
        Нежелательное поведение: лок остаётся висеть до TTL и блокирует ретраи.
        """
        guard = build_guard(idempotency_settings, in_memory_storage)

        with pytest.raises(ValueError):
            async with guard:
                raise ValueError("business logic failed")

        assert in_memory_storage.release_calls == 1
        assert in_memory_storage.raw(f"idempotency:{KEY}") is None

    @pytest.mark.asyncio
    async def test_set_result_ignored_outside_processing(self, idempotency_settings, in_memory_storage):
        """
        Проверяем: set_result вызван не в состоянии PROCESSING (например на кэш-хите).
        Успех: результат не подменяется (метод отрабатывает как no-op вне PROCESSING).
        Нежелательное поведение: перезапись уже отданного клиенту результата.
        """
        # заранее кладём готовую DONE-запись -> будет CACHE_HIT
        seed_entry(
            in_memory_storage, KEY,
            IdempotencyEntry(
                status=IdempotencyKeyStatus.DONE,
                payload_hash=compute_payload_hash(PAYLOAD),
                status_code=201,
                response={"id": "orig"},
            ),
        )
        guard = build_guard(idempotency_settings, in_memory_storage)

        async with guard as g:
            assert g._state == GuardState.CACHE_HIT
            g.set_result(status_code=500, response={"id": "hacked"})
            # кэшированный ответ остаётся исходным
            assert g.cached_response == {"id": "orig"}


# ---------------------------------------------------------------------------
# Уровень 1: ключ уже существует в Redis
# ---------------------------------------------------------------------------

class TestEntryExists:
    @pytest.mark.asyncio
    async def test_processing_entry_raises_already_processing(self, idempotency_settings, in_memory_storage):
        """
        Проверяем: по ключу уже стоит лок со статусом PROCESSING (параллельный запрос).
        Успех: поднимается IdempotencyKeyAlreadyProcessingError (клиент получит 423).
        Нежелательное поведение: вторая параллельная обработка того же ключа.
        """
        seed_entry(
            in_memory_storage, KEY,
            IdempotencyEntry(status=IdempotencyKeyStatus.PROCESSING, payload_hash="whatever"),
        )
        guard = build_guard(idempotency_settings, in_memory_storage)

        with pytest.raises(IdempotencyKeyAlreadyProcessingError):
            async with guard:
                pass

    @pytest.mark.asyncio
    async def test_done_entry_same_payload_is_cache_hit(self, idempotency_settings, in_memory_storage):
        """
        Проверяем: по ключу есть готовый результат DONE с тем же payload.
        Успех: состояние CACHE_HIT, отдаётся кэшированный ответ и код;
               повторное сохранение не выполняется.
        Нежелательное поведение: повторный запуск бизнес-логики вместо отдачи кэша.
        """
        seed_entry(
            in_memory_storage, KEY,
            IdempotencyEntry(
                status=IdempotencyKeyStatus.DONE,
                payload_hash=compute_payload_hash(PAYLOAD),
                status_code=201,
                response={"id": "cached"},
            ),
        )
        guard = build_guard(idempotency_settings, in_memory_storage)

        async with guard as g:
            assert g._state == GuardState.CACHE_HIT
            assert g.has_cached_result is True
            assert g.cached_response == {"id": "cached"}
            assert g.cached_status_code == 201

        # кэш-хит не должен повторно писать результат
        assert in_memory_storage.save_calls == 0

    @pytest.mark.asyncio
    async def test_done_entry_different_payload_raises_mismatch(self, idempotency_settings, in_memory_storage):
        """
        Проверяем: ключ переиспользован с другим payload (hash не совпал).
        Успех: поднимается IdempotencyKeyPayloadMismatchError (клиент получит 409).
        Нежелательное поведение: отдача чужого ответа под неправильным payload.
        """
        seed_entry(
            in_memory_storage, KEY,
            IdempotencyEntry(
                status=IdempotencyKeyStatus.DONE,
                payload_hash="completely-different-hash",
                status_code=201,
                response={"id": "cached"},
            ),
        )
        guard = build_guard(idempotency_settings, in_memory_storage)

        with pytest.raises(IdempotencyKeyPayloadMismatchError):
            async with guard:
                pass


# ---------------------------------------------------------------------------
# Уровень 2: fallback-поход в БД
# ---------------------------------------------------------------------------

class TestDbFallback:
    @pytest.mark.asyncio
    async def test_db_hit_returns_and_recaches(self, idempotency_settings, in_memory_storage):
        """
        Проверяем: лока в Redis нет, но db_lookup нашёл уже созданный платеж.
        Успех: состояние DB_HIT, отдаётся результат из БД, и на выходе он
               перекэшируется обратно в Redis (следующий повтор поймается уже на уровне 1).
        Нежелательное поведение: повторное создание платежа при протухшем Redis-локе.
        """
        async def db_lookup(key):
            return IdempotencyCachedResult(status_code=201, response={"id": "from-db"})

        guard = build_guard(idempotency_settings, in_memory_storage, db_lookup=db_lookup)

        async with guard as g:
            assert g._state == GuardState.DB_HIT
            assert g.cached_response == {"id": "from-db"}

        # результат осел в Redis как DONE
        entry = in_memory_storage.entry(f"idempotency:{KEY}")
        assert entry.status == IdempotencyKeyStatus.DONE
        assert entry.response == {"id": "from-db"}

    @pytest.mark.asyncio
    async def test_db_miss_proceeds_to_processing(self, idempotency_settings, in_memory_storage):
        """
        Проверяем: лока нет, db_lookup вернул None (платеж ещё не создавался).
        Успех: состояние PROCESSING, кэша нет, db_lookup вызван ровно один раз
               (защита от лавины запросов в БД - проскакивает только владелец лока).
        Нежелательное поведение: пропуск обработки или лишние обращения к БД.
        """
        calls = {"n": 0}

        async def db_lookup(key):
            calls["n"] += 1
            return None

        guard = build_guard(idempotency_settings, in_memory_storage, db_lookup=db_lookup)

        async with guard as g:
            assert g._state == GuardState.PROCESSING
            assert g.has_cached_result is False

        assert calls["n"] == 1
