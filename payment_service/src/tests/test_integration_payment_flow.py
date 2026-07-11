"""
Интеграционные тесты payment_service - связка реальных компонентов на in-memory портах.

В отличие от юнит-тестов здесь работают НАСТОЯЩИЕ PaymentService, IdempotencyGuard
и OutboxRelayService вместе, подменены только адаптеры инфраструктуры (репозитории,
хранилище идемпотентности, publisher, провайдер). Так проверяем, что модули стыкуются
и весь флоу создания платежа отрабатывает целиком:

    POST -> Two-Level Idempotency -> create() (2 короткие транзакции)
         -> outbox (payment.pending + payment.processing) -> OutboxRelay -> publish.

Плюс проверяем оба уровня идемпотентности от лица командного/HTTP флоу
(кэш в Redis и fallback в БД).

Формат документации: Проверяем / Успех / Нежелательное поведение.
"""

import contextlib
from decimal import Decimal
from types import SimpleNamespace

import pytest

from app.application.services.payment_service import PaymentService
from app.application.services.idempotency import IdempotencyService
from app.application.services.outbox_relay import OutboxRelayService
from app.domain.payments import PaymentStatus
from app.domain.outbox import OutboxStatus
from app.entrypoints.http.schemas.payments import PaymentCreate, PaymentResponse


# ---------------------------------------------------------------------------
# Фейковый провайдер
# ---------------------------------------------------------------------------

class FakeProvider:
    """Провайдер, всегда подтверждающий транзакцию заданным transaction_id."""

    def __init__(self, transaction_id="ext-tx-1"):
        self._transaction_id = transaction_id
        self.calls = 0

    async def initiate_transaction(self, request):
        self.calls += 1
        return SimpleNamespace(transaction_id=self._transaction_id)


# ---------------------------------------------------------------------------
# Сборка "приложения" на фейках
# ---------------------------------------------------------------------------

@pytest.fixture
def wired(in_memory_payment_repo, in_memory_outbox_repo, in_memory_uow,
          in_memory_storage, idempotency_settings, recording_publisher):
    """
    Собирает связку: PaymentService + IdempotencyService + OutboxRelayService
    поверх общих in-memory репозиториев (тот же outbox видит и сервис, и relay).
    """
    provider = FakeProvider()
    service = PaymentService(
        payment_repository=in_memory_payment_repo,
        uow=in_memory_uow,
        payment_provider=provider,
        outbox_repository=in_memory_outbox_repo,
    )
    idem = IdempotencyService(storage=in_memory_storage, settings=idempotency_settings)

    # scope-фабрика для relay поверх того же outbox-репозитория
    scope = SimpleNamespace(uow=in_memory_uow, outbox_repo=in_memory_outbox_repo)

    @contextlib.asynccontextmanager
    async def scope_factory():
        yield scope

    relay = OutboxRelayService(recording_publisher, scope_factory, max_publish_attempts=5)

    return SimpleNamespace(
        service=service,
        idem=idem,
        relay=relay,
        provider=provider,
        payment_repo=in_memory_payment_repo,
        outbox_repo=in_memory_outbox_repo,
        storage=in_memory_storage,
        publisher=recording_publisher,
    )


async def submit_payment(wired, idempotency_key: str):
    """
    Имитирует обработчик запроса: оборачивает create() в IdempotencyGuard
    ровно как это делают HTTP-роутер и командный консьюмер.
    Возвращает (response, created) - создан ли новый платеж или отдан кэш.
    """
    payload = PaymentCreate(amount=Decimal("100.00"), currency="RUB")
    payload_dict = payload.model_dump(mode="json")
    db_lookup = wired.service.build_idempotency_db_lookup()

    async with wired.idem(idempotency_key, payload_dict, db_lookup) as guard:
        if guard.has_cached_result and guard.cached_status_code is not None:
            return guard.cached_response, False
        created = await wired.service.create(payload, idempotency_key)
        response = PaymentResponse.model_validate(created).model_dump(mode="json")
        guard.set_result(status_code=201, response=response)
        return response, True


# ---------------------------------------------------------------------------
# Полный флоу: create -> outbox -> relay -> publish
# ---------------------------------------------------------------------------

class TestFullCreateFlow:
    @pytest.mark.asyncio
    async def test_create_then_relay_publishes_both_events(self, wired):
        """
        Проверяем: платеж создаётся и оба его события доезжают до брокера через relay.
        Успех: в БД лежит один PROCESSING-платеж; relay опубликовал два события
               (payment.pending, payment.processing) в правильном порядке и пометил их SUCCESS;
               ключ партиционирования каждого события - id платежа.
        Нежелательное поведение: потеря события, нарушение порядка, платеж без external_id,
               событие опубликовано, но не помечено SUCCESS (риск повторной публикации).
        """
        response, created = await submit_payment(wired, "flow-key-1")
        assert created is True

        # состояние платежа
        assert len(wired.payment_repo.payments) == 1
        payment = next(iter(wired.payment_repo.payments.values()))
        assert payment.status == PaymentStatus.PROCESSING
        assert payment.external_id == "ext-tx-1"

        # два события ждут публикации
        assert len(wired.outbox_repo.events) == 2

        # прогоняем relay
        await wired.relay._process_batch(batch_size=50)

        published_types = [e.metadata.event_type for e in wired.publisher.published]
        assert published_types == ["payment.pending", "payment.processing"]
        # все события помечены SUCCESS
        assert all(e.status == OutboxStatus.SUCCESS for e in wired.outbox_repo.events)
        # ключ партиционирования - id платежа
        assert all(e.data["id"] == response["id"] for e in wired.publisher.published)


# ---------------------------------------------------------------------------
# Two-Level Idempotency в сборке
# ---------------------------------------------------------------------------

class TestIdempotencyEndToEnd:
    @pytest.mark.asyncio
    async def test_level1_redis_cache_prevents_duplicate(self, wired):
        """
        Проверяем: повторный запрос с тем же ключом ловится на уровне 1 (Redis-кэш).
        Успех: второй запрос отдаёт кэшированный ответ, новый платеж не создаётся,
               провайдер второй раз не вызывается.
        Нежелательное поведение: второй платеж-дубль при retry клиента/redelivery.
        """
        first, created1 = await submit_payment(wired, "idem-key-1")
        second, created2 = await submit_payment(wired, "idem-key-1")

        assert created1 is True
        assert created2 is False
        assert first["id"] == second["id"]
        assert len(wired.payment_repo.payments) == 1
        assert wired.provider.calls == 1

    @pytest.mark.asyncio
    async def test_level2_db_fallback_prevents_duplicate(self, wired):
        """
        Проверяем: Redis-лок "протух" (кэш очищен), но платеж уже есть в БД.
        Успех: повторный запрос поднимает результат из БД (уровень 2, db_lookup),
               новый платеж не создаётся; результат перекэшируется обратно в Redis.
        Нежелательное поведение: создание второго платежа при протухшем Redis-кэше.
        """
        first, created1 = await submit_payment(wired, "idem-key-2")
        assert created1 is True

        # эмулируем истечение TTL/сброс Redis
        wired.storage._data.clear()

        second, created2 = await submit_payment(wired, "idem-key-2")

        assert created2 is False
        assert first["id"] == second["id"]
        assert len(wired.payment_repo.payments) == 1
        # результат снова осел в Redis (перекэширование при DB_HIT)
        assert wired.storage.raw("idempotency:idem-key-2") is not None
