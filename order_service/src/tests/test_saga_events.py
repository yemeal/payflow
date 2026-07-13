"""
Юнит-тесты SagaEventsHandlerService: финализация заказа по событиям саги
(saga.completed / saga.cancelled / saga.failed) из orders.events.

Инварианты под контролем:
 - дедуп (processed_events) и смена статуса - в ОДНОЙ транзакции;
 - дубль события не меняет статус второй раз (Idempotent Consumer);
 - кэш инвалидируется после смены статуса;
 - чужие события шины (order.created, inventory.*) игнорируются;
 - битый конверт -> poison (консюмер отправит его в DLQ).

Формат docstring: Проверяем / Успех / Нежелательное поведение.
"""

from __future__ import annotations

import uuid

from app.domain.orders import OrderStatus
from tests.conftest import make_order, make_saga_event


# ---------------------------------------------------------------------------
# Успешная финализация
# ---------------------------------------------------------------------------


class TestFinalization:
    async def test_saga_completed_sets_completed_and_invalidates_cache(
        self, saga_handler, session, cache
    ):
        """
        Проверяем: saga.completed переводит заказ в COMPLETED и чистит кэш.
        Успех: action=processed, статус в БД = COMPLETED, дедуп-запись создана,
            order_id инвалидирован в кэше.
        Нежелательное поведение: заказ навсегда завис в PENDING или кэш отдаёт старый статус.
        """
        order = make_order(status=OrderStatus.PENDING)
        session.orders[order.id] = order
        event = make_saga_event("saga.completed", order.id)

        action = await saga_handler.handle(event)

        assert action == "processed"
        assert session.orders[order.id].status is OrderStatus.COMPLETED
        assert cache.invalidate_calls == [order.id]
        assert uuid.UUID(event["metadata"]["event_id"]) in session.processed

    async def test_saga_cancelled_sets_cancelled_and_invalidates_cache(
        self, saga_handler, session, cache
    ):
        """
        Проверяем: saga.cancelled переводит заказ в CANCELLED и чистит кэш.
        Успех: action=processed, статус CANCELLED, кэш инвалидирован.
        Нежелательное поведение: отменённая сага оставляет заказ в PENDING.
        """
        order = make_order(status=OrderStatus.PENDING)
        session.orders[order.id] = order
        event = make_saga_event("saga.cancelled", order.id)

        action = await saga_handler.handle(event)

        assert action == "processed"
        assert session.orders[order.id].status is OrderStatus.CANCELLED
        assert cache.invalidate_calls == [order.id]

    async def test_saga_failed_maps_to_cancelled(self, saga_handler, session, cache):
        """
        Проверяем: saga.failed у заказа маппится в CANCELLED (+ алерт-лог).
        Успех: action=processed, статус CANCELLED, кэш инвалидирован.
        Нежелательное поведение: у заказа появляется статус FAILED, которого нет,
            либо пользователь видит вечный PENDING.
        """
        order = make_order(status=OrderStatus.PENDING)
        session.orders[order.id] = order
        event = make_saga_event("saga.failed", order.id, status="FAILED")

        action = await saga_handler.handle(event)

        assert action == "processed"
        assert session.orders[order.id].status is OrderStatus.CANCELLED
        assert cache.invalidate_calls == [order.id]


# ---------------------------------------------------------------------------
# Идемпотентность и guard по состоянию
# ---------------------------------------------------------------------------


class TestIdempotency:
    async def test_duplicate_event_does_not_change_status_twice(
        self, saga_handler, session, cache, orders_repo
    ):
        """
        Проверяем: повторная доставка того же события не меняет статус второй раз.
        Успех: первый раз processed (одно обновление, одна инвалидация), дубль -
            duplicate без нового UPDATE и без повторной инвалидации.
        Нежелательное поведение: событие обрабатывается дважды (гонка двух инстансов,
            повторное применение эффекта).
        """
        order = make_order(status=OrderStatus.PENDING)
        session.orders[order.id] = order
        event_id = uuid.uuid4()
        event = make_saga_event("saga.completed", order.id, event_id=event_id)

        first = await saga_handler.handle(event)
        second = await saga_handler.handle(event)

        assert first == "processed"
        assert second == "duplicate"
        assert orders_repo.update_calls == [order.id]  # ровно одно обновление
        assert cache.invalidate_calls == [order.id]  # инвалидация не повторилась
        assert session.orders[order.id].status is OrderStatus.COMPLETED

    async def test_event_for_already_final_order_is_ignored(
        self, saga_handler, session, cache, orders_repo
    ):
        """
        Проверяем: событие саги для уже финализированного заказа игнорируется (guard).
        Успех: action=ignored, статус не меняется, UPDATE не вызывается;
            дедуп при этом зафиксирован (повтор не поможет).
        Нежелательное поведение: перезапись COMPLETED в CANCELLED опоздавшим событием.
        """
        order = make_order(status=OrderStatus.COMPLETED)
        session.orders[order.id] = order
        event = make_saga_event("saga.cancelled", order.id)

        action = await saga_handler.handle(event)

        assert action == "ignored"
        assert session.orders[order.id].status is OrderStatus.COMPLETED
        assert orders_repo.update_calls == []

    async def test_event_for_unknown_order_is_ignored(self, saga_handler, session):
        """
        Проверяем: событие про несуществующий заказ.
        Успех: action=ignored, дедуп-запись всё равно создана (событие в никуда).
        Нежелательное поведение: бесконечный nack по событию, которое некуда применить.
        """
        event = make_saga_event("saga.completed", uuid.uuid4())

        action = await saga_handler.handle(event)

        assert action == "ignored"
        assert uuid.UUID(event["metadata"]["event_id"]) in session.processed


# ---------------------------------------------------------------------------
# Чужие события и битые конверты
# ---------------------------------------------------------------------------


class TestRoutingAndPoison:
    async def test_own_order_created_event_is_ignored(self, saga_handler, session):
        """
        Проверяем: собственное событие order.created (общая шина) не обрабатывается.
        Успех: action=ignored, дедуп-запись не создаётся (мы его не 'обработали').
        Нежелательное поведение: консюмер реагирует на свой же старт саги.
        """
        order_id = uuid.uuid4()
        event = make_saga_event("order.created", order_id)

        action = await saga_handler.handle(event)

        assert action == "ignored"
        assert session.processed == {}

    async def test_inventory_event_is_ignored(self, saga_handler):
        """
        Проверяем: событие участника (inventory.reserved) на общей шине.
        Успех: action=ignored - это не финализация заказа.
        Нежелательное поведение: попытка сменить статус заказа по чужому шагу саги.
        """
        event = make_saga_event("inventory.reserved", uuid.uuid4())

        assert await saga_handler.handle(event) == "ignored"

    async def test_missing_metadata_is_poison(self, saga_handler):
        """
        Проверяем: конверт без metadata-объекта.
        Успех: action=poison (консюмер отправит его в orders.events.dlq).
        Нежелательное поведение: падение обработчика или тихое проглатывание брака.
        """
        assert await saga_handler.handle({"data": {"orderId": str(uuid.uuid4())}}) == "poison"

    async def test_bad_event_id_is_poison(self, saga_handler):
        """
        Проверяем: финальное событие с невалидным event_id.
        Успех: action=poison (дедуп по битому id невозможен).
        Нежелательное поведение: исключение вместо детерминированного DLQ-пути.
        """
        event = make_saga_event("saga.completed", uuid.uuid4())
        event["metadata"]["event_id"] = "not-a-uuid"

        assert await saga_handler.handle(event) == "poison"

    async def test_bad_order_id_is_poison(self, saga_handler):
        """
        Проверяем: финальное событие с невалидным orderId в data.
        Успех: action=poison.
        Нежелательное поведение: попытка залочить строку по мусорному ключу.
        """
        event = make_saga_event("saga.completed", uuid.uuid4())
        event["data"]["orderId"] = "not-a-uuid"

        assert await saga_handler.handle(event) == "poison"
