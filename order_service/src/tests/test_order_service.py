"""
Юнит-тесты OrderService: атомарность создания (заказ + order.created в одной
транзакции), RBAC на уровне выборок, отмена только из PENDING и Cache-Aside.

Формат docstring каждого теста:
    Проверяем: какое поведение под контролем.
    Успех: что должно случиться, чтобы тест был зелёным.
    Нежелательное поведение: ради чего тест существует (что он ловит).

Инфраструктуры нет: фейки из conftest моделируют транзакционность (pending ->
commit), поэтому dual write ловится, а не маскируется зелёным тестом.
"""

from __future__ import annotations

import uuid
from decimal import Decimal

import pytest

from app.application.services.order_service import OrderService
from app.domain.exceptions.orders import (
    OrderCancellationNotAllowedError,
    OrderNotFoundError,
)
from app.domain.orders import OrderStatus
from app.domain.outbox import OutboxKind, OutboxStatus
from tests.conftest import (
    EVENTS_TOPIC,
    FakeOutboxRepository,
    FakeUOW,
    make_order,
    make_order_create,
)


# ---------------------------------------------------------------------------
# create_order: атомарность и конверт события
# ---------------------------------------------------------------------------


class TestCreateOrder:
    async def test_persists_order_and_outbox_in_single_transaction(
        self, order_service, session
    ):
        """
        Проверяем: заказ и событие order.created коммитятся в ОДНОЙ транзакции.
        Успех: ровно один commit, а его снимок содержит и заказ, и outbox-запись.
        Нежелательное поведение: два отдельных коммита (dual write) - заказ есть,
            а событие могло бы потеряться.
        """
        user_id = uuid.uuid4()

        order = await order_service.create_order(user_id, make_order_create())

        assert order.status is OrderStatus.PENDING
        assert session.commits == 1
        assert session.rollbacks == 0
        # снимок фиксирует состояние (заказов, outbox) на момент каждого коммита:
        # (1, 1) в единственном коммите = обе записи попали в одну транзакцию
        assert session.commit_snapshots == [(1, 1)]

    async def test_total_amount_is_computed_server_side(self, order_service):
        """
        Проверяем: сумму заказа считает сервер по позициям, а не клиент.
        Успех: total_amount = 150.00*2 + 99.50*1 = 399.50.
        Нежелательное поведение: доверие сумме из тела запроса (её там и нет).
        """
        order = await order_service.create_order(uuid.uuid4(), make_order_create())

        assert order.total_amount == Decimal("399.50")

    async def test_order_created_event_matches_contract(self, order_service, session):
        """
        Проверяем: конверт order.created соответствует contracts/orders/order-created.v1.
        Успех: EVENT в orders.events, key = order_id, metadata.event_type и data
            (orderId/userId/totalAmount/currency/items) заполнены корректно.
        Нежелательное поведение: неверный топик/ключ (ломается партиционирование саги)
            или расхождение payload со схемой.
        """
        user_id = uuid.uuid4()

        order = await order_service.create_order(user_id, make_order_create())

        assert len(session.outbox) == 1
        msg = session.outbox[0]
        assert msg.kind is OutboxKind.EVENT
        assert msg.status is OutboxStatus.PENDING
        assert msg.topic == EVENTS_TOPIC
        assert msg.type == "order.created"
        # ключ партиционирования = order_id: все сообщения саги в одну партицию
        assert msg.key == str(order.id)

        meta = msg.payload["metadata"]
        data = msg.payload["data"]
        assert meta["event_type"] == "order.created"
        assert meta["source"] == "order-service"
        assert uuid.UUID(meta["event_id"])  # валидный uuid, не заглушка
        assert data["orderId"] == str(order.id)
        assert data["userId"] == str(user_id)
        assert data["totalAmount"] == "399.50"
        assert data["currency"] == "RUB"
        # цены позиций сериализованы строкой (Decimal не json-типа)
        assert data["items"] == [
            {"productId": "sku-1", "quantity": 2, "price": "150.00"},
            {"productId": "sku-2", "quantity": 1, "price": "99.50"},
        ]

    async def test_outbox_failure_rolls_back_the_order(
        self, session, orders_repo, cache, settings
    ):
        """
        Проверяем: сбой записи в outbox откатывает и заказ (единая транзакция).
        Успех: исключение проброшено, ни заказа, ни события в committed-состоянии,
            зафиксирован ровно один rollback.
        Нежелательное поведение: заказ закоммичен без своего order.created -
            сага для него никогда не стартует.
        """
        failing_outbox = FakeOutboxRepository(session, fail_on_add=True)
        service = OrderService(
            orders=orders_repo,
            outbox=failing_outbox,
            uow=FakeUOW(session),
            cache=cache,
            settings=settings,
        )

        with pytest.raises(RuntimeError):
            await service.create_order(uuid.uuid4(), make_order_create())

        assert session.orders == {}
        assert session.outbox == []
        assert session.commits == 0
        assert session.rollbacks == 1


# ---------------------------------------------------------------------------
# RBAC: выборки фильтруются в SQL, чужой заказ = 404
# ---------------------------------------------------------------------------


class TestRBAC:
    async def test_get_foreign_order_is_404_not_403(self, order_service, session):
        """
        Проверяем: чужой заказ для обычного пользователя неотличим от несуществующего.
        Успех: get_order поднимает OrderNotFoundError (переводится в 404).
        Нежелательное поведение: 403 или выдача чужого заказа - утечка факта
            существования ресурса.
        """
        owner = uuid.uuid4()
        order = make_order(user_id=owner)
        session.orders[order.id] = order

        with pytest.raises(OrderNotFoundError):
            await order_service.get_order(order.id, uuid.uuid4(), is_admin=False)

    async def test_list_for_user_returns_only_own(self, order_service, session):
        """
        Проверяем: обычный пользователь видит в списке только свои заказы.
        Успех: возвращается лишь заказ текущего пользователя, чужой отфильтрован.
        Нежелательное поведение: чужие заказы просочились в выборку.
        """
        me = uuid.uuid4()
        mine = make_order(user_id=me)
        other = make_order(user_id=uuid.uuid4())
        session.orders[mine.id] = mine
        session.orders[other.id] = other

        result = await order_service.list_orders(me, is_admin=False)

        assert [o.id for o in result] == [mine.id]

    async def test_admin_list_sees_all_orders(self, order_service, session):
        """
        Проверяем: admin получает заказы всех пользователей.
        Успех: в списке присутствуют оба заказа (свой и чужой).
        Нежелательное поведение: администратора урезали фильтром по user_id.
        """
        a = make_order(user_id=uuid.uuid4())
        b = make_order(user_id=uuid.uuid4())
        session.orders[a.id] = a
        session.orders[b.id] = b

        result = await order_service.list_orders(uuid.uuid4(), is_admin=True)

        assert {o.id for o in result} == {a.id, b.id}


# ---------------------------------------------------------------------------
# cancel_order: только из PENDING, инвалидация кэша после коммита
# ---------------------------------------------------------------------------


class TestCancelOrder:
    async def test_cancel_pending_sets_cancelled_and_invalidates_cache(
        self, order_service, session, cache
    ):
        """
        Проверяем: отмена PENDING-заказа переводит его в CANCELLED и чистит кэш.
        Успех: статус в БД = CANCELLED, order_id попал в invalidate ПОСЛЕ коммита.
        Нежелательное поведение: статус не сменился или кэш остался с PENDING.
        """
        owner = uuid.uuid4()
        order = make_order(user_id=owner, status=OrderStatus.PENDING)
        session.orders[order.id] = order

        result = await order_service.cancel_order(order.id, owner, is_admin=False)

        assert result.status is OrderStatus.CANCELLED
        assert session.orders[order.id].status is OrderStatus.CANCELLED
        assert cache.invalidate_calls == [order.id]
        assert session.commits == 1

    async def test_cancel_non_pending_is_409(self, order_service, session, cache):
        """
        Проверяем: отмена уже финализированного заказа запрещена (fail fast, ADR-004).
        Успех: OrderCancellationNotAllowedError (409), статус не тронут, кэш не чистится.
        Нежелательное поведение: перезапись COMPLETED в CANCELLED мимо саги.
        """
        owner = uuid.uuid4()
        order = make_order(user_id=owner, status=OrderStatus.COMPLETED)
        session.orders[order.id] = order

        with pytest.raises(OrderCancellationNotAllowedError):
            await order_service.cancel_order(order.id, owner, is_admin=False)

        assert session.orders[order.id].status is OrderStatus.COMPLETED
        assert cache.invalidate_calls == []
        assert session.rollbacks == 1

    async def test_cancel_foreign_order_is_404(self, order_service, session):
        """
        Проверяем: отмена чужого заказа обычным пользователем.
        Успех: OrderNotFoundError (404), не 403 - существование не раскрываем.
        Нежелательное поведение: пользователь отменяет чужой заказ.
        """
        order = make_order(user_id=uuid.uuid4(), status=OrderStatus.PENDING)
        session.orders[order.id] = order

        with pytest.raises(OrderNotFoundError):
            await order_service.cancel_order(order.id, uuid.uuid4(), is_admin=False)


# ---------------------------------------------------------------------------
# get_order: Cache-Aside (hit / miss)
# ---------------------------------------------------------------------------


class TestGetOrderCache:
    async def test_cache_hit_skips_database(self, order_service, orders_repo, cache):
        """
        Проверяем: попадание в кэш обслуживается без похода в БД.
        Успех: заказ возвращён из кэша, ни одного чтения репозитория.
        Нежелательное поведение: лишний SELECT при наличии свежей записи в кэше.
        """
        owner = uuid.uuid4()
        order = make_order(user_id=owner)
        cache.store[order.id] = order.model_dump(mode="json")

        result = await order_service.get_order(order.id, owner, is_admin=False)

        assert result.id == order.id
        assert cache.get_calls == 1
        assert orders_repo.reads == []  # БД не трогали

    async def test_cache_hit_still_enforces_rbac(self, order_service, cache):
        """
        Проверяем: RBAC действует и на кэш-хите.
        Успех: чужой заказ из кэша даёт OrderNotFoundError (404).
        Нежелательное поведение: кэш становится дырой в обход проверки владельца.
        """
        order = make_order(user_id=uuid.uuid4())
        cache.store[order.id] = order.model_dump(mode="json")

        with pytest.raises(OrderNotFoundError):
            await order_service.get_order(order.id, uuid.uuid4(), is_admin=False)

    async def test_cache_miss_reads_db_and_populates_cache(
        self, order_service, orders_repo, cache, session
    ):
        """
        Проверяем: промах кэша идёт в БД и прогревает кэш на будущее.
        Успех: заказ найден в БД, сделан ровно один cache.set, запись появилась в кэше.
        Нежелательное поведение: результат из БД не кэшируется - следующий GET
            снова бьёт по базе.
        """
        owner = uuid.uuid4()
        order = make_order(user_id=owner)
        session.orders[order.id] = order

        result = await order_service.get_order(order.id, owner, is_admin=False)

        assert result.id == order.id
        assert cache.get_calls == 1
        assert cache.set_calls == 1
        assert order.id in cache.store
        assert "get_for_user" in orders_repo.reads
