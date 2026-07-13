"""
Бизнес-логика Order API.

Ключевой инвариант create_order: заказ и outbox-событие order.created
сохраняются В ОДНОЙ транзакции; напрямую в Kafka сервис не пишет -
публикацией занимается outbox relay (сначала БД, потом брокер).

saga_id у заказа нет (ADR-006): сагу создаёт оркестратор по событию,
корреляция - через order_id (business key).
"""

import uuid
from decimal import Decimal
from typing import Any, Protocol

import structlog

from app.application.ports.cache import OrderCacheProtocol
from app.application.ports.repositories import (
    OrderRepositoryProtocol,
    OutboxRepositoryProtocol,
)
from app.application.ports.uow import AsyncUOWProtocol
from app.core.settings import Settings
from app.domain.exceptions.orders import (
    OrderCancellationNotAllowedError,
    OrderNotFoundError,
)
from app.domain.orders import Order, OrderItem, OrderStatus
from app.domain.outbox import OutboxKind, OutboxMessage
from app.domain.clock import utc_now
from app.entrypoints.http.schemas.orders import OrderCreate

logger = structlog.get_logger()


class OrderServiceProtocol(Protocol):
    async def create_order(self, user_id: uuid.UUID, payload: OrderCreate) -> Order: ...

    async def get_order(
        self, order_id: uuid.UUID, user_id: uuid.UUID, is_admin: bool
    ) -> Order: ...

    async def list_orders(
        self, user_id: uuid.UUID, is_admin: bool, limit: int = 50, offset: int = 0
    ) -> list[Order]: ...

    async def cancel_order(
        self, order_id: uuid.UUID, user_id: uuid.UUID, is_admin: bool
    ) -> Order: ...


class OrderService:
    """DTO из entrypoints в сигнатуре - осознанный компромисс эталона
    (payment_service делает так же, отмечено в AGENTS.md как отложенный рефакторинг)."""

    def __init__(
        self,
        orders: OrderRepositoryProtocol,
        outbox: OutboxRepositoryProtocol,
        uow: AsyncUOWProtocol,
        cache: OrderCacheProtocol,
        settings: Settings,
    ) -> None:
        self._orders = orders
        self._outbox = outbox
        self._uow = uow
        self._cache = cache
        self._settings = settings

    async def create_order(self, user_id: uuid.UUID, payload: OrderCreate) -> Order:
        # сумма считается на сервере; сами цены позиций до появления каталога
        # приходят от клиента - зафиксированное упрощение MVP (итерация 3, п.2:
        # авторитетный пересчёт каталогом добавится вместе с каталогом)
        total = sum(
            (item.price * item.quantity for item in payload.items), Decimal("0")
        )
        order = Order(
            user_id=user_id,
            items=[
                OrderItem(
                    product_id=item.product_id,
                    quantity=item.quantity,
                    price=item.price,
                )
                for item in payload.items
            ],
            total_amount=total,
            currency=payload.currency,
        )

        async with self._uow:
            await self._orders.create(order)
            await self._outbox.add(self._order_created_message(order))

        logger.info(
            "order_created",
            order_id=str(order.id),
            user_id=str(user_id),
            total_amount=str(total),
        )
        return order

    async def get_order(
        self, order_id: uuid.UUID, user_id: uuid.UUID, is_admin: bool
    ) -> Order:
        # Cache-Aside: кэш -> БД -> кэш; недоступный Redis не роняет запрос
        # (адаптер деградирует мягко и возвращает None)
        cached = await self._cache.get(order_id)
        if cached is not None:
            order = Order.model_validate(cached)
            if not is_admin and order.user_id != user_id:
                # RBAC обязателен и на кэш-хите: чужой заказ неотличим от несуществующего
                raise OrderNotFoundError(f"Order {order_id} not found")
            return order

        if is_admin:
            order = await self._orders.get(order_id)
        else:
            order = await self._orders.get_for_user(order_id, user_id)
        if order is None:
            raise OrderNotFoundError(f"Order {order_id} not found")

        await self._cache.set(order.id, order.model_dump(mode="json"))
        return order

    async def list_orders(
        self, user_id: uuid.UUID, is_admin: bool, limit: int = 50, offset: int = 0
    ) -> list[Order]:
        # RBAC живёт в SQL, а не в if-е над результатом: обычный пользователь
        # получает выборку с WHERE user_id = :current, admin - без фильтра.
        # Так чужая строка физически не попадает в результат, даже если выше
        # по стеку ошибутся с проверкой роли.
        #
        # Список сознательно не кэшируем: комбинаций limit/offset/роли слишком
        # много, инвалидация такого кэша дороже самой выборки.
        if is_admin:
            return await self._orders.list_all(limit=limit, offset=offset)
        return await self._orders.list_for_user(user_id, limit=limit, offset=offset)

    async def cancel_order(
        self, order_id: uuid.UUID, user_id: uuid.UUID, is_admin: bool
    ) -> Order:
        # SELECT FOR UPDATE: между чтением статуса и его сменой не должен
        # вклиниться консюмер саги с финальным событием (иначе оба перезапишут
        # статус друг поверх друга - классическая потеря обновления).
        async with self._uow:
            order = await self._orders.get_for_update(order_id)
            if order is None:
                raise OrderNotFoundError(f"Order {order_id} not found")
            if not is_admin and order.user_id != user_id:
                # чужой заказ - 404, а не 403: не раскрываем факт существования
                raise OrderNotFoundError(f"Order {order_id} not found")

            if order.status is not OrderStatus.PENDING:
                # заказ уже финализирован сагой -> отменять нечего (409).
                # ADR-004 (fail fast): полноценная отмена во время саги
                # (команда cancel_saga оркестратору) - бэклог.
                raise OrderCancellationNotAllowedError(
                    f"Order {order_id} is {order.status.value}, only PENDING can be cancelled"
                )

            order.status = OrderStatus.CANCELLED
            order.updated_at = utc_now()
            cancelled = await self._orders.update(order)

        # инвалидация строго ПОСЛЕ коммита: сбрось мы кэш внутри транзакции,
        # параллельный GET успел бы перечитать старый статус из БД и вернуть
        # его в кэш (кэш снова разъехался бы с БД)
        await self._cache.invalidate(order_id)

        logger.info(
            "order_cancelled_by_user", order_id=str(order_id), user_id=str(user_id)
        )
        return cancelled

    def _order_created_message(self, order: Order) -> OutboxMessage:
        """Конверт contracts/orders/order-created.v1: стартовое событие саги"""
        event_id = uuid.uuid7()
        items_json = order.model_dump(mode="json")["items"]
        envelope: dict[str, Any] = {
            "metadata": {
                "event_id": str(event_id),
                "event_type": "order.created",
                "version": "1.0",
                "timestamp": utc_now().isoformat(),
                "source": "order-service",
            },
            "data": {
                "orderId": str(order.id),
                "userId": str(order.user_id),
                "items": [
                    {
                        "productId": item["product_id"],
                        "quantity": item["quantity"],
                        "price": item["price"],
                    }
                    for item in items_json
                ],
                "totalAmount": str(order.total_amount),
                "currency": order.currency,
            },
        }
        return OutboxMessage(
            kind=OutboxKind.EVENT,
            topic=self._settings.KAFKA_EVENTS_TOPIC,
            # ключ партиционирования = order_id: порядок всех событий саги заказа
            key=str(order.id),
            type="order.created",
            payload=envelope,
        )
