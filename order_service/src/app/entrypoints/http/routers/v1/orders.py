"""
Роутеры Order API. Роутеры только принимают и отдают: бизнес-логика живёт
в application/services/order_service.py. Собираются фабрикой (замыкание
держит JWT-зависимость) - глобалей уровня модуля нет.

Доменные исключения в HTTP-коды переводит routers/exception_handlers.py:
OrderNotFoundError -> 404 (в том числе на чужой заказ),
OrderCancellationNotAllowedError -> 409.
"""

import uuid
from typing import Annotated

from dishka import FromDishka
from dishka.integrations.fastapi import inject
from fastapi import APIRouter, Depends, Query, status

from app.application.services.order_service import OrderServiceProtocol
from app.core.settings import Settings
from app.entrypoints.http.schemas.orders import OrderCreate, OrderResponse
from app.entrypoints.http.security import (
    AuthenticatedUser,
    create_current_user_dependency,
)


def create_orders_router(settings: Settings) -> APIRouter:
    get_current_user = create_current_user_dependency(settings)
    CurrentUser = Annotated[AuthenticatedUser, Depends(get_current_user)]

    router = APIRouter(prefix="/orders", tags=["orders"])

    @router.post("", status_code=status.HTTP_201_CREATED, response_model=OrderResponse)
    @inject
    async def create_order(
        payload: OrderCreate,
        user: CurrentUser,
        order_service: FromDishka[OrderServiceProtocol],
    ) -> OrderResponse:
        # user_id - только из токена; заказ и order.created сохраняются
        # в одной транзакции, публикацией занимается outbox relay
        order = await order_service.create_order(user.user_id, payload)
        return OrderResponse.model_validate(order)

    @router.get("")
    @inject
    async def list_orders(
        user: CurrentUser,
        order_service: FromDishka[OrderServiceProtocol],
        limit: int = Query(default=50, ge=1, le=100),
        offset: int = Query(default=0, ge=0),
    ) -> list[OrderResponse]:
        # RBAC решается в SQL: user видит только свои заказы (WHERE user_id = ...),
        # admin - все. Роль берётся из токена, а не из параметров запроса
        orders = await order_service.list_orders(
            user.user_id, user.is_admin, limit=limit, offset=offset
        )
        return [OrderResponse.model_validate(order) for order in orders]

    @router.get("/{order_id}", response_model=OrderResponse)
    @inject
    async def get_order(
        order_id: uuid.UUID,
        user: CurrentUser,
        order_service: FromDishka[OrderServiceProtocol],
    ) -> OrderResponse:
        # чужой заказ -> OrderNotFoundError -> 404 (не 403: не раскрываем
        # существование); чтение через Cache-Aside (Redis), miss идёт в БД
        order = await order_service.get_order(order_id, user.user_id, user.is_admin)
        return OrderResponse.model_validate(order)

    @router.patch("/{order_id}/cancel", response_model=OrderResponse)
    @inject
    async def cancel_order(
        order_id: uuid.UUID,
        user: CurrentUser,
        order_service: FromDishka[OrderServiceProtocol],
    ) -> OrderResponse:
        # только из PENDING, иначе 409 (OrderCancellationNotAllowedError):
        # заказ, уже финализированный сагой, не отменяем.
        # Чужой заказ -> OrderNotFoundError -> 404 (не 403)
        order = await order_service.cancel_order(order_id, user.user_id, user.is_admin)
        return OrderResponse.model_validate(order)

    return router
