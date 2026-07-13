"""
Маппинг доменных исключений на HTTP-статусы.

Держим его на границе HTTP: домен и application-слой про коды ответов не знают.
"""

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from app.domain.exceptions.orders import (
    OrderCancellationNotAllowedError,
    OrderNotFoundError,
)


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(OrderNotFoundError)
    async def order_not_found(
        request: Request, exc: OrderNotFoundError
    ) -> JSONResponse:
        # 404 и для чужого заказа: существование ресурса не раскрываем
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={"detail": "order not found"},
        )

    @app.exception_handler(OrderCancellationNotAllowedError)
    async def cancellation_not_allowed(
        request: Request, exc: OrderCancellationNotAllowedError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={"detail": "order can be cancelled only from PENDING status"},
        )
