from fastapi import APIRouter
from .payments import router as payments_router

v1_router = APIRouter(prefix="/v1")
v1_router.include_router(payments_router, prefix="/payments")

from .payments import router, create_payment, get_payment

__all__ = (
    "router",
    "create_payment",
    "get_payment",
)
