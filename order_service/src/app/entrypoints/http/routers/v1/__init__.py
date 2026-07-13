from fastapi import APIRouter

from app.core.settings import Settings
from app.entrypoints.http.routers.v1.orders import create_orders_router


def create_v1_router(settings: Settings) -> APIRouter:
    v1_router = APIRouter(prefix="/v1")
    v1_router.include_router(create_orders_router(settings))
    return v1_router
