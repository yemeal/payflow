from fastapi import APIRouter

from app.core.settings import Settings
from app.entrypoints.http.routers.health import router as health_router
from app.entrypoints.http.routers.v1 import create_admin_router


def create_api_router(settings: Settings) -> APIRouter:
    """
    Корневой роутер без общего префикса: пути зафиксированы дизайном
    (docs/saga-design.md, 9.9) как /admin/v1/... и /health/... .
    """
    api_router = APIRouter()
    api_router.include_router(health_router)
    api_router.include_router(create_admin_router(settings))
    return api_router
