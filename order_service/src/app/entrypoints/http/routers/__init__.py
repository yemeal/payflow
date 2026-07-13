from fastapi import APIRouter

from app.core.settings import Settings
from app.entrypoints.http.routers.health import router as health_router
from app.entrypoints.http.routers.v1 import create_v1_router


def create_api_router(settings: Settings) -> APIRouter:
    api_router = APIRouter(prefix="/api")
    api_router.include_router(health_router)
    api_router.include_router(create_v1_router(settings))
    return api_router
