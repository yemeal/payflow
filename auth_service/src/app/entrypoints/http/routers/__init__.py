from fastapi import APIRouter

from app.entrypoints.http.routers.health import router as health_router
from app.entrypoints.http.routers.v1 import create_v1_router


def create_api_router() -> APIRouter:
    api_router = APIRouter(prefix="/api")
    api_router.include_router(health_router)
    api_router.include_router(create_v1_router())
    return api_router
