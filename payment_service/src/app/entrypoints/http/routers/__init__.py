from fastapi import APIRouter
from .health import router as health_router
from .v1 import v1_router

api_router = APIRouter(prefix="/api")
api_router.include_router(v1_router)
api_router.include_router(health_router)

from .exception_handlers import logger, register_exception_handlers
from .health import router, check_kafka, check_postgres, check_redis, live, ready

__all__ = (
    "logger",
    "register_exception_handlers",
    "router",
    "check_kafka",
    "check_postgres",
    "check_redis",
    "live",
    "ready",
)
