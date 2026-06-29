from fastapi import APIRouter
from .analytics import router as analytics_router

v1_router = APIRouter(prefix="/v1")
v1_router.include_router(analytics_router, prefix="/analytics")
