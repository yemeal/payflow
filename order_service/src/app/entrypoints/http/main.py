"""
Order API. Запуск: uvicorn --factory app.entrypoints.http.main:create_app
Всё состояние собирает фабрика - глобалей уровня модуля нет.
"""

from contextlib import asynccontextmanager
from typing import AsyncIterator

import structlog
from dishka.integrations.fastapi import setup_dishka
from fastapi import FastAPI
from prometheus_client import make_asgi_app

from app.core.logging import setup_logging
from app.core.settings import get_settings
from app.entrypoints.http.middlewares.request_id import RequestIdMiddleware
from app.entrypoints.http.routers import create_api_router
from app.entrypoints.http.routers.exception_handlers import register_exception_handlers
from app.infrastructure.di import create_container

logger = structlog.get_logger()


def create_app() -> FastAPI:
    setup_logging()
    settings = get_settings()
    container = create_container(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        logger.info("Application started", version=app.version)
        try:
            yield
        finally:
            logger.info("Application shutting down")
            await app.state.dishka_container.close()

    app = FastAPI(
        title="OrderFlow / Order Service",
        version="0.1.0",
        lifespan=lifespan,
    )

    app.add_middleware(RequestIdMiddleware)
    app.include_router(create_api_router(settings))
    register_exception_handlers(app)

    # метрики процесса (prometheus); кастомные счётчики появятся по мере нужды
    app.mount("/metrics", make_asgi_app())

    setup_dishka(container, app)
    return app
