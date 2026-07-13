"""
Auth Service. Запуск: uvicorn --factory app.entrypoints.http.main:create_app
Всё состояние собирает фабрика - глобалей уровня модуля нет.
"""

from contextlib import asynccontextmanager
from typing import AsyncIterator

import structlog
from dishka.integrations.fastapi import setup_dishka
from fastapi import FastAPI
from prometheus_client import make_asgi_app

from app.application.ports.security import (
    AccessTokenIssuerProtocol,
    AccessTokenVerifierProtocol,
)
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
        try:
            # Разрешаем APP-зависимость заранее: нерабочий ключ должен сломать
            # запуск, а не первый запрос пользователя на login.
            await container.get(AccessTokenIssuerProtocol)
            await container.get(AccessTokenVerifierProtocol)
            logger.info("Application started", version=app.version)
            yield
        finally:
            logger.info("Application shutting down")
            await app.state.dishka_container.close()

    app = FastAPI(
        title="OrderFlow / Auth Service",
        version="0.1.0",
        lifespan=lifespan,
    )

    app.add_middleware(RequestIdMiddleware)
    app.include_router(create_api_router())
    register_exception_handlers(
        app,
        idempotency_retry_after_seconds=settings.IDEMPOTENCY_LOCK_TTL,
    )

    # метрики процесса (prometheus)
    app.mount("/metrics", make_asgi_app())

    setup_dishka(container, app)
    return app
