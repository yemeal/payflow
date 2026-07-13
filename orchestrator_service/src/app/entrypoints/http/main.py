"""
Admin API оркестратора: read-only наблюдаемость саг, health-пробы, /metrics.

Запуск: uvicorn --factory app.entrypoints.http.main:create_app
Всё состояние собирает фабрика - глобалей уровня модуля нет.

Приложение и контейнер разведены (create_http_app принимает готовый контейнер):
тесты собирают то же самое приложение с фейковыми репозиториями, не поднимая
Postgres и Kafka.
"""

from contextlib import asynccontextmanager
from typing import AsyncIterator

import structlog
from dishka import AsyncContainer
from dishka.integrations.fastapi import setup_dishka
from fastapi import FastAPI, Response

from app.core.logging import setup_logging
from app.core.settings import Settings, get_settings
from app.entrypoints.http.middlewares.request_id import RequestIdMiddleware
from app.entrypoints.http.routers import create_api_router
from app.infrastructure.di import create_container
from app.infrastructure.observability import SagaMetrics, render_latest

logger = structlog.get_logger()


def create_http_app(
    settings: Settings,
    container: AsyncContainer,
    metrics: SagaMetrics | None = None,
) -> FastAPI:
    saga_metrics = metrics if metrics is not None else SagaMetrics()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        logger.info("orchestrator_admin_api_started", version=app.version)
        try:
            yield
        finally:
            # контейнер закрывает пул БД и продюсер - иначе висят соединения
            await app.state.dishka_container.close()
            logger.info("orchestrator_admin_api_stopped")

    app = FastAPI(
        title="OrderFlow / Saga Orchestrator Admin API",
        version="0.1.0",
        lifespan=lifespan,
    )

    app.add_middleware(RequestIdMiddleware)
    app.include_router(create_api_router(settings))

    # экспозиция метрик процесса Admin API; счётчики саг наполняет консюмер
    # в своём процессе, у каждого процесса свой /metrics-эндпоинт
    @app.get("/metrics", include_in_schema=False)
    async def metrics_endpoint() -> Response:
        body, content_type = render_latest(saga_metrics)
        return Response(content=body, media_type=content_type)

    setup_dishka(container, app)
    return app


def create_app() -> FastAPI:
    """Точка входа uvicorn --factory: собирает настройки, контейнер и приложение"""
    setup_logging()
    settings = get_settings()
    return create_http_app(settings, create_container(settings))
