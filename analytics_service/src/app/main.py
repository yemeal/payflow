from contextlib import asynccontextmanager, AbstractAsyncContextManager
from typing import AsyncIterator, Callable

import structlog
from dishka import make_async_container
from dishka.integrations.fastapi import setup_dishka
from fastapi import FastAPI

from app.core.di.provider import (
    SettingsProvider,
    DatabaseProvider,
    RedisProvider,
    RepositoriesProvider,
    ServiceProvider,
    KafkaProvider,
)
from app.core.logging import setup_logging
from app.core.middleware.request_id import RequestIdMiddleware
from app.api import api_router
from app.api.exception_handlers import register_exception_handlers

setup_logging()
logger = structlog.get_logger()


def make_lifespan() -> Callable[[FastAPI], AbstractAsyncContextManager[None]]:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        logger.info("Application started", version=app.version)
        _container = app.state.dishka_container

        from app.faststream_app import broker

        await broker.start()
        logger.info("FastStream broker started")

        try:
            yield
        finally:
            logger.info("Application shutting down")
            await broker.stop()
            logger.info("FastStream broker stopped")
            await _container.close()

    return lifespan


app = FastAPI(
    title="PayFlow / Analytics Service",
    version="1.0.0",
    lifespan=make_lifespan(),
)

app.add_middleware(RequestIdMiddleware)
app.include_router(api_router)
register_exception_handlers(app)

container = make_async_container(
    SettingsProvider(),
    DatabaseProvider(),
    RedisProvider(),
    RepositoriesProvider(),
    ServiceProvider(),
    KafkaProvider(),
)
setup_dishka(container, app)
