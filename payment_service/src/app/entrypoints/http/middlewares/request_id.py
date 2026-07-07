import time
import uuid

import structlog
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

from app.core.logging import bind_contextvars, clear_contextvars

logger = structlog.get_logger()


class RequestIdMiddleware(BaseHTTPMiddleware):
    async def dispatch(
        self,
        request: Request,
        call_next: RequestResponseEndpoint,
    ) -> Response:
        # если клиент прислал id запроса, используем его.
        # иначе сами генерим uuid
        request_id = request.headers.get("X-Request-Id", str(uuid.uuid4()))

        # привязываем request_id к запросу
        clear_contextvars()
        bind_contextvars(request_id=request_id)

        # TODO отдельный perf_counter MW
        start_time = time.perf_counter()
        logger.info(
            "request started",
            method=request.method,
            path=request.url.path,
        )

        # передаем запрос дальше по цепочке
        response: Response = await call_next(request)

        duration_ms = round((time.perf_counter() - start_time) * 1000, 2)
        logger.info(
            "request finished",
            method=request.method,
            path=request.url.path,
            status_code=response.status_code,
            duration_ms=duration_ms,
        )

        # добавляем айди запроса в заголовок ответа
        response.headers["X-Request-Id"] = request_id
        return response
