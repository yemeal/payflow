import structlog
from fastapi import FastAPI, status
from starlette.requests import Request
from starlette.responses import JSONResponse

from app.core.exceptions import PaymentNotFoundError

logger = structlog.get_logger()


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(PaymentNotFoundError)
    async def _(request: Request, exc: PaymentNotFoundError) -> JSONResponse:
        logger.info("payment_not_found", error=exc.message)
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={"error": exc.message},
        )
