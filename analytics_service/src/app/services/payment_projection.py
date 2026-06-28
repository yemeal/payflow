import structlog
from typing import Protocol

from app.schemas.events import PaymentPayload
from app.repositories.payments import PaymentRepositoryProtocol

logger = structlog.get_logger()


class PaymentProjectionServiceProtocol(Protocol):
    async def project_payment(self, payload: PaymentPayload) -> None: ...


class PaymentProjectionService:
    def __init__(self, payment_repo: PaymentRepositoryProtocol) -> None:
        self._payment_repo = payment_repo

    async def project_payment(self, payload: PaymentPayload) -> None:
        logger.info(
            "projecting_payment", payment_id=str(payload.id), status=payload.status
        )

        # Конвертируем Pydantic payload в словарь для upsert
        payment_data = payload.model_dump(mode="python")

        # Делегируем работу репозиторию
        await self._payment_repo.upsert(payment_data)
        logger.debug("payment_projected_successfully", payment_id=str(payload.id))
