from typing import Any

from app.application.services.idempotency.guard import IdempotencyGuard
from app.application.services.idempotency.protocols import (
    IdempotencyStorageProtocol,
)
from app.core.settings import Settings


class IdempotencyService:
    """Фабрика guard-объектов, которую HTTP-слой получает через DI."""

    def __init__(
        self,
        storage: IdempotencyStorageProtocol,
        settings: Settings,
    ) -> None:
        self._storage = storage
        self._settings = settings

    def __call__(
        self,
        idempotency_key: str,
        payload: dict[str, Any],
    ) -> IdempotencyGuard:
        return IdempotencyGuard(
            storage=self._storage,
            settings=self._settings,
            idempotency_key=idempotency_key,
            payload=payload,
        )
