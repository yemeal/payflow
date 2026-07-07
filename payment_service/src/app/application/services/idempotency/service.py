from typing import Awaitable, Callable

from app.core.settings import Settings
from app.application.services.idempotency.domain import IdempotencyCachedResult
from app.application.services.idempotency.guard import IdempotencyGuard
from app.application.services.idempotency.protocols import IdempotencyStorageProtocol


class IdempotencyService:
    """
    Фабрика IdempotencyGuard объектов, инжектится через DI.
    Не знает про конкретные сущности.
    Зависит от IdempotencyStorageProtocol
    """

    def __init__(
        self,
        storage: IdempotencyStorageProtocol,
        settings: Settings,
    ) -> None:
        self._storage: IdempotencyStorageProtocol = storage
        self._settings: Settings = settings

    def __call__(
        self,
        idempotency_key: str,
        payload: dict,
        db_lookup: (
            Callable[[str], Awaitable[IdempotencyCachedResult | None]] | None
        ) = None,
    ) -> IdempotencyGuard:
        return IdempotencyGuard(
            storage=self._storage,
            settings=self._settings,
            idempotency_key=idempotency_key,
            payload=payload,
            db_lookup=db_lookup,
        )
