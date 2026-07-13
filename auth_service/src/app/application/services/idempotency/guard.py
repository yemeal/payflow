from types import TracebackType
from typing import Any

import structlog

from app.application.exceptions.idempotency import (
    IdempotencyKeyAlreadyProcessingError,
    IdempotencyKeyPayloadMismatchError,
    IdempotencyStateInconsistencyError,
)
from app.application.services.idempotency.domain import IdempotencyEntry
from app.application.services.idempotency.enums import (
    GuardState,
    IdempotencyKeyStatus,
    LockAcquireStatus,
)
from app.application.services.idempotency.protocols import (
    IdempotencyStorageProtocol,
)
from app.application.utils.compute_payload_hash import compute_payload_hash
from app.core.settings import Settings

logger = structlog.get_logger()


class IdempotencyGuard:
    """
    Generic FSM из payment_service.

    Guard не знает про JWT и AuthService. HTTP-entrypoint передаёт ему key и
    payload, а затем либо получает сохранённый response, либо выполняет use case.
    """

    def __init__(
        self,
        settings: Settings,
        storage: IdempotencyStorageProtocol,
        idempotency_key: str,
        payload: dict[str, Any],
    ) -> None:
        self._payload_hash = compute_payload_hash(payload)
        self._storage = storage
        self._idempotency_key = idempotency_key
        self._state = GuardState.NEW
        self._cached_response: dict[str, Any] | None = None
        self._cached_status_code: int | None = None
        self._lock_entry = IdempotencyEntry(
            status=IdempotencyKeyStatus.PROCESSING,
            payload_hash=self._payload_hash,
        )
        self._lock_value = self._lock_entry.model_dump_json()
        self._lock_ttl = settings.IDEMPOTENCY_LOCK_TTL
        self._result_ttl = settings.IDEMPOTENCY_RESULT_TTL

    @property
    def storage_key(self) -> str:
        return f"idempotency:{self._idempotency_key}"

    @property
    def has_cached_result(self) -> bool:
        return self._cached_response is not None

    @property
    def cached_response(self) -> dict[str, Any] | None:
        return self._cached_response

    @property
    def cached_status_code(self) -> int | None:
        return self._cached_status_code

    def set_result(self, status_code: int, response: dict[str, Any]) -> None:
        """Сохраняем результат только если именно этот guard выполнял use case."""
        if self._state is not GuardState.PROCESSING:
            return
        self._cached_status_code = status_code
        self._cached_response = response
        self._state = GuardState.COMPLETED

    async def __aenter__(self) -> "IdempotencyGuard":
        result = await self._storage.acquire_lock(
            key=self.storage_key,
            lock_value=self._lock_value,
            ttl=self._lock_ttl,
        )

        match result.status:
            case LockAcquireStatus.LOCK_ACQUIRED:
                self._state = GuardState.PROCESSING
                logger.debug(
                    "idempotency processing",
                    idempotency_key=self._idempotency_key,
                )
            case LockAcquireStatus.ENTRY_EXISTS:
                entry = result.existing_entry
                if entry is None:
                    raise IdempotencyStateInconsistencyError()
                if entry.status is IdempotencyKeyStatus.PROCESSING:
                    raise IdempotencyKeyAlreadyProcessingError()
                if entry.status is IdempotencyKeyStatus.DONE:
                    if entry.payload_hash != self._payload_hash:
                        raise IdempotencyKeyPayloadMismatchError()
                    if entry.response is None or entry.status_code is None:
                        raise IdempotencyStateInconsistencyError()
                    self._cached_response = entry.response
                    self._cached_status_code = entry.status_code
                    self._state = GuardState.CACHE_HIT
                    logger.info(
                        "idempotency cache hit",
                        idempotency_key=self._idempotency_key,
                    )

        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        _exc_tb: TracebackType | None,
    ) -> None:
        if exc_type is not None and self._state is GuardState.PROCESSING:
            self._state = GuardState.FAILED

        match self._state:
            case GuardState.FAILED:
                await self._storage.release_lock(
                    key=self.storage_key,
                    expected_value=self._lock_value,
                )
                logger.warning(
                    "idempotency lock released after error",
                    idempotency_key=self._idempotency_key,
                    error_type=exc_type.__name__ if exc_type else None,
                )
            case GuardState.COMPLETED:
                result_entry = IdempotencyEntry(
                    status=IdempotencyKeyStatus.DONE,
                    payload_hash=self._payload_hash,
                    status_code=self._cached_status_code,
                    response=self._cached_response,
                )
                await self._storage.save_result(
                    key=self.storage_key,
                    entry=result_entry,
                    ttl=self._result_ttl,
                )
            case _:
                pass
