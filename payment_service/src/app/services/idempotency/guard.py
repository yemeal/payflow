from __future__ import annotations
from typing import Callable, Awaitable

import structlog

from app.core.exceptions import (
    IdempotencyKeyPayloadMismatchError,
    IdempotencyKeyAlreadyProcessingError,
    IdempotencyStateInconsistencyError,
)
from app.core.settings import Settings
from app.services.idempotency.domain import IdempotencyCachedResult, IdempotencyEntry
from app.services.idempotency.enums import (
    IdempotencyKeyStatus,
    GuardState,
    LockAcquireStatus,
)
from app.services.idempotency.protocols import IdempotencyStorageProtocol
from app.utils.compute_payload_hash import compute_payload_hash

logger = structlog.get_logger()


class IdempotencyGuard:
    """
    Контекстный менеджер для идемпотентной обработки запросов, использующий FSM.
    Не завсисит от конкретных хранилищ, хранилища инжектятся через DI
    с помощью адаптеров, реализующих интерфейс `IdempotencyStorageProtocol`

    db_lookup -> функция похода в БД для обеспечения Two-Level проверки.
    Она принимает на вход ключ идемпотентности и возвращает `IdempotencyCachedResult | None`

    Пример применения:
    ```python
        async with IdempotencyService() as service:
            if service.has_cached_result:
                return service.cached_response
            result = await business_logic()
            service.set_result(status_code=201, response=result)
            return result
    ```
    """

    def __init__(
        self,
        settings: Settings,
        storage: IdempotencyStorageProtocol,
        idempotency_key: str,
        payload: dict,
        db_lookup: (
            Callable[[str], Awaitable[IdempotencyCachedResult | None]] | None
        ) = None,
    ) -> None:
        self._payload_hash = compute_payload_hash(payload)
        self._storage = storage
        self._idempotency_key = idempotency_key

        self._state = GuardState.NEW  # при создании - состояние new

        self._cached_response: dict | None = None
        self._cached_status_code: int | None = None

        self._lock_entry: IdempotencyEntry = IdempotencyEntry(
            status=IdempotencyKeyStatus.PROCESSING, payload_hash=self._payload_hash
        )
        self._lock_value: str = self._lock_entry.model_dump_json()
        self._lock_ttl = settings.IDEMPOTENCY_LOCK_TTL
        self._result_ttl = settings.IDEMPOTENCY_RESULT_TTL

        self._db_lookup = db_lookup

    @property
    def redis_idempotency_key(self) -> str:
        return f"idempotency:{self._idempotency_key}"

    @property
    def has_cached_result(self) -> bool:
        """Есть ли кешированный результат (из редиса или бд)"""
        return self._cached_response is not None

    @property
    def cached_response(self) -> dict | None:
        """Кешированный response (JSON-serializable dict)"""
        return self._cached_response

    @property
    def cached_status_code(self) -> int | None:
        """HTTP-статус кешированного ответа"""
        return self._cached_status_code

    def set_result(self, status_code: int, response: dict) -> None:
        """Вызывается после успешной бизнес-логики"""
        if self._state != GuardState.PROCESSING:
            return  # изменить результат можно только если мы реально его процессили

        self._cached_status_code = status_code
        self._cached_response = response
        self._state = GuardState.COMPLETED

    async def __aenter__(self) -> IdempotencyGuard:
        # захватываем лок сразу при входе
        result = await self._storage.acquire_lock(
            key=self.redis_idempotency_key,
            lock_value=self._lock_value,
            ttl=self._lock_ttl,
        )

        match result.status:
            case LockAcquireStatus.LOCK_ACQUIRED:
                self._state = GuardState.LOCK_ACQUIRED
                logger.info(
                    "idempotency_lock_acquired",
                    idempotency_key=self._idempotency_key,
                )

                # Сделали лок - идем в БД проверять наличие готового рез-та с таким же ключом идемпотентности.
                # Таким образом в БД не летят 100 запросов, проскакивает лишь один
                # (в случае, если нам была передана подходящая функция для похода в БД)
                if self._db_lookup is not None:
                    existing = await self._db_lookup(self._idempotency_key)
                    if existing is not None:
                        # если в БД существует сущность с таким ключом идемпотентности
                        self._cached_response = existing.response
                        self._cached_status_code = existing.status_code
                        self._state = GuardState.DB_HIT
                        logger.info(
                            "idempotency_cache_miss_found_in_db",
                            idempotency_key=self._idempotency_key,
                        )
                    else:
                        self._state = GuardState.PROCESSING
                        logger.info(
                            "idempotency_not_found_in_db, processing",
                            idempotency_key=self._idempotency_key,
                        )
                else:
                    self._state = GuardState.PROCESSING
                    logger.debug(
                        "idempotency_processing",
                        idempotency_key=self._idempotency_key,
                    )

            case LockAcquireStatus.ENTRY_EXISTS:
                entry = result.existing_entry
                if entry is None:
                    raise IdempotencyStateInconsistencyError(
                        "ENTRY_EXISTS status must provide existing_entry"
                    )

                if entry.status == IdempotencyKeyStatus.PROCESSING:
                    raise IdempotencyKeyAlreadyProcessingError

                if entry.status == IdempotencyKeyStatus.DONE:
                    if entry.payload_hash == self._payload_hash:
                        self._cached_response = entry.response
                        self._cached_status_code = entry.status_code
                        self._state = GuardState.CACHE_HIT
                        logger.info(
                            "idempotency_cache_hit",
                            idempotency_key=self._idempotency_key,
                        )
                    else:
                        raise IdempotencyKeyPayloadMismatchError

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type is not None and self._state == GuardState.PROCESSING:
            self._state = GuardState.FAILED

        match self._state:
            case GuardState.FAILED:
                # в случае фейла освобождаем блокировку
                await self._storage.release_lock(
                    key=self.redis_idempotency_key, expected_value=self._lock_value
                )
                logger.warning(
                    "idempotency_lock_released_due_to_error",
                    idempotency_key=self._idempotency_key,
                    error_type=exc_type.__name__ if exc_type else None,
                    error=str(exc_val),
                )

            case GuardState.COMPLETED | GuardState.DB_HIT:
                # в случае успеха (COMPLETED) или поднятия из БД (DB_HIT),
                # кешируем результат в Redis
                result_entry = IdempotencyEntry(
                    status=IdempotencyKeyStatus.DONE,
                    payload_hash=self._payload_hash,
                    status_code=self._cached_status_code,
                    response=self._cached_response,
                )
                await self._storage.save_result(
                    key=self.redis_idempotency_key,
                    entry=result_entry,
                    ttl=self._result_ttl,
                )

            case _:
                # CACHE_HIT или другие промежуточные состояния ничего не делают при выходе
                pass
