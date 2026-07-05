from typing import Protocol

from app.services.idempotency.domain import AcquireLockResult, IdempotencyEntry


class IdempotencyStorageProtocol(Protocol):
    """
    Абстракция хранилища идемпотентности.

    IdempotencyGuard зависит ТОЛЬКО от этого протокола
    Конкретные реализации (Redis, PostgreSQL, in-memory) скрыты за этим интерфейсом
    """

    async def acquire_lock(
        self,
        key: str,
        lock_value: str,
        ttl: int,
    ) -> AcquireLockResult:
        """
        Атомарно попытаться захватить lock

        Возвращает AcquireLockResult:
            - LOCK_ACQUIRED: lock захвачен, existing_entry = None
            - ENTRY_EXISTS: ключ уже существует, existing_entry содержит текущую запись
        """
        ...

    async def release_lock(
        self,
        key: str,
        expected_value: str,
    ) -> bool:
        """
        Освободить lock, только если текущее значение совпадает с expected_value.

        Возвращает True, если lock был успешно удалён.
        """
        ...

    async def save_result(
        self,
        key: str,
        entry: IdempotencyEntry,
        ttl: int,
    ) -> None:
        """
        Сохранить результат обработки (status=DONE) с TTL.

        Перезаписывает текущее значение ключа.
        """
        ...
