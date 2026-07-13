from enum import IntEnum, StrEnum


class LockAcquireStatus(IntEnum):
    LOCK_ACQUIRED = 1
    ENTRY_EXISTS = 2


class IdempotencyKeyStatus(StrEnum):
    PROCESSING = "PROCESSING"
    DONE = "DONE"


class GuardState(StrEnum):
    NEW = "NEW"
    LOCK_ACQUIRED = "LOCK_ACQUIRED"
    CACHE_HIT = "CACHE_HIT"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
