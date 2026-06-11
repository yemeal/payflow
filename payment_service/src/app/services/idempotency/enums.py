from enum import Enum


class IdempotencyKeyStatus(Enum):
    PROCESSING = "PROCESSING"
    DONE = "DONE"


class LockStatus(Enum):
    LOCKED = "LOCKED"
    EXISTS = "EXISTS"
