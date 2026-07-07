from enum import Enum, IntEnum


class LockAcquireStatus(IntEnum):
    """
    Результат попытки захвата lock.

    Числовые коды используются как протокол между Python и Lua
    Lua возвращает числа (1, 2), Python преобразует их через этот enum
    Строковые сравнения не используются
    """

    LOCK_ACQUIRED = 1
    ENTRY_EXISTS = 2


class IdempotencyKeyStatus(str, Enum):
    """Статус записи идемпотентности (хранится в Redis/DB)"""

    PROCESSING = "PROCESSING"
    DONE = "DONE"


class GuardState(str, Enum):
    """
    Состояния конечного автомата IdempotencyGuard.

    Переходы:
        NEW -> LOCK_ACQUIRED (lock успешно захвачен)
        NEW -> CACHE_HIT (запись уже существует в Redis)
        LOCK_ACQUIRED -> DB_HIT (результат найден в БД)
        LOCK_ACQUIRED -> PROCESSING (результат не найден, бизнес-логика выполняется)
        PROCESSING -> COMPLETED (бизнес-логика успешно завершена)
        PROCESSING -> FAILED (произошла ошибка)
        DB_HIT -> COMPLETED (DB-результат закеширован обратно в Redis)
    """

    NEW = "NEW"
    LOCK_ACQUIRED = "LOCK_ACQUIRED"
    CACHE_HIT = "CACHE_HIT"
    DB_HIT = "DB_HIT"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
