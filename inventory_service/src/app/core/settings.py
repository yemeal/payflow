from functools import cache
from typing import Literal

from pydantic import Field, PostgresDsn
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="forbid",
        case_sensitive=False,
    )

    # --- database (stock_items + reservations + processed_commands + outbox) ---
    DATABASE_HOST: str
    DATABASE_PORT: int
    DATABASE_USER: str
    DATABASE_PASSWORD: str
    DATABASE_NAME: str
    RUN_MIGRATIONS: bool = False

    @property
    def DATABASE_URL(self) -> str:
        dsn = PostgresDsn.build(
            scheme="postgresql+asyncpg",
            username=self.DATABASE_USER,
            password=self.DATABASE_PASSWORD,
            host=self.DATABASE_HOST,
            port=self.DATABASE_PORT,
            path=self.DATABASE_NAME,
        )
        return str(dsn)

    # --- логирование ---
    DEV_LOGS: bool
    LOG_LEVEL: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    mute_loggers_raw: str = Field(default="", validation_alias="MUTE_LOGGERS")

    @property
    def MUTE_LOGGERS(self) -> list[str]:
        return [
            logger.strip()
            for logger in self.mute_loggers_raw.split(",")
            if logger.strip()
        ]

    # --- kafka ---
    KAFKA_BOOTSTRAP_SERVERS: str
    # команды от оркестратора
    KAFKA_COMMANDS_TOPIC: str = "inventory.commands"
    # ответные события (inventory.reserved / reserve-failed / reservation-committed /
    # commit-failed / reservation-cancelled) идут в общую шину саги
    KAFKA_EVENTS_TOPIC: str = "orders.events"
    KAFKA_CONSUMER_GROUP: str = "inventory-service-commands"

    # DLQ-конвенция: парный топик <исходный топик>.dlq (contracts/README),
    # отдельной переменной не заводим - топик выводится из имени исходного
    @property
    def KAFKA_COMMANDS_DLQ_TOPIC(self) -> str:
        return f"{self.KAFKA_COMMANDS_TOPIC}.dlq"

    # --- резервы ---
    # ttlSeconds приходит в команде (contracts/inventory/reserve.v1) - используем
    # его; это дефолт на случай отсутствия поля. Инвариант конфигурации
    # (docs/saga-design.md, 9.8): TTL резерва >= дедлайн оплаты + буфер
    # (у оркестратора 1800 + 300 = 2100)
    RESERVATION_DEFAULT_TTL_SECONDS: int = 2100

    # --- фоновый поллер автоистечения резервов ---
    EXPIRY_POLLER_INTERVAL_SECONDS: float = 10.0
    EXPIRY_POLLER_BATCH_SIZE: int = 100

    # --- outbox relay ---
    # после скольких неудачных попыток публикации запись помечается FAILED
    # ("ядовитая", разбор вручную)
    OUTBOX_MAX_PUBLISH_ATTEMPTS: int = 5
    OUTBOX_RELAY_POLL_INTERVAL_SECONDS: float = 2.0
    OUTBOX_RELAY_BATCH_SIZE: int = 50


@cache
def get_settings() -> Settings:
    return Settings()
