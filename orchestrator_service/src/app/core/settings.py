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

    # --- database (sagas + processed_events + outbox - источник правды оркестратора) ---
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

    # --- kafka: топики ---
    KAFKA_BOOTSTRAP_SERVERS: str
    # шина саги заказа: order.created, ответы inventory, финальные saga.*
    KAFKA_ORDERS_EVENTS_TOPIC: str = "orders.events"
    # события реального payment_service (PayFlow)
    KAFKA_PAYMENTS_EVENTS_TOPIC: str = "payments.events"
    # команды участникам
    KAFKA_INVENTORY_COMMANDS_TOPIC: str = "inventory.commands"
    KAFKA_PAYMENTS_COMMANDS_TOPIC: str = "payments.commands"
    # DLQ-конвенция: парный топик = <исходный топик>.dlq (contracts/README)

    # --- kafka: consumer group ---
    KAFKA_CONSUMER_GROUP: str = "orchestrator-saga"

    # --- retry-политика шага саги ---
    SAGA_MAX_STEP_ATTEMPTS: int = 3
    # exponential backoff: base * 2^attempt, плюс jitter (доля от задержки);
    # без jitter ретраи синхронизируются и устраивают thundering herd
    SAGA_RETRY_BACKOFF_BASE_SECONDS: float = 2.0
    SAGA_RETRY_BACKOFF_JITTER: float = 0.2

    # --- таймауты шагов (per-step значения задаёт SagaDefinition) ---
    SAGA_DEFAULT_STEP_TIMEOUT_SECONDS: float = 30.0
    # шаг оплаты: человек платит руками, дедлайн бизнесовый
    PAYMENT_WAIT_TIMEOUT_SECONDS: int = 1800
    # инвариант (итерация 3, п.1): TTL резерва >= дедлайн оплаты + буфер;
    # проверяется fail-fast при сборке SagaDefinition
    RESERVATION_TTL_SECONDS: int = 2100
    RESERVATION_TTL_BUFFER_SECONDS: int = 300

    # --- фоновый поллер (retry + deadline) ---
    SAGA_POLLER_INTERVAL_SECONDS: float = 1.0
    SAGA_POLLER_BATCH_SIZE: int = 100

    # --- outbox relay ---
    OUTBOX_MAX_PUBLISH_ATTEMPTS: int = 5
    OUTBOX_RELAY_POLL_INTERVAL_SECONDS: float = 2.0
    OUTBOX_RELAY_BATCH_SIZE: int = 50

    # --- admin api (read-only, роль admin из JWT auth_service) ---
    JWT_SECRET: str
    JWT_ALGORITHM: str = "HS256"

    # --- метрики (prometheus) ---
    METRICS_PORT: int = 9100


@cache
def get_settings() -> Settings:
    return Settings()
