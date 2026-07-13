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

    # --- database ---
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

    # --- redis (кэш статусов заказов, Cache-Aside) ---
    REDIS_HOST: str
    REDIS_PORT: int
    REDIS_DB: int = 2
    ORDER_CACHE_TTL_SECONDS: int = 60

    @property
    def REDIS_URL(self) -> str:
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"

    # --- jwt (проверка access-токенов, выданных auth_service) ---
    JWT_SECRET: str
    JWT_ALGORITHM: str = "HS256"

    # --- kafka ---
    KAFKA_BOOTSTRAP_SERVERS: str
    # общая шина событий саги: сюда outbox relay публикует order.created,
    # отсюда же консьюмер читает финальные saga.completed / saga.cancelled
    KAFKA_EVENTS_TOPIC: str = "orders.events"
    KAFKA_EVENTS_DLQ_TOPIC: str = "orders.events.dlq"
    KAFKA_CONSUMER_GROUP: str = "order-service-saga-events"

    # --- outbox ---
    OUTBOX_MAX_PUBLISH_ATTEMPTS: int = 5


@cache
def get_settings() -> Settings:
    return Settings()
