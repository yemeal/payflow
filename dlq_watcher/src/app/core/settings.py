from functools import cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="forbid",
        case_sensitive=False,
    )

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
    # DLQ-конвенция (contracts/README): у каждого топика есть парный <топик>.dlq;
    # watcher подписывается на все разом по regex-паттерну
    KAFKA_DLQ_PATTERN: str = ".*\\.dlq$"
    KAFKA_CONSUMER_GROUP: str = "dlq-watcher"

    # --- метрики ---
    METRICS_PORT: int = 9100


@cache
def get_settings() -> Settings:
    return Settings()
