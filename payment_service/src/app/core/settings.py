from functools import cache
from typing import Literal

from pydantic import PostgresDsn, Field
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

    # --- logs ---
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

    # # по умолчанию pydantic все сложные типы пытается преобразовать как JSON,
    # # но в нашем случае нам это не нужно (значения передаются не как валидная JSON-строка, а разделенные запятой)
    # # поэтому аннотируем поле с NoDecode, чтобы он передавал сырую строку в кастомный валидатор
    # MUTE_LOGGERS: Annotated[list[str], NoDecode] = []
    #
    # @field_validator("MUTE_LOGGERS", mode="before")
    # @classmethod
    # def parse_mute_loggers(cls, v: str | list[str]) -> list[str]:
    #     if isinstance(v, str):
    #         return [logger.strip() for logger in v.split(",") if logger.strip()]
    #     return v

    # --- redis ---
    REDIS_HOST: str
    REDIS_PORT: int
    REDIS_DB: int = 0

    @property
    def REDIS_URL(self) -> str:
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"

    # --- idempotency --
    IDEMPOTENCY_LOCK_TTL: int = 60  # время жизни лока в секундах (по дефолту 60)
    IDEMPOTENCY_RESULT_TTL: int = (
        48 * 60 * 60
    )  # какое количество времени результат хранится в кеше

    # --- payment provider ---
    CIRCUIT_BREAKER_MAX_ATTEMPTS: int
    CIRCUIT_BREAKER_RECOVERY_TIMEOUT: float
    PAYMENT_PROVIDER_URL: str

    # --- kafka ---
    KAFKA_BOOTSTRAP_SERVERS: str


@cache
def get_settings() -> Settings:
    return Settings()
