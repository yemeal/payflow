from functools import cache
from pathlib import Path
from typing import Literal, Self

from pydantic import Field, PostgresDsn, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from app.domain.exceptions import DomainErrors


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

    # --- redis / HTTP idempotency ---
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = Field(default=6379, ge=1, le=65535)
    REDIS_DB: int = Field(default=0, ge=0)
    IDEMPOTENCY_LOCK_TTL: int = Field(default=30, gt=0)
    IDEMPOTENCY_RESULT_TTL: int = Field(default=5 * 60, gt=0)

    @property
    def REDIS_URL(self) -> str:
        return (
            f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"
            "?socket_connect_timeout=2&socket_timeout=2"
        )

    # --- jwt ---
    # Алгоритм не настраивается извне: issuer и verifier используют только RS256.
    JWT_PRIVATE_KEY_PATH: Path
    JWT_PUBLIC_KEY_PATH: Path
    JWT_ACTIVE_KEY_ID: str
    JWT_ISSUER: str = "payflow-auth"
    JWT_SERVICE_AUDIENCE: str = "auth-service"
    jwt_audiences_raw: str = Field(
        default="auth-service,order-service",
        validation_alias="JWT_AUDIENCES",
    )
    JWT_CLOCK_SKEW_SECONDS: int = 30
    ACCESS_TOKEN_TTL_SECONDS: int = 15 * 60
    AUTH_SESSION_IDLE_TTL_SECONDS: int = 30 * 24 * 60 * 60

    @field_validator(
        "JWT_ACTIVE_KEY_ID",
        "JWT_ISSUER",
        "JWT_SERVICE_AUDIENCE",
    )
    @classmethod
    def validate_jwt_text_setting(cls, value: str) -> str:
        normalized_value = value.strip()
        if not normalized_value:
            raise DomainErrors.Token.INVALID_CONFIGURATION()
        return normalized_value

    @field_validator(
        "ACCESS_TOKEN_TTL_SECONDS",
        "AUTH_SESSION_IDLE_TTL_SECONDS",
    )
    @classmethod
    def validate_jwt_ttl(cls, ttl_seconds: int) -> int:
        if ttl_seconds <= 0:
            raise DomainErrors.Token.INVALID_CONFIGURATION()
        return ttl_seconds

    @field_validator("JWT_CLOCK_SKEW_SECONDS")
    @classmethod
    def validate_jwt_clock_skew(cls, seconds: int) -> int:
        if seconds < 0:
            raise DomainErrors.Token.INVALID_CONFIGURATION()
        return seconds

    @field_validator("jwt_audiences_raw")
    @classmethod
    def validate_jwt_audiences(cls, raw_audiences: str) -> str:
        audiences = [
            audience.strip()
            for audience in raw_audiences.split(",")
        ]
        if not audiences or any(not audience for audience in audiences):
            raise DomainErrors.Token.INVALID_CONFIGURATION()
        return ",".join(audiences)

    @property
    def JWT_AUDIENCES(self) -> frozenset[str]:
        return frozenset(self.jwt_audiences_raw.split(","))

    @model_validator(mode="after")
    def validate_local_service_audience(self) -> Self:
        if self.JWT_SERVICE_AUDIENCE not in self.JWT_AUDIENCES:
            raise DomainErrors.Token.INVALID_CONFIGURATION()
        return self


@cache
def get_settings() -> Settings:
    return Settings()
