"""Конфигурация приложения из окружения и dotenv-файлов."""

from pydantic import AmqpDsn, Field, PostgresDsn, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from app.const.auth import DEVELOPMENT_API_KEY, MIN_API_KEY_LENGTH


class Settings(BaseSettings):
    """Настройки backend с production-проверкой секрета."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    debug: bool
    db_url: PostgresDsn
    api_key: SecretStr
    rabbitmq_url: AmqpDsn
    relay_poll_interval_seconds: float = Field(default=1, gt=0)
    relay_batch_size: int = Field(default=100, gt=0)
    consumer_concurrency: int = Field(default=4, ge=1)
    publish_confirm_timeout_seconds: float = Field(default=5, gt=0)
    rabbitmq_connect_timeout_seconds: float = Field(default=5, gt=0)
    processing_min_seconds: float = Field(default=2, ge=0)
    processing_max_seconds: float = Field(default=5, ge=0)
    processing_success_rate: float = Field(default=0.9, ge=0, le=1)
    webhook_timeout_seconds: float = Field(default=5, gt=0)
    max_delivery_attempts: int = Field(default=3, ge=1)
    retry_base_delay_seconds: float = Field(default=2, gt=0)

    @property
    def async_db_url(self) -> str:
        """Вернуть SQLAlchemy DSN с драйвером asyncpg.

        Returns:
            DSN PostgreSQL для asyncpg.
        """
        return str(self.db_url).replace("postgresql://", "postgresql+asyncpg://", 1)

    @model_validator(mode="after")
    def _validate_processing_range(self) -> "Settings":
        """Проверить порядок границ длительности процессинга."""
        if self.processing_min_seconds > self.processing_max_seconds:
            raise ValueError("PROCESSING_MIN_SECONDS не может превышать PROCESSING_MAX_SECONDS.")
        return self

    @model_validator(mode="after")
    def _guard_production_secret(self) -> "Settings":
        """Проверить секрет API в production-режиме."""
        api_key = self.api_key.get_secret_value()
        if not self.debug:
            if api_key == DEVELOPMENT_API_KEY:
                raise ValueError("При DEBUG=false API_KEY не может содержать тестовое значение.")
            if len(api_key) < MIN_API_KEY_LENGTH:
                raise ValueError(f"При DEBUG=false API_KEY должен быть не короче {MIN_API_KEY_LENGTH} символов.")
        return self


settings = Settings()
