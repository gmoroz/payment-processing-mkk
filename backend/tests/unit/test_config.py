"""Unit-тесты Settings и prod-guard."""

from pathlib import Path

import pytest
from pydantic import PostgresDsn, SecretStr, ValidationError

from app.config import Settings


def _settings(**changes: object) -> Settings:
    """Создать Settings с базовыми тестовыми значениями."""
    settings_values: dict[str, object] = {
        "debug": True,
        "db_url": "postgresql://app:localdev@127.0.0.1:45432/app",
        "api_key": "dev-secret-change-me-not-for-prod!!",
        "rabbitmq_url": "amqp://guest:guest@127.0.0.1:5672/app",
    }
    settings_values.update(changes)
    return Settings.model_validate(settings_values)


def test_settings_without_environment_or_dotenv_requires_configuration(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Без окружения и .env обязательные настройки не подменяются примером."""
    example_file = Path(__file__).resolve().parents[2] / ".env.example"
    (tmp_path / ".env.example").write_text(example_file.read_text(encoding="utf-8"), encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    for variable in ("DEBUG", "DB_URL", "API_KEY", "RABBITMQ_URL"):
        monkeypatch.delenv(variable, raising=False)

    with pytest.raises(ValidationError):
        Settings()


def test_settings_requires_rabbitmq_url(monkeypatch: pytest.MonkeyPatch) -> None:
    """RABBITMQ_URL обязателен для запуска worker."""
    monkeypatch.delenv("RABBITMQ_URL", raising=False)

    with pytest.raises(ValidationError, match="rabbitmq_url"):
        Settings(
            _env_file=None,
            debug=True,
            db_url=PostgresDsn("postgresql://app:localdev@127.0.0.1:45432/app"),
            api_key=SecretStr("dev-secret-change-me-not-for-prod!!"),
        )


def test_settings_accepts_dev_api_key_when_debug() -> None:
    """В DEBUG разрешён короткий dev-ключ."""
    settings = _settings()

    assert settings.debug is True
    assert settings.async_db_url.startswith("postgresql+asyncpg://")


def test_settings_rejects_dev_api_key_when_not_debug() -> None:
    """В production запрещён тестовый API_KEY."""
    with pytest.raises(ValidationError, match="API_KEY"):
        _settings(debug=False)


def test_settings_rejects_short_api_key_when_not_debug() -> None:
    """В production API_KEY должен быть достаточно длинным."""
    with pytest.raises(ValidationError, match="API_KEY"):
        _settings(debug=False, api_key="short-but-unique-not-in-dev-set")


def test_settings_rejects_invalid_processing_range() -> None:
    """Нижняя граница процессинга не может быть больше верхней."""
    with pytest.raises(ValidationError, match="PROCESSING_MIN_SECONDS"):
        _settings(processing_min_seconds=6, processing_max_seconds=5)
