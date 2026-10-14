"""Схемы платежей."""

import math
from datetime import datetime
from decimal import Decimal
from typing import Annotated
from uuid import UUID

from pydantic import AnyHttpUrl, BaseModel, ConfigDict, Field, StringConstraints, field_validator

from app.const.payments import (
    PAYMENT_DESCRIPTION_MAX_LENGTH,
    WEBHOOK_URL_MAX_LENGTH,
)
from app.enums import Currency, PaymentStatus


def _require_postgres_text(value: str) -> None:
    """Проверить совместимость строки с текстовым типом PostgreSQL."""
    if "\x00" in value:
        raise ValueError("string contains a null byte")
    # Суррогаты не кодируются в UTF-8: без проверки расчёт отпечатка запроса даёт 500 вместо 422.
    value.encode("utf-8")


def _require_postgres_json(value: object) -> None:
    """Проверить совместимость значения с JSONB PostgreSQL."""
    if isinstance(value, str):
        _require_postgres_text(value)
    elif isinstance(value, float) and not math.isfinite(value):
        raise ValueError("number must be finite")
    elif isinstance(value, dict):
        for metadata_key, metadata_value in value.items():
            _require_postgres_text(metadata_key)
            _require_postgres_json(metadata_value)
    elif isinstance(value, list):
        for metadata_value in value:
            _require_postgres_json(metadata_value)


class PaymentCreate(BaseModel):
    """Данные для создания платежа."""

    amount: Decimal = Field(gt=Decimal("0"), max_digits=18, decimal_places=2)
    currency: Currency
    description: Annotated[
        str,
        StringConstraints(
            strip_whitespace=True,
            min_length=1,
            max_length=PAYMENT_DESCRIPTION_MAX_LENGTH,
        ),
    ]
    metadata: dict[str, object] = Field(default_factory=dict)
    webhook_url: AnyHttpUrl = Field(max_length=WEBHOOK_URL_MAX_LENGTH)

    @field_validator("description")
    @staticmethod
    def description_is_storable(value: str) -> str:
        """Проверить, что описание сохранится в PostgreSQL.

        Args:
            value: Описание платежа после нормализации пробелов.

        Returns:
            Проверенное описание платежа.
        """
        _require_postgres_text(value)
        return value

    @field_validator("metadata")
    @staticmethod
    def metadata_is_storable(value: dict[str, object]) -> dict[str, object]:
        """Проверить, что metadata сохранится в PostgreSQL JSONB.

        Args:
            value: Metadata платежа.

        Returns:
            Проверенная metadata платежа.
        """
        _require_postgres_json(value)
        return value


class PaymentAccepted(BaseModel):
    """Ответ на принятый платёж."""

    payment_id: UUID
    status: PaymentStatus
    created_at: datetime


class PaymentRead(BaseModel):
    """Полное представление платежа."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, object] = Field(validation_alias="metadata_json")
    status: PaymentStatus
    idempotency_key: str
    webhook_url: AnyHttpUrl
    created_at: datetime
    processed_at: datetime | None
