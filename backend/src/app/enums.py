"""Доменные перечисления."""

from enum import StrEnum


class Currency(StrEnum):
    """Поддерживаемая валюта платежа."""

    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentStatus(StrEnum):
    """Статус платежа."""

    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class ErrorCode(StrEnum):
    """Машинный код ошибки API."""

    NOT_FOUND = "not_found"
    VALIDATION_ERROR = "validation_error"
    UNAUTHORIZED = "unauthorized"
    IDEMPOTENCY_KEY_REUSED = "idempotency_key_reused"
    INTERNAL_ERROR = "internal_error"
