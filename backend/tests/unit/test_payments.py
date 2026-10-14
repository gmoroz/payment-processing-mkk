"""Unit-тесты схем и fingerprint платежа."""

from decimal import Decimal

import pytest
from pydantic import ValidationError

from app.const.payments import WEBHOOK_URL_MAX_LENGTH
from app.schemas.payments import PaymentCreate
from app.services.payments import calculate_request_fingerprint


def _payload(**changes: object) -> PaymentCreate:
    """Создать валидное тело платежа с изменениями."""
    values: dict[str, object] = {
        "amount": "10.00",
        "currency": "RUB",
        "description": "description",
        "metadata": {"first": 1, "second": {"nested": True}},
        "webhook_url": "https://example.com/webhook",
    }
    values.update(changes)
    return PaymentCreate.model_validate(values)


def test_payment_schema_accepts_decimal_string_and_json_number() -> None:
    """Сумма принимается строкой и числом без перехода в float в схеме."""
    decimal_string_payload = _payload(amount="125.50")
    integer_string_payload = _payload(amount="1")
    number_payload = _payload(amount=1.5)

    assert decimal_string_payload.amount == Decimal("125.50")
    assert integer_string_payload.amount == Decimal("1")
    assert number_payload.amount == Decimal("1.5")


@pytest.mark.parametrize(
    "amount",
    ["0", "-1.00", "1.001", "not-a-number"],
)
def test_payment_schema_rejects_invalid_amount(amount: str) -> None:
    """Сумма с неверным значением или масштабом отклоняется."""
    with pytest.raises(ValidationError):
        _payload(amount=amount)


def test_payment_schema_defaults_metadata_to_empty_object() -> None:
    """Отсутствующая metadata становится пустым объектом."""
    payload = _payload()
    payload_without_metadata = PaymentCreate.model_validate(
        {
            "amount": "10.00",
            "currency": "RUB",
            "description": "description",
            "webhook_url": "https://example.com/webhook",
        }
    )

    assert payload.metadata
    assert payload_without_metadata.metadata == {}


def test_payment_schema_preserves_metadata_and_strips_description() -> None:
    """Ключи и значения metadata сохраняются, описание нормализуется."""
    payload = _payload(
        description=" description ",
        metadata={" key ": " value ", "nested": {" inner ": " nested "}},
    )

    assert payload.description == "description"
    assert payload.metadata == {" key ": " value ", "nested": {" inner ": " nested "}}


def test_payment_schema_rejects_whitespace_only_description() -> None:
    """Описание из одних пробелов отклоняется до сохранения."""
    with pytest.raises(ValidationError):
        _payload(description="   ")


def test_payment_schema_rejects_webhook_url_when_normalized_length_exceeds_column() -> None:
    """URL, который после percent-encoding длиннее колонки, отклоняется."""
    raw_url = "https://example.com/" + ("я" * 339)

    assert len(raw_url) <= WEBHOOK_URL_MAX_LENGTH
    with pytest.raises(ValidationError):
        _payload(webhook_url=raw_url)


def test_payment_fingerprint_ignores_metadata_key_order() -> None:
    """Порядок ключей metadata не меняет fingerprint."""
    first = _payload(metadata={"a": 1, "b": {"c": 2}})
    second = _payload(metadata={"b": {"c": 2}, "a": 1})

    assert calculate_request_fingerprint(first) == calculate_request_fingerprint(second)


def test_payment_fingerprint_ignores_amount_scale() -> None:
    """Сумма с разным числом нулей после точки даёт один fingerprint."""
    first = _payload(amount="10.5")
    second = _payload(amount="10.50")

    assert calculate_request_fingerprint(first) == calculate_request_fingerprint(second)


@pytest.mark.parametrize("field_name", tuple(PaymentCreate.model_fields))
def test_payment_fingerprint_changes_for_each_payment_field(field_name: str) -> None:
    """Изменение каждого поля PaymentCreate меняет fingerprint."""
    replacements: dict[str, object] = {
        "amount": "10.01",
        "currency": "USD",
        "description": "other",
        "metadata": {"a": 2},
        "webhook_url": "https://example.com/other",
    }
    assert set(replacements) == set(PaymentCreate.model_fields)

    first = _payload()
    second = _payload(**{field_name: replacements[field_name]})

    assert calculate_request_fingerprint(first) != calculate_request_fingerprint(second)
