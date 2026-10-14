"""Проверки бизнес-правил обработки платежа."""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from app.database.models.payments import Payment
from app.database.repositories.payments import PaymentRepository
from app.enums import Currency, PaymentStatus
from app.services.payment_finalization import finalize_payment
from app.services.payment_processing import simulate_processing


@pytest.mark.parametrize(
    ("success_rate", "random_value", "expected_status"),
    [
        (0.9, 0.89, PaymentStatus.SUCCEEDED),
        (0.9, 0.95, PaymentStatus.FAILED),
    ],
)
async def test_simulate_processing_with_zero_duration_and_boundary_returns_expected_status(
    monkeypatch: pytest.MonkeyPatch,
    success_rate: float,
    random_value: float,
    expected_status: PaymentStatus,
) -> None:
    """Граница success_rate определяет статус при нулевой длительности."""
    monkeypatch.setattr("app.services.payment_processing.random.random", lambda: random_value)

    status = await simulate_processing(0, 0, success_rate)

    assert status is expected_status


async def test_finalize_with_final_payment_keeps_existing_status() -> None:
    """Повторная доставка не меняет уже финальный статус платежа."""
    repository = AsyncMock(spec=PaymentRepository)
    payment_id = uuid4()
    created_at = datetime.now(UTC)
    payment = Payment(
        id=payment_id,
        amount=Decimal("10.00"),
        currency=Currency.RUB,
        description="description",
        metadata_json={},
        status=PaymentStatus.FAILED,
        idempotency_key="key",
        request_fingerprint="fingerprint",
        webhook_url="https://example.com/webhook",
        created_at=created_at,
        processed_at=None,
    )
    repository.get_by_id_for_update.return_value = payment

    result = await finalize_payment(repository, payment_id, PaymentStatus.SUCCEEDED)

    assert result.id == payment_id
    assert result.status is PaymentStatus.FAILED
    assert payment.status is PaymentStatus.FAILED
