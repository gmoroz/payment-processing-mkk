"""Интеграционные тесты API платежей на PostgreSQL."""

import asyncio
from collections.abc import AsyncIterator
from uuid import UUID

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.config import settings
from app.database.models.payments import Outbox, Payment
from app.database.repositories.payments import PaymentRepository
from app.database.session import get_session
from app.main import create_app
from tests.fixtures.database import _test_db_url

PAYLOAD = {
    "amount": "1250.50",
    "currency": "RUB",
    "description": "test payment",
    "metadata": {"order": "42", "nested": {"value": True}},
    "webhook_url": "https://example.com/webhook",
}
API_HEADERS = {"X-API-Key": settings.api_key.get_secret_value()}


def _headers(key: str) -> dict[str, str]:
    """Сформировать заголовки API-запроса."""
    return {**API_HEADERS, "Idempotency-Key": key}


async def test_payment_creation_persists_payment_and_one_outbox(
    client: AsyncClient,
    db_session: AsyncSession,
) -> None:
    """Создание платежа сохраняет pending и ровно одно событие outbox."""
    response = await client.post("/api/v1/payments", json=PAYLOAD, headers=_headers("create-key"))

    assert response.status_code == 202
    response_body = response.json()
    payment = await db_session.get(Payment, UUID(response_body["payment_id"]))
    outbox_events = list(await db_session.scalars(select(Outbox)))

    assert response_body["status"] == "pending"
    assert payment is not None
    assert payment.status.value == "pending"
    assert len(outbox_events) == 1
    assert outbox_events[0].event_type == "payments.new"
    assert outbox_events[0].payload == {"payment_id": response_body["payment_id"]}


async def test_payment_repeated_request_returns_original_without_second_outbox(
    client: AsyncClient,
    db_session: AsyncSession,
) -> None:
    """Повтор с тем же телом возвращает исходный платёж и не дублирует outbox."""
    first = await client.post("/api/v1/payments", json=PAYLOAD, headers=_headers("repeat-key"))
    second = await client.post("/api/v1/payments", json=PAYLOAD, headers=_headers("repeat-key"))

    assert first.status_code == 202
    assert second.status_code == 202
    assert second.json() == first.json()
    assert await db_session.scalar(select(func.count()).select_from(Outbox)) == 1


async def test_payment_reused_key_with_different_body_returns_idempotency_error(client: AsyncClient) -> None:
    """Повторный ключ с другим телом возвращает idempotency_key_reused."""
    first = await client.post("/api/v1/payments", json=PAYLOAD, headers=_headers("different-key"))
    changed_payload = {**PAYLOAD, "description": "different payment"}
    second = await client.post("/api/v1/payments", json=changed_payload, headers=_headers("different-key"))

    assert first.status_code == 202
    assert second.status_code == 422
    assert second.json()["error_code"] == "idempotency_key_reused"


async def test_payment_get_returns_all_fields(client: AsyncClient) -> None:
    """GET существующего платежа возвращает полный набор полей."""
    created = await client.post("/api/v1/payments", json=PAYLOAD, headers=_headers("get-key"))

    response = await client.get(f"/api/v1/payments/{created.json()['payment_id']}", headers=API_HEADERS)

    assert response.status_code == 200
    assert response.json() == {
        "id": created.json()["payment_id"],
        "amount": "1250.50",
        "currency": "RUB",
        "description": "test payment",
        "metadata": PAYLOAD["metadata"],
        "status": "pending",
        "idempotency_key": "get-key",
        "webhook_url": "https://example.com/webhook",
        "created_at": created.json()["created_at"],
        "processed_at": None,
    }


async def test_payment_get_missing_returns_not_found(client: AsyncClient) -> None:
    """GET отсутствующего платежа возвращает not_found."""
    response = await client.get(
        "/api/v1/payments/00000000-0000-0000-0000-000000000099",
        headers=API_HEADERS,
    )

    assert response.status_code == 404
    assert response.json() == {"error_code": "not_found", "message": "Payment not found"}


@pytest.mark.parametrize(
    ("payload", "key"),
    [
        ({**PAYLOAD, "amount": "0"}, "zero-key"),
        ({**PAYLOAD, "amount": "-1.00"}, "negative-key"),
        ({**PAYLOAD, "amount": "1.001"}, "scale-key"),
        ({**PAYLOAD, "amount": "not-a-number"}, "nan-key"),
        ({**PAYLOAD, "currency": "GBP"}, "currency-key"),
        ({**PAYLOAD, "webhook_url": "ftp://example.com/hook"}, "url-key"),
    ],
)
async def test_payment_invalid_payload_returns_validation_error(
    client: AsyncClient,
    payload: dict[str, object],
    key: str,
) -> None:
    """Некорректные поля платежа не доходят до INSERT."""
    response = await client.post("/api/v1/payments", json=payload, headers=_headers(key))

    assert response.status_code == 422
    assert response.json()["error_code"] == "validation_error"


async def test_payment_concurrent_same_key_creates_one_payment_and_outbox(
    migrated_test_database: None,
) -> None:
    """Два реальных параллельных запроса с одним ключом дедуплицируются БД."""
    race_key = "race-key"
    async_url = _test_db_url().replace("postgresql://", "postgresql+asyncpg://", 1)
    engine = create_async_engine(async_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)

    async def request_with_session(session: AsyncSession) -> dict[str, object]:
        application = create_app()

        async def override_session() -> AsyncIterator[AsyncSession]:
            async with session.begin():
                yield session

        application.dependency_overrides[get_session] = override_session
        async with AsyncClient(transport=ASGITransport(app=application), base_url="http://test") as http:
            response = await http.post("/api/v1/payments", json=PAYLOAD, headers=_headers(race_key))
        assert response.status_code == 202
        response_body = response.json()
        assert isinstance(response_body, dict)
        return {str(key): value for key, value in response_body.items()}

    try:
        async with factory() as first_session, factory() as second_session:
            responses = await asyncio.gather(
                request_with_session(first_session),
                request_with_session(second_session),
            )

        async with factory() as verification_session:
            payments = list(
                await verification_session.scalars(select(Payment).where(Payment.idempotency_key == race_key)),
            )
            payment_ids = [str(payment.id) for payment in payments]
            outbox_count = await verification_session.scalar(
                select(func.count()).select_from(Outbox).where(Outbox.payload["payment_id"].astext.in_(payment_ids)),
            )

        assert responses[0]["payment_id"] == responses[1]["payment_id"]
        assert len(payments) == 1
        assert outbox_count == 1
    finally:
        async with factory() as cleanup_session, cleanup_session.begin():
            raced_ids = await cleanup_session.scalars(select(Payment.id).where(Payment.idempotency_key == race_key))
            payment_ids = [str(payment_id) for payment_id in raced_ids]
            if payment_ids:
                await cleanup_session.execute(
                    delete(Outbox).where(Outbox.payload["payment_id"].astext.in_(payment_ids)),
                )
            await cleanup_session.execute(delete(Payment).where(Payment.idempotency_key == race_key))
        await engine.dispose()


async def test_payment_outbox_failure_rolls_back_payment(
    client: AsyncClient,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ошибка вставки outbox откатывает платёж в той же транзакции."""

    def fail_outbox(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("outbox failure")

    monkeypatch.setattr(PaymentRepository, "create_outbox_event", fail_outbox)
    with pytest.raises(RuntimeError, match="outbox failure"):
        await client.post("/api/v1/payments", json=PAYLOAD, headers=_headers("rollback-key"))

    await db_session.rollback()
    assert await db_session.scalar(select(func.count()).select_from(Payment)) == 0
