"""Контракт HTTP: validation и messages."""

import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient, Headers

from app.config import settings
from app.const.http import API_KEY_HEADER
from app.database.session import get_session
from app.enums import PaymentStatus
from app.main import create_app
from app.schemas.payments import PaymentAccepted, PaymentCreate
from app.services.payments import PaymentService

API_KEY = settings.api_key.get_secret_value()
PAYMENT_ID = "00000000-0000-0000-0000-000000000001"


class _FakeSession:
    """Заглушка AsyncSession для запросов без записи."""

    async def get(self, _model: object, _entity_id: UUID, **_kwargs: object) -> None:
        """Имитировать отсутствие платежа."""
        return None


async def _fake_session() -> AsyncIterator[_FakeSession]:
    """Подменить сессию БД в API-зависимостях."""
    yield _FakeSession()


@pytest_asyncio.fixture
async def contract_client() -> AsyncIterator[AsyncClient]:
    """Клиент против приложения без статики и с сессией-заглушкой."""
    application = create_app()
    application.dependency_overrides[get_session] = _fake_session
    async with AsyncClient(transport=ASGITransport(app=application), base_url="http://test") as http:
        yield http


AUTH_HEADERS = {API_KEY_HEADER: API_KEY}


async def test_non_json_body_returns_422_validation_error(contract_client: AsyncClient) -> None:
    """Не-JSON при Content-Type application/json -> 422 validation_error."""
    response = await contract_client.post(
        "/api/v1/payments",
        content=b"not-json{{{",
        headers={**AUTH_HEADERS, "Idempotency-Key": "contract-key", "Content-Type": "application/json"},
    )

    assert response.status_code == 422
    assert response.json() == {
        "error_code": "validation_error",
        "message": "Request validation failed",
    }


async def test_missing_idempotency_key_returns_validation_error(contract_client: AsyncClient) -> None:
    """POST без Idempotency-Key -> 422 validation_error."""
    response = await contract_client.post(
        "/api/v1/payments",
        json={
            "amount": "10.00",
            "currency": "RUB",
            "description": "test",
            "webhook_url": "https://example.com/webhook",
        },
        headers=AUTH_HEADERS,
    )

    assert response.status_code == 422
    assert response.json()["error_code"] == "validation_error"


@pytest.mark.parametrize(
    ("method", "path"),
    [("GET", f"/api/v1/payments/{PAYMENT_ID}"), ("POST", "/api/v1/payments")],
)
async def test_api_endpoint_without_key_returns_unauthorized(
    contract_client: AsyncClient,
    method: str,
    path: str,
) -> None:
    """API-эндпоинт без X-API-Key -> 401 unauthorized."""
    response = await contract_client.request(method, path)

    assert response.status_code == 401
    assert response.json() == {"error_code": "unauthorized", "message": "Unauthorized"}


async def test_api_endpoint_with_wrong_key_returns_unauthorized(contract_client: AsyncClient) -> None:
    """API-эндпоинт с неверным X-API-Key -> 401 unauthorized."""
    response = await contract_client.get(
        f"/api/v1/payments/{PAYMENT_ID}",
        headers={API_KEY_HEADER: "wrong-key"},
    )

    assert response.status_code == 401
    assert response.json()["error_code"] == "unauthorized"


async def test_unknown_api_route_returns_english_not_found(contract_client: AsyncClient) -> None:
    """Неизвестный /api/... -> 404 not_found с английским Route not found."""
    response = await contract_client.get("/api/v1/nope")

    assert response.status_code == 404
    assert response.json() == {"error_code": "not_found", "message": "Route not found"}


async def test_unsupported_method_returns_standard_405_with_allow(contract_client: AsyncClient) -> None:
    """Неподдерживаемый метод отдаётся стандартным ответом FastAPI с заголовком Allow."""
    response = await contract_client.put(f"/api/v1/payments/{PAYMENT_ID}", headers=AUTH_HEADERS)

    assert response.status_code == 405
    assert response.json() == {"detail": "Method Not Allowed"}
    assert response.headers["allow"] == "GET"


async def test_missing_payment_returns_english_payment_not_found(contract_client: AsyncClient) -> None:
    """Отсутствующий платёж -> 404 not_found с английским Payment not found."""
    response = await contract_client.get(f"/api/v1/payments/{PAYMENT_ID}", headers=AUTH_HEADERS)

    assert response.status_code == 404
    assert response.json() == {"error_code": "not_found", "message": "Payment not found"}


async def test_health_without_api_key_returns_unauthorized(contract_client: AsyncClient) -> None:
    """GET /health без X-API-Key -> 401 unauthorized."""
    response = await contract_client.get("/health")

    assert response.status_code == 401
    assert response.json() == {"error_code": "unauthorized", "message": "Unauthorized"}


async def test_health_with_wrong_api_key_returns_unauthorized(contract_client: AsyncClient) -> None:
    """GET /health с неверным X-API-Key -> 401 unauthorized."""
    response = await contract_client.get("/health", headers={API_KEY_HEADER: "wrong-key"})

    assert response.status_code == 401
    assert response.json() == {"error_code": "unauthorized", "message": "Unauthorized"}


async def test_health_with_api_key_returns_ok(contract_client: AsyncClient) -> None:
    """GET /health с верным X-API-Key -> 200 с признаком готовности."""
    response = await contract_client.get("/health", headers=AUTH_HEADERS)

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


async def _accept_without_database(
    _service: PaymentService,
    _payload: PaymentCreate,
    _idempotency_key: str,
) -> PaymentAccepted:
    """Вернуть принятый платёж без обращения к базе данных."""
    return PaymentAccepted(
        payment_id=uuid4(),
        status=PaymentStatus.PENDING,
        created_at=datetime.now(UTC),
    )


async def _session_fails_on_commit() -> AsyncIterator[object]:
    """Вернуть объект и завершиться ошибкой commit."""
    yield object()
    raise RuntimeError("commit failed")


def _payment_body(**changes: object) -> bytes:
    """Сформировать JSON-тело платежа для HTTP-теста."""
    body: dict[str, object] = {
        "amount": "10.00",
        "currency": "RUB",
        "description": "description",
        "metadata": {},
        "webhook_url": "https://example.com/hook",
    }
    body.update(changes)
    return json.dumps(body).encode()


async def test_payment_commit_failure_returns_server_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Падение commit до отправки ответа видно клиенту как 5xx, а не 202."""
    monkeypatch.setattr(PaymentService, "create", _accept_without_database)
    application = create_app()
    application.dependency_overrides[get_session] = _session_fails_on_commit
    # Клиент уже получил статус, если генератор сессии закрылся после send.
    transport = ASGITransport(app=application, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://test") as http:
        response = await http.post(
            "/api/v1/payments",
            content=_payment_body(),
            headers={**AUTH_HEADERS, "Idempotency-Key": "commit-fails", "Content-Type": "application/json"},
        )

    assert response.status_code >= 500
    assert response.json() == {"error_code": "internal_error", "message": "Internal Server Error"}


async def test_api_key_with_non_ascii_byte_returns_unauthorized(contract_client: AsyncClient) -> None:
    """Байт X-API-Key выше 0x7F не роняет сравнение и даёт 401."""
    headers = Headers({API_KEY_HEADER: "\xff"}, encoding="iso-8859-1")

    response = await contract_client.get(f"/api/v1/payments/{PAYMENT_ID}", headers=headers)

    assert response.status_code == 401
    assert response.json() == {"error_code": "unauthorized", "message": "Unauthorized"}


@pytest.mark.parametrize(
    "changes",
    [
        {"description": "bad\u0000text"},
        {"description": "bad\ud800text"},
        {"description": "\ud800"},
        {"metadata": {"bad\u0000": 1}},
        {"metadata": {"note": "bad\u0000text"}},
        {"metadata": {"note": "bad\ud800"}},
        {"metadata": {"nested": {"value": float("nan")}}},
        {"metadata": {"values": [float("inf"), float("-inf")]}},
    ],
)
async def test_payment_unrepresentable_value_returns_validation_error(
    contract_client: AsyncClient,
    changes: dict[str, object],
) -> None:
    """NaN, Infinity, NUL и одиночный суррогат отклоняются до записи в Postgres."""
    response = await contract_client.post(
        "/api/v1/payments",
        content=_payment_body(**changes),
        headers={**AUTH_HEADERS, "Idempotency-Key": "unrepresentable", "Content-Type": "application/json"},
    )

    assert response.status_code == 422
    assert response.json() == {
        "error_code": "validation_error",
        "message": "Request validation failed",
    }
