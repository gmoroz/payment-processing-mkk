"""Unit-тесты обработчиков ошибок API."""

import json
from unittest.mock import MagicMock

import starlette.exceptions as starlette_exceptions
from fastapi.exceptions import RequestValidationError

from app.api.error_handlers import (
    route_not_found_handler,
    service_error_handler,
    unhandled_error_handler,
    validation_error_handler,
)
from app.const.http import (
    INTERNAL_ERROR_MESSAGE,
    PAYMENT_NOT_FOUND_MESSAGE,
    ROUTE_NOT_FOUND_MESSAGE,
    VALIDATION_ERROR_MESSAGE,
)
from app.enums import ErrorCode
from app.services.exceptions import NotFoundError


async def test_service_error_handler_returns_unified_body() -> None:
    """ServiceError превращается в {error_code, message}."""
    response = await service_error_handler(MagicMock(), NotFoundError())

    assert response.status_code == 404
    assert response.body is not None
    assert b"not_found" in response.body
    assert PAYMENT_NOT_FOUND_MESSAGE.encode() in response.body


async def test_validation_error_handler_returns_422() -> None:
    """RequestValidationError даёт единый validation_error."""
    error = RequestValidationError([{"type": "missing", "loc": ("body",), "msg": "Field required", "input": None}])

    response = await validation_error_handler(MagicMock(), error)

    assert response.status_code == 422
    assert ErrorCode.VALIDATION_ERROR.value.encode() in response.body
    payload = json.loads(bytes(response.body))
    assert payload["message"] == VALIDATION_ERROR_MESSAGE


async def test_http_404_message_is_neutral_and_keeps_headers() -> None:
    """JSON-404 неизвестного маршрута не отдаёт сырой detail и сохраняет заголовки."""
    error = starlette_exceptions.HTTPException(status_code=404, detail="Not Found", headers={"X-Trace": "abc"})

    response = await route_not_found_handler(MagicMock(), error)

    payload = json.loads(bytes(response.body))
    assert response.status_code == 404
    assert payload == {"error_code": "not_found", "message": ROUTE_NOT_FOUND_MESSAGE}
    assert response.headers["x-trace"] == "abc"


async def test_unhandled_error_maps_to_internal_error() -> None:
    """Необработанное исключение отдаёт JSON 500 с internal_error."""
    response = await unhandled_error_handler(MagicMock(), RuntimeError("secret stack"))

    payload = json.loads(bytes(response.body))
    assert response.status_code == 500
    assert payload == {"error_code": "internal_error", "message": INTERNAL_ERROR_MESSAGE}
    assert b"secret" not in response.body
