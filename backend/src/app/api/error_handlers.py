"""Глобальные обработчики ошибок API."""

import logging

import starlette.exceptions as starlette_exceptions
from fastapi import Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette import status as starlette_status

from app.const.http import (
    INTERNAL_ERROR_MESSAGE,
    ROUTE_NOT_FOUND_MESSAGE,
    VALIDATION_ERROR_MESSAGE,
)
from app.enums import ErrorCode
from app.services.exceptions import ServiceError

logger = logging.getLogger(__name__)


async def service_error_handler(_request: Request, error: ServiceError) -> JSONResponse:
    """Преобразовать доменную ошибку в единый JSON-ответ.

    Args:
        _request: Входящий запрос.
        error: Доменная ошибка.

    Returns:
        Единый JSON-ответ с кодом доменной ошибки.
    """
    return JSONResponse(
        status_code=error.status_code,
        content={"error_code": error.error_code, "message": error.message},
    )


async def route_not_found_handler(
    _request: Request,
    error: starlette_exceptions.HTTPException,
) -> JSONResponse:
    """Преобразовать 404 неизвестного маршрута в единый JSON-ответ.

    Args:
        _request: Входящий запрос.
        error: HTTP-ошибка Starlette со статусом 404.

    Returns:
        JSON-ответ not_found с заголовками исходной ошибки.
    """
    return JSONResponse(
        status_code=starlette_status.HTTP_404_NOT_FOUND,
        content={"error_code": ErrorCode.NOT_FOUND, "message": ROUTE_NOT_FOUND_MESSAGE},
        headers=error.headers,
    )


async def validation_error_handler(_request: Request, _error: RequestValidationError) -> JSONResponse:
    """Преобразовать ошибку Pydantic в единый JSON-ответ.

    Args:
        _request: Входящий запрос.
        _error: Ошибка валидации запроса.

    Returns:
        Единый JSON-ответ об ошибке валидации.
    """
    return JSONResponse(
        status_code=starlette_status.HTTP_422_UNPROCESSABLE_CONTENT,
        content={"error_code": ErrorCode.VALIDATION_ERROR, "message": VALIDATION_ERROR_MESSAGE},
    )


async def unhandled_error_handler(_request: Request, error: Exception) -> JSONResponse:
    """Преобразовать необработанное исключение в единый JSON 500 без деталей.

    Args:
        _request: Входящий запрос.
        error: Необработанное исключение.

    Returns:
        Нейтральный JSON-ответ с кодом 500.
    """
    logger.exception("Unhandled error", exc_info=error)
    return JSONResponse(
        status_code=starlette_status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"error_code": ErrorCode.INTERNAL_ERROR, "message": INTERNAL_ERROR_MESSAGE},
    )
