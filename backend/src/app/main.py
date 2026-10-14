"""FastAPI-приложение."""

from fastapi import FastAPI
from fastapi import status as http_status
from fastapi.exceptions import RequestValidationError

from app.api import api_router
from app.api.dependencies import ApiKeyDep
from app.api.error_handlers import (
    route_not_found_handler,
    service_error_handler,
    unhandled_error_handler,
    validation_error_handler,
)
from app.config import settings
from app.const.http import API_PREFIX
from app.schemas.common import ErrorRead
from app.services.exceptions import ServiceError


def create_app() -> FastAPI:
    """Собрать API и обработчики ошибок.

    Returns:
        Настроенное FastAPI-приложение.
    """
    application = FastAPI(
        title="payment-processing-mkk",
        openapi_url="/openapi.json" if settings.debug else None,
        docs_url="/docs" if settings.debug else None,
        redoc_url=None,
    )
    application.exception_handler(ServiceError)(service_error_handler)
    application.exception_handler(RequestValidationError)(validation_error_handler)
    application.exception_handler(http_status.HTTP_404_NOT_FOUND)(route_not_found_handler)
    application.exception_handler(Exception)(unhandled_error_handler)
    application.include_router(api_router, prefix=API_PREFIX)

    @application.get(
        "/health",
        responses={http_status.HTTP_401_UNAUTHORIZED: {"model": ErrorRead}},
    )
    async def health(_api_key: ApiKeyDep) -> dict[str, str]:
        """Вернуть признак готовности процесса."""
        return {"status": "ok"}

    return application


app = create_app()
