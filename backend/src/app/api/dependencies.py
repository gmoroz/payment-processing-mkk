"""Явная проводка зависимостей FastAPI."""

import secrets
from typing import Annotated

from fastapi import Depends, Security
from fastapi.security import APIKeyHeader
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.const.http import API_KEY_HEADER
from app.database.repositories.payments import PaymentRepository
from app.database.session import get_session
from app.services.exceptions import UnauthorizedError
from app.services.payments import PaymentService

# function: транзакция закрывается до send; request-scope коммитит уже после ответа.
SessionDep = Annotated[AsyncSession, Depends(get_session, scope="function")]

api_key_header = APIKeyHeader(name=API_KEY_HEADER, auto_error=False)


def require_api_key(api_key: Annotated[str | None, Security(api_key_header)]) -> None:
    """Проверить API-ключ запроса.

    Args:
        api_key: Значение API-ключа из заголовка.
    """
    configured_key = settings.api_key.get_secret_value().encode("utf-8")
    provided_key = b"" if api_key is None else api_key.encode("latin-1")
    if not secrets.compare_digest(provided_key, configured_key):
        raise UnauthorizedError()


ApiKeyDep = Annotated[None, Depends(require_api_key)]


def get_payment_service(session: SessionDep) -> PaymentService:
    """Собрать сервис платежей.

    Args:
        session: Сессия базы данных текущего запроса.

    Returns:
        Сервис платежей.
    """
    return PaymentService(PaymentRepository(session))


PaymentServiceDep = Annotated[PaymentService, Depends(get_payment_service)]
