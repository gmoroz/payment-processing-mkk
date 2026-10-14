"""Общие API-схемы."""

from pydantic import BaseModel

from app.enums import ErrorCode


class ErrorRead(BaseModel):
    """Единый формат ошибки API."""

    error_code: ErrorCode
    message: str
