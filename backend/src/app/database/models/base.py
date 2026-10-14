"""Базовые классы ORM."""

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """База декларативных ORM-моделей."""
