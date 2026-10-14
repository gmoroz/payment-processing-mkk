"""Асинхронная сессия PostgreSQL и транзакционная граница."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.config import settings

engine = create_async_engine(settings.async_db_url)
session_factory = async_sessionmaker(engine, expire_on_commit=False)


@asynccontextmanager
async def transaction(
    session_maker: async_sessionmaker[AsyncSession],
) -> AsyncIterator[AsyncSession]:
    """Открыть транзакцию и закрыть её после обработки.

    Args:
        session_maker: Фабрика асинхронных сессий.

    Yields:
        Сессия с активной транзакцией.
    """
    async with session_maker() as session, session.begin():
        yield session


async def get_session() -> AsyncIterator[AsyncSession]:
    """Открыть сессию и завершить транзакцию после обработки запроса.

    Yields:
        Сессия с активной транзакцией.
    """
    async with transaction(session_factory) as session:
        yield session
