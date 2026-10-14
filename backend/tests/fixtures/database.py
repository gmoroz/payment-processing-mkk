"""Фикстуры реальной тестовой PostgreSQL."""

import os
import subprocess
import sys
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from app.config import settings
from app.database.session import get_session
from app.main import create_app


def _test_db_url() -> str:
    """Сформировать URL тестовой базы данных."""
    return f"{str(settings.db_url).rsplit('/', maxsplit=1)[0]}/app_test"


@pytest_asyncio.fixture(scope="session")
async def migrated_test_database() -> AsyncIterator[None]:
    """Пересоздать схему тестовой БД и применить Alembic head."""
    async_url = _test_db_url().replace("postgresql://", "postgresql+asyncpg://", 1)
    reset_engine = create_async_engine(async_url)
    async with reset_engine.begin() as connection:
        await connection.execute(text("DROP SCHEMA public CASCADE"))
        await connection.execute(text("CREATE SCHEMA public"))
    await reset_engine.dispose()
    migration = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        check=False,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "DB_URL": _test_db_url(),
            "DEBUG": "true",
            "API_KEY": "dev-secret-change-me-not-for-prod!!",
        },
    )
    if migration.returncode != 0:
        pytest.fail(f"Alembic завершился с кодом {migration.returncode}:\n{migration.stdout}\n{migration.stderr}")
    yield


@pytest_asyncio.fixture(scope="session")
async def database_engine(migrated_test_database: None) -> AsyncIterator[AsyncEngine]:
    """Создать единый engine для savepoint-сессий тестов."""
    async_url = _test_db_url().replace("postgresql://", "postgresql+asyncpg://", 1)
    engine = create_async_engine(async_url)
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture
async def db_session(database_engine: AsyncEngine) -> AsyncIterator[AsyncSession]:
    """Открыть savepoint-сессию с откатом после теста."""
    async with database_engine.connect() as connection:
        transaction = await connection.begin()
        factory = async_sessionmaker(
            bind=connection,
            expire_on_commit=False,
            join_transaction_mode="create_savepoint",
        )
        async with factory() as session:
            yield session
        if transaction.is_active:
            await transaction.rollback()


@pytest_asyncio.fixture
async def client(db_session: AsyncSession) -> AsyncIterator[AsyncClient]:
    """Создать HTTP-клиент с транзакционной сессией теста."""
    application = create_app()

    async def override_session() -> AsyncIterator[AsyncSession]:
        yield db_session

    application.dependency_overrides[get_session] = override_session
    async with AsyncClient(transport=ASGITransport(app=application), base_url="http://test") as http_client:
        yield http_client
