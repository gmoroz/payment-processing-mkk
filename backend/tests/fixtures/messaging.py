"""Фикстуры RabbitMQ, committed PostgreSQL и webhook-получателя."""

import asyncio
import json
from collections.abc import AsyncIterator
from datetime import datetime
from uuid import UUID

import aio_pika
import httpx
import pytest
import pytest_asyncio
from aio_pika.abc import AbstractChannel, AbstractIncomingMessage
from faststream.rabbit import RabbitBroker
from httpx import ASGITransport, AsyncClient
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from app.config import Settings, settings
from app.consumer import PaymentMessageConsumer, register_payment_consumer
from app.database.models.payments import Outbox, Payment
from app.database.session import get_session
from app.main import create_app
from app.messaging.broker import create_broker
from app.messaging.topology import declare_payment_topology, retry_queue


class WebhookServer:
    """Записать webhook-запросы и вернуть заданную последовательность статусов."""

    def __init__(self, statuses: list[int]) -> None:
        self._statuses = statuses
        self.bodies: list[dict[str, object]] = []
        self.received_at: list[float] = []
        self._received = asyncio.Event()

    @property
    def url(self) -> str:
        """Вернуть URL тестового webhook-получателя."""
        return "http://webhook.test/webhook"

    async def handle(self, request: httpx.Request) -> httpx.Response:
        """Записать JSON webhook и вернуть следующий HTTP-статус."""
        decoded_body = json.loads(request.content)
        assert isinstance(decoded_body, dict)
        self.bodies.append(decoded_body)
        self.received_at.append(asyncio.get_running_loop().time())
        status = self._statuses.pop(0) if self._statuses else 200
        self._received.set()
        return httpx.Response(status, request=request)

    def set_statuses(self, statuses: list[int]) -> None:
        """Задать последовательность HTTP-статусов следующих запросов."""
        self._statuses = statuses

    async def wait_for_calls(self, expected_count: int, timeout: float = 3) -> None:
        """Дождаться заданного числа webhook-запросов."""
        while len(self.bodies) < expected_count:
            await asyncio.wait_for(self._received.wait(), timeout=timeout)
            self._received.clear()


async def receive_queue_message(channel: AbstractChannel, queue_name: str) -> AbstractIncomingMessage | None:
    """Получить одно сообщение очереди без ожидания новых сообщений."""
    queue = await channel.declare_queue(queue_name, passive=True)
    message = await queue.get(fail=False)
    if message is not None:
        await message.ack()
    return message


async def wait_for_queue_message(
    channel: AbstractChannel,
    queue_name: str,
    timeout: float = 3,
) -> AbstractIncomingMessage | None:
    """Дождаться сообщения очереди, проверяя её через открытый канал."""
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        message = await receive_queue_message(channel, queue_name)
        if message is not None:
            return message
        await asyncio.sleep(0.01)
    return None


@pytest.fixture(scope="session")
def rabbitmq_settings() -> Settings:
    """Вернуть быстрые настройки worker для vhost app_test."""
    test_url = f"{str(settings.rabbitmq_url).rsplit('/', maxsplit=1)[0]}/app_test"
    return settings.model_copy(
        update={
            "rabbitmq_url": test_url,
            "processing_min_seconds": 0,
            "processing_max_seconds": 0,
            "processing_success_rate": 1,
            "retry_base_delay_seconds": 0.01,
            "webhook_timeout_seconds": 1,
            "relay_poll_interval_seconds": 0.01,
        }
    )


@pytest_asyncio.fixture
async def webhook_server() -> AsyncIterator[WebhookServer]:
    """Создать запись тестовых webhook-запросов."""
    yield WebhookServer([])


@pytest_asyncio.fixture
async def consumer_http_client(
    rabbitmq_settings: Settings,
    webhook_server: WebhookServer,
) -> AsyncIterator[AsyncClient]:
    """Создать HTTP-клиент consumer с MockTransport."""
    transport = httpx.MockTransport(webhook_server.handle)
    async with AsyncClient(transport=transport, timeout=rabbitmq_settings.webhook_timeout_seconds) as http_client:
        yield http_client


@pytest_asyncio.fixture(scope="session")
async def committed_test_engine(migrated_test_database: None) -> AsyncIterator[AsyncEngine]:
    """Создать единый engine для committed test-сессий."""
    test_url = f"{str(settings.async_db_url).rsplit('/', maxsplit=1)[0]}/app_test"
    test_engine = create_async_engine(test_url)
    yield test_engine
    await test_engine.dispose()


@pytest_asyncio.fixture
async def committed_session_factory(
    committed_test_engine: AsyncEngine,
) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    """Открыть независимые сессии test-БД с настоящими commit."""
    factory = async_sessionmaker(committed_test_engine, expire_on_commit=False)
    yield factory
    async with factory() as session, session.begin():
        await session.execute(delete(Outbox))
        await session.execute(delete(Payment))


@pytest_asyncio.fixture
async def committed_client(
    committed_session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[AsyncClient]:
    """Создать HTTP-клиент API с committed test-сессиями."""
    application = create_app()

    async def override_session() -> AsyncIterator[AsyncSession]:
        async with committed_session_factory() as session, session.begin():
            yield session

    application.dependency_overrides[get_session] = override_session
    async with AsyncClient(transport=ASGITransport(app=application), base_url="http://test") as http_client:
        yield http_client


@pytest_asyncio.fixture(scope="session")
async def rabbitmq_channel(rabbitmq_settings: Settings) -> AsyncIterator[AbstractChannel]:
    """Открыть публичный aio-pika channel на сессию тестов."""
    # RobustChannel кэширует passive declare и оставляет устаревший message_count.
    connection = await aio_pika.connect(str(rabbitmq_settings.rabbitmq_url))
    channel = await connection.channel()
    yield channel
    await connection.close()


@pytest_asyncio.fixture
async def rabbitmq_broker(
    rabbitmq_channel: AbstractChannel,
    rabbitmq_settings: Settings,
) -> AsyncIterator[RabbitBroker]:
    """Запустить отдельный broker и очистить его очереди перед тестом."""
    broker = create_broker(rabbitmq_settings)
    await broker.connect()
    await declare_payment_topology(broker, rabbitmq_settings)
    retry_queue_names = [
        retry_queue(attempt, rabbitmq_settings.retry_base_delay_seconds).name
        for attempt in range(1, rabbitmq_settings.max_delivery_attempts)
    ]
    for queue_name in ("payments.new", "payments.new.dlq", *retry_queue_names):
        queue = await rabbitmq_channel.declare_queue(queue_name, passive=True)
        await queue.purge()
    yield broker
    await broker.stop()


async def start_payment_consumer(
    broker: RabbitBroker,
    session_maker: async_sessionmaker[AsyncSession],
    worker_settings: Settings,
    http_client: AsyncClient,
) -> PaymentMessageConsumer:
    """Зарегистрировать и запустить consumer для интеграционного теста."""
    consumer = PaymentMessageConsumer(broker, session_maker, worker_settings, http_client=http_client)
    register_payment_consumer(broker, consumer, worker_settings)
    await broker.start()
    return consumer


async def get_payment_status(
    session_maker: async_sessionmaker[AsyncSession],
    payment_id: UUID,
) -> tuple[str, datetime | None]:
    """Прочитать статус и processed_at платежа из test-БД."""
    async with session_maker() as session:
        payment = await session.get(Payment, payment_id)
    assert payment is not None
    return payment.status.value, payment.processed_at
