"""Unit-тесты обработки невалидных RabbitMQ-сообщений."""

from unittest.mock import AsyncMock

import pytest
from aio_pika.exceptions import AMQPException
from faststream.middlewares import AckPolicy
from faststream.rabbit.subscriber import RabbitSubscriber
from httpx import AsyncClient

from app.config import settings
from app.consumer import PaymentMessageConsumer, register_payment_consumer
from app.database.session import session_factory
from app.messaging.broker import create_broker


@pytest.fixture
def consumer(consumer_http_client: AsyncClient) -> PaymentMessageConsumer:
    """Создать consumer с тестовыми настройками."""
    return PaymentMessageConsumer(
        create_broker(settings),
        session_factory,
        settings,
        http_client=consumer_http_client,
    )


@pytest.mark.parametrize(
    ("body", "headers"),
    [
        ({"unexpected": "body"}, {}),
    ],
)
async def test_consumer_with_invalid_input_rejects_message_without_retry(
    consumer: PaymentMessageConsumer,
    body: object,
    headers: dict[str, object],
) -> None:
    """Невалидное тело сразу направляется в DLQ."""
    message = AsyncMock()
    message.headers = headers

    await consumer.handle(body, message)

    message.reject.assert_awaited_once_with(requeue=False)


async def test_consumer_when_retry_publish_fails_rejects_without_requeue(
    consumer_http_client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Неудача публикации retry отправляет исходную доставку в DLQ."""
    broker = AsyncMock()
    broker.publish.side_effect = AMQPException("unroutable")
    consumer = PaymentMessageConsumer(
        broker,
        session_factory,
        settings,
        http_client=consumer_http_client,
    )
    monkeypatch.setattr(consumer, "_get_payment", AsyncMock(side_effect=ConnectionRefusedError()))
    message = AsyncMock()
    message.headers = {}
    message.correlation_id = "correlation"
    message.message_id = "message"

    await consumer.handle({"payment_id": "c6104d71-c420-4451-a8f4-bc498c2b4424"}, message)

    broker.publish.assert_awaited_once()
    message.reject.assert_awaited_once_with(requeue=False)
    message.nack.assert_not_awaited()
    message.ack.assert_not_awaited()


async def test_consumer_when_database_connection_is_refused_retries_message(
    consumer_http_client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Отказ подключения к Postgres запускает retry доставки."""
    broker = AsyncMock()
    consumer = PaymentMessageConsumer(
        broker,
        session_factory,
        settings,
        http_client=consumer_http_client,
    )
    monkeypatch.setattr(consumer, "_get_payment", AsyncMock(side_effect=ConnectionRefusedError()))
    message = AsyncMock()
    message.headers = {}
    message.correlation_id = "correlation"
    message.message_id = "message"

    await consumer.handle({"payment_id": "c6104d71-c420-4451-a8f4-bc498c2b4424"}, message)

    broker.publish.assert_awaited_once()
    message.ack.assert_awaited_once()
    message.reject.assert_not_awaited()


async def test_consumer_registration_uses_bounded_concurrency_and_reject_policy(
    consumer_http_client: AsyncClient,
) -> None:
    """Регистрация задаёт prefetch и reject-on-error policy."""
    broker = create_broker(settings)
    consumer = PaymentMessageConsumer(
        broker,
        session_factory,
        settings,
        http_client=consumer_http_client,
    )

    register_payment_consumer(broker, consumer, settings)
    subscriber = next(iter(broker.subscribers))
    assert isinstance(subscriber, RabbitSubscriber)
    assert subscriber.channel is not None

    assert subscriber.channel.prefetch_count == settings.consumer_concurrency
    assert subscriber.channel.on_return_raises is True
    assert subscriber.ack_policy is AckPolicy.REJECT_ON_ERROR
