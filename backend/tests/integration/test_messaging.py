"""Интеграционные проверки outbox relay, consumer, retry и DLQ."""

import json
import socket
from uuid import UUID, uuid4

import pytest
from aio_pika.abc import AbstractChannel, AbstractIncomingMessage
from aio_pika.exceptions import AMQPException
from faststream.rabbit import RabbitBroker
from faststream.rabbit.annotations import RabbitMessage
from httpx import AsyncClient
from sqlalchemy import select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.config import Settings
from app.const.messaging import DELIVERY_ATTEMPT_HEADER, PAYMENTS_DLQ_NAME
from app.database.models.payments import Outbox
from app.database.session import transaction
from app.enums import PaymentStatus
from app.messaging.topology import payment_exchange, retry_queue
from app.relay import OutboxRelay
from tests.fixtures.messaging import (
    WebhookServer,
    get_payment_status,
    receive_queue_message,
    start_payment_consumer,
    wait_for_queue_message,
)


def _headers(idempotency_key: str) -> dict[str, str]:
    """Сформировать заголовки API-запроса платежа."""
    return {
        "X-API-Key": "dev-secret-change-me-not-for-prod!!",
        "Idempotency-Key": idempotency_key,
    }


async def _create_payment(client: AsyncClient, webhook_url: str) -> UUID:
    """Создать платёж через API."""
    response = await client.post(
        "/api/v1/payments",
        headers=_headers(f"messaging-{uuid4()}"),
        json={
            "amount": "1250.50",
            "currency": "USD",
            "description": "Платёж для доставки события",
            "metadata": {"source": "integration-test"},
            "webhook_url": webhook_url,
        },
    )

    assert response.status_code == 202
    return UUID(response.json()["payment_id"])


async def _outbox_event(
    session_maker: async_sessionmaker[AsyncSession],
    payment_id: UUID,
) -> Outbox:
    """Найти outbox-событие для платежа."""
    async with session_maker() as session:
        events = list(await session.scalars(select(Outbox)))
    matching_events = [event for event in events if event.payload["payment_id"] == str(payment_id)]
    assert len(matching_events) == 1
    return matching_events[0]


async def _assert_outbox_published_state(
    session_maker: async_sessionmaker[AsyncSession],
    payment_ids: list[UUID],
    expected_published: list[bool],
) -> None:
    """Проверить published_at у событий заданных платежей."""
    events = [await _outbox_event(session_maker, payment_id) for payment_id in payment_ids]
    assert [event.published_at is not None for event in events] == expected_published


async def test_transaction_with_closed_postgres_port_raises_driver_error() -> None:
    """Недоступный Postgres возвращает ошибку драйвера, а не SQLAlchemyError."""
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    port = listener.getsockname()[1]
    listener.close()
    engine = create_async_engine(f"postgresql+asyncpg://app:localdev@127.0.0.1:{port}/app")
    session_maker = async_sessionmaker(engine)

    try:
        with pytest.raises(ConnectionRefusedError) as error_info:
            async with transaction(session_maker) as session:
                await session.execute(text("SELECT 1"))
    finally:
        await engine.dispose()

    assert not isinstance(error_info.value, SQLAlchemyError)


async def _publish_payment_event_and_receive_dlq(
    broker: RabbitBroker,
    channel: AbstractChannel,
    payment_id: UUID,
) -> AbstractIncomingMessage | None:
    """Опубликовать payment event и получить результат dead-letter маршрутизации."""
    await broker.publish(
        {"payment_id": str(payment_id)},
        exchange=payment_exchange(),
        routing_key="payments.new",
        headers={DELIVERY_ATTEMPT_HEADER: 1},
    )
    return await wait_for_queue_message(channel, PAYMENTS_DLQ_NAME)


async def test_relay_with_unpublished_event_publishes_once_and_marks_outbox(
    committed_client: AsyncClient,
    committed_session_factory: async_sessionmaker[AsyncSession],
    rabbitmq_broker: RabbitBroker,
    rabbitmq_channel: AbstractChannel,
    rabbitmq_settings: Settings,
    webhook_server: WebhookServer,
) -> None:
    """Relay публикует единственное неопубликованное событие и отмечает outbox."""
    payment_id = await _create_payment(committed_client, webhook_server.url)
    event = await _outbox_event(committed_session_factory, payment_id)
    relay = OutboxRelay(rabbitmq_broker, committed_session_factory, rabbitmq_settings)

    assert await relay.publish_batch() == 1

    published_event = await _outbox_event(committed_session_factory, payment_id)
    message = await receive_queue_message(rabbitmq_channel, "payments.new")

    assert published_event.published_at is not None
    assert message is not None
    assert message.message_id == str(event.id)
    assert json.loads(message.body)["payment_id"] == str(payment_id)
    assert await relay.publish_batch() == 0
    assert await receive_queue_message(rabbitmq_channel, "payments.new") is None


async def test_mandatory_publish_to_unknown_queue_raises(
    rabbitmq_broker: RabbitBroker,
    rabbitmq_settings: Settings,
) -> None:
    """Mandatory-публикация в отсутствующую очередь не считается успешной."""
    with pytest.raises(AMQPException):
        await rabbitmq_broker.publish(
            {"payment_id": str(uuid4())},
            queue="payments.unknown",
            mandatory=True,
            timeout=rabbitmq_settings.publish_confirm_timeout_seconds,
        )


async def test_relay_with_unroutable_event_publishes_routable_event(
    committed_client: AsyncClient,
    committed_session_factory: async_sessionmaker[AsyncSession],
    rabbitmq_broker: RabbitBroker,
    rabbitmq_channel: AbstractChannel,
    rabbitmq_settings: Settings,
    webhook_server: WebhookServer,
) -> None:
    """Relay публикует доступное событие после ошибки маршрутизации другого."""
    unroutable_payment_id = await _create_payment(committed_client, webhook_server.url)
    routable_payment_id = await _create_payment(committed_client, webhook_server.url)
    unroutable_event = await _outbox_event(committed_session_factory, unroutable_payment_id)
    async with committed_session_factory() as session, session.begin():
        stored_event = await session.get(Outbox, unroutable_event.id)
        assert stored_event is not None
        stored_event.event_type = "payments.unknown"

    relay = OutboxRelay(rabbitmq_broker, committed_session_factory, rabbitmq_settings)

    assert await relay.publish_batch() == 1

    unpublished_event = await _outbox_event(committed_session_factory, unroutable_payment_id)
    published_event = await _outbox_event(committed_session_factory, routable_payment_id)
    message = await receive_queue_message(rabbitmq_channel, "payments.new")

    assert unpublished_event.published_at is None
    assert published_event.published_at is not None
    assert message is not None
    assert json.loads(message.body)["payment_id"] == str(routable_payment_id)


async def test_payment_message_full_path_finalizes_payment_and_delivers_webhook(
    committed_client: AsyncClient,
    committed_session_factory: async_sessionmaker[AsyncSession],
    rabbitmq_broker: RabbitBroker,
    rabbitmq_settings: Settings,
    webhook_server: WebhookServer,
    consumer_http_client: AsyncClient,
) -> None:
    """POST проходит relay и consumer до final status и webhook."""
    await start_payment_consumer(
        rabbitmq_broker,
        committed_session_factory,
        rabbitmq_settings,
        consumer_http_client,
    )
    payment_id = await _create_payment(committed_client, webhook_server.url)
    relay = OutboxRelay(rabbitmq_broker, committed_session_factory, rabbitmq_settings)

    assert await relay.publish_batch() == 1
    await webhook_server.wait_for_calls(1)
    status, processed_at = await get_payment_status(committed_session_factory, payment_id)
    api_response = await committed_client.get(
        f"/api/v1/payments/{payment_id}",
        headers={"X-API-Key": rabbitmq_settings.api_key.get_secret_value()},
    )

    assert status == "succeeded"
    assert processed_at is not None
    assert api_response.status_code == 200
    api_body = api_response.json()
    assert webhook_server.bodies == [
        {
            "payment_id": str(payment_id),
            "status": "succeeded",
            "amount": "1250.50",
            "currency": "USD",
            "processed_at": api_body["processed_at"],
        }
    ]


async def test_failed_processing_finishes_once_without_retry(
    committed_client: AsyncClient,
    committed_session_factory: async_sessionmaker[AsyncSession],
    rabbitmq_broker: RabbitBroker,
    rabbitmq_channel: AbstractChannel,
    rabbitmq_settings: Settings,
    webhook_server: WebhookServer,
    consumer_http_client: AsyncClient,
) -> None:
    """Бизнес-исход failed финализируется и не попадает в retry или DLQ."""
    processing_settings = rabbitmq_settings.model_copy(update={"processing_success_rate": 0})
    await start_payment_consumer(
        rabbitmq_broker,
        committed_session_factory,
        processing_settings,
        consumer_http_client,
    )
    payment_id = await _create_payment(committed_client, webhook_server.url)
    relay = OutboxRelay(rabbitmq_broker, committed_session_factory, rabbitmq_settings)

    assert await relay.publish_batch() == 1
    await webhook_server.wait_for_calls(1)
    status, processed_at = await get_payment_status(committed_session_factory, payment_id)

    assert status == "failed"
    assert processed_at is not None
    assert len(webhook_server.bodies) == 1
    assert webhook_server.bodies[0]["status"] == "failed"
    assert await receive_queue_message(rabbitmq_channel, "payments.new.dlq") is None
    for attempt in range(1, rabbitmq_settings.max_delivery_attempts):
        assert (
            await receive_queue_message(
                rabbitmq_channel,
                retry_queue(attempt, rabbitmq_settings.retry_base_delay_seconds).name,
            )
            is None
        )


async def test_finalization_failure_retries_three_attempts_then_dlq(
    committed_client: AsyncClient,
    committed_session_factory: async_sessionmaker[AsyncSession],
    rabbitmq_broker: RabbitBroker,
    rabbitmq_channel: AbstractChannel,
    rabbitmq_settings: Settings,
    webhook_server: WebhookServer,
    consumer_http_client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ошибка финализации проходит две retry-очереди и заканчивается в DLQ."""
    payment_id = await _create_payment(committed_client, webhook_server.url)
    relay = OutboxRelay(rabbitmq_broker, committed_session_factory, rabbitmq_settings)

    consumer = await start_payment_consumer(
        rabbitmq_broker,
        committed_session_factory,
        rabbitmq_settings,
        consumer_http_client,
    )
    observed_attempts: list[int] = []
    published_retry_queues: list[str] = []
    original_retry_or_dead_letter = consumer._retry_or_dead_letter

    async def fail_finalization(_payment_id: UUID, _simulated_status: object) -> None:
        raise SQLAlchemyError("finalization failed")

    async def record_retry_route(body: object, message: RabbitMessage, attempt: int) -> None:
        observed_attempts.append(attempt)
        if attempt < rabbitmq_settings.max_delivery_attempts:
            published_retry_queues.append(retry_queue(attempt, rabbitmq_settings.retry_base_delay_seconds).name)
        await original_retry_or_dead_letter(body, message, attempt)

    monkeypatch.setattr(consumer, "_finalize_payment", fail_finalization)
    monkeypatch.setattr(consumer, "_retry_or_dead_letter", record_retry_route)
    assert await relay.publish_batch() == 1

    dlq_message = await wait_for_queue_message(rabbitmq_channel, "payments.new.dlq", timeout=5)
    status, processed_at = await get_payment_status(committed_session_factory, payment_id)

    assert dlq_message is not None
    assert dlq_message.headers[DELIVERY_ATTEMPT_HEADER] == 3
    assert observed_attempts == [1, 2, 3]
    assert published_retry_queues == [
        retry_queue(1, rabbitmq_settings.retry_base_delay_seconds).name,
        retry_queue(2, rabbitmq_settings.retry_base_delay_seconds).name,
    ]
    assert status == "pending"
    assert processed_at is None


async def test_duplicate_message_with_final_payment_does_not_repeat_simulation(
    committed_client: AsyncClient,
    committed_session_factory: async_sessionmaker[AsyncSession],
    rabbitmq_broker: RabbitBroker,
    rabbitmq_settings: Settings,
    webhook_server: WebhookServer,
    consumer_http_client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Дубликат финального платежа повторяет webhook без новой симуляции."""
    simulation_calls = 0

    async def simulate(*_settings: float) -> PaymentStatus:
        nonlocal simulation_calls
        simulation_calls += 1
        return PaymentStatus.SUCCEEDED

    monkeypatch.setattr("app.consumer.simulate_processing", simulate)
    await start_payment_consumer(
        rabbitmq_broker,
        committed_session_factory,
        rabbitmq_settings,
        consumer_http_client,
    )
    payment_id = await _create_payment(committed_client, webhook_server.url)
    relay = OutboxRelay(rabbitmq_broker, committed_session_factory, rabbitmq_settings)

    assert await relay.publish_batch() == 1
    await webhook_server.wait_for_calls(1)
    await rabbitmq_broker.publish(
        {"payment_id": str(payment_id)},
        exchange=payment_exchange(),
        routing_key="payments.new",
        headers={DELIVERY_ATTEMPT_HEADER: 1},
    )
    await webhook_server.wait_for_calls(2)
    status, processed_at = await get_payment_status(committed_session_factory, payment_id)

    assert simulation_calls == 1
    assert status == "succeeded"
    assert processed_at is not None
    assert len(webhook_server.bodies) == 2


async def test_webhook_always_fails_after_three_attempts_moves_message_to_dlq(
    committed_client: AsyncClient,
    committed_session_factory: async_sessionmaker[AsyncSession],
    rabbitmq_broker: RabbitBroker,
    rabbitmq_channel: AbstractChannel,
    rabbitmq_settings: Settings,
    webhook_server: WebhookServer,
    consumer_http_client: AsyncClient,
) -> None:
    """Три неуспешных webhook-доставки направляют сообщение в DLQ."""
    webhook_server.set_statuses([500, 500, 500])
    await start_payment_consumer(
        rabbitmq_broker,
        committed_session_factory,
        rabbitmq_settings,
        consumer_http_client,
    )
    payment_id = await _create_payment(committed_client, webhook_server.url)
    relay = OutboxRelay(rabbitmq_broker, committed_session_factory, rabbitmq_settings)

    assert await relay.publish_batch() == 1
    await webhook_server.wait_for_calls(3)
    dlq_message = await wait_for_queue_message(rabbitmq_channel, PAYMENTS_DLQ_NAME)
    status, processed_at = await get_payment_status(committed_session_factory, payment_id)

    assert dlq_message is not None
    assert dlq_message.headers[DELIVERY_ATTEMPT_HEADER] == 3
    assert webhook_server.received_at[1] - webhook_server.received_at[0] >= 0.005
    assert webhook_server.received_at[2] - webhook_server.received_at[1] >= 0.015
    assert status == "succeeded"
    assert processed_at is not None


async def test_webhook_fails_once_then_succeeds_without_dlq(
    committed_client: AsyncClient,
    committed_session_factory: async_sessionmaker[AsyncSession],
    rabbitmq_broker: RabbitBroker,
    rabbitmq_channel: AbstractChannel,
    rabbitmq_settings: Settings,
    webhook_server: WebhookServer,
    consumer_http_client: AsyncClient,
) -> None:
    """Вторая успешная доставка webhook не оставляет сообщения в DLQ."""
    webhook_server.set_statuses([500, 204])
    await start_payment_consumer(rabbitmq_broker, committed_session_factory, rabbitmq_settings, consumer_http_client)
    payment_id = await _create_payment(committed_client, webhook_server.url)
    relay = OutboxRelay(rabbitmq_broker, committed_session_factory, rabbitmq_settings)

    assert await relay.publish_batch() == 1
    await webhook_server.wait_for_calls(2)
    dlq_message = await receive_queue_message(rabbitmq_channel, PAYMENTS_DLQ_NAME)
    status, processed_at = await get_payment_status(committed_session_factory, payment_id)

    assert dlq_message is None
    assert status == "succeeded"
    assert processed_at is not None
    assert len(webhook_server.bodies) == 2


async def test_missing_payment_moves_message_to_dlq_without_retry(
    committed_session_factory: async_sessionmaker[AsyncSession],
    rabbitmq_broker: RabbitBroker,
    rabbitmq_channel: AbstractChannel,
    rabbitmq_settings: Settings,
    consumer_http_client: AsyncClient,
) -> None:
    """Неизвестный payment_id сразу направляется в DLQ."""
    await start_payment_consumer(
        rabbitmq_broker,
        committed_session_factory,
        rabbitmq_settings,
        consumer_http_client,
    )
    payment_id = uuid4()

    dlq_message = await _publish_payment_event_and_receive_dlq(rabbitmq_broker, rabbitmq_channel, payment_id)

    assert dlq_message is not None
    assert json.loads(dlq_message.body) == {"payment_id": str(payment_id)}
    assert dlq_message.headers[DELIVERY_ATTEMPT_HEADER] == 1


async def test_invalid_message_body_moves_message_to_dlq_without_retry(
    committed_session_factory: async_sessionmaker[AsyncSession],
    rabbitmq_broker: RabbitBroker,
    rabbitmq_channel: AbstractChannel,
    rabbitmq_settings: Settings,
    consumer_http_client: AsyncClient,
) -> None:
    """Невалидное тело сообщения сразу направляется в DLQ."""
    await start_payment_consumer(
        rabbitmq_broker,
        committed_session_factory,
        rabbitmq_settings,
        consumer_http_client,
    )

    await rabbitmq_broker.publish(
        "not a payment event",
        exchange=payment_exchange(),
        routing_key="payments.new",
        headers={DELIVERY_ATTEMPT_HEADER: 1},
    )
    dlq_message = await wait_for_queue_message(rabbitmq_channel, PAYMENTS_DLQ_NAME)

    assert dlq_message is not None
    assert dlq_message.body.decode() == "not a payment event"
    assert dlq_message.headers[DELIVERY_ATTEMPT_HEADER] == 1


async def test_unhandled_consumer_error_is_rejected_to_dlq(
    committed_session_factory: async_sessionmaker[AsyncSession],
    rabbitmq_broker: RabbitBroker,
    rabbitmq_channel: AbstractChannel,
    rabbitmq_settings: Settings,
    consumer_http_client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Необработанная ошибка handler не оставляет delivery unacked."""
    consumer = await start_payment_consumer(
        rabbitmq_broker,
        committed_session_factory,
        rabbitmq_settings,
        consumer_http_client,
    )

    async def fail_get_payment(_payment_id: UUID) -> None:
        raise RuntimeError("unexpected handler failure")

    monkeypatch.setattr(consumer, "_get_payment", fail_get_payment)
    payment_id = uuid4()
    dlq_message = await _publish_payment_event_and_receive_dlq(rabbitmq_broker, rabbitmq_channel, payment_id)

    assert dlq_message is not None
    assert json.loads(dlq_message.body) == {"payment_id": str(payment_id)}
