"""FastStream consumer событий новых платежей."""

import asyncio
import logging
from uuid import UUID

import httpx
from faststream import FastStream
from faststream.rabbit import Channel, RabbitBroker
from faststream.rabbit.annotations import RabbitMessage
from pydantic import ValidationError
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings, settings
from app.const.messaging import DELIVERY_ATTEMPT_HEADER
from app.database.repositories.payments import PaymentRepository
from app.database.session import engine, session_factory, transaction
from app.enums import PaymentStatus
from app.messaging.broker import BROKER_PUBLISH_ERRORS, create_broker
from app.messaging.topology import declare_payment_topology, payment_exchange, payment_queue, retry_queue
from app.schemas.events import PaymentEvent
from app.schemas.payments import PaymentRead
from app.services.payment_finalization import finalize_payment
from app.services.payment_processing import simulate_processing
from app.worker_logging import configure_worker_logging

logger = logging.getLogger(__name__)
WORKER_DB_ERRORS = (SQLAlchemyError, OSError, TimeoutError)


class WebhookDeliveryError(Exception):
    """Webhook завершился HTTP-статусом вне диапазона 2xx."""


def create_http_client(consumer_settings: Settings) -> httpx.AsyncClient:
    """Создать HTTP-клиент для доставки webhook.

    Args:
        consumer_settings: Настройки таймаута webhook.

    Returns:
        Настроенный HTTP-клиент.
    """
    return httpx.AsyncClient(timeout=consumer_settings.webhook_timeout_seconds)


class PaymentMessageConsumer:
    """Обработать событие платежа и доставить его webhook."""

    def __init__(
        self,
        broker: RabbitBroker,
        session_maker: async_sessionmaker[AsyncSession],
        consumer_settings: Settings,
        *,
        http_client: httpx.AsyncClient,
    ) -> None:
        self._broker = broker
        self._session_maker = session_maker
        self._settings = consumer_settings
        self._http_client = http_client

    async def handle(self, body: object, message: RabbitMessage) -> None:
        """Обработать одну доставку RabbitMQ.

        Args:
            body: Декодированное тело сообщения.
            message: Сообщение с ручным подтверждением.
        """
        try:
            event = PaymentEvent.model_validate(body)
        except ValidationError:
            logger.warning("Получено невалидное событие платежа")
            await message.reject(requeue=False)
            return

        attempt = int(message.headers.get(DELIVERY_ATTEMPT_HEADER, 1))
        try:
            payment = await self._get_payment(event.payment_id)
        except WORKER_DB_ERRORS:
            logger.exception("Ошибка БД при чтении payment_id=%s", event.payment_id)
            await self._retry_or_dead_letter(body, message, attempt)
            return

        if payment is None:
            logger.warning("Платёж из сообщения не найден: payment_id=%s", event.payment_id)
            await message.reject(requeue=False)
            return

        if payment.status is PaymentStatus.PENDING:
            simulated_status = await simulate_processing(
                self._settings.processing_min_seconds,
                self._settings.processing_max_seconds,
                self._settings.processing_success_rate,
            )
            try:
                payment = await self._finalize_payment(event.payment_id, simulated_status)
            except WORKER_DB_ERRORS:
                logger.exception("Ошибка БД при финализации payment_id=%s", event.payment_id)
                await self._retry_or_dead_letter(body, message, attempt)
                return
        try:
            await self._send_webhook(payment)
        except (httpx.RequestError, WebhookDeliveryError) as error:
            logger.warning(
                "Не доставлен webhook payment_id=%s attempt=%s reason=%r",
                event.payment_id,
                attempt,
                error,
            )
            await self._retry_or_dead_letter(body, message, attempt)
            return

        await message.ack()

    async def _get_payment(self, payment_id: UUID) -> PaymentRead | None:
        """Прочитать платёж в отдельной транзакции."""
        async with transaction(self._session_maker) as session:
            payment = await PaymentRepository(session).get_by_id(payment_id)
            return PaymentRead.model_validate(payment) if payment is not None else None

    async def _finalize_payment(self, payment_id: UUID, simulated_status: PaymentStatus) -> PaymentRead:
        """Финализировать платёж в отдельной транзакции."""
        async with transaction(self._session_maker) as session:
            return await finalize_payment(PaymentRepository(session), payment_id, simulated_status)

    async def _send_webhook(self, payment: PaymentRead) -> None:
        """Отправить сериализованный платёж на webhook."""
        payload = {
            "payment_id": str(payment.id),
            **payment.model_dump(mode="json", include={"status", "amount", "currency", "processed_at"}),
        }
        response = await self._http_client.post(str(payment.webhook_url), json=payload)
        if not response.is_success:
            raise WebhookDeliveryError(f"HTTP {response.status_code}")

    async def _retry_or_dead_letter(self, body: object, message: RabbitMessage, attempt: int) -> None:
        """Повторить доставку или отправить сообщение в DLQ."""
        if attempt >= self._settings.max_delivery_attempts:
            await message.reject(requeue=False)
            return
        try:
            await self._broker.publish(
                body,
                queue=retry_queue(attempt, self._settings.retry_base_delay_seconds).name,
                headers={**message.headers, DELIVERY_ATTEMPT_HEADER: attempt + 1},
                correlation_id=message.correlation_id,
                message_id=message.message_id,
                mandatory=True,
                persist=True,
                timeout=self._settings.publish_confirm_timeout_seconds,
            )
        except BROKER_PUBLISH_ERRORS:
            logger.exception("Не опубликована повторная доставка payment message")
            await message.reject(requeue=False)
            return
        await message.ack()


def register_payment_consumer(
    broker: RabbitBroker,
    consumer: PaymentMessageConsumer,
    consumer_settings: Settings,
) -> None:
    """Зарегистрировать единственный handler основной очереди.

    Args:
        broker: FastStream broker.
        consumer: Сервис обработки доставок.
        consumer_settings: Настройки параллелизма consumer.
    """

    broker.subscriber(
        payment_queue(),
        exchange=payment_exchange(),
        channel=Channel(
            prefetch_count=consumer_settings.consumer_concurrency,
            on_return_raises=True,
        ),
    )(consumer.handle)


async def run_consumer(consumer_settings: Settings = settings) -> None:
    """Запустить consumer и корректно закрыть broker вместе с FastStream.

    Args:
        consumer_settings: Настройки broker и обработки платежей.
    """
    broker = create_broker(consumer_settings)
    try:
        await broker.connect()
        await declare_payment_topology(broker, consumer_settings)
        async with create_http_client(consumer_settings) as http_client:
            consumer = PaymentMessageConsumer(
                broker,
                session_factory,
                consumer_settings,
                http_client=http_client,
            )
            register_payment_consumer(broker, consumer, consumer_settings)
            await FastStream(broker).run()
    finally:
        await broker.stop()
        await engine.dispose()


def main() -> None:
    """Запустить consumer как модуль Python."""
    configure_worker_logging()
    asyncio.run(run_consumer())


if __name__ == "__main__":
    main()
