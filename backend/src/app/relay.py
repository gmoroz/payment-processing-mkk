"""Outbox relay для подтверждённой публикации в RabbitMQ."""

import asyncio
import logging
import signal
from uuid import UUID

from faststream.rabbit import RabbitBroker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings, settings
from app.database.models.payments import Outbox
from app.database.repositories.payments import PaymentRepository
from app.database.session import engine, session_factory, transaction
from app.messaging.broker import BROKER_PUBLISH_ERRORS, create_broker
from app.messaging.topology import declare_payment_topology, payment_exchange
from app.worker_logging import configure_worker_logging

logger = logging.getLogger(__name__)
RELAY_ITERATION_ERRORS = (SQLAlchemyError, *BROKER_PUBLISH_ERRORS)


class OutboxRelay:
    """Публиковать неопубликованные записи outbox подтверждённо."""

    def __init__(
        self,
        broker: RabbitBroker,
        session_maker: async_sessionmaker[AsyncSession],
        relay_settings: Settings,
    ) -> None:
        self._broker = broker
        self._session_maker = session_maker
        self._settings = relay_settings

    async def _publish_event(self, event: Outbox) -> None:
        """Опубликовать одно событие outbox с подтверждением."""
        await self._broker.publish(
            event.payload,
            exchange=payment_exchange(),
            routing_key=event.event_type,
            message_id=str(event.id),
            correlation_id=str(event.payload["payment_id"]),
            mandatory=True,
            persist=True,
            timeout=self._settings.publish_confirm_timeout_seconds,
        )

    async def publish_batch(self) -> int:
        """Опубликовать один batch событий outbox.

        Returns:
            Число событий с подтверждённой публикацией.
        """
        async with transaction(self._session_maker) as session:
            repository = PaymentRepository(session)
            events = await repository.get_unpublished_outbox_batch(self._settings.relay_batch_size)
            if not events:
                return 0

            published_event_ids: list[UUID] = []
            for event in events:
                try:
                    await self._publish_event(event)
                except BROKER_PUBLISH_ERRORS:
                    logger.exception("Не опубликовано outbox-событие event_id=%s", event.id)
                    continue
                published_event_ids.append(event.id)

            if not published_event_ids:
                return 0
            await repository.mark_outbox_published(published_event_ids)
            return len(published_event_ids)


async def run_relay(relay_settings: Settings = settings) -> None:
    """Публиковать outbox до сигнала завершения процесса.

    Args:
        relay_settings: Настройки broker и polling.
    """
    stop_event = asyncio.Event()
    event_loop = asyncio.get_running_loop()
    for shutdown_signal in (signal.SIGINT, signal.SIGTERM):
        event_loop.add_signal_handler(shutdown_signal, stop_event.set)

    broker = create_broker(relay_settings)
    relay = OutboxRelay(broker, session_factory, relay_settings)
    try:
        await broker.connect()
        await declare_payment_topology(broker, relay_settings)
        while not stop_event.is_set():
            try:
                await relay.publish_batch()
            except RELAY_ITERATION_ERRORS:
                logger.exception("Ошибка итерации outbox relay")
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=relay_settings.relay_poll_interval_seconds)
            except TimeoutError:
                continue
    finally:
        for shutdown_signal in (signal.SIGINT, signal.SIGTERM):
            event_loop.remove_signal_handler(shutdown_signal)
        await broker.stop()
        await engine.dispose()


def main() -> None:
    """Запустить outbox relay как модуль Python."""
    configure_worker_logging()
    asyncio.run(run_relay())


if __name__ == "__main__":
    main()
