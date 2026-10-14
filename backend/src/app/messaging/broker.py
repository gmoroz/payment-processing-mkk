"""Фабрика FastStream RabbitMQ broker."""

from aio_pika.exceptions import AMQPException
from faststream.middlewares import AckPolicy
from faststream.rabbit import Channel, RabbitBroker

from app.config import Settings

BROKER_PUBLISH_ERRORS = (AMQPException, ConnectionError, OSError, TimeoutError)


def create_broker(settings: Settings) -> RabbitBroker:
    """Создать broker с ручным подтверждением сообщений.

    Args:
        settings: Настройки подключения RabbitMQ.

    Returns:
        Настроенный FastStream broker.
    """
    return RabbitBroker(
        str(settings.rabbitmq_url),
        timeout=settings.rabbitmq_connect_timeout_seconds,
        default_channel=Channel(on_return_raises=True),
        ack_policy=AckPolicy.REJECT_ON_ERROR,
    )
