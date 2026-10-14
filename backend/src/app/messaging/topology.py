"""Идемпотентное объявление RabbitMQ-топологии платежей."""

from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from app.config import Settings
from app.const.messaging import (
    PAYMENTS_DLQ_NAME,
    PAYMENTS_DLX_EXCHANGE_NAME,
    PAYMENTS_EXCHANGE_NAME,
    PAYMENTS_NEW_EVENT,
    PAYMENTS_NEW_QUEUE_NAME,
    PAYMENTS_RETRY_QUEUE_NAME_TEMPLATE,
)


def payment_exchange() -> RabbitExchange:
    """Вернуть direct exchange событий платежей.

    Returns:
        Durable exchange событий.
    """
    return RabbitExchange(PAYMENTS_EXCHANGE_NAME, type=ExchangeType.DIRECT, durable=True)


def payment_dlx_exchange() -> RabbitExchange:
    """Вернуть direct exchange dead-letter сообщений.

    Returns:
        Durable dead-letter exchange.
    """
    return RabbitExchange(PAYMENTS_DLX_EXCHANGE_NAME, type=ExchangeType.DIRECT, durable=True)


def payment_queue() -> RabbitQueue:
    """Вернуть основную очередь платежей.

    Returns:
        Durable очередь с DLX в DLQ.
    """
    return RabbitQueue(
        PAYMENTS_NEW_QUEUE_NAME,
        durable=True,
        arguments={
            "x-dead-letter-exchange": PAYMENTS_DLX_EXCHANGE_NAME,
            "x-dead-letter-routing-key": PAYMENTS_NEW_EVENT,
        },
    )


def payment_dlq() -> RabbitQueue:
    """Вернуть очередь dead-letter сообщений.

    Returns:
        Durable DLQ.
    """
    return RabbitQueue(PAYMENTS_DLQ_NAME, durable=True)


def retry_delay_milliseconds(retry_number: int, base_delay_seconds: float) -> int:
    """Рассчитать TTL повторной доставки в миллисекундах.

    Args:
        retry_number: Порядковый номер повторной доставки.
        base_delay_seconds: Базовая задержка в секундах.

    Returns:
        Длительность TTL retry-очереди в миллисекундах.
    """
    return int(base_delay_seconds * float(2 ** (retry_number - 1)) * 1000)


def retry_queue(retry_number: int, base_delay_seconds: float) -> RabbitQueue:
    """Вернуть очередь отложенной повторной доставки.

    Args:
        retry_number: Порядковый номер повторной доставки.
        base_delay_seconds: Базовая задержка retry.

    Returns:
        Durable очередь с TTL и DLX в основную очередь.
    """
    delay_milliseconds = retry_delay_milliseconds(retry_number, base_delay_seconds)
    queue_name = PAYMENTS_RETRY_QUEUE_NAME_TEMPLATE.format(delay_milliseconds=delay_milliseconds)
    return RabbitQueue(
        queue_name,
        durable=True,
        arguments={
            "x-message-ttl": delay_milliseconds,
            "x-dead-letter-exchange": PAYMENTS_EXCHANGE_NAME,
            "x-dead-letter-routing-key": PAYMENTS_NEW_EVENT,
        },
    )


async def declare_payment_topology(broker: RabbitBroker, settings: Settings) -> None:
    """Объявить exchange и очереди обработки платежей.

    Args:
        broker: Подключённый FastStream broker.
        settings: Настройки числа попыток и retry-задержек.
    """
    events_exchange = payment_exchange()
    dlx_exchange = payment_dlx_exchange()
    declared_events_exchange = await broker.declare_exchange(events_exchange)
    declared_dlx_exchange = await broker.declare_exchange(dlx_exchange)

    main_queue = payment_queue()
    dlq = payment_dlq()
    declared_main_queue = await broker.declare_queue(main_queue)
    declared_dlq = await broker.declare_queue(dlq)
    await declared_main_queue.bind(declared_events_exchange, routing_key=PAYMENTS_NEW_EVENT)
    await declared_dlq.bind(declared_dlx_exchange, routing_key=PAYMENTS_NEW_EVENT)

    for retry_number in range(1, settings.max_delivery_attempts):
        await broker.declare_queue(retry_queue(retry_number, settings.retry_base_delay_seconds))
