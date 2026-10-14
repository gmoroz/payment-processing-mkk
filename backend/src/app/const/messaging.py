"""Константы RabbitMQ-топологии платежей."""

PAYMENTS_EXCHANGE_NAME = "payments"
PAYMENTS_NEW_EVENT = "payments.new"
PAYMENTS_NEW_QUEUE_NAME = PAYMENTS_NEW_EVENT
PAYMENTS_RETRY_QUEUE_NAME_TEMPLATE = "payments.retry.{delay_milliseconds}ms"
PAYMENTS_DLX_EXCHANGE_NAME = "payments.dlx"
PAYMENTS_DLQ_NAME = "payments.new.dlq"
DELIVERY_ATTEMPT_HEADER = "x-delivery-attempt"
