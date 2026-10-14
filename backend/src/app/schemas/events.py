"""Схемы сообщений RabbitMQ."""

from uuid import UUID

from pydantic import BaseModel


class PaymentEvent(BaseModel):
    """Событие создания платежа."""

    payment_id: UUID
