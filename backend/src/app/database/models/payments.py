"""ORM-модели платежей и outbox."""

from datetime import datetime
from decimal import Decimal
from enum import Enum
from uuid import UUID, uuid4

from sqlalchemy import DateTime, Index, Numeric, String, text
from sqlalchemy import Enum as SqlEnum
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PostgreSQLUUID
from sqlalchemy.orm import Mapped, mapped_column

from app.const.payments import (
    IDEMPOTENCY_KEY_MAX_LENGTH,
    OUTBOX_EVENT_TYPE_MAX_LENGTH,
    PAYMENT_DESCRIPTION_MAX_LENGTH,
    REQUEST_FINGERPRINT_LENGTH,
    WEBHOOK_URL_MAX_LENGTH,
)
from app.database.models.base import Base
from app.enums import Currency, PaymentStatus


def _enum_values(enum_class: type[Enum]) -> list[str]:
    """Получить значения строкового enum для PostgreSQL."""
    return [member.value for member in enum_class]


class Payment(Base):
    """Платёж, принятый API."""

    __tablename__ = "payments"

    id: Mapped[UUID] = mapped_column(PostgreSQLUUID(as_uuid=True), primary_key=True, default=uuid4)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    currency: Mapped[Currency] = mapped_column(
        SqlEnum(
            Currency,
            name="currency_enum",
            native_enum=True,
            values_callable=_enum_values,
        ),
        nullable=False,
    )
    description: Mapped[str] = mapped_column(String(PAYMENT_DESCRIPTION_MAX_LENGTH), nullable=False)
    metadata_json: Mapped[dict[str, object]] = mapped_column("metadata", JSONB, nullable=False)
    status: Mapped[PaymentStatus] = mapped_column(
        SqlEnum(
            PaymentStatus,
            name="payment_status_enum",
            native_enum=True,
            values_callable=_enum_values,
        ),
        nullable=False,
    )
    idempotency_key: Mapped[str] = mapped_column(
        String(IDEMPOTENCY_KEY_MAX_LENGTH),
        nullable=False,
        unique=True,
    )
    request_fingerprint: Mapped[str] = mapped_column(String(REQUEST_FINGERPRINT_LENGTH), nullable=False)
    webhook_url: Mapped[str] = mapped_column(String(WEBHOOK_URL_MAX_LENGTH), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=text("now()"), nullable=False)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class Outbox(Base):
    """Непубликованное или опубликованное событие платежа."""

    __tablename__ = "outbox"
    __table_args__ = (
        Index(
            "ix_outbox_unpublished_created_at",
            "created_at",
            postgresql_where=text("published_at IS NULL"),
        ),
    )

    id: Mapped[UUID] = mapped_column(PostgreSQLUUID(as_uuid=True), primary_key=True, default=uuid4)
    event_type: Mapped[str] = mapped_column(String(OUTBOX_EVENT_TYPE_MAX_LENGTH), nullable=False)
    payload: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=text("now()"), nullable=False)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
