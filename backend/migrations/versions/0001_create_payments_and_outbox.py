"""Создать таблицы платежей и outbox.

Revision ID: 0001
Revises:
Create Date: 2026-10-14
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Создать таблицы платежей и outbox."""
    currency_enum = sa.Enum("RUB", "USD", "EUR", name="currency_enum")
    payment_status_enum = sa.Enum("pending", "succeeded", "failed", name="payment_status_enum")
    op.create_table(
        "payments",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("amount", sa.Numeric(18, 2), nullable=False),
        sa.Column("currency", currency_enum, nullable=False),
        sa.Column("description", sa.String(length=1000), nullable=False),
        sa.Column("metadata", postgresql.JSONB(), nullable=False),
        sa.Column("status", payment_status_enum, nullable=False),
        sa.Column("idempotency_key", sa.String(length=255), nullable=False),
        sa.Column("request_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("webhook_url", sa.String(length=2048), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("idempotency_key"),
    )
    op.create_table(
        "outbox",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("event_type", sa.String(length=255), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_outbox_unpublished_created_at",
        "outbox",
        ["created_at"],
        postgresql_where=sa.text("published_at IS NULL"),
    )


def downgrade() -> None:
    """Удалить таблицы платежей и outbox."""
    op.drop_index("ix_outbox_unpublished_created_at", table_name="outbox")
    op.drop_table("outbox")
    op.drop_table("payments")
    sa.Enum("RUB", "USD", "EUR", name="currency_enum").drop(op.get_bind(), checkfirst=True)
    sa.Enum("pending", "succeeded", "failed", name="payment_status_enum").drop(op.get_bind(), checkfirst=True)
