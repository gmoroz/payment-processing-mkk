"""Репозиторий платежей и outbox."""

from datetime import UTC, datetime
from uuid import UUID, uuid4

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.const.messaging import PAYMENTS_NEW_EVENT
from app.database.models.payments import Outbox, Payment
from app.enums import PaymentStatus
from app.schemas.payments import PaymentCreate


class PaymentRepository:
    """Операции PostgreSQL для платежей и outbox."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def insert_if_absent(
        self,
        payload: PaymentCreate,
        idempotency_key: str,
        request_fingerprint: str,
    ) -> Payment | None:
        """Вставить платёж, не создавая дубликат по ключу идемпотентности.

        Args:
            payload: Данные нового платежа.
            idempotency_key: Ключ идемпотентности запроса.
            request_fingerprint: Отпечаток тела запроса.

        Returns:
            Вставленный платёж или None при конфликте ключа.
        """
        payment_id = uuid4()
        statement = (
            insert(Payment)
            .values(
                id=payment_id,
                status=PaymentStatus.PENDING,
                idempotency_key=idempotency_key,
                request_fingerprint=request_fingerprint,
                **payload.model_dump(exclude={"metadata", "webhook_url"}),
                metadata_json=payload.metadata,
                webhook_url=str(payload.webhook_url),
            )
            .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
            .returning(Payment)
        )
        return (await self._session.scalars(statement)).one_or_none()

    async def get_by_idempotency_key(self, idempotency_key: str) -> Payment:
        """Получить существующий платёж по ключу идемпотентности.

        Args:
            idempotency_key: Ключ идемпотентности платежа.

        Returns:
            Найденный платёж.
        """
        statement = select(Payment).where(Payment.idempotency_key == idempotency_key)
        return (await self._session.execute(statement)).scalar_one()

    async def get_by_id(self, payment_id: UUID) -> Payment | None:
        """Получить платёж по идентификатору.

        Args:
            payment_id: Идентификатор платежа.

        Returns:
            Платёж или None, если он отсутствует.
        """
        return await self._session.get(Payment, payment_id)

    async def get_by_id_for_update(self, payment_id: UUID) -> Payment:
        """Заблокировать платёж для финализации статуса.

        Args:
            payment_id: Идентификатор платежа.

        Returns:
            Заблокированный платёж.
        """
        statement = select(Payment).where(Payment.id == payment_id).with_for_update()
        return (await self._session.execute(statement)).scalar_one()

    def create_outbox_event(self, payload: dict[str, object]) -> None:
        """Создать событие платежа в outbox.

        Args:
            payload: Данные события.
        """
        self._session.add(Outbox(event_type=PAYMENTS_NEW_EVENT, payload=payload))

    async def get_unpublished_outbox_batch(self, batch_size: int) -> list[Outbox]:
        """Заблокировать batch неопубликованных событий outbox.

        Args:
            batch_size: Максимальное число событий в batch.

        Returns:
            Список заблокированных событий.
        """
        statement = (
            select(Outbox)
            .where(Outbox.published_at.is_(None))
            .order_by(Outbox.created_at, Outbox.id)
            .with_for_update(skip_locked=True)
            .limit(batch_size)
        )
        return list(await self._session.scalars(statement))

    async def mark_outbox_published(self, event_ids: list[UUID]) -> None:
        """Зафиксировать подтверждённые публикации одним обновлением.

        Args:
            event_ids: Идентификаторы outbox-событий.
        """
        statement = update(Outbox).where(Outbox.id.in_(event_ids)).values(published_at=datetime.now(UTC))
        await self._session.execute(statement)
