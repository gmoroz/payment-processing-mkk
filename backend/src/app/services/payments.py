"""Сервисные сценарии платежей."""

import hashlib
import json
from uuid import UUID

from app.database.repositories.payments import PaymentRepository
from app.schemas.payments import PaymentAccepted, PaymentCreate, PaymentRead
from app.services.exceptions import IdempotencyKeyReusedError, NotFoundError


def calculate_request_fingerprint(payload: PaymentCreate) -> str:
    """Рассчитать SHA-256 канонического тела платежа.

    Args:
        payload: Данные платежа.

    Returns:
        Hex-строка SHA-256.
    """
    payment_fields = payload.model_dump(mode="json")
    payment_fields["amount"] = format(payload.amount, ".2f")
    encoded_body = json.dumps(
        payment_fields,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded_body).hexdigest()


class PaymentService:
    """Бизнес-логика создания и чтения платежей."""

    def __init__(self, repository: PaymentRepository) -> None:
        self._repository = repository

    async def create(self, payload: PaymentCreate, idempotency_key: str) -> PaymentAccepted:
        """Создать платёж и событие outbox либо вернуть идемпотентный результат.

        Args:
            payload: Данные нового платежа.
            idempotency_key: Ключ идемпотентности запроса.

        Returns:
            Результат принятия платежа.
        """
        fingerprint = calculate_request_fingerprint(payload)
        payment = await self._repository.insert_if_absent(payload, idempotency_key, fingerprint)
        if payment is None:
            payment = await self._repository.get_by_idempotency_key(idempotency_key)
            if payment.request_fingerprint != fingerprint:
                raise IdempotencyKeyReusedError()
        else:
            self._repository.create_outbox_event({"payment_id": str(payment.id)})
        return PaymentAccepted(
            payment_id=payment.id,
            status=payment.status,
            created_at=payment.created_at,
        )

    async def get(self, payment_id: UUID) -> PaymentRead:
        """Получить платёж по идентификатору.

        Args:
            payment_id: Идентификатор платежа.

        Returns:
            Полное представление платежа.
        """
        payment = await self._repository.get_by_id(payment_id)
        if payment is None:
            raise NotFoundError()
        return PaymentRead.model_validate(payment)
