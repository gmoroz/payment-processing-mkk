"""Сервис финализации платежа в consumer."""

from datetime import UTC, datetime
from uuid import UUID

from app.database.repositories.payments import PaymentRepository
from app.enums import PaymentStatus
from app.schemas.payments import PaymentRead


async def finalize_payment(
    repository: PaymentRepository,
    payment_id: UUID,
    simulated_status: PaymentStatus,
) -> PaymentRead:
    """Обновить pending-платёж и вернуть актуальный статус.

    Args:
        repository: Репозиторий платежей в текущей транзакции.
        payment_id: Идентификатор платежа.
        simulated_status: Бизнес-исход процессинга.

    Returns:
        Платёж после блокировки.
    """
    payment = await repository.get_by_id_for_update(payment_id)
    if payment.status is PaymentStatus.PENDING:
        payment.status = simulated_status
        payment.processed_at = datetime.now(UTC)
    return PaymentRead.model_validate(payment)
