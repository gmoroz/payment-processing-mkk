"""Маршруты платежей."""

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Header
from fastapi import status as http_status

from app.api.dependencies import ApiKeyDep, PaymentServiceDep
from app.const.payments import IDEMPOTENCY_KEY_MAX_LENGTH
from app.schemas.common import ErrorRead
from app.schemas.payments import PaymentAccepted, PaymentCreate, PaymentRead

router = APIRouter(prefix="/payments", tags=["payments"])
IdempotencyKeyHeader = Annotated[
    str,
    Header(alias="Idempotency-Key", min_length=1, max_length=IDEMPOTENCY_KEY_MAX_LENGTH),
]


@router.post(
    "",
    status_code=http_status.HTTP_202_ACCEPTED,
    responses={
        http_status.HTTP_401_UNAUTHORIZED: {"model": ErrorRead},
        http_status.HTTP_422_UNPROCESSABLE_CONTENT: {"model": ErrorRead},
    },
)
async def create_payment(
    payload: PaymentCreate,
    idempotency_key: IdempotencyKeyHeader,
    service: PaymentServiceDep,
    _api_key: ApiKeyDep,
) -> PaymentAccepted:
    """Принять новый платёж."""
    return await service.create(payload, idempotency_key)


@router.get(
    "/{payment_id}",
    responses={
        http_status.HTTP_401_UNAUTHORIZED: {"model": ErrorRead},
        http_status.HTTP_404_NOT_FOUND: {"model": ErrorRead},
        http_status.HTTP_422_UNPROCESSABLE_CONTENT: {"model": ErrorRead},
    },
)
async def get_payment(payment_id: UUID, service: PaymentServiceDep, _api_key: ApiKeyDep) -> PaymentRead:
    """Получить платёж по идентификатору."""
    return await service.get(payment_id)
