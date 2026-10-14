"""Иерархия ошибок. Внешние исключения заворачивать в подкласс ServiceError."""

from starlette import status

from app.const.http import IDEMPOTENCY_KEY_REUSED_MESSAGE, PAYMENT_NOT_FOUND_MESSAGE, UNAUTHORIZED_MESSAGE
from app.enums import ErrorCode


class ServiceError(Exception):
    """Базовая доменная ошибка: несёт код статуса, машинный код и сообщение."""

    def __init__(self, status_code: int, error_code: str, message: str) -> None:
        self.status_code = status_code
        self.error_code = error_code
        self.message = message
        super().__init__(f"{error_code}: {message}")


class NotFoundError(ServiceError):
    """Ошибка отсутствующего платежа."""

    def __init__(self) -> None:
        super().__init__(status.HTTP_404_NOT_FOUND, ErrorCode.NOT_FOUND, PAYMENT_NOT_FOUND_MESSAGE)


class UnauthorizedError(ServiceError):
    """Ошибка отсутствующей или неверной аутентификации."""

    def __init__(self) -> None:
        super().__init__(status.HTTP_401_UNAUTHORIZED, ErrorCode.UNAUTHORIZED, UNAUTHORIZED_MESSAGE)


class IdempotencyKeyReusedError(ServiceError):
    """Ошибка повторного использования ключа с другим телом."""

    def __init__(self) -> None:
        super().__init__(
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            ErrorCode.IDEMPOTENCY_KEY_REUSED,
            IDEMPOTENCY_KEY_REUSED_MESSAGE,
        )
