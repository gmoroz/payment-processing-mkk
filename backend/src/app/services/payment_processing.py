"""Бизнес-правила асинхронной обработки платежа."""

import asyncio
import random

from app.enums import PaymentStatus


async def simulate_processing(
    minimum_seconds: float,
    maximum_seconds: float,
    success_rate: float,
) -> PaymentStatus:
    """Подождать случайную длительность и вернуть бизнес-исход.

    Args:
        minimum_seconds: Нижняя граница ожидания.
        maximum_seconds: Верхняя граница ожидания.
        success_rate: Вероятность успешного процессинга.

    Returns:
        Финальный статус платежа.
    """
    duration = random.uniform(minimum_seconds, maximum_seconds)
    await asyncio.sleep(duration)
    if random.random() < success_rate:
        return PaymentStatus.SUCCEEDED
    return PaymentStatus.FAILED
