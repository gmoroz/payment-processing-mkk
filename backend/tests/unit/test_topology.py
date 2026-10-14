"""Проверки топологии retry."""

from app.messaging.topology import retry_queue


def test_retry_queue_with_successive_attempts_doubles_ttl_and_name() -> None:
    """Каждая следующая retry-очередь удваивает TTL и отражает его в имени."""
    first_queue = retry_queue(retry_number=1, base_delay_seconds=2)
    second_queue = retry_queue(retry_number=2, base_delay_seconds=2)

    assert first_queue.arguments["x-message-ttl"] == 2000
    assert first_queue.name == "payments.retry.2000ms"
    assert second_queue.arguments["x-message-ttl"] == 4000
    assert second_queue.name == "payments.retry.4000ms"


def test_retry_queue_with_different_base_delay_changes_name_and_ttl() -> None:
    """Изменение базовой задержки меняет TTL и имя retry-очереди."""
    queue = retry_queue(retry_number=1, base_delay_seconds=3)

    assert queue.arguments["x-message-ttl"] == 3000
    assert queue.name == "payments.retry.3000ms"
