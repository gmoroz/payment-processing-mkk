"""Сквозная проверка корневого Compose."""

import json
import os
import subprocess
import time
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import httpx
import pytest

JsonObject = dict[str, object]
PROJECT_ROOT = Path(__file__).resolve().parents[3]


@dataclass(frozen=True)
class SmokeEnvironment:
    """Параметры живого Compose-стека."""

    api_url: str
    api_key: str
    compose_project: str


@dataclass(frozen=True)
class PaymentScenario:
    """Данные платежа, созданного для проверки идемпотентности и webhook."""

    body: JsonObject
    idempotency_key: str
    payment_id: str
    creation_status: int


def _required_environment(name: str) -> str:
    """Получить обязательную переменную окружения."""
    value = os.environ.get(name)
    if not value:
        raise pytest.UsageError(f"Переменная окружения {name} обязательна для e2e.")
    return value


@pytest.fixture(scope="session")
def smoke_environment() -> SmokeEnvironment:
    """Прочитать адрес API, ключ и имя Compose-проекта из окружения."""
    return SmokeEnvironment(
        api_url=_required_environment("API_URL").rstrip("/"),
        api_key=_required_environment("API_KEY"),
        compose_project=_required_environment("COMPOSE_PROJECT_NAME"),
    )


@pytest.fixture(scope="session")
def http_client() -> Iterator[httpx.Client]:
    """Создать HTTP-клиент с ограниченными таймаутами запросов."""
    timeout = httpx.Timeout(10.0, connect=5.0)
    with httpx.Client(timeout=timeout) as client:
        yield client


@pytest.fixture(scope="session")
def payment_body() -> JsonObject:
    """Вернуть тело платежа с webhook-echo внутри Compose-сети."""
    return {
        "amount": "10.00",
        "currency": "USD",
        "description": "pytest-e2e",
        "metadata": {"source": "pytest-e2e"},
        "webhook_url": "http://webhook-echo/smoke",
    }


@pytest.fixture(scope="session")
def payment_scenario(
    http_client: httpx.Client,
    payment_body: JsonObject,
    smoke_environment: SmokeEnvironment,
) -> PaymentScenario:
    """Создать один платеж для проверок статуса, идемпотентности и webhook."""
    idempotency_key = f"pytest-e2e-{uuid4().hex}"
    response = http_client.post(
        f"{smoke_environment.api_url}/api/v1/payments",
        headers={**_auth_headers(smoke_environment), "Idempotency-Key": idempotency_key},
        json=payment_body,
    )
    response_body = response.json()
    payment_id = response_body.get("payment_id") if isinstance(response_body, dict) else None
    if not isinstance(payment_id, str):
        raise AssertionError(f"Создание платежа не вернуло payment_id: {response.text}")
    return PaymentScenario(
        body=payment_body,
        idempotency_key=idempotency_key,
        payment_id=payment_id,
        creation_status=response.status_code,
    )


def _auth_headers(environment: SmokeEnvironment) -> dict[str, str]:
    """Сформировать заголовок авторизации API."""
    return {"X-API-Key": environment.api_key}


def _wait_for_final_payment(
    client: httpx.Client,
    environment: SmokeEnvironment,
    payment_id: str,
    deadline_seconds: float = 60.0,
) -> JsonObject:
    """Дождаться финального статуса платежа."""
    deadline = time.monotonic() + deadline_seconds
    last_body: JsonObject = {}
    while True:
        response = client.get(
            f"{environment.api_url}/api/v1/payments/{payment_id}",
            headers=_auth_headers(environment),
        )
        if response.status_code != 200:
            raise AssertionError(f"GET платежа вернул HTTP {response.status_code}: {response.text}")
        response_body = response.json()
        if not isinstance(response_body, dict):
            raise AssertionError(f"GET платежа вернул не-объект: {response.text}")
        last_body = response_body
        if response_body.get("status") != "pending":
            return response_body
        if time.monotonic() >= deadline:
            raise AssertionError(f"Платёж не перешёл в финальное состояние: {last_body}")
        time.sleep(1.0)


def _compose_command(environment: SmokeEnvironment) -> tuple[str, ...]:
    """Собрать базовую команду Docker Compose."""
    return (
        "docker",
        "compose",
        "--project-name",
        environment.compose_project,
        "--file",
        str(PROJECT_ROOT / "compose.yml"),
        "--project-directory",
        str(PROJECT_ROOT),
    )


def _compose_logs(compose_command: tuple[str, ...], service: str) -> str:
    """Получить логи сервиса Compose."""
    completed = subprocess.run(
        [*compose_command, "logs", "--no-color", "--no-log-prefix", service],
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )
    return f"{completed.stdout}\n{completed.stderr}"


def _queue_listing(compose_command: tuple[str, ...]) -> str:
    """Получить список очередей RabbitMQ в Compose."""
    completed = subprocess.run(
        [
            *compose_command,
            "exec",
            "-T",
            "rabbitmq",
            "rabbitmqctl",
            "list_queues",
            "-p",
            "/",
            "name",
            "messages",
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )
    return f"{completed.stdout}\n{completed.stderr}"


def _dlq_count(queue_listing: str) -> int:
    """Извлечь число сообщений из платежной DLQ."""
    for line in queue_listing.splitlines():
        fields = line.split()
        if fields and fields[0] == "payments.new.dlq":
            return int(fields[1])
    return 0


def _find_webhook_body(logs: str, payment_id: str) -> JsonObject | None:
    """Найти в логах тело webhook платежа."""
    for line in logs.splitlines():
        json_start = line.find("{")
        if json_start < 0:
            continue
        try:
            event = json.loads(line[json_start:])
        except json.JSONDecodeError:
            continue
        if not isinstance(event, dict):
            continue
        request = event.get("request")
        if not isinstance(request, dict):
            continue
        body = request.get("body")
        if isinstance(body, str):
            try:
                body = json.loads(body)
            except json.JSONDecodeError:
                continue
        if isinstance(body, dict) and body.get("payment_id") == payment_id:
            return body
    return None


def _wait_for_webhook(
    compose_command: tuple[str, ...],
    payment_id: str,
    deadline_seconds: float = 30.0,
) -> JsonObject:
    """Дождаться webhook платежа."""
    deadline = time.monotonic() + deadline_seconds
    last_logs = ""
    while True:
        last_logs = _compose_logs(compose_command, "webhook-echo")
        webhook_body = _find_webhook_body(last_logs, payment_id)
        if webhook_body is not None:
            return webhook_body
        if time.monotonic() >= deadline:
            raise AssertionError(f"Webhook не найден в логах:\n{last_logs}")
        time.sleep(1.0)


def _wait_for_dlq(
    compose_command: tuple[str, ...],
    payment_id: str,
    deadline_seconds: float = 45.0,
) -> None:
    """Дождаться одной записи платежа в DLQ после третьей попытки."""
    deadline = time.monotonic() + deadline_seconds
    last_queue_listing = ""
    last_consumer_logs = ""
    while True:
        last_queue_listing = _queue_listing(compose_command)
        last_consumer_logs = _compose_logs(compose_command, "consumer")
        count = _dlq_count(last_queue_listing)
        if count == 1 and f"payment_id={payment_id} attempt=3" in last_consumer_logs:
            return
        if time.monotonic() >= deadline:
            raise AssertionError(
                f"Ожидались attempt=3 и одна запись DLQ. Очереди:\n{last_queue_listing}\n"
                f"Логи consumer:\n{last_consumer_logs}",
            )
        time.sleep(2.0)


def test_payment_without_api_key_returns_401(
    http_client: httpx.Client,
    payment_body: JsonObject,
    smoke_environment: SmokeEnvironment,
) -> None:
    """Запрос создания платежа без X-API-Key возвращает 401."""
    response = http_client.post(
        f"{smoke_environment.api_url}/api/v1/payments",
        headers={"Idempotency-Key": f"pytest-no-auth-{uuid4().hex}"},
        json=payment_body,
    )

    assert response.status_code == 401


def test_payment_creation_with_valid_body_returns_202(payment_scenario: PaymentScenario) -> None:
    """Создание платежа с корректным телом возвращает 202."""
    assert payment_scenario.creation_status == 202


def test_payment_status_after_processing_reaches_final_state_with_processed_at(
    http_client: httpx.Client,
    payment_scenario: PaymentScenario,
    smoke_environment: SmokeEnvironment,
) -> None:
    """Созданный платеж получает succeeded или failed и непустой processed_at."""
    payment = _wait_for_final_payment(http_client, smoke_environment, payment_scenario.payment_id)

    assert payment["status"] in {"succeeded", "failed"}
    assert isinstance(payment["processed_at"], str)
    assert payment["processed_at"]


def test_payment_replay_with_same_key_and_body_returns_same_payment_id(
    http_client: httpx.Client,
    payment_scenario: PaymentScenario,
    smoke_environment: SmokeEnvironment,
) -> None:
    """Повтор с тем же ключом и телом возвращает исходный payment_id."""
    response = http_client.post(
        f"{smoke_environment.api_url}/api/v1/payments",
        headers={**_auth_headers(smoke_environment), "Idempotency-Key": payment_scenario.idempotency_key},
        json=payment_scenario.body,
    )
    response_body = response.json()

    assert response.status_code == 202
    assert response_body["payment_id"] == payment_scenario.payment_id


def test_payment_reuse_with_different_body_returns_422_idempotency_key_reused(
    http_client: httpx.Client,
    payment_scenario: PaymentScenario,
    smoke_environment: SmokeEnvironment,
) -> None:
    """Повтор с тем же ключом и другим телом возвращает idempotency_key_reused."""
    different_body = {**payment_scenario.body, "description": "pytest-e2e-different"}
    response = http_client.post(
        f"{smoke_environment.api_url}/api/v1/payments",
        headers={**_auth_headers(smoke_environment), "Idempotency-Key": payment_scenario.idempotency_key},
        json=different_body,
    )
    response_body = response.json()

    assert response.status_code == 422
    assert response_body["error_code"] == "idempotency_key_reused"


def test_webhook_echo_after_processing_contains_final_payment_state(
    http_client: httpx.Client,
    payment_scenario: PaymentScenario,
    smoke_environment: SmokeEnvironment,
) -> None:
    """Webhook-echo получает payment_id, финальный статус и processed_at."""
    payment = _wait_for_final_payment(http_client, smoke_environment, payment_scenario.payment_id)
    compose_command = _compose_command(smoke_environment)
    webhook_body = _wait_for_webhook(compose_command, payment_scenario.payment_id)

    assert webhook_body["status"] == payment["status"]
    assert webhook_body["processed_at"] == payment["processed_at"]


def test_dlq_after_unreachable_webhook_attempt_3_contains_one_message(
    http_client: httpx.Client,
    payment_body: JsonObject,
    smoke_environment: SmokeEnvironment,
) -> None:
    """Недоступный webhook после третьей попытки оставляет ровно одно сообщение в DLQ."""
    dlq_body = {**payment_body, "description": "pytest-e2e-dlq", "webhook_url": "http://nonexistent-host.invalid/hook"}
    response = http_client.post(
        f"{smoke_environment.api_url}/api/v1/payments",
        headers={**_auth_headers(smoke_environment), "Idempotency-Key": f"pytest-dlq-{uuid4().hex}"},
        json=dlq_body,
    )
    response_body = response.json()
    payment_id = response_body.get("payment_id") if isinstance(response_body, dict) else None
    if not isinstance(payment_id, str):
        raise AssertionError(f"Создание DLQ-платежа не вернуло payment_id: {response.text}")

    compose_command = _compose_command(smoke_environment)
    _wait_for_dlq(compose_command, payment_id)
