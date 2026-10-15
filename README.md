# payment-processing-mkk

## Что это

Асинхронный микросервис обработки платежей. API принимает платёж, сохраняет его со статусом `pending` и передаёт событие в RabbitMQ. Consumer эмулирует процессинг, переводит платёж в `succeeded` или `failed` и отправляет webhook.

Платёж содержит:

- `id` — UUID;
- `amount` — положительная сумма строкой вида `125.50` или JSON-числом: до двух знаков после десятичного разделителя и до 18 цифр всего;
- `currency` — `RUB`, `USD` или `EUR`;
- `description` — строка длиной от 1 до 1000 символов;
- `metadata` — JSON-объект;
- `status` — `pending`, `succeeded` или `failed`;
- `idempotency_key` — уникальный ключ запроса;
- `webhook_url` — HTTP(S)-URL;
- `created_at` и `processed_at`.

Платёжные методы API:

- `POST /api/v1/payments` — принять платёж, ответ `202 Accepted`;
- `GET /api/v1/payments/{payment_id}` — получить платёж, ответ `200 OK`.

Все эндпоинты, включая `GET /health`, требуют статический заголовок `X-API-Key`. `GET /health` возвращает `{"status":"ok"}`.

Документация API доступна по адресу `/docs` при `DEBUG=true`. В корневом compose включите её командой `DEBUG=true docker compose up --build -d --wait`.

## Стек

- Python, FastAPI, Uvicorn, Pydantic, SQLAlchemy, `asyncpg` и Alembic;
- PostgreSQL, RabbitMQ, FastStream и Docker Compose.

Версии Python-зависимостей задаются в `backend/pyproject.toml` и `backend/uv.lock`, версии образов — в `compose.yml` и `backend/compose.yml`.

## Быстрый старт через Docker

Корневой compose-файл поднимает PostgreSQL, RabbitMQ, одноразовый сервис миграций, API, consumer, outbox relay и `webhook-echo`.

`API_KEY` обязателен: без него не выполнится ни одна команда `docker compose`. Сгенерируйте случайный ключ в файл `.env` в корне репозитория. Compose читает этот файл сам, поэтому `logs` и `down` работают из любого терминала. Файл `.env` исключён из git.

```sh
echo "API_KEY=$(openssl rand -hex 32)" > .env

docker compose up --build -d --wait
```

По умолчанию API доступен на `http://127.0.0.1:8000`, а RabbitMQ Management UI — на `http://127.0.0.1:15672`. Оба порта привязаны только к loopback. Переопределите их переменными окружения до запуска:

```sh
export API_PORT=18000
export RABBITMQ_MANAGEMENT_PORT=25672
docker compose up --build -d --wait
```

Остановить стек и удалить его volumes:

```sh
docker compose down -v --remove-orphans
```

## Примеры API

Ниже приведены команды и ответы из фактического прогона compose-стека. UUID и даты в новом запуске будут другими. В примерах используется `http://webhook-echo` — это DNS-имя сервиса внутри сети Compose, доступное consumer.

Подготовьте переменные. Строка `. ./.env` загружает `API_KEY` в текущий shell:

```sh
. ./.env
export API_URL="http://127.0.0.1:${API_PORT:-8000}"
export IDEMPOTENCY_KEY=readme-key-2
export PAYMENT_BODY='{"amount":"125.50","currency":"RUB","description":"Тестовый платёж","metadata":{"order_id":"readme-2"},"webhook_url":"http://webhook-echo"}'
```

### Создание

```sh
curl -sS -i -X POST "$API_URL/api/v1/payments" \
  -H "X-API-Key: $API_KEY" \
  -H "Idempotency-Key: $IDEMPOTENCY_KEY" \
  -H 'Content-Type: application/json' \
  -d "$PAYMENT_BODY"
```

Ответ `202 Accepted`:

```json
{"payment_id":"03a3850d-9e0d-4eeb-b9f5-d7623db2a282","status":"pending","created_at":"2026-10-14T18:49:06.252702Z"}
```

### Повтор с тем же ключом и тем же телом

```sh
curl -sS -i -X POST "$API_URL/api/v1/payments" \
  -H "X-API-Key: $API_KEY" \
  -H "Idempotency-Key: $IDEMPOTENCY_KEY" \
  -H 'Content-Type: application/json' \
  -d "$PAYMENT_BODY"
```

Ответ `202 Accepted`. Возвращается тот же `payment_id`; статус отражает текущее состояние исходного платежа.

```json
{"payment_id":"03a3850d-9e0d-4eeb-b9f5-d7623db2a282","status":"succeeded","created_at":"2026-10-14T18:49:06.252702Z"}
```

### Тот же ключ с другим телом

Изменение суммы даёт `422 Unprocessable Entity`:

```sh
curl -sS -i -X POST "$API_URL/api/v1/payments" \
  -H "X-API-Key: $API_KEY" \
  -H "Idempotency-Key: $IDEMPOTENCY_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"amount":"125.51","currency":"RUB","description":"Тестовый платёж","metadata":{"order_id":"readme-2"},"webhook_url":"http://webhook-echo"}'
```

```json
{"error_code":"idempotency_key_reused","message":"Idempotency key was reused with a different request"}
```

### Получение платежа

```sh
curl -sS -i "$API_URL/api/v1/payments/03a3850d-9e0d-4eeb-b9f5-d7623db2a282" \
  -H "X-API-Key: $API_KEY"
```

Ответ `200 OK`:

```json
{"id":"03a3850d-9e0d-4eeb-b9f5-d7623db2a282","amount":"125.50","currency":"RUB","description":"Тестовый платёж","metadata":{"order_id":"readme-2"},"status":"succeeded","idempotency_key":"readme-key-2","webhook_url":"http://webhook-echo/","created_at":"2026-10-14T18:49:06.252702Z","processed_at":"2026-10-14T18:49:09.091212Z"}
```

### Запрос без API-ключа

```sh
curl -sS -i "$API_URL/api/v1/payments/03a3850d-9e0d-4eeb-b9f5-d7623db2a282"
```

Ответ `401 Unauthorized`:

```json
{"error_code":"unauthorized","message":"Unauthorized"}
```

Ответы `401`, `404`, `422` и `500` имеют единый формат `{ "error_code": "...", "message": "..." }`. Для несуществующего платежа используется `404` с `error_code: "not_found"`. Неподдерживаемый метод на существующем пути возвращает стандартный ответ FastAPI `405 {"detail": "Method Not Allowed"}` с заголовком `Allow`.

## Webhook и DLQ

После успешного ответа webhook-сервера посмотрите доставку в логах `webhook-echo`:

```sh
docker compose logs --no-color webhook-echo
```

Тело webhook содержит `payment_id`, `status`, строковую `amount`, `currency` и `processed_at`, например:

```json
{"payment_id":"03a3850d-9e0d-4eeb-b9f5-d7623db2a282","status":"succeeded","amount":"125.50","currency":"RUB","processed_at":"2026-10-14T18:49:09.091212Z"}
```

RabbitMQ Management UI находится по адресу `http://127.0.0.1:${RABBITMQ_MANAGEMENT_PORT:-15672}`. Для корневого compose используются учётные данные `app` / `localdev`. В разделе **Queues and Streams** выберите очередь `payments.new.dlq`.

Чтобы увидеть путь до DLQ, создайте платёж с недоступным webhook:

```sh
curl -sS -X POST "$API_URL/api/v1/payments" \
  -H "X-API-Key: $API_KEY" \
  -H 'Idempotency-Key: readme-dlq' \
  -H 'Content-Type: application/json' \
  -d '{"amount":"10.00","currency":"USD","description":"DLQ","metadata":{},"webhook_url":"http://nonexistent-host.invalid/hook"}'

docker compose logs consumer
```

Платёж получит финальный статус. В логах consumer появятся три предупреждения `Не доставлен webhook ... attempt=1..3 reason=ConnectError('...')` с паузами 2 и 4 секунды, затем сообщение окажется в `payments.new.dlq`.

## Архитектура и поток данных

1. API валидирует тело и `Idempotency-Key`. В одной транзакции PostgreSQL создаются запись в таблице `payments` со статусом `pending` и запись в таблице `outbox` с событием `payments.new`.
2. Outbox relay периодически выбирает неопубликованные записи с `FOR UPDATE SKIP LOCKED`, публикует событие в RabbitMQ с publisher confirm и только после подтверждения отмечает запись как опубликованную.
3. Consumer получает событие из `payments.new`. Для `pending`-платежа он ждёт случайное время от 2 до 5 секунд и выбирает `succeeded` с вероятностью 0.9 либо `failed` с вероятностью 0.1. Затем в отдельной транзакции фиксирует финальный статус и `processed_at`.
4. Тот же consumer отправляет webhook с финальным состоянием. Сообщение подтверждается только после успешного ответа webhook-сервера.
5. Ошибка HTTP-доставки или сетевой сбой отправляет сообщение в TTL retry-очередь. После исчерпания попыток сообщение попадает в DLQ. Если RabbitMQ не подтвердил публикацию в retry-очередь, сообщение сразу уходит в DLQ, чтобы не потеряться.

Топология RabbitMQ:

- direct exchange `payments` принимает routing key `payments.new`;
- durable queue `payments.new` привязана к `payments` с routing key `payments.new`;
- direct exchange `payments.dlx` принимает dead-letter сообщения;
- durable queue `payments.new.dlq` привязана к `payments.dlx` с routing key `payments.new`;
- при стандартном `MAX_DELIVERY_ATTEMPTS=3` создаются `payments.retry.2000ms` с TTL 2 секунды и `payments.retry.4000ms` с TTL 4 секунды; обе очереди возвращают сообщения через exchange `payments` с routing key `payments.new`.

## Гарантии и решения

### Идемпотентность

Платёж имеет уникальный `Idempotency-Key`. Один и тот же ключ с тем же каноническим телом возвращает исходный платёж и `202`. Повторное использование ключа с другим телом возвращает `422` и `idempotency_key_reused`.

### Гарантии доставки

Если relay завершится после публикации, но до отметки `published_at`, событие может быть опубликовано повторно: доставка события имеет гарантию at-least-once.

Повторная доставка уже финализированного события не меняет `succeeded` или `failed`, но повторяет webhook. Поэтому доставка webhook также at-least-once (см. «Ограничения»).

Бизнес-исход `failed` не запускает retry процессинга. Retry применяется к ошибкам чтения/финализации, сетевой доставке webhook и ответу webhook вне диапазона 2xx. Невалидное сообщение, невалидный номер попытки и событие с несуществующим платежом сразу отклоняются без повторной доставки и через DLX попадают в DLQ. Задержку между попытками выдерживают TTL retry-очередей, обработчик не ждёт, delayed-message plugin не используется.

Миграции применяет одноразовый сервис `migrate` командой `alembic upgrade head`. API не запускает миграции при старте и зависит от успешного завершения `migrate`.

## Конфигурация

При локальном запуске `Settings` читает переменные окружения и `.env`; `.env.example` сам по себе не используется. Цели `make lint` и `make test` перед запуском явно экспортируют значения из `.env.example`, затем `.env`. В Docker-образ эти файлы не попадают: для корневого compose значения `DB_URL`, `RABBITMQ_URL`, `DEBUG` и `API_KEY` передаются самим compose-файлом, остальные берутся из значений по умолчанию.

| Переменная | Значение по умолчанию или требование | Назначение |
| --- | --- | --- |
| `DEBUG` | `true` в `backend/.env.example`; `false` в корневом compose | Режим отладки |
| `DB_URL` | Обязательна для `Settings`; в корневом compose `postgresql://app:localdev@postgres:5432/app` | DSN PostgreSQL |
| `API_KEY` | Обязательна в корневом compose; в `.env.example` тестовое значение | Статический ключ `X-API-Key` |
| `RABBITMQ_URL` | Обязательна для `Settings`; в корневом compose `amqp://app:localdev@rabbitmq:5672/` | DSN RabbitMQ |
| `RABBITMQ_CONNECT_TIMEOUT_SECONDS` | `5` | Таймаут подключения к RabbitMQ |
| `RELAY_POLL_INTERVAL_SECONDS` | `1` | Интервал опроса outbox |
| `RELAY_BATCH_SIZE` | `100` | Максимальный batch outbox |
| `CONSUMER_CONCURRENCY` | `4` | `prefetch_count` consumer |
| `PUBLISH_CONFIRM_TIMEOUT_SECONDS` | `5` | Таймаут publisher confirm |
| `PROCESSING_MIN_SECONDS` | `2` | Нижняя граница эмуляции процессинга |
| `PROCESSING_MAX_SECONDS` | `5` | Верхняя граница эмуляции процессинга |
| `PROCESSING_SUCCESS_RATE` | `0.9` | Вероятность `succeeded` |
| `WEBHOOK_TIMEOUT_SECONDS` | `5` | Таймаут HTTP webhook |
| `MAX_DELIVERY_ATTEMPTS` | `3` | Общее число обработок сообщения |
| `RETRY_BASE_DELAY_SECONDS` | `2` | Базовая задержка retry |

Для `DEBUG=false` ключ не может быть тестовым значением и должен содержать не менее 32 символов. Переменные портов относятся к корневому compose и не являются полями `Settings`: `API_PORT` по умолчанию `8000`, `RABBITMQ_MANAGEMENT_PORT` по умолчанию `15672`.

## Разработка и проверки

Основные команды запускайте через `make`:

```sh
make install
make format
make lint
make test
make smoke
make check
```

`make smoke` проверяет сквозной сценарий корневого compose: отказ `401` без ключа, идемпотентность, финальный статус, доставку webhook в `webhook-echo` и DLQ. Полный список целей — `make help`.

## Ограничения

- `webhook_url` проверяется как HTTP(S)-URI и по длине, но приватные адреса не фильтруются. Для production нужен egress-контроль с allowlist; в Compose это позволяет отправлять webhook в локальный `webhook-echo`.
- При повторной доставке одного события (at-least-once relay) параллельные доставки могут дважды выполнить эмуляцию процессинга. Финальный статус фиксирует первая транзакция под блокировкой строки, вторая его не меняет; при интеграции с реальным шлюзом ему передаётся `payment_id` как ключ идемпотентности.
- Доставка webhook at-least-once: получатель обязан быть идемпотентным и дедуплицировать уведомления по `payment_id`.
- Процессинг эмулируется случайной задержкой и вероятностью успеха; интеграции с платёжным провайдером нет.
- API-ключ статический и задаётся через окружение; механизма пользователей и ротации ключей в сервисе нет.

## Контакт

Telegram: [@morozov_ml](https://t.me/morozov_ml)
