.DEFAULT_GOAL := help
SHELL := /usr/bin/env bash

BACKEND_DIR := backend
BACKEND_UV := uv --directory $(BACKEND_DIR) run --locked
BACKEND_ENV_FILES := --env-file $(BACKEND_DIR)/.env.example $(if $(wildcard $(BACKEND_DIR)/.env),--env-file $(BACKEND_DIR)/.env)
TEST_COMPOSE := docker compose --project-name payment-processing-mkk-test --file $(BACKEND_DIR)/compose.yml --project-directory $(BACKEND_DIR)
ROOT_COMPOSE := docker compose --file compose.yml --project-directory .
HADOLINT_IMAGE := hadolint/hadolint@sha256:32dac94127fd60b7b7e3fbfc65e1383b9b5e25c9bfd7b8536de7a539fe68a12d

define LOAD_BACKEND_ENV
set -a; . "$(BACKEND_DIR)/.env.example"; if [ -f "$(BACKEND_DIR)/.env" ]; then . "$(BACKEND_DIR)/.env"; fi; set +a;
endef

.PHONY: help install format lint test test-down smoke check

help: ## Показать доступные команды
	@awk 'BEGIN {FS = ":.*## "}; /^[a-zA-Z0-9_-]+:.*## / {printf "  %-12s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install: ## Установить зависимости backend
	uv --directory $(BACKEND_DIR) sync --locked

format: ## Исправить и отформатировать Python-код
	$(BACKEND_UV) ruff check --fix src tests
	$(BACKEND_UV) ruff format src tests

lint: ## Выполнить статические проверки и проверить контракты
	uv --directory $(BACKEND_DIR) lock --check
	$(BACKEND_UV) ruff check src tests
	$(BACKEND_UV) ruff format --check src tests
	$(BACKEND_UV) mypy src tests
	$(BACKEND_UV) vulture
	$(BACKEND_UV) deptry src
	docker run --rm -v "$(CURDIR)/$(BACKEND_DIR):/work:ro" $(HADOLINT_IMAGE) /bin/hadolint --no-color /work/Dockerfile
	API_KEY=lint-api-key $(ROOT_COMPOSE) config -q

test: ## Запустить интеграционные тесты с Postgres и RabbitMQ
	@set -e; \
	$(LOAD_BACKEND_ENV) \
	trap '$(MAKE) --no-print-directory test-down' EXIT; \
	$(TEST_COMPOSE) $(BACKEND_ENV_FILES) up -d --wait; \
	$(BACKEND_UV) alembic upgrade head; \
	$(BACKEND_UV) alembic check; \
	$(BACKEND_UV) pytest --cov --cov-report=term-missing --cov-fail-under=80

test-down:
	$(TEST_COMPOSE) $(BACKEND_ENV_FILES) down --remove-orphans --timeout 10

smoke: ## Проверить полный сценарий корневого Compose
	@set -Eeuo pipefail; \
	$(LOAD_BACKEND_ENV) \
	project="payment-processing-mkk-smoke-$$(openssl rand -hex 8)"; api_key="$$(openssl rand -hex 32)"; \
	export API_KEY="$$api_key" API_URL="http://127.0.0.1:$${API_PORT:-8000}" COMPOSE_PROJECT_NAME="$$project"; \
	compose="$(ROOT_COMPOSE) --project-name $$project"; \
	cleanup() { status=$$?; if [ "$$status" -ne 0 ]; then $$compose logs --no-color >&2 || true; fi; $$compose down -v --remove-orphans || true; exit "$$status"; }; \
	trap cleanup EXIT; \
	$$compose up --build --wait; \
	$(BACKEND_UV) pytest tests/e2e

check: ## Выполнить lint, test и smoke
	$(MAKE) lint
	$(MAKE) test
	$(MAKE) smoke
