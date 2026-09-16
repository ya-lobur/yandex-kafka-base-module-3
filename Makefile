COMPOSE = docker compose
CLI = uv run --locked kafka-practice
ARGS ?=

.DEFAULT_GOAL := help
.PHONY: help install up register status consume smoke sql logs ui down reset

help: ## Показать команды
	@awk 'BEGIN {FS = ":.*## "} /^[a-z-]+:.*## / {printf "  %-12s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install: ## Создать .venv и установить зависимости из uv.lock
	uv sync --locked

up: install ## Собрать стенд, дождаться сервисов и зарегистрировать Debezium
	$(COMPOSE) config --quiet
	$(COMPOSE) up -d --build --wait --wait-timeout 240
	$(CLI) register

register: install ## Создать/обновить конфигурацию коннектора
	$(CLI) register

status: ## Проверить состояние коннектора и задачи
	$(CLI) status

consume: ## Читать события обеих таблиц (Ctrl+C — выход; ARGS='--limit 9')
	$(CLI) consume $(ARGS)

smoke: up ## Запустить стенд и проверить CDC, Prometheus и Grafana
	$(CLI) smoke $(ARGS)

sql: ## Открыть psql в базе shop
	$(COMPOSE) exec postgres psql -v ON_ERROR_STOP=1 -U postgres-user -d shop

logs: ## Показать последние логи (ARGS='-f' — следить)
	$(COMPOSE) logs --tail=100 $(ARGS)

ui: up ## Включить Kafka UI на localhost:8080
	$(COMPOSE) --profile ui up -d --wait kafka-ui

down: ## Остановить стенд, сохранив данные
	$(COMPOSE) --profile ui down

reset: ## УДАЛИТЬ все данные только этого Compose-проекта
	$(COMPOSE) --profile ui down --volumes --remove-orphans
