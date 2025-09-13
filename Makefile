.PHONY: help install migrate run-api run-worker run-flower run-all stop-all test lint format

help:
	@echo "Available commands:"
	@echo "  make install       - Install dependencies"
	@echo "  make migrate       - Run database migrations"
	@echo "  make run-api       - Run FastAPI server"
	@echo "  make run-worker    - Run Celery worker"
	@echo "  make run-flower    - Run Flower monitoring"
	@echo "  make run-all       - Run all services"
	@echo "  make stop-all      - Stop all services"
	@echo "  make test          - Run tests"
	@echo "  make lint          - Run linters"
	@echo "  make format        - Format code"

install:
	poetry install

migrate:
	poetry run alembic upgrade head

run-api:
	poetry run python scripts/start.py

run-worker:
	poetry run celery -A app.celery_app worker \
		--loglevel=info \
		--queues=default,backfill,backfill_high \
		--concurrency=4

run-flower:
	poetry run celery -A app.celery_app flower \
		--port=5555 \
		--basic_auth=admin:admin

run-worker-verbose:
	poetry run celery -A app.celery_app worker \
		--loglevel=debug \
		--queues=default,backfill,backfill_high \
		--concurrency=2 \
		--traceback

# Run all services in separate terminals (requires tmux)
run-all:
	@echo "Starting all services..."
	@tmux new-session -d -s energyexe-api 'make run-api'
	@tmux new-session -d -s energyexe-worker 'make run-worker'
	@tmux new-session -d -s energyexe-flower 'make run-flower'
	@echo "Services started in tmux sessions:"
	@echo "  - API: tmux attach -t energyexe-api"
	@echo "  - Worker: tmux attach -t energyexe-worker"
	@echo "  - Flower: tmux attach -t energyexe-flower"

stop-all:
	@echo "Stopping all services..."
	@tmux kill-session -t energyexe-api 2>/dev/null || true
	@tmux kill-session -t energyexe-worker 2>/dev/null || true
	@tmux kill-session -t energyexe-flower 2>/dev/null || true
	@echo "All services stopped"

test:
	poetry run pytest

lint:
	poetry run flake8 app
	poetry run mypy app

format:
	poetry run black app
	poetry run isort app

# Development helpers
dev-reset-db:
	poetry run alembic downgrade base
	poetry run alembic upgrade head

dev-shell:
	poetry run python

dev-inspect-queue:
	poetry run celery -A app.celery_app inspect active

dev-purge-queue:
	poetry run celery -A app.celery_app purge -f