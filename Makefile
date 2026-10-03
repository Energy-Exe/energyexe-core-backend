.PHONY: help install migrate run-api test lint format dev-reset-db dev-shell

help:
	@echo "Available commands:"
	@echo "  make install       - Install dependencies (Poetry, local dev)"
	@echo "  make migrate       - Run database migrations"
	@echo "  make run-api       - Run FastAPI server (PORT from .env, default 8001)"
	@echo "  make test          - Run tests"
	@echo "  make lint          - Run linters"
	@echo "  make format        - Format code"
	@echo "  make dev-reset-db  - Downgrade to base and re-apply all migrations"
	@echo "  make dev-shell     - Python shell in the Poetry environment"
	@echo ""
	@echo "Scheduled jobs run in AWS EventBridge (docs/operations/scheduled-jobs.md);"
	@echo "there is no worker process to start locally."

install:
	poetry install

migrate:
	poetry run alembic upgrade head

run-api:
	poetry run python scripts/start.py

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
