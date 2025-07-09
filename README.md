# EnergyExe Core Backend

A modern, production-ready FastAPI backend application with best practices and comprehensive architecture.

## Features

- **FastAPI Framework**: High-performance, easy-to-use, fast to code
- **Async/Await**: Full async support with SQLAlchemy 2.0
- **Database**: PostgreSQL with async drivers and migrations
- **Authentication**: JWT-based authentication with password hashing
- **Security**: Built-in security headers and CORS support
- **Logging**: Structured logging with request tracking
- **Testing**: Comprehensive test suite with pytest
- **Code Quality**: Pre-commit hooks, linting, and formatting
- **Docker**: Multi-stage builds for development and production
- **Database Migrations**: Alembic for database schema management

## Quick Start

### Prerequisites

- Python 3.11+
- Poetry 1.7+ (for dependency management)
- PostgreSQL 15+
- Docker (optional)

### Local Development

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd energyexe-core-backend
   ```

2. **Install Poetry** (if not already installed)
   ```bash
   curl -sSL https://install.python-poetry.org | python3 -
   # Or using pip: pip install poetry
   ```

3. **Install dependencies**
   ```bash
   poetry install --with dev,test
   ```

4. **Set up environment variables**
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

5. **Set up database**
   ```bash
   # Start PostgreSQL and create database
   poetry run alembic upgrade head
   ```

6. **Run the application**
   ```bash
   poetry run python scripts/start.py
   # Or directly:
   poetry run uvicorn app.main:app --reload
   ```

The API will be available at `http://localhost:8000`

### Docker Development

1. **Start all services**
   ```bash
   docker-compose up -d
   ```

2. **Run migrations**
   ```bash
   docker-compose exec api alembic upgrade head
   ```

The API will be available at `http://localhost:8000`

## API Documentation

- **Interactive API docs**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

## Project Structure

```
energyexe-core-backend/
├── app/
│   ├── api/                 # API routes
│   │   └── v1/
│   │       ├── endpoints/   # API endpoints
│   │       └── router.py    # Main router
│   ├── core/                # Core configuration
│   │   ├── config.py        # Settings
│   │   ├── database.py      # Database setup
│   │   ├── security.py      # Security utilities
│   │   └── exceptions.py    # Exception handlers
│   ├── models/              # Database models
│   ├── schemas/             # Pydantic schemas
│   ├── services/            # Business logic
│   └── main.py              # FastAPI app
├── alembic/                 # Database migrations
├── tests/                   # Test suite
├── scripts/                 # Utility scripts
├── docker-compose.yml       # Docker services
├── Dockerfile              # Docker build
├── pyproject.toml          # Python project config
└── .env.example            # Environment variables
```

## Development Commands

```bash
# Run tests
poetry run pytest

# Run tests with coverage
poetry run pytest --cov=app

# Format code
poetry run black .
poetry run isort .

# Lint code
poetry run flake8 .
poetry run mypy .

# Create database migration
poetry run alembic revision --autogenerate -m "Migration message"

# Apply migrations
poetry run alembic upgrade head

# Install pre-commit hooks
poetry run pre-commit install

# Add new dependencies
poetry add <package-name>

# Add development dependencies
poetry add --group dev <package-name>

# Update dependencies
poetry update

# Show dependency tree
poetry show --tree
```

## Environment Variables

Key environment variables (see `.env.example` for complete list):

- `DATABASE_URL`: PostgreSQL connection string
- `SECRET_KEY`: JWT signing key
- `DEBUG`: Enable debug mode
- `BACKEND_CORS_ORIGINS`: Allowed CORS origins

## Testing

The project includes comprehensive tests covering:

- API endpoints
- Authentication flows
- Database operations
- Error handling

Run tests with:
```bash
pytest -v
```

## Deployment

### Production with Docker

```bash
# Build production image
docker build --target production -t energyexe-backend .

# Run production container
docker run -p 8000:8000 energyexe-backend
```

## Contributing

1. Install Poetry and dependencies: `poetry install --with dev,test`
2. Install pre-commit hooks: `poetry run pre-commit install`
3. Create feature branch: `git checkout -b feature/your-feature`
4. Make changes and ensure tests pass: `poetry run pytest`
5. Format code: `poetry run black . && poetry run isort .`
6. Submit pull request

## License

This project is licensed under the MIT License. 