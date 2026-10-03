# EnergyExe Core Backend - Technical Architecture Documentation

This document explains the technical architecture, design decisions, and implementation details of the EnergyExe Core Backend FastAPI project.

## Table of Contents

1. [Project Overview](#project-overview)
2. [Architecture Design](#architecture-design)
3. [Core Components](#core-components)
4. [Database Layer](#database-layer)
5. [API Layer](#api-layer)
6. [Security Implementation](#security-implementation)
7. [Testing Strategy](#testing-strategy)
8. [Development & Deployment](#development--deployment)
9. [Design Patterns & Best Practices](#design-patterns--best-practices)

## Project Overview

The EnergyExe Core Backend is built using modern Python web development practices with FastAPI as the core framework. The architecture follows a layered approach with clear separation of concerns, emphasizing maintainability, scalability, and developer experience.

### Technology Stack

- **Framework**: FastAPI (Python 3.11)
- **Dependency Management**: Poetry for local development (`pyproject.toml` / `poetry.lock`); the Docker image installs from `requirements.txt` — keep both in step
- **Database**: PostgreSQL (RDS) with SQLAlchemy 2.0 async ORM + asyncpg; Alembic migrations
- **Cache / rate limit**: Valkey (ElastiCache Serverless) via `app/core/redis.py`
- **Authentication**: JWT tokens with bcrypt password hashing
- **Validation**: Pydantic v2 for request/response validation
- **Testing**: pytest with async support (SQLite in-memory for unit tests)
- **Logging / errors**: structlog with request ids; Sentry SDK pointed at self-hosted GlitchTip
- **Containerization / hosting**: Docker multi-stage build (`pip install -r requirements.txt`), AWS Fargate behind an ALB, Terraform in `infra/`
- **Scheduling**: AWS EventBridge (Lambda-triggered imports; ECS run-task for the nightly weather and pipeline jobs) — no in-process scheduler

## Architecture Design

### Layered Architecture

```
┌─────────────────┐
│   API Layer    │  ← FastAPI routes, request/response handling
├─────────────────┤
│  Service Layer  │  ← Business logic, data validation
├─────────────────┤
│   Model Layer   │  ← Database models, ORM
├─────────────────┤
│ Database Layer  │  ← PostgreSQL, connection management
└─────────────────┘
```

### Directory Structure Reasoning

```
app/
├── api/v1/                 # versioned API; endpoints/ (one module per resource) + router.py
├── core/                   # config, database, deps (auth dependencies), middleware, exceptions,
│                           #   observability (GlitchTip), redis (cache + rate limit), audit, security
├── cron/                   # job bodies for EventBridge-run ECS tasks (pipeline_daily.py); no scheduler here
├── models/                 # SQLAlchemy 2.0 ORM models
├── prompts/                # brain-agent system prompts (+ client variant)
│   └── reports/            # <report_type>/<section>.txt narrative prompts for the reports platform
├── scada_preview/          # isolated local-only preview API (own FastAPI app, own settings)
├── schemas/                # Pydantic request/response schemas
├── services/               # business logic, one service per domain
│   ├── opportunity_schemas/  # one detector module per opportunity schema + registry/orchestrator
│   └── reports/            # reports platform: registry, orchestrator, data_builders/, pdf/, narratives
├── templates/email/        # Jinja2 templates for Resend transactional email
├── utils/                  # shared helpers (date bounds, ramp-up, unit resolver)
└── main.py                 # app factory, lifespan (startup sweepers), middleware, /health
```

(`find app -maxdepth 2 -type d` is the source of truth for this tree.)

**Why this structure?**
- **Separation of Concerns**: Each layer has a single responsibility
- **Testability**: Easy to mock and test individual components
- **Scalability**: New features can be added without affecting existing code
- **Maintainability**: Clear organization makes code easy to navigate

## Core Components

### 1. Application Configuration (`app/core/config.py`)

```python
class Settings(BaseSettings):
    PROJECT_NAME: str = "EnergyExe Core Backend"
    DEBUG: bool = False
    SECRET_KEY: str = secrets.token_urlsafe(32)
    DATABASE_URL: Optional[PostgresDsn] = None
    # ... other settings
```

**Design Decisions:**
- **Pydantic Settings**: Type-safe configuration with validation
- **Environment Variables**: 12-factor app compliance
- **Cached Settings**: `@lru_cache()` for performance
- **Separate URLs**: Different database URLs for sync/async operations

### 2. Database Configuration (`app/core/database.py`)

```python
# Async engine for high-performance database operations
engine = create_async_engine(
    settings.database_url_async,
    echo=settings.DB_ECHO,
    pool_size=settings.DB_POOL_SIZE,
    max_overflow=settings.DB_MAX_OVERFLOW,
    future=True,
)

# Session factory with proper resource management
async_session_factory = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)
```

**Why Async Database?**
- **Performance**: Non-blocking I/O operations
- **Scalability**: Handle more concurrent requests
- **Modern SQLAlchemy**: Takes advantage of SQLAlchemy 2.0 features

### 3. Exception Handling (`app/core/exceptions.py`)

```python
class BaseCustomException(Exception):
    def __init__(self, message: str, status_code: int = status.HTTP_400_BAD_REQUEST):
        self.message = message
        self.status_code = status_code
        super().__init__(self.message)

async def custom_exception_handler(request: Request, exc: BaseCustomException):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": {
                "type": type(exc).__name__,
                "message": exc.message,
                "request_id": getattr(request.state, "request_id", "unknown"),
            }
        },
    )
```

**Exception Strategy Benefits:**
- **Consistent Error Format**: All errors follow the same structure
- **Request Tracking**: Each error includes a unique request ID
- **Type Safety**: Custom exceptions with specific status codes
- **Debugging**: Structured error information for easy troubleshooting

### 4. Middleware (`app/core/middleware.py`)

```python
class LoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        request_id = str(uuid.uuid4())
        start_time = time.time()
        
        # Add request ID for tracing
        request.state.request_id = request_id
        
        # Log request details
        logger.info("Request started", request_id=request_id, ...)
        
        response = await call_next(request)
        
        # Add performance metrics
        process_time = time.time() - start_time
        response.headers["X-Process-Time"] = str(round(process_time, 4))
        
        return response
```

**Middleware Benefits:**
- **Request Tracing**: Unique ID for each request
- **Performance Monitoring**: Track response times
- **Security Headers**: Automatic security header injection
- **Structured Logging**: Consistent log format across the application

## Database Layer

### Model Design (`app/models/user.py`)

```python
class User(Base):
    __tablename__ = "users"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    email: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    username: Mapped[str] = mapped_column(String(100), unique=True, index=True)
    hashed_password: Mapped[str] = mapped_column(String(255), nullable=False)
    # ... other fields
    
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )
```

**Model Design Principles:**
- **Type Hints**: Full typing with SQLAlchemy 2.0 `Mapped` types
- **Constraints**: Database-level constraints for data integrity
- **Indexing**: Strategic indexes for query performance
- **Audit Fields**: Automatic timestamps for change tracking

### Migration Strategy (Alembic)

```python
# alembic/env.py - Async migration support
async def run_async_migrations():
    connectable = async_engine_from_config(configuration, prefix="sqlalchemy.")
    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)
```

**Migration Benefits:**
- **Version Control**: Database schema changes tracked in git
- **Async Support**: Compatible with async database operations
- **Rollback Capability**: Safe schema changes with rollback support
- **Team Collaboration**: Consistent database state across environments

## API Layer

### Schema Design (`app/schemas/user.py`)

```python
class UserBase(BaseModel):
    email: EmailStr
    username: str = Field(..., min_length=3, max_length=100)
    first_name: Optional[str] = Field(None, max_length=100)

class UserCreate(UserBase):
    password: str = Field(..., min_length=8, max_length=100)

class UserResponse(UserBase):
    id: int
    created_at: datetime
    
    class Config:
        from_attributes = True  # Pydantic v2 compatibility
```

**Schema Strategy:**
- **Inheritance**: Base schemas for code reuse
- **Validation**: Field-level validation with clear error messages
- **Security**: Password fields only in input schemas
- **API Versioning**: Separate schemas allow API evolution

### Service Layer (`app/services/user.py`)

```python
class UserService:
    def __init__(self, db: AsyncSession):
        self.db = db
    
    async def create(self, user_data: UserCreate) -> User:
        # Validation logic
        existing_user = await self.get_by_email(user_data.email)
        if existing_user:
            raise ValidationException("User with this email already exists")
        
        # Business logic
        hashed_password = get_password_hash(user_data.password)
        user = User(email=user_data.email, hashed_password=hashed_password, ...)
        
        # Database operations
        self.db.add(user)
        await self.db.commit()
        await self.db.refresh(user)
        
        return user
```

**Service Layer Benefits:**
- **Business Logic Isolation**: Core logic separated from API concerns
- **Reusability**: Services can be used by different API endpoints
- **Testing**: Easy to unit test business logic
- **Transaction Management**: Proper database transaction handling

### API Endpoints (`app/api/v1/endpoints/auth.py`)

```python
@router.post("/register", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
async def register(user_data: UserCreate, db: AsyncSession = Depends(get_db)):
    user_service = UserService(db)
    try:
        user = await user_service.create(user_data)
        logger.info("User registered", user_id=user.id, username=user.username)
        return user
    except ValidationException as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=e.message)
```

**API Design Principles:**
- **Dependency Injection**: Clean separation using FastAPI's DI system
- **HTTP Status Codes**: Proper REST API status codes
- **Error Handling**: Consistent error responses
- **Logging**: Structured logging for API operations

## Security Implementation

### Authentication Flow

```python
# JWT Token Creation
def create_access_token(subject: Union[str, Any], expires_delta: timedelta = None) -> str:
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode = {"exp": expire, "sub": str(subject)}
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

# Password Hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def get_password_hash(password: str) -> str:
    return pwd_context.hash(password)
```

### Dependency Injection for Auth

```python
async def get_current_user(
    db: AsyncSession = Depends(get_db),
    credentials: HTTPAuthorizationCredentials = Depends(security),
) -> User:
    token = credentials.credentials
    username = verify_token(token)
    
    if username is None:
        raise AuthenticationException("Could not validate credentials")
    
    user = await UserService(db).get_by_username(username)
    if not user or not user.is_active:
        raise AuthenticationException("User not found or inactive")
    
    return user
```

**Security Features:**
- **JWT Tokens**: Stateless authentication
- **Password Hashing**: bcrypt for secure password storage
- **Token Expiration**: Configurable token lifetime
- **User Status**: Active/inactive user support
- **Role-based Access**: Superuser privileges

## Testing Strategy

### Test Configuration (`tests/conftest.py`)

```python
# In-memory database for fast tests
TEST_DATABASE_URL = "sqlite+aiosqlite:///:memory:"

@pytest_asyncio.fixture
async def db_session():
    # Create fresh database for each test
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    async with TestSessionLocal() as session:
        yield session
    
    # Clean up after test
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
```

### Test Examples (`tests/test_auth.py`)

```python
def test_register_user(client: TestClient, user_data):
    response = client.post("/api/v1/auth/register", json=user_data)
    assert response.status_code == 201
    data = response.json()
    assert data["email"] == user_data["email"]
    assert "hashed_password" not in data  # Security check
```

**Testing Benefits:**
- **Isolation**: Each test uses a fresh database
- **Fast Execution**: In-memory SQLite for speed
- **Async Support**: Native async test support
- **Comprehensive Coverage**: API, service, and model testing

## Dependency Management

Two dependency manifests exist and both must be kept in step:

- **`pyproject.toml` + `poetry.lock`** — what developers install locally (`poetry install --with dev,test`) and what `make`, `scripts/start.py` and the test commands assume.
- **`requirements.txt`** — what the **Docker image** installs (`pip install -r requirements.txt` in the `Dockerfile`). This is the only manifest that reaches staging and production. A package added only with `poetry add` will import fine on a laptop and fail on Fargate.

### Why Poetry locally?

Poetry provides several advantages over traditional pip-based dependency management:

```toml
[tool.poetry.dependencies]
python = "^3.11"
fastapi = {extras = ["all"], version = "^0.104.0"}
sqlalchemy = "^2.0.0"

[tool.poetry.group.dev.dependencies]
pytest = "^7.4.0"
black = "^23.0.0"
mypy = "^1.7.0"
```

**Poetry Benefits:**
- **Dependency Resolution**: Automatic conflict resolution
- **Lock File**: `poetry.lock` ensures reproducible builds
- **Virtual Environments**: Automatic virtual environment management
- **Semantic Versioning**: Caret constraints for safe updates
- **Grouped Dependencies**: Separate dev, test, and production dependencies

### Key Poetry Commands

```bash
# Install all dependencies
poetry install --with dev,test

# Add new dependency
poetry add requests

# Add development dependency
poetry add --group dev pytest-mock

# Update dependencies
poetry update

# Show dependency tree
poetry show --tree

# Run commands in virtual environment
poetry run python script.py
poetry run pytest
```

### Dependency Groups

The project uses Poetry's dependency groups for organization:

- **Main dependencies**: Core application requirements
- **Dev dependencies**: Development tools (linting, formatting)
- **Test dependencies**: Testing-specific packages

## Development & Deployment

### Docker Strategy

The `Dockerfile` is a `python:3.11-slim` multi-stage build **without Poetry**: both stages run
`pip install -r requirements.txt`, copy the source, and run as a non-root `app` user on port
**8001**.

- `development` target: `uvicorn app.main:app --host 0.0.0.0 --port 8001 --reload` (used by `docker-compose.yml`).
- `production` target: `uvicorn ... --port 8001 --workers 4`, a `/health` HEALTHCHECK, and
  `ARG GIT_SHA` → `ENV SENTRY_RELEASE` so every GlitchTip event carries the commit it came from
  (CI passes `--build-arg GIT_SHA=$GITHUB_SHA`). The ECS task definition (`infra/ecs.tf`)
  overrides the CMD to run `alembic upgrade head` first and `--workers 1`.

### Docker Compose for Development

`docker-compose.yml` runs three services: `postgres` (15-alpine), `valkey` (7-alpine) and `api`
(the `development` image target, published on `localhost:8001`). It is a convenience for a
self-contained local stack; most day-to-day development runs the API directly with Poetry on
port 8002 (see `README.md`, "Port").

### Deployment

Staging-first, driven by GitHub Actions with OIDC (no stored AWS keys):

1. Push to `staging` → `.github/workflows/deploy-staging.yml` builds the image, pushes to the
   staging ECR repo and redeploys the staging Fargate service.
2. Merge `staging` → `master` → `deploy-aws.yml` **promotes** the exact staging image to the prod
   ECR repo and redeploys prod. No rebuild, so prod runs what staging tested.

Infrastructure is Terraform in `infra/` (prod) and `infra/staging/`; see `infra/README.md`.
Recurring work is scheduled by EventBridge, documented in `docs/operations/scheduled-jobs.md`.

### Code Quality Tools

```yaml
# .pre-commit-config.yaml
repos:
  - repo: https://github.com/psf/black
    hooks:
      - id: black
        args: [--line-length=100]
  
  - repo: https://github.com/pycqa/isort
    hooks:
      - id: isort
        args: [--profile=black]
```

**Quality Assurance:**
- **Automated Formatting**: Consistent code style
- **Pre-commit Hooks**: Catch issues before commit
- **Type Checking**: Static analysis with mypy
- **Linting**: Code quality checks with flake8

## Design Patterns & Best Practices

### 1. Dependency Injection Pattern

FastAPI's dependency injection system is used throughout:

```python
# Database session injection
async def get_db() -> AsyncSession:
    async with async_session_factory() as session:
        yield session

# Authentication dependency
async def get_current_user(db: AsyncSession = Depends(get_db)) -> User:
    # Authentication logic
```

### 2. Repository/Service Pattern

Business logic is encapsulated in service classes:

```python
class UserService:
    def __init__(self, db: AsyncSession):
        self.db = db
    
    async def create(self, user_data: UserCreate) -> User:
        # Business logic here
```

### 3. Factory Pattern

Session and engine factories for clean resource management:

```python
async_session_factory = async_sessionmaker(engine, class_=AsyncSession)
```

### 4. Middleware Pattern

Cross-cutting concerns handled by middleware:

```python
app.add_middleware(LoggingMiddleware)
app.add_middleware(SecurityHeadersMiddleware)
```

## Performance Considerations

### 1. Async Operations
- All database operations are async
- Non-blocking I/O for better concurrency
- Proper connection pooling

### 2. Database Optimization
- Strategic indexing on frequently queried fields
- Connection pooling configuration
- Query optimization with SQLAlchemy 2.0

### 3. Caching Strategy
- Settings cached with `@lru_cache()`
- Redis integration ready for session/data caching

### 4. Request Processing
- Structured logging for performance monitoring
- Request timing middleware
- Efficient JSON serialization with Pydantic

## Scalability Features

### 1. Horizontal Scaling
- Stateless authentication with JWT
- Database connection pooling
- Containerized deployment

### 2. API Versioning
- Version-specific routes (`/api/v1/`)
- Backward compatibility support
- Schema evolution strategy

### 3. Configuration Management
- Environment-based configuration
- 12-factor app compliance
- Secret management ready

## Monitoring & Observability

### 1. Structured Logging
```python
logger.info("User created", user_id=user.id, username=user.username)
```

### 2. Request Tracing
- `LoggingMiddleware` (`app/core/middleware.py`) assigns a unique `request_id` per request and
  binds it into every structlog line; `SecurityHeadersMiddleware` adds the standard headers.

### 3. Health Checks
`GET /health` in `app/main.py` — used by the ALB target group and the Dockerfile HEALTHCHECK.

### 4. Error tracking and cron monitors
`app/core/observability.py` initialises the Sentry SDK against self-hosted GlitchTip when
`SENTRY_DSN` is set (no-op otherwise) and exposes `cron_checkin()` for the scheduled tasks.
See `docs/operations/observability.md`.

This architecture provides a solid foundation for building scalable, maintainable web applications with modern Python practices. The modular design allows for easy extension and modification as requirements evolve. 