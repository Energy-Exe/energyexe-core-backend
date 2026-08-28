"""Pytest configuration and fixtures."""

import asyncio
import os

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.core.database import Base
from app.core.database import get_db as database_get_db
from app.core.deps import get_db
from app.main import create_application
from app.models.audit_log import AuditLog
from app.models.invitation import Invitation

# Import only auth-related models to avoid JSONB issues with SQLite
# These models don't use JSONB columns and are safe for SQLite testing
from app.models.user import User
from app.models.user_consent import UserConsent
from app.models.user_feature import UserFeature
from app.services.audit_log import set_audit_session_factory

# Force testing environment
os.environ["TESTING"] = "true"

# Use SQLite for testing
TEST_DATABASE_URL = "sqlite+aiosqlite:///:memory:"

# Define tables that are safe for SQLite (no JSONB columns)
# We'll only create these tables for testing
AUTH_TEST_TABLES = [
    User.__table__,
    AuditLog.__table__,
    Invitation.__table__,
    UserFeature.__table__,
    UserConsent.__table__,
]


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()


@pytest.fixture(scope="function")
async def test_engine():
    """Create a test engine for each test function."""
    engine = create_async_engine(
        TEST_DATABASE_URL,
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )

    # Create only auth-related tables (avoid JSONB-using tables)
    async with engine.begin() as connection:
        for table in AUTH_TEST_TABLES:
            await connection.run_sync(table.create, checkfirst=True)

    # Audit rows are written through their own session (EPR-124); point that
    # writer at the test engine so they land in (and are visible from) the test DB.
    set_audit_session_factory(
        async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
    )

    yield engine

    set_audit_session_factory(None)

    # Drop only the tables we created
    async with engine.begin() as connection:
        for table in reversed(AUTH_TEST_TABLES):
            await connection.run_sync(table.drop, checkfirst=True)
    await engine.dispose()


@pytest.fixture(scope="function")
async def test_session(test_engine):
    """Create a test database session."""
    session_factory = async_sessionmaker(
        bind=test_engine, class_=AsyncSession, expire_on_commit=False
    )

    async with session_factory() as session:
        yield session


@pytest.fixture(scope="function")
def client(test_session, event_loop):
    """Create a test client with database dependency override."""
    app = create_application()

    # Override get_db dependency to return our test session
    async def override_get_db():
        yield test_session

    # Two identical get_db functions exist (app.core.deps and app.core.database) and
    # endpoints import either one; override both or those routes hit the real engine.
    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[database_get_db] = override_get_db

    with TestClient(app) as test_client:
        yield test_client

    # Clear overrides
    app.dependency_overrides.clear()


@pytest.fixture
def user_data():
    """Sample user data for testing."""
    return {
        "email": "test@example.com",
        "username": "testuser",
        "password": "testpassword123",
        "first_name": "Test",
        "last_name": "User",
        "is_active": True,
    }
