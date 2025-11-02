"""Application configuration settings."""

import os
import secrets
from functools import lru_cache
from typing import Any, Dict, List, Optional, Union

from pydantic import AnyHttpUrl, PostgresDsn, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_ignore_empty=True,
        extra="ignore",
    )

    # Basic settings
    PROJECT_NAME: str = "EnergyExe Core Backend"
    DEBUG: bool = False
    API_V1_STR: str = "/api/v1"
    SECRET_KEY: str = secrets.token_urlsafe(32)

    # Server settings
    HOST: str = "0.0.0.0"
    PORT: int = 8001
    RELOAD: bool = False

    # Security
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 14400  # 10 days
    ALLOWED_HOSTS: List[str] = ["*"]

    # CORS
    BACKEND_CORS_ORIGINS: List[str] = [
        "https://dashboard.energyexe.com",
        "http://localhost:5173",  # Vite dev server
        "http://localhost:3000",  # Alternative dev server
    ]

    @field_validator("BACKEND_CORS_ORIGINS", mode="before")
    @classmethod
    def assemble_cors_origins(cls, v: Union[str, List[str]]) -> List[str]:
        """Parse CORS origins."""
        if isinstance(v, str):
            if not v.startswith("["):
                return [i.strip() for i in v.split(",")]
            else:
                # Handle JSON array string format
                import json

                try:
                    return json.loads(v)
                except json.JSONDecodeError:
                    raise ValueError(f"Invalid JSON format for CORS origins: {v}")
        elif isinstance(v, list):
            return v
        raise ValueError(f"CORS origins must be string or list, got {type(v)}")

    # Database
    DATABASE_URL: Optional[PostgresDsn] = None
    DB_POOL_SIZE: int = 5
    DB_MAX_OVERFLOW: int = 10
    DB_ECHO: bool = False

    # Database connection health settings (for cloud PostgreSQL)
    DB_POOL_PRE_PING: bool = True  # Test connections before use
    DB_POOL_RECYCLE: int = 300  # Recycle connections after 5 minutes
    DB_POOL_TIMEOUT: int = 30  # Wait max 30s for connection from pool
    DB_COMMAND_TIMEOUT: int = 60  # Query timeout: 60 seconds

    # Redis (optional)
    REDIS_URL: Optional[str] = None

    # Email settings (optional)
    SMTP_TLS: bool = True
    SMTP_PORT: Optional[int] = None
    SMTP_HOST: Optional[str] = None
    SMTP_USER: Optional[str] = None
    SMTP_PASSWORD: Optional[str] = None
    EMAILS_FROM_EMAIL: Optional[str] = None
    EMAILS_FROM_NAME: Optional[str] = None

    # Testing
    TESTING: bool = os.getenv("TESTING", "false").lower() == "true"

    # Logging
    LOG_LEVEL: str = "INFO"

    # ENTSOE Integration
    ENTSOE_API_KEY: str = "3b00489d-a886-48a4-95ad-981da57f7b62"
    ENTSOE_DEFAULT_AREAS: str = "DE_LU,FR,ES,GB,NL,BE"
    ENTSOE_FETCH_BATCH_DAYS: int = 7
    ENTSOE_RATE_LIMIT_REQUESTS: int = 100
    ENTSOE_RATE_LIMIT_PERIOD: int = 60

    # Elexon Integration
    ELEXON_API_KEY: str = "ytitiohgylom033"

    # EIA Integration
    EIA_API_KEY: str = "bLXfqlf12SKY6t6kIz03IKGgoTfTBxr9pOLKiZeZ"
    
    # Valkey/Redis settings for Celery
    VALKEY_PUBLIC_HOST: str = "valkey-production-515f.up.railway.app"
    VALKEY_PUBLIC_PORT: str = "6379"
    VALKEY_PASSWORD: str = "roKX3R37u09uQhjf~YjWnScP11nrdU7p"
    VALKEY_USER: str = "default"
    
    # Celery settings
    CELERY_BROKER_URL: Optional[str] = None
    CELERY_RESULT_BACKEND: Optional[str] = None

    @property
    def database_url_sync(self) -> str:
        """Get synchronous database URL for Alembic."""
        # Use SQLite for testing
        if self.TESTING:
            return "sqlite:///./test.db"

        if not self.DATABASE_URL:
            return "postgresql://postgres:RwaN9FJDCgP2AhuALxZ4Wa7QfvbKXQ647AAickORJ0rq5N6lUG19UneFJJTJ9Jnv@146.235.201.245:5432/energyexe_db"

        url = str(self.DATABASE_URL)
        if url.startswith("postgresql+asyncpg://"):
            return url.replace("postgresql+asyncpg://", "postgresql://")
        return url

    @property
    def database_url_async(self) -> str:
        """Get asynchronous database URL for SQLAlchemy."""
        # Use SQLite for testing
        if self.TESTING:
            return "sqlite+aiosqlite:///:memory:"

        if not self.DATABASE_URL:
            return "postgresql+asyncpg://postgres:RwaN9FJDCgP2AhuALxZ4Wa7QfvbKXQ647AAickORJ0rq5N6lUG19UneFJJTJ9Jnv@146.235.201.245:5432/energyexe_db"

        url = str(self.DATABASE_URL)
        if url.startswith("postgresql://") and not url.startswith("postgresql+asyncpg://"):
            return url.replace("postgresql://", "postgresql+asyncpg://")
        return url


def get_settings() -> Settings:
    """Get application settings."""
    return Settings()
