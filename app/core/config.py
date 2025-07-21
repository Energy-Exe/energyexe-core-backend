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
    PORT: int = 8000
    RELOAD: bool = False

    # Security
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
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
