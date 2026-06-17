"""Application configuration settings."""

import os
import secrets
from typing import Any, Dict, List, Optional, Union

from pydantic import PostgresDsn, field_validator
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
        "https://app.energyexe.com",
        "http://localhost:5173",  # Vite dev server
        "http://localhost:3000",  # Alternative dev server
        "http://localhost:3005",  # Admin UI dev
        "http://localhost:3006",  # Client UI dev
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
    DB_COMMAND_TIMEOUT: int = (
        180  # Query timeout: 3 minutes (large analytics queries on big zones can run 60–120s)
    )

    # Wall-clock bound on the per-windfarm peer-aggregate refresh in the
    # pipeline. Peer-agg is best-effort (it updates zone/country averages for
    # the vs-zone API) and recomputes the whole group across all peers per
    # metric/year — pathologically slow on big zones (GB ~200 combos). Without
    # a bound, a slow or connection-dropped refresh froze the pipeline for
    # ~80 min and blocked every subsequent windfarm. On timeout we log and move
    # on; the nightly run re-attempts. 0 disables the bound.
    PIPELINE_PEER_AGG_TIMEOUT_S: int = 120

    # Redis (optional)
    REDIS_URL: Optional[str] = None

    # Email settings (Resend)
    RESEND_API_KEY: Optional[str] = None  # Set via environment variable
    EMAILS_FROM_EMAIL: str = "noreply@energyexe.com"
    EMAILS_FROM_NAME: str = "EnergyExe"

    # Client Portal settings
    CLIENT_PORTAL_URL: str = "http://localhost:3000"
    ADMIN_PORTAL_URL: str = "http://localhost:3005"
    EMAIL_VERIFICATION_EXPIRE_HOURS: int = 24
    PASSWORD_RESET_EXPIRE_HOURS: int = 1
    INVITATION_EXPIRE_DAYS: int = 7
    SUPPORT_EMAIL: str = "hello@energyexe.com"

    # Legal document versions — bumping either triggers re-acceptance on next
    # login. Must match the corresponding constants in the client portal
    # (energyexe-client-ui/src/lib/legal-versions.ts) and the bundled markdown.
    TERMS_VERSION: str = "2026-05-13"
    PRIVACY_VERSION: str = "2026-05-13"

    # Testing
    TESTING: bool = os.getenv("TESTING", "false").lower() == "true"

    # Logging
    LOG_LEVEL: str = "INFO"

    # Error tracking (GlitchTip — Sentry-API-compatible). Empty DSN disables it,
    # so local dev and tests run without a tracker. See app/core/observability.py.
    SENTRY_DSN: Optional[str] = None
    SENTRY_ENVIRONMENT: str = "production"
    SENTRY_TRACES_SAMPLE_RATE: float = 0.0

    # ENTSOE Integration
    ENTSOE_API_KEY: str = ""  # Set via environment variable
    ENTSOE_DEFAULT_AREAS: str = "DE_LU,FR,ES,GB,NL,BE"
    ENTSOE_FETCH_BATCH_DAYS: int = 7
    ENTSOE_RATE_LIMIT_REQUESTS: int = 100
    ENTSOE_RATE_LIMIT_PERIOD: int = 60

    # Elexon Integration
    ELEXON_API_KEY: str = ""  # Set via environment variable

    # EIA Integration
    EIA_API_KEY: str = ""  # Set via environment variable

    # CDS API (Copernicus Climate Data Store) for ERA5 weather data
    CDSAPI_URL: str = "https://cds.climate.copernicus.eu/api"
    CDSAPI_KEY: str = ""  # Set via environment variable

    # LLM / AI Commentary Generation
    ANTHROPIC_API_KEY: Optional[str] = None  # Claude API key
    OPENAI_API_KEY: Optional[str] = None  # OpenAI API key
    LLM_PROVIDER: str = "openai"  # claude, openai
    LLM_MODEL: str = "gpt-4o"  # claude-3-5-sonnet-20241022, gpt-4o, gpt-4-turbo, gpt-5, etc.
    LLM_CACHE_DURATION_HOURS: int = 24
    LLM_MAX_COST_PER_REPORT: float = 0.50  # USD

    # Brain Agent default model (used as fallback when caller omits one)
    BRAIN_MODEL: str = "claude-sonnet-4-6"

    # Brain Agent — Postgres read-only role.
    # When the password is set, the agent's bash env uses these credentials so
    # every DB connection it spawns is grant-restricted to SELECT only. See
    # alembic migration a1b2c3d4e5f6_add_brain_agent_ro_role.py.
    BRAIN_AGENT_RO_USER: str = "brain_agent_ro"
    BRAIN_AGENT_RO_PASSWORD: str = ""

    # Brain Agent — source code access for codebase exploration
    CODE_REPOS_DIR: str = "/tmp/energyexe-repos"
    GITHUB_TOKEN: str = ""  # PAT for cloning private repos at startup

    # AWS S3 for brain agent image persistence
    S3_BUCKET_NAME: str = ""  # Empty = S3 disabled (local dev)
    S3_REGION: str = "eu-north-1"  # Same region as RDS

    # Valkey/Redis settings for Celery
    VALKEY_PUBLIC_HOST: str = ""  # Set via environment variable
    VALKEY_PUBLIC_PORT: str = "6379"
    VALKEY_PASSWORD: str = ""  # Set via environment variable
    VALKEY_USER: str = "default"

    # Celery settings
    CELERY_BROKER_URL: Optional[str] = None
    CELERY_RESULT_BACKEND: Optional[str] = None

    @property
    def s3_enabled(self) -> bool:
        """Check if S3 image storage is configured."""
        return bool(self.S3_BUCKET_NAME)

    @property
    def database_url_sync(self) -> str:
        """Get synchronous database URL for Alembic."""
        # Use SQLite for testing
        if self.TESTING:
            return "sqlite:///./test.db"

        if not self.DATABASE_URL:
            raise ValueError("DATABASE_URL environment variable is required")

        url = str(self.DATABASE_URL)
        if url.startswith("postgresql+asyncpg://"):
            return url.replace("postgresql+asyncpg://", "postgresql://")
        return url

    @property
    def database_url_agent_ro(self) -> Optional[str]:
        """Sync Postgres URL using the brain-agent read-only role.

        Returns ``None`` when ``BRAIN_AGENT_RO_PASSWORD`` is not configured —
        the caller falls back to the regular URL with a session-level read-only
        guard. Once the password is set, the agent always connects with
        ``BRAIN_AGENT_RO_USER`` so the database itself enforces SELECT-only.
        """
        if not self.BRAIN_AGENT_RO_PASSWORD:
            return None
        if not self.DATABASE_URL:
            return None

        from urllib.parse import quote, urlparse, urlunparse

        base = self.database_url_sync  # already in sync form
        parsed = urlparse(base)
        host = parsed.hostname or ""
        port = f":{parsed.port}" if parsed.port else ""
        user = quote(self.BRAIN_AGENT_RO_USER, safe="")
        pw = quote(self.BRAIN_AGENT_RO_PASSWORD, safe="")
        netloc = f"{user}:{pw}@{host}{port}"
        return urlunparse(parsed._replace(netloc=netloc))

    @property
    def database_url_async(self) -> str:
        """Get asynchronous database URL for SQLAlchemy."""
        # Use SQLite for testing
        if self.TESTING:
            return "sqlite+aiosqlite:///:memory:"

        if not self.DATABASE_URL:
            raise ValueError("DATABASE_URL environment variable is required")

        url = str(self.DATABASE_URL)
        if url.startswith("postgresql://") and not url.startswith("postgresql+asyncpg://"):
            return url.replace("postgresql://", "postgresql+asyncpg://")
        return url


def get_settings() -> Settings:
    """Get application settings."""
    return Settings()
