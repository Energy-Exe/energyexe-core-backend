"""Explicit local configuration, with no dotenv or ambient credential lookup."""

import os
from dataclasses import dataclass
from typing import Mapping

from sqlalchemy.engine import make_url


@dataclass(frozen=True)
class PreviewSettings:
    database_url: str
    host: str = "127.0.0.1"
    port: int = 8012

    def __post_init__(self):
        url = make_url(self.database_url)
        if (
            url.drivername != "postgresql+asyncpg"
            or url.host not in {"127.0.0.1", "::1"}
            or url.database != "energyexe_sfe_preview"
            or url.query
        ):
            raise ValueError(
                "Preview requires a loopback energyexe_sfe_preview PostgreSQL database"
            )
        if self.host not in {"127.0.0.1", "::1"}:
            raise ValueError("Preview must bind to a loopback address")
        if not 1024 <= self.port <= 65535:
            raise ValueError("Invalid preview port")

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None):
        env = os.environ if env is None else env
        blocked = {
            key
            for key in env
            if key.startswith(("AWS_", "ANTHROPIC_", "OPENAI_", "PG", "SMTP_", "RESEND_"))
            or key in {"DATABASE_URL", "SECRET_KEY", "SENTRY_DSN", "REDIS_URL"}
        }
        if blocked:
            raise ValueError(
                "Preview refuses inherited external configuration: " + ", ".join(sorted(blocked))
            )
        if "SFE_PREVIEW_DATABASE_URL" not in env:
            raise ValueError("Set SFE_PREVIEW_DATABASE_URL explicitly; no .env is loaded")
        return cls(
            database_url=env["SFE_PREVIEW_DATABASE_URL"],
            host=env.get("SFE_PREVIEW_HOST", "127.0.0.1"),
            port=int(env.get("SFE_PREVIEW_PORT", "8012")),
        )
