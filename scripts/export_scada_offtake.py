#!/usr/bin/env python3
"""Export an owner's private SCADA terms and platform FX for offline preparation."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import os
import subprocess
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from sqlalchemy import text
from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import ArgumentError
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.pool import NullPool

from app.services.scada_offtake_export import ExportRequest, ScadaOfftakeExporter, empty_snapshot
from app.services.scada_private_files import private_output_root, write_private_json


def explicit_source(value: str | None) -> tuple[URL, dict]:
    # Intentionally never reads DATABASE_URL, app settings or any .env file.
    if not value or not value.strip():
        raise ValueError(
            "Set SCADA_OFFTAKE_SOURCE_DATABASE_URL or --source-database-url explicitly"
        )
    try:
        url = make_url(value)
    except (ArgumentError, ValueError) as error:
        raise ValueError("Invalid explicit PostgreSQL connection") from error
    if (
        url.drivername not in ("postgresql", "postgresql+asyncpg")
        or not url.database
        or not url.host
    ):
        raise ValueError("An explicit PostgreSQL host and database are required")
    url = url.set(drivername="postgresql+asyncpg")
    # libpq URLs often spell this sslmode; asyncpg accepts the same values as ssl.
    if "sslmode" in url.query:
        query = dict(url.query)
        if "ssl" in query and query["ssl"] != query["sslmode"]:
            raise ValueError("Conflicting PostgreSQL SSL settings")
        query["ssl"] = query.pop("sslmode")
        url = url.set(query=query)
    # Credentials and connection query options never enter an export or an error message.
    identity = f"postgresql://{url.host}:{url.port or 5432}/{url.database}"
    return url, {"connection_sha256": hashlib.sha256(identity.encode()).hexdigest()}


def source_version() -> dict:
    files = [
        Path(__file__).resolve(),
        REPO_ROOT / "app/services/scada_offtake_export.py",
        REPO_ROOT / "app/services/scada_private_files.py",
        REPO_ROOT / "app/services/scada_ppa_service.py",
        REPO_ROOT / "app/services/exchange_rate_service.py",
        REPO_ROOT / "app/services/financial_data_service.py",
    ]
    try:
        revision = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, capture_output=True, text=True, check=True
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        revision = "unavailable"
    return {
        "backend_revision": revision,
        "exporter_version": "1",
        "file_sha256": {
            str(path.relative_to(REPO_ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in files
        },
    }


async def export_snapshot(request: ExportRequest, source_url: URL, source: dict) -> dict:
    engine = create_async_engine(
        source_url,
        poolclass=NullPool,
        echo=False,
        isolation_level="REPEATABLE READ",
        connect_args={
            "server_settings": {
                "default_transaction_read_only": "on",
                "application_name": "scada-private-offtake-export",
            },
            "command_timeout": 120,
        },
    )
    try:
        async with AsyncSession(engine, expire_on_commit=False) as db:
            async with db.begin():
                await db.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
                await db.execute(text("SET LOCAL search_path TO public"))
                result = await db.execute(
                    text(
                        """
                    SELECT current_setting('transaction_read_only') AS read_only,
                           current_setting('transaction_isolation') AS isolation,
                           pg_current_snapshot()::text AS snapshot,
                           transaction_timestamp()::text AS started_at
                """
                    )
                )
                transaction = dict(result.mappings().one())
                if (
                    transaction["read_only"] != "on"
                    or transaction["isolation"] != "repeatable read"
                ):
                    raise ValueError("Source transaction does not meet read isolation requirements")
                source = {**source, "transaction": transaction}
                return await ScadaOfftakeExporter(db).build_snapshot(request, source)
    except Exception as error:
        # Exception strings can carry private SQL parameters or connection credentials.
        return empty_snapshot(
            request,
            {**source, "failure_type": type(error).__name__},
            blocked_reason="source_access_or_read_failure",
        )
    finally:
        await engine.dispose()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--farm", required=True)
    parser.add_argument("--owner-user-id", required=True, type=int)
    parser.add_argument("--from", dest="period_start", required=True, type=date.fromisoformat)
    parser.add_argument("--to", dest="period_end", required=True, type=date.fromisoformat)
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument(
        "--reporting-currency", type=str.upper, choices=["EUR", "GBP", "NOK", "DKK", "USD"]
    )
    parser.add_argument(
        "--source-database-url",
        default=os.environ.get("SCADA_OFFTAKE_SOURCE_DATABASE_URL"),
        help="Explicit PostgreSQL connection (prefer SCADA_OFFTAKE_SOURCE_DATABASE_URL)",
    )
    parser.add_argument(
        "--forbidden-root",
        action="append",
        type=Path,
        default=[],
        help="Additional configured shared/sync root (repeatable)",
    )
    args = parser.parse_args(argv)
    try:
        request = ExportRequest(
            args.farm,
            args.owner_user_id,
            args.period_start,
            args.period_end,
            args.reporting_currency,
        )
        url, source = explicit_source(args.source_database_url)
        output_root = private_output_root(
            args.output_root, extra_forbidden=tuple(args.forbidden_root)
        )
        snapshot = asyncio.run(export_snapshot(request, url, {**source, **source_version()}))
        path = write_private_json(output_root, snapshot["snapshot_id"] + ".json", snapshot)
    except (ValueError, OSError) as error:
        # No source URL / SQL / contract values in console output.
        print(
            f"Export blocked ({type(error).__name__}); check explicit source and private output configuration.",
            file=sys.stderr,
        )
        return 2
    status = snapshot["readiness"]["status"]
    print(f"Private snapshot: {path}\nReadiness: {status}; PPA monetary activation is disabled.")
    return 2 if status == "blocked" else 0


if __name__ == "__main__":
    raise SystemExit(main())
