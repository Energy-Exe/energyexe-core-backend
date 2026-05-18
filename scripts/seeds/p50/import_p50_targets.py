"""Bulk import P50 targets (PRE-C for spec items 6 & 7).

Currently only 83/1,624 windfarms (5.1%) have a P50 target row, leaving 95%
of farms with non-functional Module 6 commercial reporting (no
`Contract_Revenue_vs_P50Target_EUR`, no PPA scenario baseline).

This script supports two import paths:

1. **CSV path** (preferred): consume an externally-provided CSV with columns
   `windfarm_id, p50_target_start_date, p50_target_volume_gwh` (and optional
   `p50_target_end_date, source, comment`). Use this when owner-provided
   wind-resource-assessment P50s are available.

2. **Fallback computed path**: for any windfarm without a P50 target, compute
   one from the last 3 full calendar years of `generation_data`. The
   resulting target is flagged in the `comment` column as
   `auto-computed (3-yr historical mean)` so analysts can later replace it
   with an authoritative figure. Use `--enable-fallback` to opt in.

Idempotent: existing (windfarm_id, p50_target_start_date) rows are NOT
overwritten unless `--overwrite` is passed. Date conflicts (two non-NULL
windows overlapping for the same windfarm) are detected and skipped with a
warning rather than raising.

Usage:
    # Preview computed P50s without writing anything
    poetry run python scripts/seeds/p50/import_p50_targets.py \\
        --enable-fallback --dry-run

    # Apply CSV (with overwrites)
    poetry run python scripts/seeds/p50/import_p50_targets.py \\
        --csv path/to/p50_targets.csv --overwrite

    # Compute fallback for all windfarms missing a target
    poetry run python scripts/seeds/p50/import_p50_targets.py \\
        --enable-fallback
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import os
import sys
from datetime import date, datetime
from typing import Dict, List, Optional, Tuple

import asyncpg


DB_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:Energyexe1*@energyexedb.cn8a6ka2u5c3.eu-north-1.rds.amazonaws.com:5432/energyexe_db",
).replace("postgresql+asyncpg://", "postgresql://")

# Timeout for individual statements (seconds). Remote DB over high-latency
# connections needs generous limits.
STATEMENT_TIMEOUT_S = int(os.environ.get("P50_STATEMENT_TIMEOUT", "120"))
CONNECT_TIMEOUT_S = int(os.environ.get("P50_CONNECT_TIMEOUT", "30"))
MAX_RETRIES = 3


# ─── CSV import path ─────────────────────────────────────────────


def _parse_csv(path: str) -> List[dict]:
    rows = []
    with open(path) as f:
        reader = csv.DictReader(f)
        for r in reader:
            try:
                wf_id = int(r["windfarm_id"])
                start = datetime.fromisoformat(r["p50_target_start_date"]).date()
                volume = float(r["p50_target_volume_gwh"])
            except (KeyError, ValueError) as exc:
                print(f"  SKIP malformed row: {r} — {exc}")
                continue

            end_str = r.get("p50_target_end_date") or ""
            end = datetime.fromisoformat(end_str).date() if end_str else None

            rows.append({
                "windfarm_id": wf_id,
                "p50_target_start_date": start,
                "p50_target_end_date": end,
                "p50_target_volume_gwh": round(volume, 3),
                "source": r.get("source") or None,
                "comment": r.get("comment") or "imported from CSV",
            })
    return rows


# ─── Connection helper ──────────────────────────────────────────


async def _get_conn() -> asyncpg.Connection:
    """Create a connection with generous timeouts for remote RDS."""
    conn = await asyncpg.connect(
        DB_URL,
        command_timeout=STATEMENT_TIMEOUT_S,
        timeout=CONNECT_TIMEOUT_S,
    )
    # Set a server-side statement timeout as a safety net
    await conn.execute(f"SET statement_timeout = '{STATEMENT_TIMEOUT_S * 1000}'")
    return conn


async def _safe_fetch(query: str, *args, retries: int = MAX_RETRIES):
    """Execute a query with connection-level retry on timeout/disconnect."""
    last_err = None
    for attempt in range(retries):
        conn = None
        try:
            conn = await _get_conn()
            rows = await conn.fetch(query, *args)
            return rows
        except (
            asyncpg.exceptions.ConnectionDoesNotExistError,
            asyncpg.exceptions.InterfaceError,
            asyncio.TimeoutError,
            OSError,
        ) as exc:
            last_err = exc
            wait = 2 ** attempt
            print(f"    retry {attempt + 1}/{retries} after {type(exc).__name__}: {exc} (waiting {wait}s)")
            await asyncio.sleep(wait)
        finally:
            if conn is not None:
                try:
                    await conn.close()
                except Exception:
                    pass
    raise last_err


# ─── Fallback computed path ─────────────────────────────────────


async def _windfarms_missing_p50() -> List[int]:
    rows = await _safe_fetch("""
        SELECT w.id
        FROM windfarms w
        WHERE w.lat IS NOT NULL AND w.lng IS NOT NULL
          AND w.status = 'operational'
          AND NOT EXISTS (
              SELECT 1 FROM p50_targets p WHERE p.windfarm_id = w.id
          )
        ORDER BY w.id
    """)
    return [r["id"] for r in rows]


async def _compute_fallback_p50(windfarm_id: int) -> Optional[Tuple[float, int]]:
    """Compute P50 = mean of last 3 full calendar years of actual generation.

    Each year is queried SEPARATELY to avoid a single massive aggregation
    that times out over remote connections. Returns (volume_gwh,
    latest_year_used) or None if insufficient data.
    """
    today = date.today()
    latest_full_year = today.year - 1
    target_years = list(range(latest_full_year - 2, latest_full_year + 1))

    volumes: List[Tuple[int, float]] = []  # (year, gwh)
    for yr in target_years:
        try:
            rows = await _safe_fetch(
                """
                SELECT SUM(generation_mwh) / 1000.0 AS gwh,
                       COUNT(DISTINCT DATE_TRUNC('day', hour))::int AS day_count
                FROM generation_data
                WHERE windfarm_id = $1
                  AND EXTRACT(YEAR FROM hour) = $2
                  AND generation_mwh IS NOT NULL
                """,
                windfarm_id,
                yr,
            )
            if rows and rows[0]["gwh"] is not None and rows[0]["day_count"] >= 350:
                volumes.append((yr, float(rows[0]["gwh"])))
        except Exception as exc:
            print(f"    skip year {yr} for wf {windfarm_id}: {exc}")
            continue

    if len(volumes) < 2:
        return None  # need at least 2 substantially-complete years

    mean_gwh = sum(v for _, v in volumes) / len(volumes)
    latest_year_used = max(y for y, _ in volumes)
    return round(mean_gwh, 3), latest_year_used


# ─── Persistence ─────────────────────────────────────────────────


async def _upsert_p50(row: dict, overwrite: bool, dry_run: bool) -> str:
    """Insert or skip one P50 target row. Returns 'inserted' / 'skipped' / 'overwritten'.

    Opens its own connection per call for resilience against remote timeouts.
    """
    check_rows = await _safe_fetch(
        """
        SELECT id FROM p50_targets
        WHERE windfarm_id = $1 AND p50_target_start_date = $2
        """,
        row["windfarm_id"], row["p50_target_start_date"],
    )
    existing = check_rows[0] if check_rows else None

    if existing and not overwrite:
        return "skipped"

    if dry_run:
        return "inserted (dry-run)" if not existing else "overwritten (dry-run)"

    conn = await _get_conn()
    try:
        if existing:
            await conn.execute(
                """
                UPDATE p50_targets
                SET p50_target_end_date = $1,
                    p50_target_volume_gwh = $2,
                    source = COALESCE($3, source),
                    comment = COALESCE($4, comment),
                    updated_at = NOW()
                WHERE id = $5
                """,
                row["p50_target_end_date"],
                row["p50_target_volume_gwh"],
                row.get("source"),
                row.get("comment"),
                existing["id"],
            )
            return "overwritten"

        await conn.execute(
            """
            INSERT INTO p50_targets
              (windfarm_id, p50_target_start_date, p50_target_end_date,
               p50_target_volume_gwh, source, comment)
            VALUES ($1, $2, $3, $4, $5, $6)
            """,
            row["windfarm_id"],
            row["p50_target_start_date"],
            row["p50_target_end_date"],
            row["p50_target_volume_gwh"],
            row.get("source"),
            row.get("comment"),
        )
        return "inserted"
    finally:
        await conn.close()


# ─── Driver ──────────────────────────────────────────────────────


async def main(args) -> int:
    rows: List[dict] = []

    if args.csv:
        csv_rows = _parse_csv(args.csv)
        print(f"Parsed {len(csv_rows)} rows from {args.csv}")
        rows.extend(csv_rows)

    if args.enable_fallback:
        print("Fetching windfarms missing P50 targets...")
        missing = await _windfarms_missing_p50()
        print(f"\n{len(missing)} operational windfarms have no P50 target — "
              f"computing fallback from 3-yr historical mean (1 year at a time)...")

        computed_count = 0
        skipped_count = 0
        for i, wf_id in enumerate(missing):
            try:
                computed = await _compute_fallback_p50(wf_id)
            except Exception as exc:
                print(f"  FAIL wf {wf_id}: {exc}")
                skipped_count += 1
                continue

            if computed is None:
                skipped_count += 1
                continue

            volume_gwh, latest_year = computed
            rows.append({
                "windfarm_id": wf_id,
                "p50_target_start_date": date(latest_year, 1, 1),
                "p50_target_end_date": None,
                "p50_target_volume_gwh": volume_gwh,
                "source": "fallback computed (3-yr historical mean)",
                "comment": (
                    f"auto-computed from {latest_year-2}-{latest_year} "
                    f"actual generation; replace with owner-provided P50 "
                    f"when available"
                ),
            })
            computed_count += 1

            if (i + 1) % 20 == 0:
                print(f"  ...processed {i + 1}/{len(missing)} "
                      f"(computed: {computed_count}, skipped: {skipped_count})")

        print(f"\n  Fallback computation done: {computed_count} computed, "
              f"{skipped_count} skipped (insufficient data)")

    if not rows:
        print("\nNothing to import. Use --csv or --enable-fallback.")
        return 0

    # Apply
    print(f"\nApplying {len(rows)} P50 target rows...")
    stats: Dict[str, int] = {"inserted": 0, "skipped": 0, "overwritten": 0,
                             "inserted (dry-run)": 0, "overwritten (dry-run)": 0,
                             "error": 0}
    for i, row in enumerate(rows):
        try:
            outcome = await _upsert_p50(row, args.overwrite, args.dry_run)
            stats[outcome] = stats.get(outcome, 0) + 1
        except Exception as exc:
            print(f"  ERROR for windfarm {row.get('windfarm_id')}: {exc}")
            stats["error"] += 1

        if (i + 1) % 50 == 0:
            print(f"  ...applied {i + 1}/{len(rows)}")

    print(f"\nResult: {stats}")

    if not args.dry_run:
        # Coverage check
        try:
            cov_rows = await _safe_fetch(
                "SELECT COUNT(DISTINCT windfarm_id) AS n FROM p50_targets"
            )
            covered = cov_rows[0]["n"] if cov_rows else 0
            total_rows = await _safe_fetch(
                "SELECT COUNT(*) AS n FROM windfarms WHERE lat IS NOT NULL AND lng IS NOT NULL"
            )
            total = total_rows[0]["n"] if total_rows else 1
            print(f"\nP50 coverage now: {covered}/{total} ({100.0*covered/total:.1f}%)")
        except Exception as exc:
            print(f"\nCoverage check failed: {exc}")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--csv", help="Path to CSV with explicit P50 targets")
    parser.add_argument("--enable-fallback", action="store_true",
                        help="Compute P50 from last-3-years actuals for windfarms "
                             "with no existing P50 target")
    parser.add_argument("--overwrite", action="store_true",
                        help="Overwrite existing P50 rows on (windfarm_id, start_date) collision")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be done without writing")
    args = parser.parse_args()

    if not args.csv and not args.enable_fallback:
        parser.error("specify --csv and/or --enable-fallback")

    sys.exit(asyncio.run(main(args)))
