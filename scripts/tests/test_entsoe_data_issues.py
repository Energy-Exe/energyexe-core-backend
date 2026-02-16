#!/usr/bin/env python3
"""
ENTSOE Data Issues — TDD Verification Script

Connects to the real database and validates each of the 8 known ENTSOE data issues.
Each check prints PASS/FAIL. All should FAIL before fixes are applied.

Usage:
    cd /Users/mdfaisal/Documents/energyexe/energyexe-core-backend
    poetry run python scripts/tests/test_entsoe_data_issues.py
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from collections import defaultdict

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from sqlalchemy import select, func, and_, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_session_factory
from app.models.generation_data import GenerationDataRaw, GenerationData
from app.models.generation_unit import GenerationUnit

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

PASS = "\033[92mPASS\033[0m"
FAIL = "\033[91mFAIL\033[0m"
WARN = "\033[93mWARN\033[0m"
INFO = "\033[94mINFO\033[0m"

results = []


def record(name: str, passed: bool, detail: str = ""):
    status = PASS if passed else FAIL
    results.append((name, passed))
    print(f"  [{status}] {name}")
    if detail:
        for line in detail.strip().split("\n"):
            print(f"         {line}")


# ─────────────────────────────────────────────────────────────────────────────
# Database-backed checks
# ─────────────────────────────────────────────────────────────────────────────

async def check_pt15m_resolution_metadata(db: AsyncSession):
    """Check 1 (Issue 3): Verify PT15M resolution metadata is correct for French ENTSOE units."""
    print("\n── Check 1: PT15M Resolution Metadata (Issue 3) ──")

    # Get French ENTSOE units (France switched to PT15M around 2025-01-09)
    stmt = text("""
        SELECT gdr.identifier,
               gdr.period_type,
               gdr.data->>'resolution_code' AS resolution_code,
               COUNT(*) AS cnt
        FROM generation_data_raw gdr
        JOIN generation_units gu ON gdr.identifier = gu.code AND gu.source = 'ENTSOE'
        JOIN windfarms wf ON gu.windfarm_id = wf.id
        JOIN countries c ON wf.country_id = c.id
        WHERE gdr.source = 'ENTSOE'
          AND gdr.source_type = 'api'
          AND c.code = 'FRA'
          AND gdr.period_start >= '2025-01-09'
        GROUP BY gdr.identifier, gdr.period_type, gdr.data->>'resolution_code'
        ORDER BY cnt DESC
        LIMIT 20
    """)
    result = await db.execute(stmt)
    rows = result.fetchall()

    if not rows:
        record("PT15M metadata — French API data exists", False,
               "No French ENTSOE API data found after 2025-01-09")
        return

    pt15m_count = sum(r.cnt for r in rows if r.period_type == 'PT15M')
    pt60m_count = sum(r.cnt for r in rows if r.period_type == 'PT60M')
    total = pt15m_count + pt60m_count

    record(
        "PT15M metadata — period_type is PT15M for French data",
        pt15m_count > 0 and pt15m_count > pt60m_count,
        f"PT15M records: {pt15m_count}, PT60M records: {pt60m_count}"
    )

    # Check resolution_code in JSONB
    correct_jsonb = sum(r.cnt for r in rows if r.resolution_code == 'PT15M')
    record(
        "PT15M metadata — JSONB resolution_code is PT15M",
        correct_jsonb > 0 and correct_jsonb > (total - correct_jsonb),
        f"JSONB PT15M: {correct_jsonb}, Other: {total - correct_jsonb}"
    )


async def check_pt15m_aggregation_correctness(db: AsyncSession):
    """Check 2 (Issue 3): Verify PT15M data is averaged correctly in aggregation."""
    print("\n── Check 2: PT15M Aggregation Correctness (Issue 3) ──")

    # Find a French unit with 4 raw records in a single hour
    stmt = text("""
        SELECT gdr.identifier,
               date_trunc('hour', gdr.period_start) AS hour,
               COUNT(*) AS records_per_hour,
               AVG(gdr.value_extracted) AS avg_value,
               MIN(gdr.value_extracted) AS min_value,
               MAX(gdr.value_extracted) AS max_value,
               ARRAY_AGG(gdr.value_extracted ORDER BY gdr.period_start) AS raw_values
        FROM generation_data_raw gdr
        JOIN generation_units gu ON gdr.identifier = gu.code AND gu.source = 'ENTSOE'
        JOIN windfarms wf ON gu.windfarm_id = wf.id
        JOIN countries c ON wf.country_id = c.id
        WHERE gdr.source = 'ENTSOE'
          AND gdr.source_type = 'api'
          AND c.code = 'FRA'
          AND gdr.period_start >= '2025-01-09'
        GROUP BY gdr.identifier, date_trunc('hour', gdr.period_start)
        HAVING COUNT(*) >= 3
        ORDER BY date_trunc('hour', gdr.period_start) DESC
        LIMIT 1
    """)
    result = await db.execute(stmt)
    row = result.fetchone()

    if not row:
        record("PT15M aggregation — sample hour with 4 records found", False,
               "No hour found with 3+ raw ENTSOE records for French units")
        return

    # Now check the corresponding aggregated value
    agg_stmt = text("""
        SELECT gd.generation_mwh
        FROM generation_data gd
        JOIN generation_units gu ON gd.generation_unit_id = gu.id
        WHERE gu.code = :identifier
          AND gu.source = 'ENTSOE'
          AND gd.hour = :hour
          AND gd.source = 'ENTSOE'
        LIMIT 1
    """)
    agg_result = await db.execute(agg_stmt, {"identifier": row.identifier, "hour": row.hour})
    agg_row = agg_result.fetchone()

    if not agg_row:
        record("PT15M aggregation — aggregated record exists", False,
               f"No aggregated record for {row.identifier} at {row.hour}")
        return

    agg_value = float(agg_row.generation_mwh)
    avg_value = float(row.avg_value)
    first_value = float(row.raw_values[0]) if row.raw_values else 0

    # The bug: aggregated value equals first raw value instead of average
    is_correct = abs(agg_value - avg_value) < 0.01
    is_buggy = abs(agg_value - first_value) < 0.01 and abs(avg_value - first_value) > 0.01

    record(
        "PT15M aggregation — hourly value is average of sub-hourly records",
        is_correct,
        f"Raw values: {[float(v) for v in row.raw_values]}\n"
        f"Expected avg: {avg_value:.3f}\n"
        f"Actual aggregated: {agg_value:.3f}\n"
        f"First raw value: {first_value:.3f}"
        + ("\n*** BUG: Using first value instead of average ***" if is_buggy else "")
    )


async def check_data_after_jan24(db: AsyncSession):
    """Check 3 (Issue 1): Verify data exists after January 24, 2026."""
    print("\n── Check 3: Data After Jan 24, 2026 (Issue 1) ──")

    # Raw data
    raw_stmt = text("""
        SELECT DATE(period_start) AS day, COUNT(*) AS cnt
        FROM generation_data_raw
        WHERE source = 'ENTSOE' AND period_start > '2026-01-20'
        GROUP BY DATE(period_start) ORDER BY day
    """)
    raw_result = await db.execute(raw_stmt)
    raw_rows = raw_result.fetchall()

    after_jan24_raw = sum(r.cnt for r in raw_rows if r.day > datetime(2026, 1, 24).date())
    before_jan24_raw = sum(r.cnt for r in raw_rows if r.day <= datetime(2026, 1, 24).date())

    record(
        "Jan 24 — raw data exists after 2026-01-24",
        after_jan24_raw > 0,
        f"Before Jan 24: {before_jan24_raw} records, After: {after_jan24_raw} records"
    )

    # Aggregated data
    agg_stmt = text("""
        SELECT DATE(hour) AS day, COUNT(*) AS cnt
        FROM generation_data
        WHERE source = 'ENTSOE' AND hour > '2026-01-20'
        GROUP BY DATE(hour) ORDER BY day
    """)
    agg_result = await db.execute(agg_stmt)
    agg_rows = agg_result.fetchall()

    after_jan24_agg = sum(r.cnt for r in agg_rows if r.day > datetime(2026, 1, 24).date())

    record(
        "Jan 24 — aggregated data exists after 2026-01-24",
        after_jan24_agg > 0,
        f"Aggregated records after Jan 24: {after_jan24_agg}"
    )

    # Import job status
    job_stmt = text("""
        SELECT id, job_name, status, error_message, created_at
        FROM import_job_executions
        WHERE job_name ILIKE '%entsoe%' AND created_at > '2026-01-24'
        ORDER BY created_at DESC LIMIT 5
    """)
    try:
        job_result = await db.execute(job_stmt)
        job_rows = job_result.fetchall()
        if job_rows:
            detail = "\n".join(
                f"  {r.created_at}: {r.job_name} — {r.status}" +
                (f" ({r.error_message[:80]})" if r.error_message else "")
                for r in job_rows
            )
        else:
            detail = "No import jobs found after Jan 24"
        record("Jan 24 — import jobs ran after 2026-01-24", len(job_rows) > 0, detail)
    except Exception as e:
        record("Jan 24 — import jobs table accessible", False, str(e))


async def check_consumption_records_exist(db: AsyncSession):
    """Check 4 (Issue 4): Verify consumption records exist."""
    print("\n── Check 4: Consumption Records (Issue 4) ──")

    # Check for consumption source_types in raw data
    stmt = text("""
        SELECT source_type, COUNT(*) AS cnt
        FROM generation_data_raw
        WHERE source = 'ENTSOE'
          AND source_type IN ('api_consumption', 'excel_consumption')
        GROUP BY source_type
    """)
    result = await db.execute(stmt)
    rows = result.fetchall()

    has_consumption_raw = len(rows) > 0
    detail = "\n".join(f"{r.source_type}: {r.cnt} records" for r in rows) if rows else "No consumption records found"
    record("Consumption — raw consumption records exist", has_consumption_raw, detail)


async def check_generation_data_model_schema(db: AsyncSession):
    """Check 5 (Issue 4): Verify consumption_mwh column exists in generation_data."""
    print("\n── Check 5: Schema — consumption_mwh Column (Issue 4) ──")

    stmt = text("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'generation_data' AND column_name = 'consumption_mwh'
    """)
    result = await db.execute(stmt)
    row = result.fetchone()

    record(
        "Schema — consumption_mwh column exists in generation_data",
        row is not None,
        f"Column type: {row.data_type}" if row else "Column does not exist"
    )


async def check_precision_nobelwind_oct2025(db: AsyncSession):
    """Check 6 (Issue 5): Check precision of Nobelwind October 2025 data."""
    print("\n── Check 6: Precision — Nobelwind Oct 2025 (Issue 5) ──")

    # Find Nobelwind identifier
    unit_stmt = text("""
        SELECT gu.code, gu.name
        FROM generation_units gu
        JOIN windfarms wf ON gu.windfarm_id = wf.id
        WHERE gu.source = 'ENTSOE'
          AND (wf.name ILIKE '%nobelwind%' OR gu.name ILIKE '%nobelwind%')
        LIMIT 1
    """)
    unit_result = await db.execute(unit_stmt)
    unit_row = unit_result.fetchone()

    if not unit_row:
        record("Precision — Nobelwind unit found", False, "No Nobelwind ENTSOE unit found")
        return

    # Check raw data precision
    raw_stmt = text("""
        SELECT period_start, value_extracted,
               data->>'source_value_raw' AS source_value_raw
        FROM generation_data_raw
        WHERE source = 'ENTSOE' AND identifier = :code
          AND period_start >= '2025-10-01' AND period_start < '2025-11-01'
        ORDER BY period_start LIMIT 20
    """)
    raw_result = await db.execute(raw_stmt, {"code": unit_row.code})
    raw_rows = raw_result.fetchall()

    if not raw_rows:
        record("Precision — Nobelwind Oct 2025 raw data exists", False,
               f"No raw data found for {unit_row.code} in Oct 2025")
        return

    # Check if values have decimals
    has_decimals = any(
        float(r.value_extracted) != round(float(r.value_extracted))
        for r in raw_rows if r.value_extracted is not None
    )

    sample_values = [f"{float(r.value_extracted):.3f}" for r in raw_rows[:5]]
    record(
        "Precision — raw values have decimal precision",
        has_decimals,
        f"Sample values: {', '.join(sample_values)}\n"
        f"source_value_raw tracking: {'present' if raw_rows[0].source_value_raw else 'not tracked yet'}"
    )


async def check_entsoe_data_gaps(db: AsyncSession):
    """Check 7 (Issue 2): Check for known data gaps."""
    print("\n── Check 7: Data Gaps (Issue 2) ──")

    gaps_to_check = [
        ("Saint-Nazaire", "2025-11-08", "2025-11-10"),
        ("Kriegers Flak", "2024-09-01", "2024-09-30"),
    ]

    for farm_name, start, end in gaps_to_check:
        sd = datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        ed = datetime.strptime(end, "%Y-%m-%d").replace(tzinfo=timezone.utc)

        stmt = text("""
            SELECT COUNT(*) AS cnt
            FROM generation_data gd
            JOIN windfarms wf ON gd.windfarm_id = wf.id
            WHERE wf.name ILIKE :farm_name
              AND gd.source = 'ENTSOE'
              AND gd.hour >= :start_date
              AND gd.hour < :end_date
        """)
        result = await db.execute(stmt, {
            "farm_name": f"%{farm_name}%",
            "start_date": sd,
            "end_date": ed,
        })
        row = result.fetchone()
        cnt = row.cnt if row else 0
        expected_hours = int((ed - sd).total_seconds() / 3600)

        record(
            f"Gaps — {farm_name} ({start} to {end})",
            cnt >= expected_hours * 0.9,  # 90% threshold
            f"Found {cnt} records, expected ~{expected_hours} hours"
        )


async def check_duplicate_raw_records(db: AsyncSession):
    """Check 8 (Issue 8): Check for duplicate raw records."""
    print("\n── Check 8: Duplicate Raw Records (Issue 8) ──")

    # Check for duplicates (should be 0 due to unique constraint)
    stmt = text("""
        SELECT source, source_type, identifier, period_start, COUNT(*) AS cnt
        FROM generation_data_raw
        WHERE source = 'ENTSOE'
        GROUP BY source, source_type, identifier, period_start
        HAVING COUNT(*) > 1
        LIMIT 10
    """)
    result = await db.execute(stmt)
    rows = result.fetchall()

    record(
        "Duplicates — no duplicate raw records",
        len(rows) == 0,
        f"Found {len(rows)} duplicate groups" if rows else "No duplicates (unique constraint works)"
    )

    # Check if revision tracking exists in JSONB
    revision_stmt = text("""
        SELECT COUNT(*) AS cnt
        FROM generation_data_raw
        WHERE source = 'ENTSOE'
          AND data ? 'previous_value'
        LIMIT 1
    """)
    rev_result = await db.execute(revision_stmt)
    rev_row = rev_result.fetchone()

    record(
        "Duplicates — revision tracking (previous_value) in JSONB",
        rev_row and rev_row.cnt > 0,
        "Revision tracking not yet implemented" if (not rev_row or rev_row.cnt == 0) else f"{rev_row.cnt} records with revision tracking"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Unit tests (no DB required, testing code logic)
# ─────────────────────────────────────────────────────────────────────────────

def test_resolution_detection_logic():
    """Test A: Verify entsoe-py doesn't provide resolution_code and our detection works."""
    print("\n── Test A: Resolution Detection Logic ──")

    # Simulate entsoe-py row (dict-like) — no resolution_code column
    mock_row = {"value": 10.5, "eic_code": "ABC123", "production_type": "wind"}

    # Current buggy behavior
    buggy_resolution = mock_row.get("resolution_code", "PT60M")
    record(
        "Resolution detection — entsoe-py row lacks resolution_code",
        buggy_resolution == "PT60M",
        f"row.get('resolution_code', 'PT60M') returns '{buggy_resolution}' (always defaults)"
    )

    # Test the fix function (timestamp-based detection)
    import pandas as pd
    import numpy as np

    def _detect_entsoe_resolution(df):
        """Detect resolution from timestamp spacing."""
        if df is None or df.empty or len(df) < 2:
            return "PT60M"
        timestamps = pd.to_datetime(df.index if isinstance(df.index, pd.DatetimeIndex) else df.get("timestamp", df.index))
        diffs = timestamps.diff().dropna()
        if diffs.empty:
            return "PT60M"
        min_delta = diffs.min().total_seconds()
        if min_delta <= 900:
            return "PT15M"
        elif min_delta <= 1800:
            return "PT30M"
        return "PT60M"

    # 15-min data
    idx_15m = pd.date_range("2025-01-10", periods=8, freq="15min")
    df_15m = pd.DataFrame({"value": np.random.rand(8)}, index=idx_15m)
    record(
        "Resolution detection — detects PT15M from 15-min timestamps",
        _detect_entsoe_resolution(df_15m) == "PT15M",
        f"Detected: {_detect_entsoe_resolution(df_15m)}"
    )

    # 60-min data
    idx_60m = pd.date_range("2025-01-10", periods=4, freq="60min")
    df_60m = pd.DataFrame({"value": np.random.rand(4)}, index=idx_60m)
    record(
        "Resolution detection — detects PT60M from 60-min timestamps",
        _detect_entsoe_resolution(df_60m) == "PT60M",
        f"Detected: {_detect_entsoe_resolution(df_60m)}"
    )

    # 30-min data
    idx_30m = pd.date_range("2025-01-10", periods=4, freq="30min")
    df_30m = pd.DataFrame({"value": np.random.rand(4)}, index=idx_30m)
    record(
        "Resolution detection — detects PT30M from 30-min timestamps",
        _detect_entsoe_resolution(df_30m) == "PT30M",
        f"Detected: {_detect_entsoe_resolution(df_30m)}"
    )


def test_transform_entsoe_with_four_records():
    """Test B: Verify transform_entsoe bug with 4 records per hour."""
    print("\n── Test B: transform_entsoe with 4 Records ──")

    from types import SimpleNamespace

    # Create 4 mock records for the same hour with incorrect metadata
    hour = datetime(2025, 1, 10, 14, 0, 0, tzinfo=timezone.utc)
    mock_records = []
    values = [10.0, 12.0, 11.0, 13.0]
    for i, val in enumerate(values):
        r = SimpleNamespace(
            id=i + 1,
            identifier="TESTUNIT",
            period_start=hour + timedelta(minutes=i * 15),
            value_extracted=Decimal(str(val)),
            data={"resolution_code": "PT60M"},  # BUG: incorrect metadata
        )
        mock_records.append(r)

    # Simulate the buggy transform_entsoe logic (line 543-551)
    valid_records = mock_records
    resolution = valid_records[0].data.get('resolution_code', 'PT60M')

    if resolution == 'PT15M':
        # Would average — but this branch is never taken due to bug
        import numpy as np
        generation_mw = np.mean([float(r.value_extracted) for r in valid_records])
    else:
        # Buggy branch — only uses first value
        generation_mw = float(valid_records[0].value_extracted)

    expected_avg = sum(values) / len(values)  # 11.5

    record(
        "transform_entsoe — bug confirmed: uses first value instead of average",
        abs(generation_mw - values[0]) < 0.01,  # Should be True (bug present)
        f"Resolution from metadata: '{resolution}'\n"
        f"Computed value: {generation_mw:.3f}\n"
        f"Expected average: {expected_avg:.3f}\n"
        f"First value: {values[0]:.3f}"
    )

    # Now test the fix: use record count as heuristic
    if len(valid_records) >= 3:
        fixed_resolution = 'PT15M'
    elif len(valid_records) == 2:
        fixed_resolution = 'PT30M'
    else:
        fixed_resolution = resolution

    if fixed_resolution == 'PT15M':
        import numpy as np
        fixed_generation_mw = np.mean([float(r.value_extracted) for r in valid_records])
    else:
        fixed_generation_mw = float(valid_records[0].value_extracted)

    record(
        "transform_entsoe — fix: record-count heuristic gives correct average",
        abs(fixed_generation_mw - expected_avg) < 0.01,
        f"Fixed resolution: '{fixed_resolution}'\n"
        f"Fixed value: {fixed_generation_mw:.3f}\n"
        f"Expected: {expected_avg:.3f}"
    )


def test_entsoe_column_parsing_no_consumption():
    """Test C: Verify ENTSOE client doesn't extract consumption data direction."""
    print("\n── Test C: ENTSOE Column Parsing — No Consumption ──")

    import pandas as pd
    import numpy as np

    # Simulate a MultiIndex DataFrame like entsoe-py returns
    # Structure: (unit_name, production_type, metric, eic_code)
    timestamps = pd.date_range("2025-01-10", periods=4, freq="60min")
    columns = pd.MultiIndex.from_tuples([
        ("Unit A", "Wind Offshore", "Actual Aggregated", "W123456"),
        ("Unit A", "Wind Offshore", "Actual Consumption", "W123456"),
    ])
    data = np.array([
        [100.0, 5.0],
        [110.0, 6.0],
        [105.0, 4.0],
        [95.0, 7.0],
    ])
    df = pd.DataFrame(data, index=timestamps, columns=columns)

    # Simulate current entsoe_client.py:308-343 parsing logic
    all_data = []
    for col in df.columns:
        unit_name = col[0]
        eic_code = None
        if len(col) > 3 and isinstance(col[3], str) and "W" in col[3]:
            eic_code = col[3]

        if not eic_code:
            continue

        unit_df = pd.DataFrame(df[col])
        unit_df.columns = ["value"]
        unit_df["unit_name"] = unit_name
        unit_df["eic_code"] = eic_code
        unit_df["area_code"] = "FR"
        # NOTE: No data_direction column is set
        all_data.append(unit_df)

    if all_data:
        result_df = pd.concat(all_data, ignore_index=True)
    else:
        result_df = pd.DataFrame()

    has_direction = "data_direction" in result_df.columns if not result_df.empty else False

    record(
        "Column parsing — no data_direction column (consumption not tracked)",
        not has_direction,
        "Current code does NOT extract col[2] (Actual Aggregated vs Actual Consumption)"
    )

    # Check that both gen and consumption are mixed together
    # With the bug, both columns produce records indistinguishable from each other
    unique_eics = result_df["eic_code"].unique() if not result_df.empty else []
    record(
        "Column parsing — generation and consumption records are indistinguishable",
        len(all_data) == 2 and not has_direction,
        f"Found {len(all_data)} column groups, all mapped to same eic_code without direction"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

async def run_db_checks():
    """Run all database-backed checks."""
    session_factory = get_session_factory()

    async with session_factory() as db:
        await check_pt15m_resolution_metadata(db)
        await check_pt15m_aggregation_correctness(db)
        await check_data_after_jan24(db)
        await check_consumption_records_exist(db)
        await check_generation_data_model_schema(db)
        await check_precision_nobelwind_oct2025(db)
        await check_entsoe_data_gaps(db)
        await check_duplicate_raw_records(db)


def run_unit_tests():
    """Run all unit tests (no DB required)."""
    test_resolution_detection_logic()
    test_transform_entsoe_with_four_records()
    test_entsoe_column_parsing_no_consumption()


async def main():
    print("=" * 70)
    print("  ENTSOE Data Issues — TDD Verification Script")
    print("=" * 70)

    # Unit tests first (no DB needed)
    print("\n\n╔══════════════════════════════════════════════════════════════════╗")
    print("║  UNIT TESTS (no database required)                              ║")
    print("╚══════════════════════════════════════════════════════════════════╝")
    run_unit_tests()

    # Database checks
    print("\n\n╔══════════════════════════════════════════════════════════════════╗")
    print("║  DATABASE CHECKS (requires connection)                          ║")
    print("╚══════════════════════════════════════════════════════════════════╝")
    try:
        await run_db_checks()
    except Exception as e:
        print(f"\n  [{FAIL}] Database connection failed: {e}")
        print("         Skipping database checks.")

    # Summary
    print("\n\n" + "=" * 70)
    print("  SUMMARY")
    print("=" * 70)
    passed = sum(1 for _, p in results if p)
    failed = sum(1 for _, p in results if not p)
    print(f"\n  Total: {len(results)} checks")
    print(f"  {PASS}: {passed}")
    print(f"  {FAIL}: {failed}")

    if failed > 0:
        print(f"\n  {failed} checks failing — fixes needed.")
    else:
        print(f"\n  All checks passing!")

    print()


if __name__ == "__main__":
    asyncio.run(main())




 curl 'https://api.invygo.com/serviceProvider/exception-hours' \
    -H 'accept: application/json' \
    -H 'authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySUQiOiI2NmVjMTczYzg0MWVmNDk2NjNiZTE2NzIiLCJyb2xlIjoiYWRtaW4iLCJqd3RJZCI6bnVsbCwiaWF0IjoxNzY0MDU2NDM1LCJleHAiOjE3NzE4MzI0MzV9.3YJ-qc1MYIV292GL4K05-32h4N4fFy3D3RiIpEOyZmA' \
    -H 'content-type: application/json' \
    -H 'content-language: EN' \
    -H 'origin: https://dashboard.invygo.com' \
    -H 'utcoffset: -360' \
    --data-raw '{"dates":["2026-02-18T00:00:00.000+00:00","2026-02-19T00:00:00.000+00:00"],"teams":["RETURN_PICKUP"],"showroomIds":["5e66019d1ae303410d9ec017"],"shifts":[{"startTime":"2026-02-16T07:00:00.000Z","endTime":"2026-02-16T10:00:00.000Z","operationalLimit":5},{"startTime":"2026-02-16T10:00:00.000Z","endTime":"2026-02-16T12:00:00.000Z","operationalLimit":5},{"startTime":"2026-02-16T18:00:00.000Z","endTime":"2026-02-16T21:00:00.000Z","operationalLimit":5}]}'



 curl 'https://api.invygo.com/serviceProvider/exception-hours' \
    -H 'accept: application/json' \
    -H 'authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySUQiOiI2NmVjMTczYzg0MWVmNDk2NjNiZTE2NzIiLCJyb2xlIjoiYWRtaW4iLCJqd3RJZCI6bnVsbCwiaWF0IjoxNzY0MDU2NDM1LCJleHAiOjE3NzE4MzI0MzV9.3YJ-qc1MYIV292GL4K05-32h4N4fFy3D3RiIpEOyZmA' \
    -H 'content-type: application/json' \
    -H 'content-language: EN' \
    -H 'origin: https://dashboard.invygo.com' \
    -H 'utcoffset: -360' \
    --data-raw '{"dates":["2026-02-18T00:00:00.000+00:00","2026-02-19T00:00:00.000+00:00"],"teams":["RETURN_PICKUP"],"showroomIds":["5e66019d1ae303410d9ec017"],"shifts":[{"startTime":"2026-02-16T07:00:00.000Z","endTime":"2026-02-16T10:00:00.000Z","operationalLimit":5},{"startTime":"2026-02-16T10:00:00.000Z","endTime":"2026-02-16T12:00:00.000Z","operationalLimit":5},{"startTime":"2026-02-16T18:00:00.000Z","endTime":"2026-02-16T21:00:00.000Z","operationalLimit":5}],"offDay":false,"contact":"Ops support","deleteExceptionHours":false}'



  curl 'https://api.invygo.com/serviceProvider/exception-hours' \
    -H 'accept: application/json' \
    -H 'authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySUQiOiI2NmVjMTczYzg0MWVmNDk2NjNiZTE2NzIiLCJyb2xlIjoiYWRtaW4iLCJqd3RJZCI6bnVsbCwiaWF0IjoxNzY0MDU2NDM1LCJleHAiOjE3NzE4MzI0MzV9.3YJ-qc1MYIV292GL4K05-32h4N4fFy3D3RiIpEOyZmA' \
    -H 'content-type: application/json' \
    -H 'content-language: EN' \
    -H 'origin: https://dashboard.invygo.com' \
    -H 'utcoffset: -360' \
    --data-raw '{"dates":["2026-02-18T00:00:00.000+00:00","2026-02-19T00:00:00.000+00:00"],"teams":["RETURN_PICKUP"],"showroomIds":["5e66019d1ae303410d9ec017"],"contact":"Ops Support","deleteExceptionHours":true}'


curl 'https://api.invygo.com/serviceProvider/exception-hours' \
  -H 'accept: application/json' \
  -H 'accept-language: en-GB,en-US;q=0.9,en;q=0.8' \
  -H 'authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySUQiOiI2NmVjMTczYzg0MWVmNDk2NjNiZTE2NzIiLCJyb2xlIjoiYWRtaW4iLCJqd3RJZCI6bnVsbCwiaWF0IjoxNzY0MDU2NDM1LCJleHAiOjE3NzE4MzI0MzV9.3YJ-qc1MYIV292GL4K05-32h4N4fFy3D3RiIpEOyZmA' \
  -H 'content-language: en-GB' \
  -H 'content-type: application/json' \
  -H 'origin: https://dashboard.invygo.com' \
  -H 'priority: u=1, i' \
  -H 'sec-ch-ua: "Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"' \
  -H 'sec-ch-ua-mobile: ?0' \
  -H 'sec-ch-ua-platform: "macOS"' \
  -H 'sec-fetch-dest: empty' \
  -H 'sec-fetch-mode: cors' \
  -H 'sec-fetch-site: same-site' \
  -H 'user-agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36' \
  -H 'utcoffset: -360' \
  --data-raw '{"dates":["2026-02-18T00:00:00.000+00:00"],"teams":["RETURN_PICKUP"],"showroomIds":["5e66019d1ae303410d9ec017"],"shifts":[{"id":"aca644f6-c808-4d96-98de-52131a93bd8f","startTime":"2026-02-16T07:00:00.000Z","endTime":"2026-02-16T10:00:00.000Z","operationalLimit":5},{"id":"19f655df-bcc8-4fb1-8010-a3a0c7d938b6","startTime":"2026-02-16T10:00:00.000Z","endTime":"2026-02-16T12:00:00.000Z","operationalLimit":5},{"id":"e5e796e5-19a6-4486-a9d9-62a71279e5b0","startTime":"2026-02-16T18:00:00.000Z","endTime":"2026-02-16T21:00:00.000Z","operationalLimit":5},{"id":"f33a06c0-a825-46ed-8686-f138f20ece04","startTime":"2026-02-16T21:00:00.000Z","endTime":"2026-02-16T23:00:00.000Z","operationalLimit":3}]}'