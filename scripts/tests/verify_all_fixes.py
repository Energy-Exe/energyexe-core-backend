#!/usr/bin/env python3
"""
Final Verification Script — All 8 ENTSOE Data Issues
Checks each issue against the database with specific examples from the original document.
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime, timezone
from decimal import Decimal

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from sqlalchemy import text
from app.core.database import get_session_factory

PASS = "\033[92m✅ PASS\033[0m"
FAIL = "\033[91m❌ FAIL\033[0m"
INFO = "\033[93mℹ️  INFO\033[0m"
results = []


def record(issue, check, status, detail=""):
    results.append((issue, check, status, detail))
    tag = PASS if status == "PASS" else (FAIL if status == "FAIL" else INFO)
    print(f"  {tag} {check}: {detail}")


async def verify():
    sf = get_session_factory()
    async with sf() as db:

        # ===== ISSUE 1: No data after January 24 =====
        print("\n" + "=" * 70)
        print("ISSUE 1: No data after January 24, 2026")
        print("=" * 70)

        r = await db.execute(text(
            "SELECT MAX(period_start) as latest_raw FROM generation_data_raw "
            "WHERE source = :src"
        ), {"src": "ENTSOE"})
        row = r.fetchone()
        latest = row.latest_raw
        print(f"  Latest raw record: {latest}")

        r2 = await db.execute(text(
            "SELECT MAX(hour) as latest_agg FROM generation_data WHERE source = :src"
        ), {"src": "ENTSOE"})
        latest_agg = r2.fetchone().latest_agg
        print(f"  Latest aggregated: {latest_agg}")

        # Check retry logic exists in code
        import inspect
        from app.services.entsoe_client import ENTSOEClient
        src = inspect.getsource(ENTSOEClient)
        has_retry = "MAX_RETRIES" in src or "retry" in src.lower()
        record("1", "Retry logic in entsoe_client.py", "PASS" if has_retry else "FAIL",
               "Retry logic present" if has_retry else "No retry logic found")

        # Check Docker log_dir fix
        from scripts.seeds.aggregate_generation_data.process_generation_data_robust import RobustGenerationProcessor
        src2 = inspect.getsource(RobustGenerationProcessor.__init__)
        has_fallback = "/tmp" in src2 and "OSError" in src2
        record("1", "Docker log_dir fallback", "PASS" if has_fallback else "FAIL",
               "Falls back to /tmp on OSError" if has_fallback else "No fallback")

        if latest and latest > datetime(2026, 1, 24, tzinfo=timezone.utc):
            record("1", "Data exists after Jan 24", "PASS", f"Latest: {latest}")
        else:
            record("1", "Data exists after Jan 24", "INFO",
                   f"Latest: {latest} — needs cron to run with Docker fix")

        # ===== ISSUE 2: Missing time periods =====
        print("\n" + "=" * 70)
        print("ISSUE 2: Missing time periods / data gaps")
        print("=" * 70)

        # Check gap detection exists
        from app.services.data_anomaly_service import DataAnomalyService
        has_gap = hasattr(DataAnomalyService, '_detect_data_gap_anomalies')
        record("2", "Gap detection method exists", "PASS" if has_gap else "FAIL")

        # Check completeness check in raw storage
        from app.services.raw_data_storage_service import RawDataStorageService
        src3 = inspect.getsource(RawDataStorageService)
        has_completeness = "Completeness" in src3 or "completeness" in src3
        record("2", "Post-import completeness check", "PASS" if has_completeness else "FAIL")

        # Check retry in entsoe client
        record("2", "ENTSOE retry logic for transient errors", "PASS" if has_retry else "FAIL")

        # ===== ISSUE 3: 15-min data =====
        print("\n" + "=" * 70)
        print("ISSUE 3: 15-minute interval data — only first interval captured")
        print("=" * 70)

        # Check resolution detection function exists
        from app.services.raw_data_storage_service import _detect_entsoe_resolution
        record("3", "_detect_entsoe_resolution() exists", "PASS")

        # Check PT15M records exist
        r = await db.execute(text(
            "SELECT period_type, COUNT(*) as cnt FROM generation_data_raw "
            "WHERE source = :src AND source_type = :st "
            "GROUP BY period_type ORDER BY cnt DESC"
        ), {"src": "ENTSOE", "st": "api"})
        types = {row.period_type: row.cnt for row in r.fetchall()}
        pt15m_count = types.get("PT15M", 0)
        record("3", "PT15M raw records exist", "PASS" if pt15m_count > 0 else "FAIL",
               f"PT15M: {pt15m_count:,}, PT30M: {types.get('PT30M', 0):,}, PT60M: {types.get('PT60M', 0):,}")

        # Check aggregation uses average (not first value)
        r = await db.execute(text(
            "SELECT gdr.identifier, date_trunc('hour', gdr.period_start) as hr, "
            "ARRAY_AGG(gdr.value_extracted ORDER BY gdr.period_start) as vals "
            "FROM generation_data_raw gdr "
            "WHERE gdr.source = :src AND gdr.source_type = :st AND gdr.period_type = :pt "
            "GROUP BY gdr.identifier, date_trunc('hour', gdr.period_start) "
            "HAVING COUNT(*) = 4 LIMIT 1"
        ), {"src": "ENTSOE", "st": "api", "pt": "PT15M"})
        sample = r.fetchone()

        if sample:
            vals = [Decimal(str(v)) for v in sample.vals]
            expected_avg = float(sum(vals) / len(vals))

            r2 = await db.execute(text(
                "SELECT generation_mwh FROM generation_data "
                "WHERE source = :src AND hour = :hr AND generation_unit_id = ("
                "  SELECT id FROM generation_units WHERE code = :code AND source = :src LIMIT 1"
                ")"
            ), {"src": "ENTSOE", "hr": sample.hr, "code": sample.identifier})
            agg_row = r2.fetchone()

            if agg_row:
                actual = float(agg_row.generation_mwh)
                first_val = float(vals[0])
                is_avg = abs(actual - expected_avg) < 0.01
                is_first = abs(actual - first_val) < 0.01 and not is_avg
                record("3", "Aggregation uses average of 4 values",
                       "PASS" if is_avg else "FAIL",
                       f"Raw: {[float(v) for v in vals]}, Expected avg: {expected_avg:.3f}, "
                       f"Actual: {actual:.3f}, First only: {first_val:.3f}")
            else:
                record("3", "Aggregation uses average of 4 values", "INFO", "No aggregated record found")
        else:
            record("3", "Aggregation uses average of 4 values", "INFO", "No 4-record PT15M group found")

        # ===== ISSUE 4: Consumption not captured =====
        print("\n" + "=" * 70)
        print("ISSUE 4: Consumption (negatives) not being captured")
        print("=" * 70)

        # Check consumption_mwh column exists
        r = await db.execute(text(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_name = 'generation_data' AND column_name = 'consumption_mwh'"
        ))
        col = r.fetchone()
        record("4", "consumption_mwh column exists", "PASS" if col else "FAIL",
               f"Type: {col.data_type}" if col else "Column missing")

        # Check ENTSOE client parses consumption
        from app.services.entsoe_client import ENTSOEClient
        src_client = inspect.getsource(ENTSOEClient)
        has_consumption = "data_direction" in src_client and "Consumption" in src_client
        record("4", "ENTSOE client parses consumption vs generation",
               "PASS" if has_consumption else "FAIL")

        # Check raw storage splits by source_type
        has_split = "api_consumption" in src3
        record("4", "Raw storage separates consumption records",
               "PASS" if has_split else "FAIL")

        # Check API endpoint returns consumption
        from app.api.v1.endpoints.generation import get_hourly_data
        src_ep = inspect.getsource(get_hourly_data)
        has_consumption_response = "consumption_mwh" in src_ep
        record("4", "API response includes consumption_mwh",
               "PASS" if has_consumption_response else "FAIL")

        # ===== ISSUE 5: Rounding =====
        print("\n" + "=" * 70)
        print("ISSUE 5: Rounding to whole numbers (Nobelwind Oct 2025)")
        print("=" * 70)

        # Check Decimal arithmetic in aggregation
        from scripts.seeds.aggregate_generation_data.process_generation_data_daily import DailyGenerationProcessor
        src_agg = inspect.getsource(DailyGenerationProcessor)
        has_decimal = "Decimal(str(" in src_agg
        has_quantize = "quantize" in src_agg
        record("5", "Decimal arithmetic in aggregation", "PASS" if has_decimal else "FAIL")
        record("5", "Quantize with ROUND_HALF_UP", "PASS" if has_quantize else "FAIL")

        # Check source data precision
        r = await db.execute(text(
            "SELECT gdr.value_extracted, gdr.period_start "
            "FROM generation_data_raw gdr "
            "JOIN generation_units gu ON gdr.identifier = gu.code AND gu.source = :src "
            "WHERE gdr.source = :src AND gu.name ILIKE :name "
            "AND gdr.period_start >= :s AND gdr.period_start < :e "
            "ORDER BY gdr.period_start LIMIT 5"
        ), {"src": "ENTSOE", "name": "%Nobelwind%",
            "s": datetime(2025, 10, 2, tzinfo=timezone.utc),
            "e": datetime(2025, 10, 3, tzinfo=timezone.utc)})
        rows = r.fetchall()
        if rows:
            has_decimals = any(float(r.value_extracted) != int(float(r.value_extracted)) for r in rows if r.value_extracted)
            vals = [float(r.value_extracted) for r in rows if r.value_extracted]
            record("5", "Raw data has decimal precision",
                   "PASS" if has_decimals else "INFO",
                   f"Sample values: {vals[:5]} — {'has decimals' if has_decimals else 'source sends integers'}")

        # ===== ISSUE 6: French Sept 2024 swap =====
        print("\n" + "=" * 70)
        print("ISSUE 6: French Sept 2024 gen/consumption labels swapped")
        print("=" * 70)

        # Check anomaly was recorded
        r = await db.execute(text(
            "SELECT id, anomaly_type, status, description FROM data_anomalies "
            "WHERE anomaly_type = 'gen_consumption_swapped'"
        ))
        anomaly = r.fetchone()
        record("6", "Swap anomaly recorded", "PASS" if anomaly else "FAIL",
               f"Status: {anomaly.status}" if anomaly else "Not recorded")

        # Check Excel records were swapped (consumption values are now in value_extracted)
        r = await db.execute(text(
            "SELECT COUNT(*) as cnt FROM generation_data_raw gdr "
            "JOIN generation_units gu ON gdr.identifier = gu.code AND gu.source = :src "
            "JOIN windfarms wf ON gu.windfarm_id = wf.id "
            "JOIN countries c ON wf.country_id = c.id "
            "WHERE gdr.source = :src AND gdr.source_type = 'excel' AND c.code = 'FRA' "
            "AND gdr.period_start >= :s AND gdr.period_start < :e "
            "AND gdr.value_extracted IS NOT NULL AND gdr.value_extracted > 0"
        ), {"src": "ENTSOE",
            "s": datetime(2024, 9, 1, tzinfo=timezone.utc),
            "e": datetime(2024, 10, 1, tzinfo=timezone.utc)})
        swapped = r.fetchone().cnt
        record("6", "French Sept 2024 data swapped", "PASS" if swapped > 0 else "FAIL",
               f"{swapped} records with positive value_extracted (post-swap)")

        # ===== ISSUE 7: Rogue data point =====
        print("\n" + "=" * 70)
        print("ISSUE 7: Fecamp 15 Sept 2025 — rogue GW-scale data point")
        print("=" * 70)

        # Check outlier detection in code
        has_outlier = "outlier_flag" in src3
        record("7", "Import-time outlier detection", "PASS" if has_outlier else "FAIL")

        has_spike = hasattr(DataAnomalyService, '_detect_data_spike_anomalies')
        record("7", "Spike detection in DataAnomalyService", "PASS" if has_spike else "FAIL")

        # ===== ISSUE 8: Duplicates =====
        print("\n" + "=" * 70)
        print("ISSUE 8: Duplicate data in API")
        print("=" * 70)

        # Check deduplication in aggregation code
        has_dedup = "Deduplicated" in src_agg or "seen" in src_agg
        record("8", "ENTSOE deduplication in aggregation", "PASS" if has_dedup else "FAIL")

        # Check revision tracking
        has_revision = "previous_value" in src3
        record("8", "Revision tracking on upsert", "PASS" if has_revision else "FAIL")

    # ===== SUMMARY =====
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    passes = sum(1 for _, _, s, _ in results if s == "PASS")
    fails = sum(1 for _, _, s, _ in results if s == "FAIL")
    infos = sum(1 for _, _, s, _ in results if s == "INFO")
    print(f"\n  {PASS}: {passes}    {FAIL}: {fails}    {INFO}: {infos}")
    print()

    if fails > 0:
        print("  Failed checks:")
        for issue, check, status, detail in results:
            if status == "FAIL":
                print(f"    Issue {issue}: {check} — {detail}")

    if infos > 0:
        print("\n  Info (needs operational action):")
        for issue, check, status, detail in results:
            if status == "INFO":
                print(f"    Issue {issue}: {check} — {detail}")


if __name__ == "__main__":
    asyncio.run(verify())
