"""Investigate unit 12803 'Valsneset testpark' vs active sibling 12782 'Valsneset'
on wf 7224. From Bucket D audit: 24,072 overlapping hours, 78% value drift.

Goal: determine whether 12803 is
  (a) a separate physical asset (test turbines) bleeding into wf 7224's totals
      via source-grouped aggregation → detach, OR
  (b) duplicate ingestion of the same data with sign/scale bug → fix one, OR
  (c) historical pre-commercial data of the same site → fine to leave once we
      verify no double-count exists in the aggregator path.

Checks:
  1. Metadata of both units + the windfarm.
  2. Is there a separate Valsneset testpark windfarm row in `windfarms`?
  3. NVE codes (per-farm stable) — do they match? Different codes => different farms.
  4. Time-range comparison: when does each unit have data, when do they overlap.
  5. Side-by-side hourly comparison for overlap window (sample).
  6. Per-hour group-by-source aggregation on wf 7224 — does the value double up?
  7. Check raw NVE data for each code; see what NVE actually publishes.
"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sqlalchemy import text
from app.core.database import get_session_factory


def banner(t):
    print()
    print("=" * 100)
    print(t)
    print("=" * 100)


async def main():
    S = get_session_factory()
    async with S() as db:
        # 1. Unit metadata for both 12803 and 12782
        banner("1. Unit metadata (12803 testpark vs 12782 active)")
        rs = await db.execute(text("""
            SELECT id, source, code, name, is_active, capacity_mw::float AS cap,
                   windfarm_id, start_date, end_date, first_power_date,
                   created_at, updated_at
            FROM generation_units WHERE id IN (12782, 12803) ORDER BY id
        """))
        for r in rs:
            print(f"\n  unit {r.id} '{r.name}' code={r.code} src={r.source}")
            print(f"    is_active={r.is_active} cap={r.cap}MW wf={r.windfarm_id}")
            print(f"    start={r.start_date} fpd={r.first_power_date} end={r.end_date}")
            print(f"    created={r.created_at} updated={r.updated_at}")

        # 2. Windfarm row for wf 7224
        banner("2. Windfarm 7224 metadata + any 'Valsneset testpark' windfarm")
        rs = await db.execute(text("""
            SELECT id, name, code, country_id, status, location_type, foundation_type,
                   nameplate_capacity_mw::float AS cap, first_power_date,
                   commercial_operational_date AS cod, bidzone_id, alternate_name
            FROM windfarms WHERE id = 7224 OR name ILIKE '%valsneset%'
            ORDER BY id
        """))
        for r in rs:
            print(f"\n  wf {r.id} '{r.name}' (alt='{r.alternate_name}') code={r.code}")
            print(f"    country={r.country_id} status={r.status} location={r.location_type}")
            print(f"    cap={r.cap}MW fpd={r.first_power_date} cod={r.cod}")

        # 3. All NVE units sharing the same code as each — sibling search
        banner("3. NVE units sharing codes with 12782 / 12803")
        rs = await db.execute(text("""
            SELECT id, code, name, is_active, capacity_mw::float AS cap, windfarm_id,
                   (SELECT COUNT(*) FROM generation_data WHERE generation_unit_id = gu.id) AS rows
            FROM generation_units gu
            WHERE source = 'NVE' AND code IN (
                SELECT code FROM generation_units WHERE id IN (12782, 12803)
            )
            ORDER BY code, id
        """))
        for r in rs:
            print(f"  id={r.id} code={r.code} '{r.name[:40]:<42}' "
                  f"active={r.is_active} cap={r.cap} wf={r.windfarm_id} rows={r.rows:,}")

        # 4. Time ranges + overlap matrix
        banner("4. Time-range and per-unit/year row counts")
        rs = await db.execute(text("""
            SELECT generation_unit_id, EXTRACT(YEAR FROM hour)::int AS yr,
                   COUNT(*) AS n_rows, SUM(generation_mwh)::float AS gen
            FROM generation_data
            WHERE generation_unit_id IN (12782, 12803)
            GROUP BY 1, 2 ORDER BY 1, 2
        """))
        print(f"  {'unit':>6}{'year':>6}{'rows':>10}{'gen_mwh':>14}")
        for r in rs:
            print(f"  {r.generation_unit_id:>6}{r.yr:>6}{r.n_rows:>10,}{(r.gen or 0):>14,.0f}")

        # 5. Overlap and per-hour comparison sample
        banner("5. Hour overlap + sample of overlapping hours (20 rows)")
        rs = await db.execute(text("""
            SELECT a.hour,
                   a.generation_mwh::float AS gen_a, a.capacity_factor::float AS cf_a,
                   b.generation_mwh::float AS gen_b, b.capacity_factor::float AS cf_b
            FROM generation_data a
            JOIN generation_data b ON a.hour = b.hour AND b.generation_unit_id = 12803
            WHERE a.generation_unit_id = 12782
            ORDER BY a.hour DESC
            LIMIT 20
        """))
        rows = list(rs)
        print(f"  Showing latest {len(rows)} overlapping hours:")
        print(f"  {'hour':<22}{'12782_gen':>12}{'12782_cf':>10}{'12803_gen':>12}{'12803_cf':>10}{'ratio':>10}")
        for r in rows:
            ratio = (r.gen_b / r.gen_a) if r.gen_a not in (None, 0) else None
            r_str = f"{ratio:.2f}" if ratio is not None else "-"
            print(f"  {str(r.hour)[:19]:<22}{r.gen_a:>12.2f}{(r.cf_a or 0):>10.3f}"
                  f"{r.gen_b:>12.2f}{(r.cf_b or 0):>10.3f}{r_str:>10}")

        # 6. Total overlap stats
        banner("6. Overlap aggregate stats")
        rs = await db.execute(text("""
            SELECT COUNT(*) AS n_overlap,
                   SUM(a.generation_mwh)::float AS sum_a,
                   SUM(b.generation_mwh)::float AS sum_b,
                   MIN(a.hour) AS lo, MAX(a.hour) AS hi
            FROM generation_data a
            JOIN generation_data b ON a.hour = b.hour AND b.generation_unit_id = 12803
            WHERE a.generation_unit_id = 12782
        """))
        r = rs.first()
        print(f"  Overlapping hours: {r.n_overlap:,}")
        print(f"    range: {r.lo} → {r.hi}")
        print(f"    SUM 12782 gen in overlap: {r.sum_a:,.0f}")
        print(f"    SUM 12803 gen in overlap: {r.sum_b:,.0f}")
        ratio_total = (r.sum_b / r.sum_a) if r.sum_a else None
        if ratio_total is not None:
            print(f"    ratio 12803/12782 ≈ {ratio_total:.3f}")

        # 7. Does the windfarm chart double-count? Simulate the aggregator.
        banner("7. Aggregator simulation: group by (hour, source) on wf 7224 for 2013-2015")
        rs = await db.execute(text("""
            SELECT date_trunc('year', hour) AS yr, source,
                   COUNT(*) AS n_rows,
                   SUM(generation_mwh)::float AS gen
            FROM generation_data
            WHERE windfarm_id = 7224
              AND hour >= '2013-01-01' AND hour < '2016-01-01'
            GROUP BY 1, 2 ORDER BY 1, 2
        """))
        print(f"  Per-year sum across BOTH units (12782 + 12803) attributed to wf 7224:")
        print(f"  {'year':<14}{'source':<10}{'n_rows':>10}{'gen_mwh':>14}")
        for r in rs:
            print(f"  {str(r.yr)[:10]:<14}{r.source:<10}{r.n_rows:>10,}{(r.gen or 0):>14,.0f}")

        # 7b. Compare against per-unit sum (truth without double-count concern)
        rs = await db.execute(text("""
            SELECT date_trunc('year', hour) AS yr, generation_unit_id,
                   COUNT(*) AS n_rows, SUM(generation_mwh)::float AS gen
            FROM generation_data
            WHERE windfarm_id = 7224 AND generation_unit_id IN (12782, 12803)
              AND hour >= '2013-01-01' AND hour < '2016-01-01'
            GROUP BY 1, 2 ORDER BY 1, 2
        """))
        print(f"\n  Per-year per-unit:")
        print(f"  {'year':<14}{'unit':>6}{'n_rows':>10}{'gen_mwh':>14}")
        for r in rs:
            print(f"  {str(r.yr)[:10]:<14}{r.generation_unit_id:>6}{r.n_rows:>10,}{(r.gen or 0):>14,.0f}")

        # 8. Raw NVE data for both codes
        banner("8. Raw NVE data for code on 12782 and 12803")
        rs = await db.execute(text("""
            SELECT identifier, source_type, COUNT(*) AS n_rows,
                   MIN(period_start) AS lo, MAX(period_start) AS hi
            FROM generation_data_raw
            WHERE source = 'NVE' AND identifier IN (
                SELECT code FROM generation_units WHERE id IN (12782, 12803)
            )
            GROUP BY identifier, source_type ORDER BY identifier, source_type
        """))
        for r in rs:
            print(f"  id={r.identifier:<8} type={r.source_type:<10} rows={r.n_rows:,} "
                  f"({r.lo} → {r.hi})")

        # 9. Look at unit 12782's data starting hour (does it really overlap with 12803?)
        banner("9. First/last hour per unit")
        rs = await db.execute(text("""
            SELECT generation_unit_id,
                   MIN(hour) AS lo, MAX(hour) AS hi, COUNT(*) AS n,
                   SUM(generation_mwh)::float AS gen
            FROM generation_data WHERE generation_unit_id IN (12782, 12803)
            GROUP BY 1 ORDER BY 1
        """))
        for r in rs:
            print(f"  unit {r.generation_unit_id}: {r.lo} → {r.hi}  rows={r.n:,}  gen={r.gen:,.0f}")


asyncio.run(main())
