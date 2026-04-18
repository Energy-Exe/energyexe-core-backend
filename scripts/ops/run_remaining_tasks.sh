#!/usr/bin/env bash
# ─── Run remaining operational tasks from EC2 ───────────────────
#
# This script covers the 5 pending tasks that require low-latency
# DB access (same VPC as RDS, eu-north-1).
#
# Prerequisites:
#   - EC2 instance with repo cloned + poetry installed
#   - DATABASE_URL set in .env or env
#   - CDS API credentials for weather re-import (CDSAPI_URL, CDSAPI_KEY)
#
# Usage:
#   ssh your-ec2-instance
#   cd energyexe-core-backend
#   bash scripts/ops/run_remaining_tasks.sh
#
# Each step is idempotent — safe to re-run after partial failure.

set -euo pipefail

DB_URL="${DATABASE_URL:-$(grep DATABASE_URL .env | cut -d= -f2- | tr -d '"')}"
DB_URL_PG="${DB_URL//postgresql+asyncpg/postgresql}"

echo "═══════════════════════════════════════════════════════════"
echo "  EnergyExe — Remaining operational tasks"
echo "  $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo "═══════════════════════════════════════════════════════════"

# ─── Step 1: Backfill generation concentration for 229 windfarms ──
echo ""
echo "─── Step 1: Generation Concentration backfill ─────────────"
echo "Computing capture ratio + decile shares for all windfarms"
echo "that already have power curves..."

poetry run python3 -c "
import asyncio, os, time
os.environ['TESTING'] = 'false'

async def main():
    from app.core.database import get_session_factory, init_db
    await init_db()
    factory = get_session_factory()
    from sqlalchemy import text

    async with factory() as db:
        rows = await db.execute(text(
            'SELECT DISTINCT windfarm_id FROM power_curve_bins ORDER BY windfarm_id'
        ))
        wf_ids = [r[0] for r in rows.fetchall()]

    async with factory() as db:
        year_rows = await db.execute(text('''
            SELECT DISTINCT windfarm_id, year FROM performance_summaries
            WHERE period_type = \\'year\\' ORDER BY windfarm_id, year
        '''))
        wf_years = {}
        for r in year_rows.fetchall():
            wf_years.setdefault(r[0], []).append(r[1])

    from app.services.generation_concentration_service import GenerationConcentrationService
    ok = err = skip = 0
    t0 = time.time()
    for i, wf_id in enumerate(wf_ids):
        for year in wf_years.get(wf_id, []):
            try:
                async with factory() as db:
                    svc = GenerationConcentrationService(db)
                    result = await svc.compute_for_windfarm(wf_id, year)
                    if 'error' not in result:
                        await db.commit()
                        ok += 1
                    else:
                        skip += 1
            except Exception as exc:
                err += 1
                if err <= 3: print(f'  ERR wf={wf_id} yr={year}: {exc}')
        if (i+1) % 25 == 0:
            print(f'  {i+1}/{len(wf_ids)} ({ok} ok, {skip} skip, {err} err, {time.time()-t0:.0f}s)')
    print(f'Done: {ok} inserted, {skip} skipped, {err} errors in {time.time()-t0:.0f}s')

asyncio.run(main())
"

echo ""
echo "─── Step 2: ERA5 NaN DELETE (chunked by year) ─────────────"
echo "Deleting ~98M NaN wind_speed_100m rows from weather_data..."

for YEAR in $(seq 2017 2025); do
    echo -n "  Year $YEAR: "
    RESULT=$(psql "$DB_URL_PG" -t -c "
        DELETE FROM weather_data
        WHERE EXTRACT(YEAR FROM hour) = $YEAR
          AND wind_speed_100m::text = 'NaN';
    " 2>&1)
    echo "$RESULT"
done

echo "  Checking remaining NaN rows..."
psql "$DB_URL_PG" -c "
    SELECT COUNT(*) AS remaining_nan_rows
    FROM weather_data
    WHERE wind_speed_100m::text = 'NaN'
    LIMIT 1;
"

echo ""
echo "─── Step 3: P50 targets (re-run for newly available data) ─"
echo "Re-computing P50 fallback for windfarms that now have enough data..."

poetry run python3 -c "
import asyncio, asyncpg

async def main():
    conn = await asyncpg.connect('$DB_URL_PG', command_timeout=300)
    await conn.execute(\"SET statement_timeout = '300000'\")

    result = await conn.execute('''
        WITH yearly_gen AS (
            SELECT windfarm_id, EXTRACT(YEAR FROM hour)::int AS yr,
                   SUM(generation_mwh)/1000.0 AS gwh,
                   COUNT(DISTINCT DATE_TRUNC(\\'day\\', hour)) AS day_count
            FROM generation_data WHERE generation_mwh IS NOT NULL
              AND EXTRACT(YEAR FROM hour) BETWEEN EXTRACT(YEAR FROM CURRENT_DATE)-3
                                               AND EXTRACT(YEAR FROM CURRENT_DATE)-1
            GROUP BY windfarm_id, EXTRACT(YEAR FROM hour)
            HAVING COUNT(DISTINCT DATE_TRUNC(\\'day\\', hour)) >= 350
        ), windfarm_avg AS (
            SELECT windfarm_id, ROUND(AVG(gwh)::numeric,3) AS mean_gwh,
                   MAX(yr) AS latest_year, COUNT(*) AS years_used
            FROM yearly_gen GROUP BY windfarm_id HAVING COUNT(*) >= 2
        ), missing AS (
            SELECT w.id FROM windfarms w
            WHERE w.lat IS NOT NULL AND w.lng IS NOT NULL AND w.status=\\'operational\\'
              AND NOT EXISTS (SELECT 1 FROM p50_targets p WHERE p.windfarm_id=w.id)
        )
        INSERT INTO p50_targets (windfarm_id, p50_target_start_date,
            p50_target_volume_gwh, source, comment)
        SELECT a.windfarm_id, MAKE_DATE(a.latest_year,1,1), a.mean_gwh,
            \\'fallback computed (3-yr historical mean)\\',
            FORMAT(\\'auto-computed from %s-%s\\',a.latest_year-2,a.latest_year)
        FROM windfarm_avg a JOIN missing m ON m.id=a.windfarm_id
        ON CONFLICT (windfarm_id, p50_target_start_date) DO NOTHING
    ''')
    print(f'P50 insert result: {result}')

    cov = await conn.fetchval('SELECT COUNT(DISTINCT windfarm_id) FROM p50_targets')
    total = await conn.fetchval(\"SELECT COUNT(*) FROM windfarms WHERE lat IS NOT NULL\")
    print(f'P50 coverage: {cov}/{total} ({100.0*cov/total:.1f}%)')
    await conn.close()

asyncio.run(main())
"

echo ""
echo "─── Step 4: Re-run pipeline for unblocked windfarms ───────"
echo "After ERA5 NaN cleanup, weather re-import is needed first."
echo "Run: poetry run python scripts/backfill_pipeline.py"
echo "(Requires CDS API re-import to complete — see docs/spec_items_1_to_6_plan.md)"

echo ""
echo "═══════════════════════════════════════════════════════════"
echo "  Steps 1-3 complete. Step 4 requires CDS API re-import."
echo "  $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo "═══════════════════════════════════════════════════════════"
