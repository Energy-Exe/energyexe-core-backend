"""Investigate unit 10103 Dalry (ELEXON) — has -358,775 MWh on wf 7282.

Questions:
  1. What does the windfarm record say it is? (Tech type, capacity, country, status.)
  2. What's the per-year distribution of the negative generation? Constant, or
     localised to a period?
  3. How many rows have generation_mwh < 0 vs >= 0? Is it 100% negative or mixed?
  4. What's the raw ELEXON payload look like? Is it negative there too (data
     source is wrong), or positive (ingestion is sign-flipping)?
  5. Are there other ELEXON units with > 0 negative-generation rows? If yes,
     scale of potential systemic bug.
  6. What identifier is used? (BMU code — can we tell if it's wind or BESS?)
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
        # 1. Unit 10120 (Dalry) + windfarm 7282 metadata
        banner("1. Unit 10120 (Dalry) + windfarm metadata")
        rs = await db.execute(text("""
            SELECT gu.id, gu.source, gu.code, gu.name, gu.is_active,
                   gu.capacity_mw::float AS cap, gu.windfarm_id,
                   gu.start_date, gu.end_date, gu.first_power_date,
                   gu.created_at, gu.updated_at
            FROM generation_units gu WHERE id = 10120
        """))
        u = rs.first()
        print(f"  unit 10120 '{u.name}' code={u.code} src={u.source}")
        print(f"    is_active={u.is_active} cap={u.cap} wf={u.windfarm_id}")
        print(f"    start={u.start_date} fpd={u.first_power_date} end={u.end_date}")
        print(f"    created={u.created_at} updated={u.updated_at}")

        rs = await db.execute(text("""
            SELECT id, name, code, country_id, status, location_type, foundation_type,
                   nameplate_capacity_mw::float AS cap, first_power_date,
                   commercial_operational_date AS cod, bidzone_id, alternate_name
            FROM windfarms WHERE id = 7282
        """))
        w = rs.first()
        if w:
            print(f"\n  windfarm 7282 '{w.name}' (alt='{w.alternate_name}') code={w.code} country={w.country_id}")
            print(f"    status={w.status} location={w.location_type} foundation={w.foundation_type}")
            print(f"    cap={w.cap}MW fpd={w.first_power_date} cod={w.cod} bidzone={w.bidzone_id}")

        # 2. Per-year breakdown of generation_mwh for Dalry
        banner("2. Per-year generation_mwh distribution (unit 10120)")
        rs = await db.execute(text("""
            SELECT EXTRACT(YEAR FROM hour)::int AS yr,
                   COUNT(*) AS n_rows,
                   SUM(generation_mwh)::float AS total_gen,
                   COUNT(*) FILTER (WHERE generation_mwh < 0) AS n_neg,
                   COUNT(*) FILTER (WHERE generation_mwh > 0) AS n_pos,
                   COUNT(*) FILTER (WHERE generation_mwh = 0) AS n_zero,
                   MIN(generation_mwh)::float AS min_g, MAX(generation_mwh)::float AS max_g,
                   AVG(generation_mwh)::float AS avg_g
            FROM generation_data
            WHERE generation_unit_id = 10120
            GROUP BY 1 ORDER BY 1
        """))
        print(f"  {'year':<6}{'rows':>8}{'pos':>8}{'neg':>8}{'zero':>8}"
              f"{'sum':>14}{'min':>10}{'max':>10}{'avg':>10}")
        for r in rs:
            print(f"  {r.yr:<6}{r.n_rows:>8,}{r.n_pos:>8,}{r.n_neg:>8,}{r.n_zero:>8,}"
                  f"{r.total_gen:>14,.0f}{r.min_g:>10,.2f}{r.max_g:>10,.2f}{r.avg_g:>10,.2f}")

        # 3. Capacity factor sanity check
        banner("3. Hourly sample (last 30 rows)")
        rs = await db.execute(text("""
            SELECT hour, generation_mwh::float AS g, capacity_factor::float AS cf,
                   capacity_mw::float AS cap, metered_mwh::float AS m
            FROM generation_data WHERE generation_unit_id = 10120
            ORDER BY hour DESC LIMIT 30
        """))
        print(f"  {'hour':<25}{'gen_mwh':>10}{'metered':>10}{'cf':>8}{'cap_mw':>8}")
        for r in rs:
            print(f"  {str(r.hour)[:19]:<25}{r.g:>10.2f}"
                  f"{(r.m if r.m is not None else 0):>10.2f}"
                  f"{(r.cf if r.cf is not None else 0):>8.4f}"
                  f"{(r.cap if r.cap is not None else 0):>8.1f}")

        # 4. Look at raw ELEXON for Dalry
        banner("4. Raw ELEXON identifier for Dalry (find the BMU code)")
        rs = await db.execute(text("""
            SELECT gum.source, gum.source_identifier, gum.is_active, gum.windfarm_id
            FROM generation_unit_mapping gum WHERE gum.generation_unit_id = 10120
        """))
        for r in rs:
            print(f"  mapping: src={r.source} ident='{r.source_identifier}' "
                  f"active={r.is_active} wf={r.windfarm_id}")

        # Look in raw for that identifier
        rs = await db.execute(text("""
            SELECT identifier, source_type, COUNT(*) AS n,
                   MIN(period_start) AS first_pt, MAX(period_start) AS last_pt,
                   AVG((data->>'quantity')::float)::float AS avg_q,
                   MIN((data->>'quantity')::float)::float AS min_q,
                   MAX((data->>'quantity')::float)::float AS max_q
            FROM generation_data_raw
            WHERE source='ELEXON' AND identifier ILIKE '%dalry%' OR identifier='T_DALRY'
            GROUP BY identifier, source_type ORDER BY identifier
        """))
        for r in rs:
            print(f"  raw id='{r.identifier}' type={r.source_type} rows={r.n:,} "
                  f"({r.first_pt} → {r.last_pt}) avg_q={r.avg_q} "
                  f"min={r.min_q} max={r.max_q}")

        # 5. Are there OTHER ELEXON units with any negative-gen rows? Quantify scale.
        banner("5. Other ELEXON units with negative-generation rows")
        rs = await db.execute(text("""
            SELECT gd.generation_unit_id, gu.name, gu.code, gu.is_active, gu.windfarm_id,
                   COUNT(*) AS n_neg,
                   MIN(gd.generation_mwh)::float AS min_g,
                   SUM(gd.generation_mwh)::float AS sum_neg_gen
            FROM generation_data gd
            JOIN generation_units gu ON gu.id = gd.generation_unit_id
            WHERE gd.source = 'ELEXON' AND gd.generation_mwh < 0
            GROUP BY gd.generation_unit_id, gu.name, gu.code, gu.is_active, gu.windfarm_id
            ORDER BY n_neg DESC LIMIT 30
        """))
        rows = list(rs)
        print(f"  Top {len(rows)} ELEXON units with any negative-generation rows:")
        print(f"  {'unit':>6}{'active':>8}{'wf':>6}  {'name':<30}{'code':<14}{'n_neg':>10}{'min_mwh':>12}{'sum_neg':>14}")
        for r in rows:
            print(f"  {r.generation_unit_id:>6}{str(r.is_active):>8}{(r.windfarm_id or 0):>6}  "
                  f"{r.name[:28]:<30}{(r.code or '')[:12]:<14}"
                  f"{r.n_neg:>10,}{r.min_g:>12,.2f}{(r.sum_neg_gen or 0):>14,.0f}")

        # 6. Total scale of negative ELEXON generation (sum across all units)
        banner("6. Aggregate scale of negative ELEXON generation")
        rs = await db.execute(text("""
            SELECT COUNT(*) AS n_rows, COUNT(DISTINCT generation_unit_id) AS n_units,
                   SUM(generation_mwh)::float AS sum_neg
            FROM generation_data WHERE source='ELEXON' AND generation_mwh < 0
        """))
        r = rs.first()
        print(f"  Total ELEXON rows with gen < 0: {r.n_rows:,}")
        print(f"  Across {r.n_units} distinct units")
        print(f"  Cumulative negative MWh: {r.sum_neg:,.0f}")

        # 7. Does the windfarm row look like wind or storage? Check capacity vs gen.
        banner("7. Other units on wf 7282 (Dalry)")
        rs = await db.execute(text("""
            SELECT gu.id, gu.source, gu.code, gu.name, gu.is_active,
                   gu.capacity_mw::float AS cap,
                   (SELECT COUNT(*) FROM generation_data WHERE generation_unit_id = gu.id) AS rows,
                   (SELECT SUM(generation_mwh)::float FROM generation_data WHERE generation_unit_id = gu.id) AS gen
            FROM generation_units gu WHERE windfarm_id = 7282
        """))
        for r in rs:
            print(f"  unit {r.id} '{r.name[:30]:<30}' src={r.source} active={r.is_active} "
                  f"cap={r.cap} rows={r.rows:,} gen={(r.gen or 0):>12,.0f}")

        # 8. What does the ENTSOE/other source say about wf 7282?
        rs = await db.execute(text("""
            SELECT source, COUNT(*) AS n_rows, SUM(generation_mwh)::float AS gen
            FROM generation_data WHERE windfarm_id = 7282 GROUP BY source ORDER BY source
        """))
        print(f"\n  All gen_data on wf 7282 by source:")
        for r in rs:
            print(f"    src={r.source} rows={r.n_rows:,} gen={(r.gen or 0):,.0f}")


asyncio.run(main())
