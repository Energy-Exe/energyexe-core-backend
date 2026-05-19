"""Re-derive the 'NO ACTION' bucket distribution for the ~294 inactive units
the remediation classifier left alone, directly from the DB.

Each inactive unit falls into exactly one of:
  A. CSV stale         - unit is actually is_active=True now
  B. Empty scaffolding - 0 rows in generation_data
  C. Orphan data       - has rows, but all rows have windfarm_id IS NULL (invisible in UI)
  D. Parallel source   - has rows attributed to a windfarm, AND unit name shares a token
                         with that windfarm name (legitimate dual-feed)
  E. Mismatch          - has rows attributed to a windfarm whose name shares NO token
                         (could be a hidden mislink — worth a closer look)

Print counts + a sample of bucket E (the only one that might hide bugs).
"""
import asyncio
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sqlalchemy import text
from app.core.database import get_session_factory


# Same exclusions the original remediation script used so we report the same population
ENTSOE_MISLINKS = {12385, 12328, 12361, 12346, 12348, 12349, 12350, 12351}
DELETE_JUNK     = {12806}
RCBKO           = {12388, 12389}
ALREADY_DELETED = {12797, 12801, 12802}  # NVE Cat D (already gone from DB but listed for clarity)
HANDLED = ENTSOE_MISLINKS | DELETE_JUNK | RCBKO | ALREADY_DELETED


def tokens(s: str) -> set:
    s = (s or "").lower()
    s = re.sub(r"[^\w]+", " ", s, flags=re.UNICODE)
    return {w for w in s.split() if len(w) >= 4 and w not in {"wind", "farm", "park", "phase"}}


async def main():
    S = get_session_factory()
    async with S() as db:
        # All inactive units (the source set the original CSV came from)
        rs = await db.execute(text("""
            SELECT gu.id, gu.source, gu.code, gu.name, gu.is_active, gu.windfarm_id,
                   (SELECT COUNT(*) FROM generation_data WHERE generation_unit_id = gu.id) AS n_rows,
                   (SELECT COUNT(DISTINCT windfarm_id) FROM generation_data
                      WHERE generation_unit_id = gu.id AND windfarm_id IS NOT NULL) AS distinct_wfs,
                   (SELECT array_agg(DISTINCT windfarm_id) FROM generation_data
                      WHERE generation_unit_id = gu.id AND windfarm_id IS NOT NULL) AS wf_ids
            FROM generation_units gu
            WHERE gu.is_active = FALSE
        """))
        all_inactive = list(rs)
        print(f"\nTotal is_active=FALSE units in DB right now: {len(all_inactive)}")

        # Pull windfarm names once
        wf_q = await db.execute(text("SELECT id, name FROM windfarms"))
        wf_name = {r.id: r.name for r in wf_q}

        buckets = {
            "A_csv_stale": [],
            "B_empty": [],
            "C_orphan_null_wf": [],
            "D_parallel_source": [],
            "E_token_mismatch": [],
        }

        for u in all_inactive:
            if u.id in HANDLED:
                continue
            if u.is_active:  # belt-and-braces (shouldn't happen given WHERE clause)
                buckets["A_csv_stale"].append(u)
                continue
            if u.n_rows == 0:
                buckets["B_empty"].append(u)
                continue
            if u.distinct_wfs == 0:
                buckets["C_orphan_null_wf"].append(u)
                continue

            # Has rows on at least one windfarm — check name match
            wids = sorted(u.wf_ids or [])
            ut = tokens(u.name)
            wf_names = [(wid, wf_name.get(wid, "?")) for wid in wids]
            target_token_match = any((ut and (ut & tokens(wn))) for _, wn in wf_names)
            if target_token_match:
                buckets["D_parallel_source"].append((u, wf_names))
            else:
                buckets["E_token_mismatch"].append((u, wf_names))

        print(f"\nBreakdown (excluding the {len(HANDLED)} already-handled units):")
        print(f"  A. CSV stale (now active)      : {len(buckets['A_csv_stale']):>4}")
        print(f"  B. Empty scaffolding (0 rows)  : {len(buckets['B_empty']):>4}")
        print(f"  C. Orphan data (wf IS NULL)    : {len(buckets['C_orphan_null_wf']):>4}")
        print(f"  D. Parallel-source (name match): {len(buckets['D_parallel_source']):>4}")
        print(f"  E. Token mismatch (LOOK HERE)  : {len(buckets['E_token_mismatch']):>4}")
        total = sum(len(v) for v in buckets.values())
        print(f"  {'-'*40}")
        print(f"  TOTAL                          : {total:>4}")

        # Bucket A — by source
        print("\n[A] CSV stale — units that look like they flipped back to active:")
        by_src = {}
        for u in buckets["A_csv_stale"]:
            by_src[u.source] = by_src.get(u.source, 0) + 1
        for k, v in sorted(by_src.items()): print(f"    {k}: {v}")

        # Bucket B — by source
        print("\n[B] Empty scaffolding — by source:")
        by_src = {}
        for u in buckets["B_empty"]:
            by_src[u.source] = by_src.get(u.source, 0) + 1
        for k, v in sorted(by_src.items()): print(f"    {k}: {v}")

        # Bucket C — by source + total row count
        print("\n[C] Orphan data (windfarm_id IS NULL) — by source:")
        by_src = {}
        for u in buckets["C_orphan_null_wf"]:
            by_src.setdefault(u.source, [0, 0])
            by_src[u.source][0] += 1
            by_src[u.source][1] += u.n_rows
        for k, (n, rows) in sorted(by_src.items()):
            print(f"    {k}: {n} units, {rows:,} orphan gen_data rows")

        print("\n[C] sample:")
        for u in buckets["C_orphan_null_wf"][:10]:
            print(f"    {u.id} '{u.name[:45]}' src={u.source} rows={u.n_rows:,}")

        # Bucket D — by source
        print("\n[D] Parallel source (name token match) — by source:")
        by_src = {}
        for u, _ in buckets["D_parallel_source"]:
            by_src[u.source] = by_src.get(u.source, 0) + 1
        for k, v in sorted(by_src.items()): print(f"    {k}: {v}")

        # Bucket E — the one we need to inspect carefully
        print(f"\n[E] Token mismatch — {len(buckets['E_token_mismatch'])} units to inspect:")
        print(f"    {'id':>6} {'src':<10} {'unit name':<42} {'rows':>8} wf attribution")
        print(f"    {'-'*100}")
        for u, wf_names in buckets["E_token_mismatch"]:
            wf_str = ", ".join(f"{wid}={n!r}" for wid, n in wf_names)
            print(f"    {u.id:>6} {u.source:<10} {u.name[:40]:<42} {u.n_rows:>8,}  {wf_str[:50]}")


asyncio.run(main())
