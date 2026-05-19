"""Deep audit of Bucket D — the 94 inactive 'parallel-source' units.

For each Bucket D unit we ask:
  1. Does an ACTIVE unit exist on the same (windfarm, source)? If not, this is
     suspicious — the inactive unit is the only feed of that source for that wf.
  2. Does the inactive unit's hour set OVERLAP with the active unit's hour set?
     Overlap → potential double-count in source-grouped aggregations.
  3. When overlap exists, do values MATCH within tolerance, or do they drift?
     A clean parallel source should agree to a few %.
  4. Date range of the inactive unit's data — is it pre-active (historical fill)
     or contemporaneous (running redundant feed)?

Categorize each into:
  D1  diff-source parallel (e.g. inactive ENTSOE next to active ELEXON) — clean
  D2  same-source historical (inactive ends before active starts) — clean
  D3  same-source overlap, values agree    — likely fine but double-count risk
  D4  same-source overlap, values disagree — bug suspect
  D5  no active sibling                    — needs decision
"""
import asyncio
import re
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sqlalchemy import text
from app.core.database import get_session_factory


HANDLED = {12385, 12328, 12361, 12346, 12348, 12349, 12350, 12351,
           12806, 12388, 12389}


def tokens(s: str) -> set:
    s = (s or "").lower()
    s = re.sub(r"[^\w]+", " ", s, flags=re.UNICODE)
    return {w for w in s.split() if len(w) >= 4 and w not in {"wind", "farm", "park", "phase"}}


def banner(t):
    print()
    print("=" * 100)
    print(t)
    print("=" * 100)


async def main():
    S = get_session_factory()
    async with S() as db:
        wf_q = await db.execute(text("SELECT id, name FROM windfarms"))
        wf_name = {r.id: r.name for r in wf_q}

        # Pull all inactive units that fall into Bucket D (has rows + token match)
        rs = await db.execute(text("""
            SELECT gu.id, gu.source, gu.name, gu.code, gu.capacity_mw::float AS cap,
                   gu.windfarm_id AS unit_wf,
                   (SELECT COUNT(*) FROM generation_data WHERE generation_unit_id = gu.id) AS n_rows,
                   (SELECT array_agg(DISTINCT windfarm_id) FROM generation_data
                      WHERE generation_unit_id = gu.id AND windfarm_id IS NOT NULL) AS wf_ids
            FROM generation_units gu
            WHERE gu.is_active = FALSE
              AND gu.id <> ALL(:handled)
              AND EXISTS (SELECT 1 FROM generation_data WHERE generation_unit_id = gu.id)
        """), {"handled": list(HANDLED)})
        candidates = []
        for u in rs:
            wids = sorted(u.wf_ids or [])
            if not wids:
                continue
            ut = tokens(u.name)
            wf_names = [(wid, wf_name.get(wid, "?")) for wid in wids]
            if any((ut and (ut & tokens(wn))) for _, wn in wf_names):
                candidates.append((u, wf_names))
        print(f"Bucket D candidates: {len(candidates)}")

        # Categorise
        cats = {"D1": [], "D2": [], "D3": [], "D4": [], "D5": []}
        details_per_unit = []

        for u, wfs in candidates:
            primary_wf = wfs[0][0]

            # Inactive unit's data range + source
            rs = await db.execute(text("""
                SELECT MIN(hour) AS lo, MAX(hour) AS hi, source,
                       SUM(generation_mwh)::float AS gen
                FROM generation_data
                WHERE generation_unit_id = :u
                  AND windfarm_id = :w
                GROUP BY source
                ORDER BY source
            """), {"u": u.id, "w": primary_wf})
            inactive_periods = list(rs)
            if not inactive_periods:
                continue
            inactive_src = inactive_periods[0].source
            i_lo, i_hi, i_gen = inactive_periods[0].lo, inactive_periods[0].hi, inactive_periods[0].gen

            # Active siblings on same windfarm
            rs = await db.execute(text("""
                SELECT gu.id, gu.source, gu.name, gu.is_active,
                       MIN(gd.hour) AS lo, MAX(gd.hour) AS hi,
                       COUNT(*) AS n, SUM(gd.generation_mwh)::float AS gen
                FROM generation_units gu
                JOIN generation_data gd ON gd.generation_unit_id = gu.id
                WHERE gu.is_active = TRUE
                  AND gd.windfarm_id = :w
                GROUP BY gu.id, gu.source, gu.name, gu.is_active
                ORDER BY gu.source
            """), {"w": primary_wf})
            actives = list(rs)
            active_same_src = [a for a in actives if a.source == inactive_src]

            entry = {
                "unit": u, "wf_id": primary_wf, "wf_name": wfs[0][1],
                "inactive_src": inactive_src, "inactive_lo": i_lo, "inactive_hi": i_hi,
                "inactive_gen": i_gen, "inactive_rows": u.n_rows,
                "actives": actives, "same_src_actives": active_same_src,
            }

            if not actives:
                cats["D5"].append(entry)
                details_per_unit.append(("D5", entry))
                continue
            if not active_same_src:
                cats["D1"].append(entry)
                details_per_unit.append(("D1", entry))
                continue

            # Same-source active — check time overlap on shared hours
            rs = await db.execute(text("""
                WITH inactive AS (
                    SELECT hour, generation_mwh::float AS g
                    FROM generation_data
                    WHERE generation_unit_id = :u AND windfarm_id = :w
                ),
                active AS (
                    SELECT gd.hour, gd.generation_mwh::float AS g
                    FROM generation_data gd
                    JOIN generation_units gu ON gu.id = gd.generation_unit_id
                    WHERE gu.is_active = TRUE
                      AND gd.windfarm_id = :w
                      AND gd.source = :s
                )
                SELECT
                  (SELECT COUNT(*) FROM inactive) AS n_inactive,
                  (SELECT COUNT(*) FROM active) AS n_active,
                  COUNT(*) FILTER (WHERE i.hour IS NOT NULL AND a.hour IS NOT NULL) AS n_overlap,
                  SUM(CASE WHEN i.hour IS NOT NULL AND a.hour IS NOT NULL
                           THEN ABS(COALESCE(i.g,0) - COALESCE(a.g,0)) END)::float AS abs_diff,
                  SUM(CASE WHEN i.hour IS NOT NULL AND a.hour IS NOT NULL
                           THEN ABS(COALESCE(i.g,0)) END)::float AS abs_i,
                  SUM(CASE WHEN i.hour IS NOT NULL AND a.hour IS NOT NULL
                           THEN ABS(COALESCE(a.g,0)) END)::float AS abs_a
                FROM inactive i
                FULL OUTER JOIN active a ON i.hour = a.hour
            """), {"u": u.id, "w": primary_wf, "s": inactive_src})
            ov = rs.first()
            entry["n_overlap"] = ov.n_overlap or 0
            entry["abs_diff"] = ov.abs_diff or 0
            entry["abs_i"] = ov.abs_i or 0
            entry["abs_a"] = ov.abs_a or 0

            if entry["n_overlap"] == 0:
                cats["D2"].append(entry)
                details_per_unit.append(("D2", entry))
            else:
                # values match within 5% of larger side?
                denom = max(entry["abs_i"], entry["abs_a"], 1e-9)
                drift = entry["abs_diff"] / denom
                entry["drift_pct"] = drift * 100
                if drift <= 0.05:
                    cats["D3"].append(entry)
                    details_per_unit.append(("D3", entry))
                else:
                    cats["D4"].append(entry)
                    details_per_unit.append(("D4", entry))

        banner("CATEGORY COUNTS")
        labels = {
            "D1": "different source from active (legit dual-feed)",
            "D2": "same source, NO hour overlap (historical fill)",
            "D3": "same source, overlap, values agree (≤5% drift)",
            "D4": "same source, overlap, values DISAGREE (BUG SUSPECT)",
            "D5": "no active sibling for that windfarm at all",
        }
        for c, label in labels.items():
            print(f"  {c}: {len(cats[c]):>3}  {label}")

        # Detail each category
        for cat in ("D5", "D4", "D3", "D2", "D1"):
            if not cats[cat]:
                continue
            banner(f"{cat} — {labels[cat]} ({len(cats[cat])} units)")
            for e in cats[cat]:
                u = e["unit"]
                extras = ""
                if cat in ("D3", "D4"):
                    extras = (f" overlap={e['n_overlap']:,} drift={e['drift_pct']:.1f}%"
                              f" gen_i={e['inactive_gen']:,.0f} vs gen_a≈{e['abs_a']:,.0f}")
                elif cat == "D2":
                    extras = f" inactive_rows={e['inactive_rows']:,} (no time overlap)"
                actives_str = ", ".join(f"{a.source}#{a.id}({a.n:,}r)" for a in e["actives"][:3])
                if len(e["actives"]) > 3:
                    actives_str += f" +{len(e['actives'])-3}"
                print(f"  {u.id:>5} {u.source:<6} '{u.name[:28]:<30}' "
                      f"wf={e['wf_id']} '{e['wf_name'][:24]:<26}' "
                      f"period={str(e['inactive_lo'])[:10]}→{str(e['inactive_hi'])[:10]} "
                      f"gen={e['inactive_gen']:>10,.0f}{extras}")
                if cat in ("D4", "D5"):
                    print(f"        actives: {actives_str or '(none)'}")

        # Per-source roll-up
        banner("Bucket D by source × category")
        by = defaultdict(lambda: defaultdict(int))
        for cat, entries in cats.items():
            for e in entries:
                by[e["unit"].source][cat] += 1
        print(f"  {'source':<18}{'D1':>5}{'D2':>5}{'D3':>5}{'D4':>5}{'D5':>5}  total")
        for src in sorted(by):
            row = by[src]
            print(f"  {src:<18}{row['D1']:>5}{row['D2']:>5}{row['D3']:>5}{row['D4']:>5}{row['D5']:>5}  "
                  f"{sum(row.values()):>5}")


asyncio.run(main())
