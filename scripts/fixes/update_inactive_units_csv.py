"""Read the original inactive-units CSV and emit a new copy with a
`remediation` column describing what we plan to do for each row.

Input : /Users/mdfaisal/Downloads/inactive_generation_units_with_source.csv
Output: /Users/mdfaisal/Downloads/inactive_generation_units_with_source_remediation.csv

Rules (applied in order — first match wins):
  1. ENTSOE mislinks → "RECONNECT to wf <id> (<name>)" — 8 specific unit_ids
  2. ELEXON 'DELETE ME' garbage → "DELETE - junk/test record"
  3. RCBKO-1 / RCBKO-2 → "PENDING REVIEW - Rentel link unclear"
  4. CSV stale (now is_active=True in DB) → "NO ACTION - CSV stale (unit is currently active)"
  5. 0 generation_data rows → "NO ACTION - empty scaffolding"
  6. All gen_data rows have windfarm_id=NULL → "NO ACTION - orphan data (doesn't surface)"
  7. unit name and target windfarm name share a token → "NO ACTION - parallel source on correct wf"
  8. fallback → "REVIEW - check"

Run:
    poetry run python scripts/fixes/update_inactive_units_csv.py
"""
import asyncio
import csv
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sqlalchemy import text
from app.core.database import get_session_factory


INPUT  = "/Users/mdfaisal/Downloads/inactive_generation_units_with_source.csv"
OUTPUT = "/Users/mdfaisal/Downloads/inactive_generation_units_with_source_remediation.csv"

# Confirmed ENTSOE mislinks — (unit_id) → (correct_wf_id, correct_wf_name)
ENTSOE_MISLINKS = {
    12385: (7404, "Ormonde"),
    12328: (7350, "Aberdeen"),
    12361: (7384, "Hornsea 1"),
    12346: (7371, "East Anglia One"),
    12348: (7373, "Galloper"),
    12349: (7373, "Galloper"),
    12350: (7373, "Galloper"),
    12351: (7373, "Galloper"),
}

DELETE_JUNK = {12806}  # 'DELETE ME' Aberdeen ELEXON unit
RCBKO       = {12388, 12389}  # Rentel link unclear

# Units flagged earlier as actually is_active=True in DB despite CSV says inactive
KNOWN_STALE_ACTIVE = {12792, 12504, 12508, 12731}


def tokens(s: str) -> set:
    s = (s or "").lower()
    # split on non-letter/digit, but keep unicode letters (ø, å, ü, é, etc.)
    s = re.sub(r"[^\w]+", " ", s, flags=re.UNICODE)
    return {w for w in s.split() if len(w) >= 4 and w not in {"wind", "farm", "park", "phase"}}


async def main():
    # 1. Load CSV
    rows = []
    with open(INPUT, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            rows.append(r)
    print(f"Loaded {len(rows)} rows from {INPUT}")

    # 2. Lookup each row in DB and build remediation
    S = get_session_factory()
    counts = {}
    out_rows = []
    async with S() as db:
        for r in rows:
            db_q = await db.execute(text("""
                SELECT gu.id, gu.is_active, gu.name AS u_name,
                       (SELECT COUNT(*) FROM generation_data WHERE generation_unit_id = gu.id) AS n_rows,
                       (SELECT COUNT(DISTINCT windfarm_id) FROM generation_data
                          WHERE generation_unit_id = gu.id AND windfarm_id IS NOT NULL) AS distinct_non_null_wfs,
                       (SELECT array_agg(DISTINCT windfarm_id) FROM generation_data
                          WHERE generation_unit_id = gu.id AND windfarm_id IS NOT NULL) AS wf_ids
                FROM generation_units gu
                WHERE source = :src AND code = :code AND name = :name
                LIMIT 1
            """), {"src": r["source"], "code": r["code"], "name": r["name"]})
            u = db_q.first()
            uid = u.id if u else None

            # Apply rules in priority order
            if uid in ENTSOE_MISLINKS:
                wf_id, wf_name = ENTSOE_MISLINKS[uid]
                remediation = f"RECONNECT to wf {wf_id} ({wf_name})"
            elif uid in DELETE_JUNK:
                remediation = "DELETE - junk/test record"
            elif uid in RCBKO:
                remediation = "PENDING REVIEW - Rentel link unclear"
            elif u is None:
                remediation = "REVIEW - row not found in DB"
            elif u.is_active or uid in KNOWN_STALE_ACTIVE:
                remediation = "NO ACTION - CSV stale (unit is currently is_active=True)"
            elif u.n_rows == 0:
                remediation = "NO ACTION - empty scaffolding (no generation_data)"
            elif u.distinct_non_null_wfs == 0:
                remediation = "NO ACTION - orphan data (windfarm_id IS NULL on rows, does not surface in UI)"
            else:
                # Has data with non-NULL windfarm_id. Check name overlap.
                wf_ids = sorted(u.wf_ids or [])
                wf_names = []
                if wf_ids:
                    wf_q = await db.execute(text("""
                        SELECT id, name FROM windfarms WHERE id = ANY(:ids)
                    """), {"ids": wf_ids})
                    wf_names = [(w.id, w.name) for w in wf_q]

                ut = tokens(u.u_name)
                target_token_match = any(
                    (ut and (ut & tokens(wn))) for _, wn in wf_names
                )
                if target_token_match:
                    remediation = (
                        f"NO ACTION - parallel source on correct wf "
                        f"({wf_ids[0]}={(wf_names[0][1] if wf_names else '?')!r})"
                    )
                else:
                    label = ", ".join(f"{wid}={n!r}" for wid, n in wf_names) or str(wf_ids)
                    remediation = (
                        f"REVIEW - {u.n_rows:,} rows attributed to non-matching wf "
                        f"[{label}] (potential mislink)"
                    )

            # Tally
            bucket = remediation.split(" - ")[0] if " - " in remediation else remediation.split(" (")[0]
            counts[bucket] = counts.get(bucket, 0) + 1

            out_rows.append({
                **r,
                "unit_id_in_db": uid if uid is not None else "",
                "n_generation_data_rows": u.n_rows if u else "",
                "remediation": remediation,
            })

    # 3. Write output
    fieldnames = list(rows[0].keys()) + ["unit_id_in_db", "n_generation_data_rows", "remediation"]
    with open(OUTPUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(out_rows)
    print(f"Wrote {len(out_rows)} rows → {OUTPUT}\n")

    # 4. Summary
    print("Remediation breakdown:")
    for bucket, n in sorted(counts.items(), key=lambda kv: -kv[1]):
        print(f"  {n:>4}  {bucket}")


asyncio.run(main())
