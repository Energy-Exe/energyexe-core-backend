"""Generate a status sheet for the inactive-units remediation effort.

Output: /Users/mdfaisal/Downloads/inactive_units_status_2026-05-13.csv

Two sections in one file:
  1. ACTIONS_TAKEN — every unit we touched (deletes + reconnects + still-open)
  2. REMAINING_INACTIVE — current snapshot of every is_active=FALSE unit
     left in the DB, classified into bucket B/C/D and with current attribution.

For deleted units we can't query the DB anymore, so we hardcode the row from
the remediation log. For everything else we query live.
"""
import asyncio
import csv
import re
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sqlalchemy import text
from app.core.database import get_session_factory


OUTPUT = "/Users/mdfaisal/Downloads/inactive_units_status_2026-05-13.csv"


# ── Permanent record of deletes (rows no longer in DB) ──
DELETED_NVE_CAT_D = [
    # (unit_id, source, code, name, prev_wf, n_gen_data_rows_removed)
    (12797, "NVE", "1",  "Fjeldskår",         None, 142_464),
    (12801, "NVE", "23", "Kvalnes",           None,  76_308),
    (12802, "NVE", "24", "Hovden Vesterålen", None, 110_030),
]

# Mislink fixes — units still in DB, just with corrected windfarm_id
ENTSOE_MISLINK_FIXES = [
    # (unit_id, wrong_wf, correct_wf, rows_moved)
    (12385, 7385, 7404, 41_366),
    (12328, 7359, 7350, 13_221),
    (12361, 7380, 7384,  9_646),
    (12346, 7370, 7371,  9_742),
    (12348, 7374, 7373,  2_519),
    (12349, 7374, 7373,  2_519),
    (12350, 7374, 7373,  2_519),
    (12351, 7374, 7373,  2_519),
]

RCBKO_PENDING = {12388: "RCBKO-1", 12389: "RCBKO-2"}
DELETE_PENDING = {12806: "DELETE ME"}


def tokens(s: str) -> set:
    s = (s or "").lower()
    s = re.sub(r"[^\w]+", " ", s, flags=re.UNICODE)
    return {w for w in s.split() if len(w) >= 4 and w not in {"wind", "farm", "park", "phase"}}


async def main():
    S = get_session_factory()
    rows_out: list[dict] = []

    async with S() as db:
        # windfarm name lookup
        wf_q = await db.execute(text("SELECT id, name FROM windfarms"))
        wf_name = {r.id: r.name for r in wf_q}

        # ── 1. RECONCILED mislinks (still in DB) ──
        for uid, wrong, correct, rows in ENTSOE_MISLINK_FIXES:
            rs = await db.execute(text("""
                SELECT id, source, code, name, is_active, windfarm_id
                FROM generation_units WHERE id = :u
            """), {"u": uid})
            u = rs.first()
            if u:
                rows_out.append({
                    "section": "ACTIONS_TAKEN",
                    "unit_id": u.id,
                    "source": u.source,
                    "code": u.code or "",
                    "unit_name": u.name,
                    "is_active_now": u.is_active,
                    "wf_id_now": u.windfarm_id,
                    "wf_name_now": wf_name.get(u.windfarm_id, ""),
                    "n_gen_data_rows": rows,
                    "action": "RECONNECT (committed 2026-05-12)",
                    "details": f"Re-attached from wf {wrong} to wf {correct} ({wf_name.get(correct, '?')})",
                })

        # ── 2. DELETED NVE Cat D (no longer in DB) ──
        for uid, src, code, name, wf, rows in DELETED_NVE_CAT_D:
            rows_out.append({
                "section": "ACTIONS_TAKEN",
                "unit_id": uid,
                "source": src,
                "code": code,
                "unit_name": name,
                "is_active_now": "[deleted]",
                "wf_id_now": "",
                "wf_name_now": "",
                "n_gen_data_rows": rows,
                "action": "DELETED (committed 2026-05-13)",
                "details": "Decommissioned NVE farm with no windfarms row; orphan data not surfaced. Raw preserved.",
            })

        # ── 3. DELETED empty-scaffolding (299, no longer in DB) — summary rows only ──
        scaffolding_breakdown = [
            ("NVE", 210), ("ENERGISTYRELSEN", 56), ("ENTSOE", 31),
            ("EIA", 1), ("ELEXON", 1),
        ]
        for src, n in scaffolding_breakdown:
            rows_out.append({
                "section": "ACTIONS_TAKEN",
                "unit_id": "[multiple]",
                "source": src,
                "code": "",
                "unit_name": f"[{n} empty scaffolding units]",
                "is_active_now": "[deleted]",
                "wf_id_now": "",
                "wf_name_now": "",
                "n_gen_data_rows": 0,
                "action": "DELETED (committed 2026-05-13)",
                "details": f"{n} {src} unit rows with zero data and no references in any table",
            })

        # ── 4. STILL-PENDING: DELETE-ME, RCBKO ──
        for uid, label in {**RCBKO_PENDING, **DELETE_PENDING}.items():
            rs = await db.execute(text("""
                SELECT id, source, code, name, is_active, windfarm_id,
                       (SELECT COUNT(*) FROM generation_data WHERE generation_unit_id = gu.id) AS n_rows
                FROM generation_units gu WHERE id = :u
            """), {"u": uid})
            u = rs.first()
            if u:
                action = "DELETE pending" if uid in DELETE_PENDING else "REVIEW pending (Rentel EIC)"
                rows_out.append({
                    "section": "ACTIONS_TAKEN",
                    "unit_id": u.id,
                    "source": u.source,
                    "code": u.code or "",
                    "unit_name": u.name,
                    "is_active_now": u.is_active,
                    "wf_id_now": u.windfarm_id,
                    "wf_name_now": wf_name.get(u.windfarm_id, ""),
                    "n_gen_data_rows": u.n_rows,
                    "action": action,
                    "details": "12806 = junk record; 12388/12389 = RCBKO mapping unclear",
                })

        # ── 5. REMAINING_INACTIVE snapshot (current state in DB) ──
        rs = await db.execute(text("""
            SELECT gu.id, gu.source, gu.code, gu.name, gu.is_active, gu.windfarm_id,
                   (SELECT COUNT(*) FROM generation_data WHERE generation_unit_id = gu.id) AS n_rows,
                   (SELECT COUNT(DISTINCT windfarm_id) FROM generation_data
                      WHERE generation_unit_id = gu.id AND windfarm_id IS NOT NULL) AS distinct_wfs,
                   (SELECT array_agg(DISTINCT windfarm_id) FROM generation_data
                      WHERE generation_unit_id = gu.id AND windfarm_id IS NOT NULL) AS wf_ids
            FROM generation_units gu
            WHERE gu.is_active = FALSE
            ORDER BY gu.id
        """))
        all_inactive = list(rs)
        excluded_ids = ({u for u, *_ in ENTSOE_MISLINK_FIXES}
                        | set(RCBKO_PENDING) | set(DELETE_PENDING))
        for u in all_inactive:
            if u.id in excluded_ids:
                continue
            wids = sorted(u.wf_ids or [])
            wnames = [(wid, wf_name.get(wid, "?")) for wid in wids]
            # Classify
            if u.n_rows == 0:
                bucket = "B (empty - skipped: has active mapping)"
            elif u.distinct_wfs == 0:
                bucket = "C (orphan data, wf=NULL)"
            else:
                ut = tokens(u.name)
                match = any(ut & tokens(n) for _, n in wnames)
                bucket = ("D (parallel source, name match)" if match
                          else "E (token mismatch - REVIEW)")
            wf_str = ", ".join(f"{wid}={n!r}" for wid, n in wnames) if wnames else ""
            rows_out.append({
                "section": "REMAINING_INACTIVE",
                "unit_id": u.id,
                "source": u.source,
                "code": u.code or "",
                "unit_name": u.name,
                "is_active_now": u.is_active,
                "wf_id_now": u.windfarm_id,
                "wf_name_now": wf_name.get(u.windfarm_id, ""),
                "n_gen_data_rows": u.n_rows,
                "action": bucket,
                "details": f"data attributed to: {wf_str}" if wf_str else "no data attributed",
            })

    # Write CSV
    fieldnames = ["section", "unit_id", "source", "code", "unit_name",
                  "is_active_now", "wf_id_now", "wf_name_now",
                  "n_gen_data_rows", "action", "details"]
    with open(OUTPUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows_out)
    print(f"Wrote {len(rows_out)} rows → {OUTPUT}")

    # Summary
    by_action = defaultdict(int)
    for r in rows_out:
        by_action[r["action"]] += 1
    print("\nSummary of rows in sheet:")
    for k, v in sorted(by_action.items(), key=lambda kv: -kv[1]):
        print(f"  {v:>4}  {k}")


asyncio.run(main())
