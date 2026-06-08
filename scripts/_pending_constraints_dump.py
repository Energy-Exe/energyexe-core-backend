"""One-off: dump pending_review structural constraint flags + windfarm context to CSV."""
import asyncio
import csv
import os

import asyncpg
from dotenv import load_dotenv

load_dotenv()

# asyncpg wants a plain postgres:// DSN, not the SQLAlchemy +asyncpg form
DSN = os.environ["DATABASE_URL"].replace("postgresql+asyncpg://", "postgresql://")

QUERY = """
SELECT
    f.id                 AS flag_id,
    f.windfarm_id,
    w.code               AS wf_code,
    w.name               AS wf_name,
    w.alternate_name,
    c.name               AS country,
    w.location_type,
    w.foundation_type,
    w.nameplate_capacity_mw,
    w.lat,
    w.lng,
    w.commercial_operational_date,
    f.period_start,
    f.period_end,
    f.duration_hours,
    f.wind_bins_affected,
    f.mean_q90_ratio,
    f.mean_q50_ratio,
    f.flag_trigger,
    f.flag_source,
    f.review_status,
    f.created_at
FROM structural_constraint_flags f
JOIN windfarms w ON w.id = f.windfarm_id
LEFT JOIN countries c ON c.id = w.country_id
WHERE f.review_status = 'pending_review'
ORDER BY w.name, f.period_start;
"""


async def main():
    conn = await asyncpg.connect(DSN)
    try:
        rows = await conn.fetch(QUERY)
    finally:
        await conn.close()

    print(f"pending_review flags: {len(rows)}")
    if not rows:
        return

    out = "scripts/_pending_constraints_raw.csv"
    cols = list(rows[0].keys())
    with open(out, "w", newline="") as fh:
        wri = csv.writer(fh)
        wri.writerow(cols)
        for r in rows:
            wri.writerow([r[c] for c in cols])
    print(f"wrote {out}")

    # quick summary to stdout
    from collections import Counter
    wf = Counter(r["wf_name"] for r in rows)
    trig = Counter(r["flag_trigger"] for r in rows)
    print("\n-- by windfarm --")
    for name, n in wf.most_common():
        print(f"  {n:3d}  {name}")
    print("\n-- by trigger --")
    for t, n in trig.most_common():
        print(f"  {n:3d}  {t}")


asyncio.run(main())
