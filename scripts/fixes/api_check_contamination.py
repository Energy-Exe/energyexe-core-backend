"""In-process simulation of what the admin UI/API currently returns for
the 5 contaminated windfarms. Calls the SAME service methods that
/api/v1/comparison/data and /api/v1/generation-data/export use, so the
output is what the frontend renders today.

Run:
    poetry run python scripts/fixes/api_check_contamination.py
"""
import asyncio
import sys
from datetime import date, datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from app.core.database import get_session_factory
from app.services.comparison_service import ComparisonService

# Cases that should be VISIBLY wrong on the live UI today
CASES = [
    {
        "label": "Hornsea 2 — pre-2022 should be EMPTY (commissioned 2022); shows phantom Ormonde",
        "wf_id": 7385,
        "wf_name": "Hornsea 2",
        "start": date(2021, 1, 1),
        "end":   date(2022, 1, 1),
        "expected_post_fix": "0 generation, 0 rows in ENTSOE",
    },
    {
        "label": "Dudgeon — 2020 ENTSOE column inflated by East Anglia One",
        "wf_id": 7370,
        "wf_name": "Dudgeon",
        "start": date(2020, 1, 1),
        "end":   date(2021, 1, 1),
        "expected_post_fix": "ELEXON unchanged, ENTSOE drops to 0 for this period",
    },
    {
        "label": "Hollandse Kust Zuid — pre-2023 should be EMPTY; shows phantom Hornsea 1",
        "wf_id": 7380,
        "wf_name": "Hollandse Kust Zuid",
        "start": date(2019, 7, 1),
        "end":   date(2022, 1, 1),
        "expected_post_fix": "0 generation",
    },
    {
        "label": "Gode Wind 1&2 — Jan-May 2021 should be empty; shows phantom Galloper",
        "wf_id": 7374,
        "wf_name": "Gode Wind 1&2",
        "start": date(2021, 1, 1),
        "end":   date(2021, 6, 1),
        "expected_post_fix": "0 generation",
    },
    {
        "label": "Beatrice — 2019 ENTSOE column polluted by ABRB0-1 (Aberdeen Bay)",
        "wf_id": 7359,
        "wf_name": "Beatrice",
        "start": date(2019, 1, 1),
        "end":   date(2020, 1, 1),
        "expected_post_fix": "ELEXON unchanged, ENTSOE drops",
    },
    {
        "label": "Ormonde — should GAIN ENTSOE pre-2022 (post-fix); currently only ELEXON",
        "wf_id": 7404,
        "wf_name": "Ormonde",
        "start": date(2015, 1, 1),
        "end":   date(2016, 1, 1),
        "expected_post_fix": "ENTSOE rows appear alongside ELEXON",
    },
    {
        "label": "Hornsea 1 — should GAIN ENTSOE 2019 commissioning data (post-fix)",
        "wf_id": 7384,
        "wf_name": "Hornsea 1",
        "start": date(2019, 7, 1),
        "end":   date(2020, 1, 1),
        "expected_post_fix": "ENTSOE rows appear",
    },
    {
        "label": "Galloper — should GAIN ENTSOE Jan-May 2021 (post-fix)",
        "wf_id": 7373,
        "wf_name": "Galloper",
        "start": date(2021, 1, 1),
        "end":   date(2021, 6, 1),
        "expected_post_fix": "ENTSOE rows appear alongside ELEXON",
    },
]


async def main():
    S = get_session_factory()
    async with S() as db:
        svc = ComparisonService(db)
        for c in CASES:
            print("=" * 100)
            print(c["label"])
            print(f"  wf={c['wf_id']} '{c['wf_name']}' period {c['start']} → {c['end']}")
            print(f"  expected post-fix: {c['expected_post_fix']}")
            print("-" * 100)
            try:
                data = await svc.get_data_by_granularity(
                    windfarm_ids=[c["wf_id"]],
                    start_date=c["start"],
                    end_date=c["end"],
                    granularity="monthly",
                    exclude_ramp_up=True,
                )
            except Exception as e:
                print(f"  ERROR: {e}")
                continue

            rows = data.get("data", []) if isinstance(data, dict) else (data or [])
            if not rows:
                print("  (no data — already correct)")
                continue

            # Surface the source-split numbers the chart legend shows
            print(f"  {'period':<22}{'source':<10}{'gen_MWh':>14}{'cf%':>8}{'cap_MW':>10}")
            for r in rows[:24]:
                period = str(r.get("period"))[:10] if r.get("period") else "-"
                src = r.get("source") or "?"
                gen = r.get("total_generation_mwh")
                cf = r.get("avg_capacity_factor")
                cap = r.get("avg_capacity")
                gen_s = f"{gen:>14,.0f}" if gen is not None else f"{'-':>14}"
                cf_s = f"{cf*100:>7.1f}%" if cf is not None else f"{'-':>8}"
                cap_s = f"{cap:>10,.1f}" if cap is not None else f"{'-':>10}"
                print(f"  {period:<22}{src:<10}{gen_s}{cf_s}{cap_s}")
            print()


asyncio.run(main())
