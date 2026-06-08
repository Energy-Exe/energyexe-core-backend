"""Analyze the raw pending-flags CSV: distinct windfarms, year/duration/trigger distributions,
and a per-windfarm research bundle (periods grouped by year)."""
import csv
import json
from collections import Counter, defaultdict
from datetime import datetime

rows = list(csv.DictReader(open("scripts/_pending_constraints_raw.csv")))
print(f"total flags: {len(rows)}")
wfs = {r["wf_name"] for r in rows}
print(f"distinct windfarms: {len(wfs)}")

def yr(s):
    return s[:4]

years = Counter(yr(r["period_start"]) for r in rows)
print("\n-- flags by year --")
for y in sorted(years):
    print(f"  {y}: {years[y]}")

trig = Counter(r["flag_trigger"] for r in rows)
print("\n-- by trigger --", dict(trig))

loc = Counter(r["location_type"] for r in rows)
print("-- by location_type --", dict(loc))
ctry = Counter(r["country"] for r in rows)
print("-- by country --", dict(ctry))

durs = sorted(int(r["duration_hours"]) for r in rows)
print(f"\nduration_hours: min={durs[0]} median={durs[len(durs)//2]} max={durs[-1]}")

# per-windfarm research bundle
bundle = {}
by_wf = defaultdict(list)
for r in rows:
    by_wf[r["wf_name"]].append(r)
for name, frs in by_wf.items():
    s = frs[0]
    bundle[name] = {
        "code": s["wf_code"],
        "country": s["country"],
        "location_type": s["location_type"],
        "foundation_type": s["foundation_type"],
        "capacity_mw": s["nameplate_capacity_mw"],
        "lat": s["lat"], "lng": s["lng"],
        "cod": s["commercial_operational_date"],
        "n_flags": len(frs),
        "periods": [
            {
                "flag_id": int(fr["flag_id"]),
                "start": fr["period_start"][:16],
                "end": fr["period_end"][:16],
                "dur_h": int(fr["duration_hours"]),
                "trigger": fr["flag_trigger"],
                "q50r": fr["mean_q50_ratio"],
                "q90r": fr["mean_q90_ratio"],
            }
            for fr in sorted(frs, key=lambda x: x["period_start"])
        ],
    }

json.dump(bundle, open("scripts/_pending_research_bundle.json", "w"), indent=2)
print(f"\nwrote scripts/_pending_research_bundle.json ({len(bundle)} windfarms)")
