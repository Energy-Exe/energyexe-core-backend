"""Merge raw pending-flag context with the per-group research findings into one CSV."""
import csv
import glob
import json
import os

RAW = "scripts/_pending_constraints_raw.csv"
OUT_DIR = "scripts/_research_out"
FINAL = "scripts/pending_constraint_review_analysis.csv"

raw = {int(r["flag_id"]): r for r in csv.DictReader(open(RAW))}

# Load all research findings, keyed by flag_id
research = {}
dupes = []
for path in sorted(glob.glob(f"{OUT_DIR}/*.json")):
    try:
        data = json.load(open(path))
    except Exception as e:
        print(f"!! could not parse {path}: {e}")
        continue
    for obj in data:
        fid = int(obj["flag_id"])
        if fid in research:
            dupes.append(fid)
        research[fid] = obj
    print(f"loaded {len(data):3d} findings from {os.path.basename(path)}")

cols = [
    "flag_id", "windfarm", "wf_code", "country", "location_type",
    "foundation_type", "capacity_mw", "period_start", "period_end",
    "duration_hours", "duration_days", "flag_trigger", "mean_q50_ratio",
    "mean_q90_ratio", "commercial_operational_date",
    "cause_category", "potential_reason", "confidence",
    "source_name", "source_url", "evidence_note",
]

missing = []
rows_out = []
for fid, r in raw.items():
    f = research.get(fid)
    if f is None:
        missing.append(fid)
    dur_h = int(r["duration_hours"])
    rows_out.append({
        "flag_id": fid,
        "windfarm": r["wf_name"],
        "wf_code": r["wf_code"],
        "country": r["country"],
        "location_type": r["location_type"],
        "foundation_type": r["foundation_type"],
        "capacity_mw": r["nameplate_capacity_mw"],
        "period_start": r["period_start"][:16],
        "period_end": r["period_end"][:16],
        "duration_hours": dur_h,
        "duration_days": round(dur_h / 24, 1),
        "flag_trigger": r["flag_trigger"],
        "mean_q50_ratio": r["mean_q50_ratio"],
        "mean_q90_ratio": r["mean_q90_ratio"],
        "commercial_operational_date": r["commercial_operational_date"],
        "cause_category": (f or {}).get("cause_category", "NOT_RESEARCHED"),
        "potential_reason": (f or {}).get("potential_reason", ""),
        "confidence": (f or {}).get("confidence", ""),
        "source_name": (f or {}).get("source_name", ""),
        "source_url": (f or {}).get("source_url", ""),
        "evidence_note": (f or {}).get("evidence_note", ""),
    })

rows_out.sort(key=lambda x: (x["country"], x["windfarm"], x["period_start"]))
with open(FINAL, "w", newline="") as fh:
    w = csv.DictWriter(fh, fieldnames=cols)
    w.writeheader()
    w.writerows(rows_out)

print(f"\nwrote {FINAL}: {len(rows_out)} rows")
print(f"research findings: {len(research)} | missing (not researched): {len(missing)}")
if missing:
    print("  missing flag_ids:", missing[:50], "..." if len(missing) > 50 else "")
if dupes:
    print("  duplicate flag_ids across groups:", sorted(set(dupes)))

from collections import Counter
print("\n-- cause_category --")
for c, n in Counter(x["cause_category"] for x in rows_out).most_common():
    print(f"  {n:3d}  {c}")
print("\n-- confidence --")
for c, n in Counter(x["confidence"] for x in rows_out).most_common():
    print(f"  {n:3d}  {c}")
