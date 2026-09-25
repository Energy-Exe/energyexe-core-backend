"""Preview or apply the published Energistyrelsen workbooks.

Preview (no writes):
    poetry run python scripts/jobs/import_vinddata.py Vinddata.xlsx --start 2023-01 --end 2026-08 \\
        --park-file Parkproduktion.xlsx --report-out preview.json

Apply, one transaction per source month, then refresh the monthly view:
    ... --apply --report-out apply.json

Turbine catalogue changes are opt-in: --add-turbines creates turbine_units
for GSRNs whose park maps to a tracked windfarm (use --park-map for parks
that have no tracked turbine yet and --model-map for DEA model spellings);
--update-decommissions applies 'Dato for afmeldning'.
"""

import argparse
import asyncio
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from app.core.database import get_session_factory
from app.services.energistyrelsen_import import reconcile_vinddata
from app.services.generation_monthly_view import refresh_generation_monthly_view


def pairs(items, cast):
    out = {}
    for item in items or []:
        if "=" not in item:
            raise SystemExit(f"Expected KEY=VALUE, got {item!r}")
        key, value = item.split("=", 1)
        out[key.strip()] = cast(value.strip())
    return out


def summarize(report):
    months = report["months"]
    totals = {key: sum(m[key] for m in months.values())
              for key in ("added", "revised", "unchanged", "preserved_absent", "from_park")}
    lines = [f"Months: {min(months, default='-')}..{max(months, default='-')} ({len(months)})",
             "Values: " + ", ".join(f"{k}={v:,}" for k, v in totals.items()),
             f"Unmatched GSRNs with values: {len(report['unmatched_gsrns']):,}"]
    parks = report.get("parks", {})
    if "mapped" in parks:
        lines.append(f"Parks: {len(parks['mapped'])} mapped, "
                     f"{len(parks.get('unmapped_with_production', {}))} with production but no tracked turbine, "
                     f"direct-wins={parks.get('direct_wins', 0)}, no-active={len(parks.get('no_active_turbines', []))}")
    if parks.get("ambiguous"):
        lines.append(f"AMBIGUOUS parks (map to >1 farm): {parks['ambiguous']}")
    turbines = report.get("turbines") or {}
    if turbines:
        pending = [d for d in turbines["decommission"] if not d["already_applied"]]
        lines.append(f"Turbines: create={len(turbines['create'])}, decommission={len(pending)} pending "
                     f"of {len(turbines['decommission'])}, unresolved={len(turbines['unresolved'])}, "
                     f"applied={turbines['applied']}")
        for row in turbines["unresolved"][:10]:
            lines.append(f"  unresolved {row['gsrn']}: {row['reasons']}")
    revised = [(m, r) for m, c in sorted(months.items()) for r in c.get("revisions", [])]
    if revised:
        lines.append(f"Historical revisions (first {min(len(revised), 10)} of {totals['revised']:,}):")
        for month, r in revised[:10]:
            lines.append(f"  {month} {r['gsrn']} wf={r['windfarm_id']} {r['old']} -> {r['new']}")
    return "\n".join(lines)


async def run(args):
    session_factory = get_session_factory()
    async with session_factory() as db:
        report = await reconcile_vinddata(
            db, args.file.read_bytes(), args.start, args.end, apply=args.apply,
            park_content=args.park_file.read_bytes() if args.park_file else None,
            park_map=pairs(args.park_map, int), model_map=pairs(args.model_map, int),
            add_turbines=args.add_turbines, update_decommissions=args.update_decommissions,
        )
    if args.apply:
        report["view_refresh"] = await refresh_generation_monthly_view()
    if args.report_out:
        args.report_out.write_text(json.dumps(report, indent=2, default=str))
        print(f"Full report written to {args.report_out}")
    else:
        print(json.dumps(report, indent=2, default=str))
    print(("APPLIED" if args.apply else "PREVIEW") + "\n" + summarize(report))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("file", type=Path, help="Vinddatasæt workbook")
    parser.add_argument("--start", required=True, help="First source month, YYYY-MM")
    parser.add_argument("--end", required=True, help="Last source month, YYYY-MM")
    parser.add_argument("--park-file", type=Path, help="Parkproduktion workbook (park totals)")
    parser.add_argument("--park-map", action="append", metavar="PARK_GUID=WINDFARM_ID",
                        help="Map a park to a windfarm (repeatable)")
    parser.add_argument("--model-map", action="append", metavar="DEA_MODEL=TURBINE_MODEL_ID",
                        help="Map a DEA model name to turbine_models.id (repeatable)")
    parser.add_argument("--add-turbines", action="store_true",
                        help="Create turbine_units for new GSRNs in tracked parks")
    parser.add_argument("--update-decommissions", action="store_true",
                        help="Apply 'Dato for afmeldning' to turbine_units")
    parser.add_argument("--apply", action="store_true", help="Write one transaction per month")
    parser.add_argument("--report-out", type=Path, help="Write the full JSON report here")
    asyncio.run(run(parser.parse_args()))
