"""Re-run AI narrative sections on existing reports (EPR-117 comment 2).

A report keeps the narrative text it was generated with, so a prompt or
vocabulary fix (e.g. "bankable P50 case" → "Generation target") only reaches an
existing report when its narrative sections are generated again. The
section-generate API is scoped to the report's owner, so this script is the
operator's path: run it inside the backend's environment — a one-off ECS task
with the prod task definition, or locally against a DB tunnel — where the
database URL and ANTHROPIC_API_KEY are configured.

Frozen reports (PDF already downloaded, or locked) are never touched: their
content is immutable by design; they need a new version from the UI.

Examples
--------
  # list live reports whose narratives still contain a term (no changes)
  poetry run python scripts/rerun_report_narratives.py --find-term bankable --dry-run

  # re-run the action plan + executive summary on those reports
  poetry run python scripts/rerun_report_narratives.py --find-term bankable

  # or on explicit reports, one section only
  poetry run python scripts/rerun_report_narratives.py --report-id 12 --report-id 15 \
      --sections executive_summary
"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path
from typing import List, Sequence

# Runnable as ``python scripts/rerun_report_narratives.py`` from the repo root
# (or /app in the image) without PYTHONPATH: put the repo root on sys.path.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# structlog emits nothing at INFO from a standalone script unless stdlib
# logging is configured first.
logging.basicConfig(level=logging.INFO, format="%(message)s")

from sqlalchemy import Text, cast, or_, select  # noqa: E402

from app.core.database import get_session_factory  # noqa: E402
from app.models.report import Report, ReportSection, ReportStatus  # noqa: E402
from app.services.reports import orchestrator  # noqa: E402
from app.services.reports.registry import get_report_type  # noqa: E402

# Pass-1 narratives first: re-running one marks Pass 2 stale, and the summary
# must be rebuilt from the refreshed sections.
DEFAULT_SECTIONS = ("action_plan", "executive_summary")


async def find_reports(term: str) -> List[dict]:
    """Reports (any status) with a live narrative containing ``term``."""
    factory = get_session_factory()
    async with factory() as db:
        stmt = (
            select(
                Report.id,
                Report.report_type,
                Report.title,
                Report.status,
                Report.locked,
                Report.pdf_downloaded_at,
            )
            .join(ReportSection, ReportSection.report_id == Report.id)
            .where(
                or_(
                    ReportSection.narrative_text.ilike(f"%{term}%"),
                    cast(ReportSection.narrative_json, Text).ilike(f"%{term}%"),
                )
            )
            .distinct()
            .order_by(Report.id)
        )
        rows = (await db.execute(stmt)).all()
    return [
        {
            "id": r.id,
            "report_type": r.report_type,
            "title": r.title,
            "status": str(r.status),
            "frozen": bool(r.locked or r.pdf_downloaded_at is not None),
        }
        for r in rows
    ]


async def describe(report_id: int) -> dict:
    factory = get_session_factory()
    async with factory() as db:
        report = await db.get(Report, report_id)
        if report is None:
            return {"id": report_id, "missing": True}
        keys = (
            await db.execute(
                select(ReportSection.section_key).where(ReportSection.report_id == report_id)
            )
        ).scalars()
        return {
            "id": report_id,
            "report_type": report.report_type,
            "title": report.title,
            "status": str(report.status),
            "frozen": report.is_frozen,
            "section_keys": set(keys),
        }


async def section_states(report_id: int, keys: Sequence[str]) -> dict:
    factory = get_session_factory()
    async with factory() as db:
        rows = (
            await db.execute(
                select(ReportSection.section_key, ReportSection.status, ReportSection.error).where(
                    ReportSection.report_id == report_id, ReportSection.section_key.in_(list(keys))
                )
            )
        ).all()
    return {r.section_key: (str(r.status), r.error) for r in rows}


async def rerun(report_id: int, sections: Sequence[str], dry_run: bool) -> None:
    info = await describe(report_id)
    if info.get("missing"):
        print(f"report {report_id}: not found — skipped")
        return
    label = f"report {report_id} ({info['report_type']}, {info['title']})"
    if info["frozen"]:
        print(f"{label}: FROZEN (PDF downloaded or locked) — skipped; needs a new version")
        return
    if info["status"] == str(ReportStatus.GENERATING):
        print(f"{label}: currently GENERATING — skipped")
        return
    spec = get_report_type(info["report_type"])
    todo = []
    for key in sections:
        section_spec = spec.section(key) if spec else None
        if section_spec is None or section_spec.narrative is None:
            print(f"{label}: '{key}' is not a narrative section of this report type — skipped")
            continue
        if key not in info["section_keys"]:
            print(f"{label}: '{key}' not part of this report — skipped")
            continue
        todo.append(section_spec)
    todo.sort(key=lambda s: s.pass_number)
    if not todo:
        return
    names = ", ".join(s.key for s in todo)
    if dry_run:
        print(f"{label}: would re-run {names}")
        return
    print(f"{label}: re-running {names} …")
    for section_spec in todo:
        await orchestrator.run_narrative_section(report_id, section_spec)
        if section_spec.pass_number == 1:
            await orchestrator.mark_pass2_stale(report_id)
    await orchestrator._finalize(report_id)
    for key, (status, error) in (await section_states(report_id, [s.key for s in todo])).items():
        suffix = f" — {error}" if error else ""
        print(f"{label}: {key} → {status}{suffix}")


async def main(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--report-id", type=int, action="append", default=[], help="report to re-run (repeatable)"
    )
    parser.add_argument(
        "--find-term", help="also select every live report whose narrative contains this term"
    )
    parser.add_argument(
        "--sections",
        default=",".join(DEFAULT_SECTIONS),
        help=f"comma-separated narrative section keys (default: {','.join(DEFAULT_SECTIONS)})",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="list what would be re-run; change nothing"
    )
    args = parser.parse_args(argv)

    ids = list(dict.fromkeys(args.report_id))
    if args.find_term:
        found = await find_reports(args.find_term)
        print(f"{len(found)} report(s) with '{args.find_term}' in a narrative:")
        for r in found:
            flag = " [FROZEN]" if r["frozen"] else ""
            print(f"  #{r['id']} {r['report_type']} {r['title']} ({r['status']}){flag}")
        ids += [r["id"] for r in found if r["id"] not in ids]
    if not ids:
        print("nothing to do (pass --report-id and/or --find-term)")
        return 0
    sections = [s.strip() for s in args.sections.split(",") if s.strip()]
    for report_id in ids:
        await rerun(report_id, sections, args.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main(sys.argv[1:])))
