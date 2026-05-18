"""Phase 1 — find the buggy endpoint behind the team's CSV.

Hits every plausible monthly-aggregation service method for windfarm 7206
across Sep-22 (a date where the bug is fully visible: admin shows 17,712
when the truth is 35,152). The method that returns ~17,712 is the bug source.

Run:
    poetry run python scripts/fixes/find_raggovidda_admin_endpoint.py
"""
import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from app.core.database import get_session_factory
from app.services.windfarm_report_service import WindfarmReportService
from app.services.comparison_service import ComparisonService

WINDFARM_ID = 7206
START = datetime(2022, 9, 1, tzinfo=timezone.utc)
END = datetime(2022, 10, 1, tzinfo=timezone.utc)

EXPECTED_TRUE_GEN = 35152.38
EXPECTED_BUGGY_GEN = 17712.43


def label(name, gen_mwh):
    if gen_mwh is None:
        return f"  [SKIP]  {name}: returned None"
    diff_true = abs(gen_mwh - EXPECTED_TRUE_GEN)
    diff_buggy = abs(gen_mwh - EXPECTED_BUGGY_GEN)
    if diff_buggy < 50:
        verdict = "  ←★ MATCHES BUGGY (12695 only)"
    elif diff_true < 50:
        verdict = "  ✓ correct (full sum)"
    else:
        verdict = f"  ?? differs from both"
    return f"  {name}: {gen_mwh:,.2f}{verdict}"


async def main():
    S = get_session_factory()
    async with S() as db:
        report_svc = WindfarmReportService(db)
        comparison_svc = ComparisonService(db)

        print(f"Target: windfarm_id={WINDFARM_ID}, period={START.date()} to {END.date()}")
        print(f"Expected (true full sum): {EXPECTED_TRUE_GEN:,.2f}")
        print(f"Expected (buggy 12695-only): {EXPECTED_BUGGY_GEN:,.2f}")
        print()

        # -- 1. windfarm_report_service.get_monthly_generation_timeseries
        try:
            rows = await report_svc.get_monthly_generation_timeseries(WINDFARM_ID, START, END)
            sep = next((r for r in rows if r['month'] == '2022-09'), None)
            gen = sep['generation_gwh'] * 1000 if sep else None
            print(label("[1] WindfarmReportService.get_monthly_generation_timeseries", gen))
        except Exception as e:
            print(f"  [ERR] get_monthly_generation_timeseries: {e}")

        # -- 2. windfarm_report_service.get_annual_summary_table
        try:
            rows = await report_svc.get_annual_summary_table(WINDFARM_ID, START, END)
            r2022 = next((r for r in rows if r['year'] == 2022), None)
            gen = r2022['total_generation_gwh'] * 1000 if r2022 else None
            print(label("[2] WindfarmReportService.get_annual_summary_table (2022)", gen))
        except Exception as e:
            print(f"  [ERR] get_annual_summary_table: {e}")

        # -- 3. windfarm_report_service._get_monthly_generation
        try:
            vals = await report_svc._get_monthly_generation(WINDFARM_ID, START, END)
            gen = vals[0] * 1000 if vals else None  # _get_monthly_generation returns GWh
            print(label("[3] WindfarmReportService._get_monthly_generation (Sep-22)", gen))
        except Exception as e:
            print(f"  [ERR] _get_monthly_generation: {e}")

        # -- 4. comparison_service.get_data_by_granularity
        try:
            data = await comparison_svc.get_data_by_granularity(
                windfarm_ids=[WINDFARM_ID],
                start_date=START.date(),
                end_date=END.date(),
                granularity="monthly",
                exclude_ramp_up=True,
            )
            # Output shape varies; try common patterns.
            sep_row = None
            if isinstance(data, dict) and 'data' in data:
                for row in data['data']:
                    if row.get('windfarm_id') == WINDFARM_ID and (
                        '2022-09' in str(row.get('period', '')) or row.get('period') == '2022-09'
                    ):
                        sep_row = row
                        break
            elif isinstance(data, list):
                for row in data:
                    if row.get('windfarm_id') == WINDFARM_ID and '2022-09' in str(row.get('period', '')):
                        sep_row = row
                        break
            gen = sep_row.get('total_generation') if sep_row else None
            print(label("[4] ComparisonService.get_data_by_granularity (monthly)", gen))
        except Exception as e:
            print(f"  [ERR] get_data_by_granularity: {e}")

        # -- 5. windfarm_report_service.get_monthly_generation_timeseries with exclude_ramp_up=False
        try:
            # Some methods take exclude_ramp_up; this one doesn't, but check the underlying behavior
            # by directly looking at _get_monthly_capacity_factors_dict
            cfs = await report_svc._get_monthly_capacity_factors_dict(WINDFARM_ID, START, END, exclude_ramp_up=True)
            cf = cfs.get('2022-09')
            print(f"  [5] WindfarmReportService._get_monthly_capacity_factors_dict (Sep-22 CF): {cf!r}")
        except Exception as e:
            print(f"  [ERR] _get_monthly_capacity_factors_dict: {e}")

        # -- 6. windfarm_timeline /generation-timeline endpoint logic (replicate inline)
        try:
            from sqlalchemy import select, and_, or_
            from app.models.generation_data import GenerationData
            from app.models.generation_unit import GenerationUnit
            from app.models.turbine_unit import TurbineUnit

            gu_q = await db.execute(select(GenerationUnit.id).where(GenerationUnit.windfarm_id == WINDFARM_ID))
            gu_ids = [r[0] for r in gu_q.all()]
            tu_q = await db.execute(select(TurbineUnit.id).where(TurbineUnit.windfarm_id == WINDFARM_ID))
            tu_ids = [r[0] for r in tu_q.all()]
            unit_conds = []
            if gu_ids:
                unit_conds.append(GenerationData.generation_unit_id.in_(gu_ids))
            if tu_ids:
                unit_conds.append(GenerationData.turbine_unit_id.in_(tu_ids))
            unit_conds.append(GenerationData.windfarm_id == WINDFARM_ID)
            q = (
                select(GenerationData)
                .where(and_(
                    GenerationData.hour >= START,
                    GenerationData.hour <= END,
                    or_(*unit_conds),
                    GenerationData.is_ramp_up == False,
                ))
                .order_by(GenerationData.hour)
                .limit(500000)
            )
            res = await db.execute(q)
            recs = res.scalars().all()
            total = sum(float(r.generation_mwh) for r in recs)
            print(label("[6] windfarm_timeline /generation-timeline?monthly (replicated)", total))
        except Exception as e:
            print(f"  [ERR] generation-timeline replicate: {e}")


asyncio.run(main())
