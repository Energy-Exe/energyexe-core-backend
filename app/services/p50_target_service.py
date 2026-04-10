"""
Service for P50 target management and analysis.

P50 targets are externally-provided annual energy production targets (GWh)
from wind resource assessments. Monthly P50 = Annual P50 / 12.
"""

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

from sqlalchemy import and_, select, func, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.generation_data import GenerationData
from app.models.p50_target import P50Target
from app.models.windfarm import Windfarm
from app.schemas.p50_target import (
    P50AnalysisResult,
    P50MonthlyDataPoint,
    P50TargetCreate,
    P50TargetResponse,
    P50TargetUpdate,
    P50YearlyGap,
)


class P50TargetService:
    def __init__(self, db: AsyncSession):
        self.db = db

    # --- CRUD ---

    async def get_targets(self, windfarm_id: int) -> List[P50Target]:
        """Get all P50 targets for a windfarm, ordered by start date."""
        result = await self.db.execute(
            select(P50Target)
            .where(P50Target.windfarm_id == windfarm_id)
            .order_by(P50Target.p50_target_start_date)
        )
        return list(result.scalars().all())

    async def get_target(self, target_id: int) -> Optional[P50Target]:
        """Get a single P50 target by ID."""
        result = await self.db.execute(
            select(P50Target).where(P50Target.id == target_id)
        )
        return result.scalar_one_or_none()

    async def get_active_target(
        self, windfarm_id: int, as_of_date: Optional[date] = None
    ) -> Optional[P50Target]:
        """Get the active P50 target for a windfarm as of a given date."""
        if as_of_date is None:
            as_of_date = date.today()

        result = await self.db.execute(
            select(P50Target)
            .where(
                and_(
                    P50Target.windfarm_id == windfarm_id,
                    P50Target.p50_target_start_date <= as_of_date,
                    (P50Target.p50_target_end_date.is_(None))
                    | (P50Target.p50_target_end_date >= as_of_date),
                )
            )
            .order_by(P50Target.p50_target_start_date.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def create_target(
        self, windfarm_id: int, data: P50TargetCreate
    ) -> P50Target:
        """Create a new P50 target.

        If no start date provided, defaults to windfarm COD month + 2 months
        rounded up to the 1st of the month.
        Validates no overlap with existing targets.
        """
        start_date = data.p50_target_start_date
        if start_date is None:
            start_date = await self._compute_default_start_date(windfarm_id)

        await self._validate_no_overlap(
            windfarm_id, start_date, data.p50_target_end_date
        )

        target = P50Target(
            windfarm_id=windfarm_id,
            p50_target_start_date=start_date,
            p50_target_end_date=data.p50_target_end_date,
            p50_target_volume_gwh=data.p50_target_volume_gwh,
            source=data.source,
            comment=data.comment,
        )
        self.db.add(target)
        await self.db.commit()
        await self.db.refresh(target)
        return target

    async def update_target(
        self, target_id: int, data: P50TargetUpdate
    ) -> Optional[P50Target]:
        """Update a P50 target. Re-validates no overlap after update."""
        target = await self.get_target(target_id)
        if target is None:
            return None

        update_fields = data.model_dump(exclude_unset=True)

        for field, value in update_fields.items():
            setattr(target, field, value)

        # Re-validate overlap with the updated dates
        await self._validate_no_overlap(
            target.windfarm_id,
            target.p50_target_start_date,
            target.p50_target_end_date,
            exclude_id=target.id,
        )

        await self.db.commit()
        await self.db.refresh(target)
        return target

    async def delete_target(self, target_id: int) -> bool:
        """Delete a P50 target. Returns True if deleted, False if not found."""
        target = await self.get_target(target_id)
        if target is None:
            return False
        await self.db.delete(target)
        await self.db.commit()
        return True

    # --- Analysis ---

    async def get_p50_analysis(
        self,
        windfarm_id: int,
        target_id: Optional[int] = None,
    ) -> Optional[P50AnalysisResult]:
        """Full P50 analysis comparing actual generation against the P50 target.

        Calculation logic (per team spec):
        - Monthly P50 = Annual P50 / 12
        - Aggregated P50 = cumulative sum of monthly P50 from start
        - Aggregated actual = cumulative sum of actual monthly generation
        - Gap = aggregated P50 - aggregated actual
        - Average annual generation excludes first year after COD
        """
        # Load windfarm
        wf_result = await self.db.execute(
            select(Windfarm).where(Windfarm.id == windfarm_id)
        )
        windfarm = wf_result.scalar_one_or_none()
        if windfarm is None:
            return None

        # Load target
        if target_id:
            target = await self.get_target(target_id)
        else:
            target = await self.get_active_target(windfarm_id)

        if target is None:
            return None

        annual_p50 = float(target.p50_target_volume_gwh)
        monthly_p50 = annual_p50 / 12.0
        p50_start = target.p50_target_start_date

        # Query monthly generation from p50_start_date
        monthly_gen = await self._get_monthly_generation(windfarm_id, p50_start)

        # Build cumulative timeseries
        monthly_data: List[P50MonthlyDataPoint] = []
        aggregated_p50 = 0.0
        aggregated_actual = 0.0

        for i, (month_str, actual_gwh) in enumerate(monthly_gen):
            aggregated_p50 += monthly_p50
            aggregated_actual += actual_gwh
            gap = aggregated_p50 - aggregated_actual
            monthly_data.append(
                P50MonthlyDataPoint(
                    month=month_str,
                    monthly_p50_gwh=round(monthly_p50, 3),
                    actual_generation_gwh=round(actual_gwh, 3),
                    aggregated_p50_gwh=round(aggregated_p50, 3),
                    aggregated_actual_gwh=round(aggregated_actual, 3),
                    aggregated_gap_gwh=round(gap, 3),
                )
            )

        # Build yearly gaps
        yearly_gaps = self._build_yearly_gaps(monthly_gen, monthly_p50)

        # Summary metrics (exclude first year after COD per team spec)
        yearly_actuals = [g.actual_generation_gwh for g in yearly_gaps]
        if len(yearly_actuals) > 1:
            # Exclude first year (COD year)
            avg_annual_gen = sum(yearly_actuals[1:]) / len(yearly_actuals[1:])
        elif yearly_actuals:
            avg_annual_gen = yearly_actuals[0]
        else:
            avg_annual_gen = 0.0

        yearly_gap_values = [g.gap_gwh for g in yearly_gaps]
        avg_annual_gap = sum(yearly_gap_values) / len(yearly_gap_values) if yearly_gap_values else 0.0

        total_gap = aggregated_p50 - aggregated_actual if monthly_data else 0.0
        gap_pct = (total_gap / avg_annual_gen * 100) if avg_annual_gen > 0 else None
        gap_months = total_gap / monthly_p50 if monthly_p50 > 0 else 0.0

        # P50 capacity factor
        p50_cf = None
        if windfarm.nameplate_capacity_mw and windfarm.nameplate_capacity_mw > 0:
            p50_cf = round(
                (annual_p50 * 1000) / (windfarm.nameplate_capacity_mw * 8760) * 100, 2
            )

        target_response = self._to_target_response(target)

        return P50AnalysisResult(
            windfarm_id=windfarm_id,
            windfarm_name=windfarm.name,
            installed_capacity_mw=windfarm.nameplate_capacity_mw,
            p50_target=target_response,
            p50_capacity_factor_pct=p50_cf,
            avg_annual_generation_gwh=round(avg_annual_gen, 3),
            avg_annual_gap_gwh=round(avg_annual_gap, 3),
            gap_from_p50_gwh=round(total_gap, 3),
            gap_pct_of_annual_avg=round(gap_pct, 1) if gap_pct is not None else None,
            gap_in_months=round(gap_months, 1),
            monthly_data=monthly_data,
            yearly_gaps=yearly_gaps,
        )

    # --- Internal helpers ---

    async def _get_monthly_generation(
        self, windfarm_id: int, start_date: date
    ) -> List[tuple]:
        """Get monthly net generation in GWh from start_date, excluding ramp-up hours.

        Uses raw SQL for performance — aggregating years of hourly data can be
        slow with the ORM. Uses the idx_gen_windfarm_hour index directly.

        Returns list of (month_str, actual_gwh) tuples.
        """
        # Set a generous statement timeout for this heavy aggregation
        await self.db.execute(text("SET LOCAL statement_timeout = '120s'"))

        result = await self.db.execute(
            text("""
                SELECT TO_CHAR(DATE_TRUNC('month', hour), 'YYYY-MM') AS month,
                       SUM(generation_mwh - COALESCE(consumption_mwh, 0)) / 1000.0 AS actual_gwh
                FROM generation_data
                WHERE windfarm_id = :windfarm_id
                  AND DATE_TRUNC('month', hour) >= DATE_TRUNC('month', CAST(:start_date AS timestamp))
                  AND is_ramp_up = false
                GROUP BY DATE_TRUNC('month', hour)
                ORDER BY DATE_TRUNC('month', hour)
            """),
            {"windfarm_id": windfarm_id, "start_date": start_date},
        )

        return [(row.month, float(row.actual_gwh or 0)) for row in result.all()]

    @staticmethod
    def _build_yearly_gaps(
        monthly_gen: List[tuple], monthly_p50: float
    ) -> List[P50YearlyGap]:
        """Build per-year gap breakdown from monthly data."""
        yearly: Dict[int, Dict[str, float]] = {}
        for month_str, actual_gwh in monthly_gen:
            year = int(month_str[:4])
            if year not in yearly:
                yearly[year] = {"actual": 0.0, "months": 0}
            yearly[year]["actual"] += actual_gwh
            yearly[year]["months"] += 1

        gaps = []
        for year in sorted(yearly.keys()):
            data = yearly[year]
            # Prorate P50 target by number of months with data in that year
            p50_for_year = monthly_p50 * data["months"]
            gap = p50_for_year - data["actual"]
            gap_months = gap / monthly_p50 if monthly_p50 > 0 else 0.0
            gaps.append(
                P50YearlyGap(
                    year=year,
                    actual_generation_gwh=round(data["actual"], 3),
                    p50_target_gwh=round(p50_for_year, 3),
                    gap_gwh=round(gap, 3),
                    gap_months=round(gap_months, 1),
                )
            )
        return gaps

    async def _compute_default_start_date(self, windfarm_id: int) -> date:
        """Compute default P50 start date: COD month + 2 months, rounded up to 1st.

        Example: COD = June 15 → July 1 + 2 months = September 1
        """
        result = await self.db.execute(
            select(Windfarm.commercial_operational_date).where(Windfarm.id == windfarm_id)
        )
        cod = result.scalar_one_or_none()

        if cod is None:
            raise ValueError(
                f"Windfarm {windfarm_id} has no commercial operational date set. "
                "Please provide p50_target_start_date explicitly."
            )

        # Round up to 1st of next month, then add 2 months
        if cod.day > 1:
            # Round up to next month
            if cod.month == 12:
                first_of_next = date(cod.year + 1, 1, 1)
            else:
                first_of_next = date(cod.year, cod.month + 1, 1)
        else:
            first_of_next = date(cod.year, cod.month, 1)

        # Add 2 months
        month = first_of_next.month + 2
        year = first_of_next.year
        if month > 12:
            month -= 12
            year += 1

        return date(year, month, 1)

    async def _validate_no_overlap(
        self,
        windfarm_id: int,
        start_date: date,
        end_date: Optional[date],
        exclude_id: Optional[int] = None,
    ) -> None:
        """Validate that the new/updated target doesn't overlap with existing ones."""
        conditions = [P50Target.windfarm_id == windfarm_id]
        if exclude_id:
            conditions.append(P50Target.id != exclude_id)

        result = await self.db.execute(
            select(P50Target).where(and_(*conditions)).order_by(P50Target.p50_target_start_date)
        )
        existing = list(result.scalars().all())

        for existing_target in existing:
            ex_start = existing_target.p50_target_start_date
            ex_end = existing_target.p50_target_end_date

            # Check overlap:
            # New target starts before existing ends AND new target ends after existing starts
            if end_date is None:
                new_end_check = True  # Ongoing, so it extends forever
            else:
                new_end_check = end_date >= ex_start

            if ex_end is None:
                ex_end_check = True  # Existing is ongoing
            else:
                ex_end_check = start_date <= ex_end

            if new_end_check and ex_end_check:
                raise ValueError(
                    f"P50 target dates overlap with existing target "
                    f"(ID: {existing_target.id}, "
                    f"start: {ex_start}, end: {ex_end or 'ongoing'}). "
                    f"Start date of a new target must be after end date of the previous target."
                )

    @staticmethod
    def _to_target_response(target: P50Target) -> P50TargetResponse:
        """Convert a P50Target model to a P50TargetResponse schema."""
        annual = float(target.p50_target_volume_gwh)
        return P50TargetResponse(
            id=target.id,
            windfarm_id=target.windfarm_id,
            p50_target_start_date=target.p50_target_start_date,
            p50_target_end_date=target.p50_target_end_date,
            p50_target_volume_gwh=annual,
            monthly_p50_gwh=round(annual / 12.0, 3),
            source=target.source,
            comment=target.comment,
            created_at=target.created_at,
            updated_at=target.updated_at,
        )
