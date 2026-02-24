"""
Service for windfarm generation data comparisons.
"""

from datetime import date, datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, case, exists
from sqlalchemy.orm import joinedload

from app.models.generation_data import GenerationData
from app.models.windfarm import Windfarm


class ComparisonService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_windfarm_comparison(
        self,
        windfarm_ids: List[int],
        start_date: date,
        end_date: date,
        granularity: str = "daily",
        exclude_ramp_up: bool = True
    ) -> Dict[str, Any]:
        """Get generation comparison data for multiple windfarms."""

        # Build base query
        if granularity == "hourly":
            period_column = GenerationData.hour
            period_format = 'YYYY-MM-DD HH24:00'
        elif granularity == "daily":
            period_column = func.date_trunc('day', GenerationData.hour)
            period_format = 'YYYY-MM-DD'
        elif granularity == "weekly":
            period_column = func.date_trunc('week', GenerationData.hour)
            period_format = 'YYYY-MM-DD'
        elif granularity == "monthly":
            period_column = func.date_trunc('month', GenerationData.hour)
            period_format = 'YYYY-MM'
        elif granularity == "quarterly":
            period_column = func.date_trunc('quarter', GenerationData.hour)
            period_format = 'YYYY-Q'
        elif granularity == "yearly":
            period_column = func.date_trunc('year', GenerationData.hour)
            period_format = 'YYYY'
        else:
            period_column = func.date_trunc('day', GenerationData.hour)
            period_format = 'YYYY-MM-DD'

        # When excluding ramp-up, null out capacity factor fields (so they don't
        # affect averages) but keep generation rows so MWh totals are preserved.
        if exclude_ramp_up:
            cf_expr = case((GenerationData.is_ramp_up == True, None), else_=GenerationData.capacity_factor)
            raw_cf_expr = case((GenerationData.is_ramp_up == True, None), else_=GenerationData.raw_capacity_factor)
            raw_cap_expr = case((GenerationData.is_ramp_up == True, None), else_=GenerationData.raw_capacity_mw)
            cap_expr = case((GenerationData.is_ramp_up == True, None), else_=GenerationData.capacity_mw)
        else:
            cf_expr = GenerationData.capacity_factor
            raw_cf_expr = GenerationData.raw_capacity_factor
            raw_cap_expr = GenerationData.raw_capacity_mw
            cap_expr = GenerationData.capacity_mw

        query = select(
            period_column.label('period'),
            GenerationData.windfarm_id,
            Windfarm.name.label('windfarm_name'),
            func.sum(GenerationData.generation_mwh - func.coalesce(GenerationData.consumption_mwh, 0)).label('total_generation'),
            func.avg(GenerationData.generation_mwh - func.coalesce(GenerationData.consumption_mwh, 0)).label('avg_generation'),
            func.max(GenerationData.generation_mwh - func.coalesce(GenerationData.consumption_mwh, 0)).label('max_generation'),
            func.min(GenerationData.generation_mwh - func.coalesce(GenerationData.consumption_mwh, 0)).label('min_generation'),
            func.avg(cf_expr).label('avg_capacity_factor'),
            func.avg(raw_cf_expr).label('avg_raw_capacity_factor'),
            func.avg(raw_cap_expr).label('avg_raw_capacity'),
            func.avg(cap_expr).label('avg_capacity'),
            func.count(GenerationData.id).label('data_points'),
            # Count of ramp-up records in this period (for frontend highlighting)
            func.sum(case((GenerationData.is_ramp_up == True, 1), else_=0)).label('ramp_up_points'),
            # Curtailment data (BOAV integration)
            # Fall back to generation_mwh when metered_mwh is NULL (non-ELEXON sources)
            func.sum(func.coalesce(GenerationData.metered_mwh, GenerationData.generation_mwh)).label('total_metered'),
            func.sum(func.coalesce(GenerationData.curtailed_mwh, 0)).label('total_curtailed'),
        ).join(
            Windfarm, GenerationData.windfarm_id == Windfarm.id
        )

        # Build WHERE conditions
        conditions = [
            GenerationData.windfarm_id.in_(windfarm_ids),
            GenerationData.hour >= datetime.combine(start_date, datetime.min.time()).replace(tzinfo=timezone.utc),
            GenerationData.hour <= datetime.combine(end_date, datetime.max.time()).replace(tzinfo=timezone.utc)
        ]

        query = query.where(and_(*conditions)).group_by(
            period_column,
            GenerationData.windfarm_id,
            Windfarm.name
        ).order_by(period_column)

        result = await self.db.execute(query)
        rows = result.all()

        # Get windfarm names for all requested IDs
        windfarms_query = select(Windfarm.id, Windfarm.name).where(Windfarm.id.in_(windfarm_ids))
        windfarms_result = await self.db.execute(windfarms_query)
        windfarm_map = {row.id: row.name for row in windfarms_result.all()}

        # Generate complete date range based on granularity
        def generate_date_range(start, end, granularity_type):
            """Generate complete date series for the given range and granularity"""
            periods = []
            current = datetime.combine(start, datetime.min.time()).replace(tzinfo=timezone.utc)
            end_dt = datetime.combine(end, datetime.min.time()).replace(tzinfo=timezone.utc)

            if granularity_type == "hourly":
                # For hourly, end_dt must be end-of-day so all 24 hours are generated
                hourly_end = end_dt + timedelta(hours=23)
                while current <= hourly_end:
                    periods.append(current)
                    current += timedelta(hours=1)
            elif granularity_type == "daily":
                while current <= end_dt:
                    periods.append(current)
                    current += timedelta(days=1)
            elif granularity_type == "weekly":
                # Start from the beginning of the week
                current = current - timedelta(days=current.weekday())
                while current <= end_dt:
                    periods.append(current)
                    current += timedelta(weeks=1)
            elif granularity_type == "monthly":
                while current <= end_dt:
                    periods.append(current)
                    # Move to first day of next month
                    if current.month == 12:
                        current = current.replace(year=current.year + 1, month=1, day=1)
                    else:
                        current = current.replace(month=current.month + 1, day=1)
            elif granularity_type == "quarterly":
                # Start from beginning of quarter
                quarter_month = ((current.month - 1) // 3) * 3 + 1
                current = current.replace(month=quarter_month, day=1)
                while current <= end_dt:
                    periods.append(current)
                    # Move to first day of next quarter
                    next_quarter_month = quarter_month + 3
                    if next_quarter_month > 12:
                        current = current.replace(year=current.year + 1, month=next_quarter_month - 12, day=1)
                        quarter_month = next_quarter_month - 12
                    else:
                        current = current.replace(month=next_quarter_month, day=1)
                        quarter_month = next_quarter_month
            elif granularity_type == "yearly":
                # Start from beginning of year
                current = current.replace(month=1, day=1)
                while current <= end_dt:
                    periods.append(current)
                    current = current.replace(year=current.year + 1, month=1, day=1)

            return periods

        # Generate all periods
        all_periods = generate_date_range(start_date, end_date, granularity)

        # Create a map of existing data
        data_map = {}
        for row in rows:
            key = (row.period.astimezone(timezone.utc) if row.period.tzinfo else row.period, row.windfarm_id)
            data_map[key] = row

        # Build complete dataset with all periods for all windfarms
        data = []
        summary = {
            'total_generation': 0,
            'avg_capacity_factor': 0,
            'windfarm_count': len(windfarm_ids),
            'total_records': 0,
            'date_range': {
                'start': str(start_date),
                'end': str(end_date)
            }
        }

        # Use weighted averaging for capacity factor
        total_capacity_factor_weighted = 0
        total_data_points_for_cf = 0

        # For each period and windfarm combination
        for period_dt in all_periods:
            for windfarm_id in windfarm_ids:
                key = (period_dt, windfarm_id)
                row = data_map.get(key)

                # Format period in UTC to prevent timezone offset issues in CSV exports
                period_utc = period_dt.astimezone(timezone.utc) if period_dt.tzinfo else period_dt

                if granularity == 'hourly':
                    period_str = period_utc.strftime('%Y-%m-%d %H:%M:%S')
                elif granularity == 'monthly':
                    period_str = period_utc.strftime('%Y-%m')
                elif granularity == 'quarterly':
                    quarter = (period_utc.month - 1) // 3 + 1
                    period_str = f"{period_utc.year}-Q{quarter}"
                elif granularity == 'yearly':
                    period_str = period_utc.strftime('%Y')
                else:
                    period_str = period_utc.strftime('%Y-%m-%d')

                # If row exists (data available for this period), use actual values
                # If row doesn't exist (no data), use 0 for generation but null for capacity factor
                if row:
                    data.append({
                        'period': period_str,
                        'windfarm_id': row.windfarm_id,
                        'windfarm_name': row.windfarm_name,
                        'total_generation': float(row.total_generation) if row.total_generation else 0,
                        'avg_generation': float(row.avg_generation) if row.avg_generation else 0,
                        'max_generation': float(row.max_generation) if row.max_generation else 0,
                        'min_generation': float(row.min_generation) if row.min_generation else 0,
                        'avg_capacity_factor': float(row.avg_capacity_factor) if row.avg_capacity_factor else 0,
                        'avg_raw_capacity_factor': float(row.avg_raw_capacity_factor) if row.avg_raw_capacity_factor else 0,
                        'avg_raw_capacity': float(row.avg_raw_capacity) if row.avg_raw_capacity else 0,
                        'avg_capacity': float(row.avg_capacity) if row.avg_capacity else 0,
                        'data_points': row.data_points,
                        'ramp_up_points': int(row.ramp_up_points) if row.ramp_up_points else 0,
                        # Curtailment data
                        'total_metered': float(row.total_metered) if row.total_metered else 0,
                        'total_curtailed': float(row.total_curtailed) if row.total_curtailed else 0,
                    })

                    summary['total_generation'] += float(row.total_generation) if row.total_generation else 0
                    summary['total_records'] += row.data_points

                    # Weight capacity factor by number of data points in each period
                    if row.avg_capacity_factor and row.data_points:
                        total_capacity_factor_weighted += float(row.avg_capacity_factor) * row.data_points
                        total_data_points_for_cf += row.data_points
                else:
                    # No data for this period - fill with 0 for generation, null for capacity factor
                    data.append({
                        'period': period_str,
                        'windfarm_id': windfarm_id,
                        'windfarm_name': windfarm_map[windfarm_id],
                        'total_generation': 0,
                        'avg_generation': 0,
                        'max_generation': 0,
                        'min_generation': 0,
                        'avg_capacity_factor': None,  # null means no data
                        'avg_raw_capacity_factor': None,  # null means no data
                        'avg_raw_capacity': None,  # null means no data
                        'avg_capacity': None,  # null means no data
                        'data_points': 0,
                        'ramp_up_points': 0,
                        # Curtailment data
                        'total_metered': 0,
                        'total_curtailed': 0,
                    })

        if total_data_points_for_cf > 0:
            summary['avg_capacity_factor'] = total_capacity_factor_weighted / total_data_points_for_cf

        return {
            'data': data,
            'summary': summary
        }

    async def get_available_windfarms(self) -> List[Dict[str, Any]]:
        """Get list of all windfarms with data availability flag (lightweight)."""

        has_data_subq = exists().where(
            GenerationData.windfarm_id == Windfarm.id
        )

        query = select(
            Windfarm.id,
            Windfarm.name,
            Windfarm.nameplate_capacity_mw,
            has_data_subq.label('has_data'),
        ).order_by(Windfarm.name)

        result = await self.db.execute(query)
        rows = result.all()

        return [
            {
                'id': row.id,
                'name': row.name,
                'capacity_mw': float(row.nameplate_capacity_mw) if row.nameplate_capacity_mw else None,
                'has_data': row.has_data,
            }
            for row in rows
        ]

    async def get_windfarm_statistics(
        self,
        windfarm_ids: List[int],
        period_days: int = 30,
        exclude_ramp_up: bool = True
    ) -> List[Dict[str, Any]]:
        """Get detailed statistics for selected windfarms."""

        end_date = date.today()
        start_date = end_date - timedelta(days=period_days)

        # When excluding ramp-up, null out capacity factor fields but keep generation rows.
        if exclude_ramp_up:
            cf_expr = case((GenerationData.is_ramp_up == True, None), else_=GenerationData.capacity_factor)
            raw_cf_expr = case((GenerationData.is_ramp_up == True, None), else_=GenerationData.raw_capacity_factor)
            raw_cap_expr = case((GenerationData.is_ramp_up == True, None), else_=GenerationData.raw_capacity_mw)
        else:
            cf_expr = GenerationData.capacity_factor
            raw_cf_expr = GenerationData.raw_capacity_factor
            raw_cap_expr = GenerationData.raw_capacity_mw

        query = select(
            Windfarm.id,
            Windfarm.name,
            Windfarm.nameplate_capacity_mw,
            func.sum(GenerationData.generation_mwh - func.coalesce(GenerationData.consumption_mwh, 0)).label('total_generation'),
            func.avg(GenerationData.generation_mwh - func.coalesce(GenerationData.consumption_mwh, 0)).label('avg_generation'),
            func.max(GenerationData.generation_mwh - func.coalesce(GenerationData.consumption_mwh, 0)).label('peak_generation'),
            func.min(GenerationData.generation_mwh - func.coalesce(GenerationData.consumption_mwh, 0)).label('min_generation'),
            func.stddev(GenerationData.generation_mwh - func.coalesce(GenerationData.consumption_mwh, 0)).label('stddev_generation'),
            func.avg(cf_expr).label('avg_capacity_factor'),
            func.max(cf_expr).label('max_capacity_factor'),
            func.min(cf_expr).label('min_capacity_factor'),
            func.avg(raw_cf_expr).label('avg_raw_capacity_factor'),
            func.max(raw_cf_expr).label('max_raw_capacity_factor'),
            func.min(raw_cf_expr).label('min_raw_capacity_factor'),
            func.avg(raw_cap_expr).label('avg_raw_capacity'),
            func.count(GenerationData.id).label('data_points'),
            func.count(case((GenerationData.generation_mwh > 0, 1))).label('active_hours'),
            # Curtailment data — fall back to generation_mwh when metered_mwh is NULL
            func.sum(func.coalesce(GenerationData.metered_mwh, GenerationData.generation_mwh)).label('total_metered'),
            func.sum(func.coalesce(GenerationData.curtailed_mwh, 0)).label('total_curtailed'),
        ).join(
            GenerationData, GenerationData.windfarm_id == Windfarm.id
        )

        # Build WHERE conditions
        stat_conditions = [
            Windfarm.id.in_(windfarm_ids),
            GenerationData.hour >= datetime.combine(start_date, datetime.min.time()).replace(tzinfo=timezone.utc),
            GenerationData.hour <= datetime.combine(end_date, datetime.max.time()).replace(tzinfo=timezone.utc)
        ]

        query = query.where(and_(*stat_conditions)).group_by(
            Windfarm.id,
            Windfarm.name,
            Windfarm.nameplate_capacity_mw
        )

        result = await self.db.execute(query)
        rows = result.all()

        stats = []
        for row in rows:
            availability = (row.active_hours / row.data_points * 100) if row.data_points > 0 else 0

            # Calculate curtailment percentage
            total_gen = float(row.total_generation) if row.total_generation else 0
            total_curtailed = float(row.total_curtailed) if row.total_curtailed else 0
            curtailment_percent = (total_curtailed / total_gen * 100) if total_gen > 0 else 0

            stats.append({
                'windfarm_id': row.id,
                'windfarm_name': row.name,
                'capacity_mw': float(row.nameplate_capacity_mw) if row.nameplate_capacity_mw else None,
                'total_generation': total_gen,
                'peak_generation': float(row.peak_generation) if row.peak_generation else 0,
                'min_generation': float(row.min_generation) if row.min_generation else 0,
                'avg_generation': float(row.avg_generation) if row.avg_generation else 0,
                'stddev_generation': float(row.stddev_generation) if row.stddev_generation else 0,
                'avg_capacity_factor': float(row.avg_capacity_factor) if row.avg_capacity_factor else 0,
                'max_capacity_factor': float(row.max_capacity_factor) if row.max_capacity_factor else 0,
                'min_capacity_factor': float(row.min_capacity_factor) if row.min_capacity_factor else 0,
                'avg_raw_capacity_factor': float(row.avg_raw_capacity_factor) if row.avg_raw_capacity_factor else 0,
                'max_raw_capacity_factor': float(row.max_raw_capacity_factor) if row.max_raw_capacity_factor else 0,
                'min_raw_capacity_factor': float(row.min_raw_capacity_factor) if row.min_raw_capacity_factor else 0,
                'avg_raw_capacity': float(row.avg_raw_capacity) if row.avg_raw_capacity else 0,
                'data_points': row.data_points,
                'period_days': period_days,
                'availability_percent': availability,
                'data_completeness': row.data_points / (period_days * 24) * 100 if period_days > 0 else 0,
                # Curtailment data
                'total_metered': float(row.total_metered) if row.total_metered else 0,
                'total_curtailed': total_curtailed,
                'curtailment_percent': curtailment_percent,
            })

        return stats