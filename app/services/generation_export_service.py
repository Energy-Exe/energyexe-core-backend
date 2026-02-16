"""Service for exporting generation data to CSV."""

from datetime import date, datetime, timezone
from typing import List, Optional, Dict, Any, AsyncGenerator
from io import StringIO
import csv

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, text
from sqlalchemy.orm import selectinload

from app.models.windfarm import Windfarm
from app.models.generation_data import GenerationData

EXPORT_QUERY_TIMEOUT = 300


class GenerationExportService:
    """Service for generating CSV exports of generation data."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_filtered_windfarm_ids(
        self,
        windfarm_ids: Optional[List[int]] = None,
        country_id: Optional[int] = None,
        region_id: Optional[int] = None,
        state_id: Optional[int] = None,
        bidzone_id: Optional[int] = None,
        market_balance_area_id: Optional[int] = None,
        control_area_id: Optional[int] = None,
        location_type: Optional[str] = None,
        status: Optional[str] = None,
        foundation_type: Optional[str] = None,
        min_capacity_mw: Optional[float] = None,
        max_capacity_mw: Optional[float] = None,
    ) -> List[int]:
        """Get windfarm IDs based on filter criteria."""

        query = select(Windfarm.id)

        conditions = []

        if windfarm_ids:
            conditions.append(Windfarm.id.in_(windfarm_ids))
        if country_id:
            conditions.append(Windfarm.country_id == country_id)
        if region_id:
            conditions.append(Windfarm.region_id == region_id)
        if state_id:
            conditions.append(Windfarm.state_id == state_id)
        if bidzone_id:
            conditions.append(Windfarm.bidzone_id == bidzone_id)
        if market_balance_area_id:
            conditions.append(Windfarm.market_balance_area_id == market_balance_area_id)
        if control_area_id:
            conditions.append(Windfarm.control_area_id == control_area_id)
        if location_type:
            conditions.append(Windfarm.location_type == location_type)
        if status:
            conditions.append(Windfarm.status == status)
        if foundation_type:
            conditions.append(Windfarm.foundation_type == foundation_type)
        if min_capacity_mw is not None:
            conditions.append(Windfarm.nameplate_capacity_mw >= min_capacity_mw)
        if max_capacity_mw is not None:
            conditions.append(Windfarm.nameplate_capacity_mw <= max_capacity_mw)

        if conditions:
            query = query.where(and_(*conditions))

        result = await self.db.execute(query)
        return [row[0] for row in result.all()]

    async def get_windfarm_metadata(
        self,
        windfarm_ids: List[int]
    ) -> Dict[int, Dict[str, Any]]:
        """Get metadata for windfarms to include in export."""

        query = select(Windfarm).where(
            Windfarm.id.in_(windfarm_ids)
        ).options(
            selectinload(Windfarm.country),
            selectinload(Windfarm.region),
            selectinload(Windfarm.bidzone),
        )

        result = await self.db.execute(query)
        windfarms = result.scalars().all()

        metadata = {}
        for wf in windfarms:
            metadata[wf.id] = {
                'windfarm_code': wf.code,
                'windfarm_name': wf.name,
                'country_code': wf.country.code if wf.country else '',
                'country_name': wf.country.name if wf.country else '',
                'region_name': wf.region.name if wf.region else '',
                'bidzone_code': wf.bidzone.code if wf.bidzone else '',
                'location_type': wf.location_type or '',
                'foundation_type': wf.foundation_type or '',
                'status': wf.status or '',
                'nameplate_capacity_mw': float(wf.nameplate_capacity_mw) if wf.nameplate_capacity_mw else '',
            }

        return metadata

    async def stream_csv_export(
        self,
        windfarm_ids: List[int],
        start_date: date,
        end_date: date,
        granularity: str,
        source: Optional[str] = None,
        include_metadata: bool = True,
    ) -> AsyncGenerator[str, None]:
        """
        Stream CSV data as an async generator.

        Yields CSV rows as strings, starting with header.
        """

        # Always get windfarm metadata (at minimum we need the codes)
        metadata = await self.get_windfarm_metadata(windfarm_ids)

        # Define period column based on granularity
        if granularity == "daily":
            period_column = func.date_trunc('day', GenerationData.hour)
        else:  # monthly
            period_column = func.date_trunc('month', GenerationData.hour)

        # Build date range
        start_dt = datetime.combine(start_date, datetime.min.time()).replace(tzinfo=timezone.utc)
        end_dt = datetime.combine(end_date, datetime.max.time()).replace(tzinfo=timezone.utc)

        # Build conditions
        conditions = [
            GenerationData.windfarm_id.in_(windfarm_ids),
            GenerationData.hour >= start_dt,
            GenerationData.hour <= end_dt,
        ]

        # Add source filter if specified
        if source:
            conditions.append(GenerationData.source == source)

        # Build aggregation query
        query = select(
            period_column.label('period'),
            GenerationData.windfarm_id,
            GenerationData.source,
            func.sum(GenerationData.generation_mwh - func.coalesce(GenerationData.consumption_mwh, 0)).label('total_generation_mwh'),
            func.avg(GenerationData.capacity_factor).label('avg_capacity_factor'),
            func.count(GenerationData.id).label('data_points'),
        ).where(
            and_(*conditions)
        ).group_by(
            period_column,
            GenerationData.windfarm_id,
            GenerationData.source,
        ).order_by(
            period_column,
            GenerationData.windfarm_id,
        )

        # Build CSV header
        if include_metadata:
            headers = [
                'period',
                'windfarm_id',
                'windfarm_code',
                'windfarm_name',
                'country_code',
                'country_name',
                'region_name',
                'bidzone_code',
                'location_type',
                'foundation_type',
                'status',
                'nameplate_capacity_mw',
                'source',
                'total_generation_mwh',
                'avg_capacity_factor',
                'data_points',
            ]
        else:
            headers = [
                'period',
                'windfarm_id',
                'windfarm_code',
                'source',
                'total_generation_mwh',
                'avg_capacity_factor',
                'data_points',
            ]

        # Yield header row
        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(headers)
        yield output.getvalue()

        await self.db.execute(text(f"SET LOCAL statement_timeout = '{EXPORT_QUERY_TIMEOUT * 1000}'"))

        result = await self.db.execute(query)
        rows = result.all()

        for row in rows:
            output = StringIO()
            writer = csv.writer(output)

            # Format period based on granularity
            if granularity == "daily":
                period_str = row.period.strftime('%Y-%m-%d')
            else:  # monthly
                period_str = row.period.strftime('%Y-%m')

            # Format numeric values
            total_gen = round(float(row.total_generation_mwh), 3) if row.total_generation_mwh else 0
            avg_cf = round(float(row.avg_capacity_factor), 4) if row.avg_capacity_factor else ''

            if include_metadata:
                wf_meta = metadata.get(row.windfarm_id, {})
                csv_row = [
                    period_str,
                    row.windfarm_id,
                    wf_meta.get('windfarm_code', ''),
                    wf_meta.get('windfarm_name', ''),
                    wf_meta.get('country_code', ''),
                    wf_meta.get('country_name', ''),
                    wf_meta.get('region_name', ''),
                    wf_meta.get('bidzone_code', ''),
                    wf_meta.get('location_type', ''),
                    wf_meta.get('foundation_type', ''),
                    wf_meta.get('status', ''),
                    wf_meta.get('nameplate_capacity_mw', ''),
                    row.source,
                    total_gen,
                    avg_cf,
                    row.data_points,
                ]
            else:
                wf_meta = metadata.get(row.windfarm_id, {})
                csv_row = [
                    period_str,
                    row.windfarm_id,
                    wf_meta.get('windfarm_code', ''),
                    row.source,
                    total_gen,
                    avg_cf,
                    row.data_points,
                ]

            writer.writerow(csv_row)
            yield output.getvalue()

    def generate_filename(
        self,
        granularity: str,
        start_date: date,
        end_date: date,
    ) -> str:
        """Generate descriptive filename for export."""

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return f"generation_export_{granularity}_{start_date}_{end_date}_{timestamp}.csv"
