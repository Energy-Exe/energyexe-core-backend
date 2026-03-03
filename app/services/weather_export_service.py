"""Service for exporting weather data to CSV."""

from datetime import date, datetime, timezone
from typing import List, Optional, Dict, Any, AsyncGenerator
from io import StringIO
import csv

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, text
from sqlalchemy.orm import selectinload

from app.models.windfarm import Windfarm
from app.models.weather_data import WeatherData

EXPORT_QUERY_TIMEOUT = 300


class WeatherExportService:
    """Service for generating CSV exports of weather data."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_filtered_windfarm_ids(
        self,
        windfarm_ids: Optional[List[int]] = None,
        country_id: Optional[int] = None,
    ) -> List[int]:
        """Get windfarm IDs based on filter criteria."""

        query = select(Windfarm.id)

        conditions = []

        if windfarm_ids:
            conditions.append(Windfarm.id.in_(windfarm_ids))
        if country_id:
            conditions.append(Windfarm.country_id == country_id)

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
            }

        return metadata

    async def stream_csv_export(
        self,
        windfarm_ids: List[int],
        start_date: date,
        end_date: date,
        include_metadata: bool = True,
    ) -> AsyncGenerator[str, None]:
        """
        Stream CSV data as an async generator.

        Yields CSV rows as strings, starting with header.
        Weather data is always hourly (no aggregation needed).
        """

        metadata = await self.get_windfarm_metadata(windfarm_ids)

        # Build date range
        start_dt = datetime.combine(start_date, datetime.min.time()).replace(tzinfo=timezone.utc)
        end_dt = datetime.combine(end_date, datetime.max.time()).replace(tzinfo=timezone.utc)

        # Build query
        conditions = [
            WeatherData.windfarm_id.in_(windfarm_ids),
            WeatherData.hour >= start_dt,
            WeatherData.hour <= end_dt,
        ]

        query = select(
            WeatherData.hour,
            WeatherData.windfarm_id,
            WeatherData.wind_speed_100m,
            WeatherData.wind_direction_deg,
            WeatherData.temperature_2m_c,
            WeatherData.source,
        ).where(
            and_(*conditions)
        ).order_by(
            WeatherData.hour,
            WeatherData.windfarm_id,
        )

        # Build CSV header
        if include_metadata:
            headers = [
                'hour_utc',
                'windfarm_id',
                'windfarm_code',
                'windfarm_name',
                'country_code',
                'country_name',
                'region_name',
                'bidzone_code',
                'wind_speed_100m',
                'wind_direction_deg',
                'temperature_2m_c',
                'source',
            ]
        else:
            headers = [
                'hour_utc',
                'windfarm_id',
                'windfarm_code',
                'wind_speed_100m',
                'wind_direction_deg',
                'temperature_2m_c',
                'source',
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

            hour_str = row.hour.strftime('%Y-%m-%d %H:%M:%S')
            wind_speed = round(float(row.wind_speed_100m), 3) if row.wind_speed_100m is not None else ''
            wind_dir = round(float(row.wind_direction_deg), 2) if row.wind_direction_deg is not None else ''
            temp = round(float(row.temperature_2m_c), 2) if row.temperature_2m_c is not None else ''

            wf_meta = metadata.get(row.windfarm_id, {})
            if include_metadata:
                csv_row = [
                    hour_str,
                    row.windfarm_id,
                    wf_meta.get('windfarm_code', ''),
                    wf_meta.get('windfarm_name', ''),
                    wf_meta.get('country_code', ''),
                    wf_meta.get('country_name', ''),
                    wf_meta.get('region_name', ''),
                    wf_meta.get('bidzone_code', ''),
                    wind_speed,
                    wind_dir,
                    temp,
                    row.source,
                ]
            else:
                csv_row = [
                    hour_str,
                    row.windfarm_id,
                    wf_meta.get('windfarm_code', ''),
                    wind_speed,
                    wind_dir,
                    temp,
                    row.source,
                ]

            writer.writerow(csv_row)
            yield output.getvalue()

    def generate_filename(
        self,
        start_date: date,
        end_date: date,
    ) -> str:
        """Generate descriptive filename for export."""

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return f"weather_export_{start_date}_{end_date}_{timestamp}.csv"
