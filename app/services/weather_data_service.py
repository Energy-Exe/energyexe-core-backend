"""Weather data service for availability and fetch operations."""
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import List, Optional
from sqlalchemy import select, func, and_, text
from sqlalchemy.ext.asyncio import AsyncSession
import subprocess
import os

from app.models.weather_data import WeatherData
from app.schemas.weather_data import (
    DateAvailability,
    WeatherFetchRequest,
    WeatherFetchJobResponse,
)


class WeatherDataService:
    """Service for weather data availability and fetch operations."""

    EXPECTED_RECORDS_PER_DAY = 38184  # 1,591 windfarms × 24 hours

    async def get_availability_calendar(
        self,
        db: AsyncSession,
        start_date: date,
        end_date: date,
        windfarm_id: Optional[int] = None,
    ) -> List[DateAvailability]:
        """
        Get weather data availability for date range.

        Args:
            db: Database session
            start_date: Start date
            end_date: End date
            windfarm_id: Optional filter for specific windfarm

        Returns:
            List of DateAvailability for each date in range
        """
        # Build query
        if windfarm_id:
            query = text("""
                SELECT
                    DATE(hour AT TIME ZONE 'UTC') as date,
                    COUNT(*) as record_count,
                    COUNT(DISTINCT windfarm_id) as windfarm_count
                FROM weather_data
                WHERE source = 'ERA5'
                  AND windfarm_id = :windfarm_id
                  AND DATE(hour AT TIME ZONE 'UTC') >= :start_date
                  AND DATE(hour AT TIME ZONE 'UTC') <= :end_date
                GROUP BY DATE(hour AT TIME ZONE 'UTC')
                ORDER BY date;
            """)
            result = await db.execute(
                query,
                {"windfarm_id": windfarm_id, "start_date": start_date, "end_date": end_date}
            )
            expected_per_day = 24  # 24 hours for single windfarm
        else:
            query = text("""
                SELECT
                    DATE(hour AT TIME ZONE 'UTC') as date,
                    COUNT(*) as record_count,
                    COUNT(DISTINCT windfarm_id) as windfarm_count
                FROM weather_data
                WHERE source = 'ERA5'
                  AND DATE(hour AT TIME ZONE 'UTC') >= :start_date
                  AND DATE(hour AT TIME ZONE 'UTC') <= :end_date
                GROUP BY DATE(hour AT TIME ZONE 'UTC')
                ORDER BY date;
            """)
            result = await db.execute(
                query,
                {"start_date": start_date, "end_date": end_date}
            )
            expected_per_day = self.EXPECTED_RECORDS_PER_DAY

        rows = result.fetchall()

        # Create dict of existing dates
        date_data = {row[0]: (row[1], row[2]) for row in rows}

        # Generate full date range
        availability = []
        current = start_date
        while current <= end_date:
            if current in date_data:
                record_count, windfarm_count = date_data[current]
                has_data = True
                is_complete = record_count >= expected_per_day
                completion_percentage = (record_count / expected_per_day) * 100
            else:
                record_count = 0
                windfarm_count = 0
                has_data = False
                is_complete = False
                completion_percentage = 0.0

            availability.append(
                DateAvailability(
                    date=current,
                    has_data=has_data,
                    record_count=record_count,
                    expected_count=expected_per_day,
                    is_complete=is_complete,
                    completion_percentage=round(completion_percentage, 2),
                    windfarm_count=windfarm_count,
                )
            )

            current += timedelta(days=1)

        return availability

    async def get_missing_dates(
        self,
        db: AsyncSession,
        start_date: date,
        end_date: date,
    ) -> List[date]:
        """
        Get list of dates with missing or incomplete data.

        Returns:
            List of dates that need data
        """
        availability = await self.get_availability_calendar(db, start_date, end_date)
        return [item.date for item in availability if not item.is_complete]

    async def trigger_fetch_for_date(
        self,
        db: AsyncSession,
        request: WeatherFetchRequest,
    ) -> WeatherFetchJobResponse:
        """
        Trigger ERA5 fetch for a specific date.

        This spawns a background process to run the fetch script.

        Args:
            db: Database session
            request: Fetch request with date

        Returns:
            Job response with job ID and status
        """
        job_id = str(uuid.uuid4())
        target_date = request.date

        # Check if data already exists (unless force_refetch)
        if not request.force_refetch:
            query = select(func.count(WeatherData.id)).where(
                func.date(WeatherData.hour) == target_date,
                WeatherData.source == 'ERA5'
            )
            result = await db.execute(query)
            existing_count = result.scalar()

            if existing_count >= self.EXPECTED_RECORDS_PER_DAY:
                return WeatherFetchJobResponse(
                    job_id=job_id,
                    date=target_date,
                    status="skipped",
                    message=f"Data already exists ({existing_count} records). Use force_refetch=true to re-fetch.",
                    started_at=datetime.now(timezone.utc),
                    completed_at=datetime.now(timezone.utc),
                )

        # Spawn background process
        script_path = os.path.join(
            os.path.dirname(__file__),
            "..",
            "..",
            "scripts",
            "seeds",
            "weather_data",
            "fetch_daily_all_windfarms.py"
        )

        log_dir = os.path.join(
            os.path.dirname(__file__),
            "..",
            "..",
            "logs",
            "weather_fetches"
        )
        os.makedirs(log_dir, exist_ok=True)

        log_file = os.path.join(log_dir, f"fetch_{target_date}_{job_id[:8]}.log")

        # Run script in background
        cmd = [
            "poetry",
            "run",
            "python",
            script_path,
            "--date",
            target_date.isoformat(),
        ]

        try:
            with open(log_file, "w") as f:
                process = subprocess.Popen(
                    cmd,
                    stdout=f,
                    stderr=subprocess.STDOUT,
                    cwd=os.path.dirname(script_path),
                    start_new_session=True,
                )

            return WeatherFetchJobResponse(
                job_id=job_id,
                date=target_date,
                status="running",
                message=f"Fetch job started. Check logs at {log_file}",
                started_at=datetime.now(timezone.utc),
                completed_at=None,
            )

        except Exception as e:
            return WeatherFetchJobResponse(
                job_id=job_id,
                date=target_date,
                status="failed",
                message=f"Failed to start fetch job: {str(e)}",
                started_at=datetime.now(timezone.utc),
                completed_at=datetime.now(timezone.utc),
            )

    async def get_fetch_job_status(
        self,
        db: AsyncSession,
        job_id: str,
    ) -> Optional[WeatherFetchJobResponse]:
        """
        Get status of a fetch job.

        Note: This is a simplified implementation. In production, you'd want
        to store job metadata in a database table.

        Args:
            db: Database session
            job_id: Job ID from trigger_fetch_for_date

        Returns:
            Job status or None if not found
        """
        # For now, return a placeholder
        # In production, implement proper job tracking with a jobs table
        return WeatherFetchJobResponse(
            job_id=job_id,
            date=date.today(),
            status="unknown",
            message="Job tracking not yet implemented. Check logs directory.",
            started_at=None,
            completed_at=None,
        )
