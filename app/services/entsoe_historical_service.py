"""Service for fetching and storing historical ENTSOE data."""

import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd
import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.models.entsoe_fetch_history import EntsoeFetchHistory
from app.services.entsoe_client import ENTSOEClient

logger = structlog.get_logger()


class ENTSOEHistoricalService:
    """Service for fetching and storing historical ENTSOE data."""

    def __init__(self, db: AsyncSession):
        self.db = db
        self.client = ENTSOEClient()
        self.settings = get_settings()

    async def fetch_and_store_historical_data(
        self,
        start_date: datetime,
        end_date: datetime,
        area_codes: List[str],
        production_types: List[str] = ["wind", "solar"],
        batch_days: int = 7,
    ) -> Dict[str, any]:
        """
        Fetch historical data in batches and store in TimescaleDB.

        Args:
            start_date: Start date for historical data
            end_date: End date for historical data
            area_codes: List of area codes to fetch
            production_types: List of production types (default: wind, solar)
            batch_days: Number of days to fetch in each batch (default 7)

        Returns:
            Dictionary with fetch results and statistics
        """

        # Create master fetch record
        fetch_record = EntsoeFetchHistory(
            request_type="historical_batch",
            start_datetime=start_date,
            end_datetime=end_date,
            area_code=",".join(area_codes),
            production_type=",".join(production_types),
            status="pending",
        )
        self.db.add(fetch_record)
        await self.db.commit()
        await self.db.refresh(fetch_record)

        results = {
            "fetch_id": fetch_record.id,
            "total_records": 0,
            "batches_processed": 0,
            "errors": [],
        }

        try:
            # Process in batches
            current_start = start_date

            while current_start < end_date:
                batch_end = min(current_start + timedelta(days=batch_days), end_date)

                logger.info(
                    "Processing batch", start=current_start.isoformat(), end=batch_end.isoformat()
                )

                for area_code in area_codes:
                    try:
                        # Fetch data
                        df, metadata = await self.client.fetch_generation_data(
                            current_start, batch_end, area_code, production_types
                        )

                        if not df.empty:
                            # Store in database
                            records_stored = await self._store_generation_data(
                                df, area_code, fetch_record.id
                            )

                            results["total_records"] += records_stored
                            logger.info(
                                "Stored generation data",
                                area_code=area_code,
                                records=records_stored,
                            )

                    except Exception as e:
                        logger.error("Error processing batch", area_code=area_code, error=str(e))
                        results["errors"].append(
                            {
                                "area_code": area_code,
                                "batch_start": current_start.isoformat(),
                                "error": str(e),
                            }
                        )

                results["batches_processed"] += 1
                current_start = batch_end

                # Small delay to avoid rate limiting
                await asyncio.sleep(1)

            # Update fetch record
            fetch_record.status = "success" if results["total_records"] > 0 else "failed"
            fetch_record.records_fetched = results["total_records"]
            fetch_record.completed_at = datetime.utcnow()

            if results["errors"]:
                fetch_record.error_message = str(results["errors"][:5])  # Store first 5 errors
                if results["total_records"] > 0:
                    fetch_record.status = "partial"

            await self.db.commit()

            logger.info(
                "Historical data fetch completed",
                fetch_id=fetch_record.id,
                total_records=results["total_records"],
                batches=results["batches_processed"],
            )

            return results

        except Exception as e:
            fetch_record.status = "failed"
            fetch_record.error_message = str(e)
            fetch_record.completed_at = datetime.utcnow()
            await self.db.commit()
            raise

    async def _store_generation_data(
        self, df: pd.DataFrame, area_code: str, fetch_history_id: int
    ) -> int:
        """
        Store generation data in TimescaleDB.

        Args:
            df: DataFrame with generation data
            area_code: Area code for the data
            fetch_history_id: ID of the fetch history record

        Returns:
            Number of records stored
        """

        # Ensure we have the required columns
        if "timestamp" not in df.columns:
            logger.error("Missing timestamp column in dataframe")
            return 0

        # Prepare data for insertion
        records = []
        for _, row in df.iterrows():
            record = {
                "time": row["timestamp"],
                "area_code": area_code,
                "production_type": row.get("production_type", "unknown"),
                "value_mw": float(row.get("value", 0)),
                "fetch_history_id": fetch_history_id,
            }
            records.append(record)

        if not records:
            return 0

        # Bulk insert using raw SQL for performance
        insert_query = text(
            """
            INSERT INTO power_generation_data 
            (time, area_code, production_type, value_mw, fetch_history_id)
            VALUES (:time, :area_code, :production_type, :value_mw, :fetch_history_id)
            ON CONFLICT (time, area_code, production_type) DO UPDATE
            SET value_mw = EXCLUDED.value_mw,
                fetch_history_id = EXCLUDED.fetch_history_id,
                created_at = CURRENT_TIMESTAMP
        """
        )

        # Execute in batches for better performance
        batch_size = 1000
        total_inserted = 0

        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            await self.db.execute(insert_query, batch)
            total_inserted += len(batch)

        await self.db.commit()

        return total_inserted

    async def get_stored_generation_data(
        self,
        start_date: datetime,
        end_date: datetime,
        area_codes: List[str],
        production_types: List[str],
        aggregation: str = "hourly",
    ) -> List[Dict]:
        """
        Query stored generation data from TimescaleDB.

        Args:
            start_date: Start date for query
            end_date: End date for query
            area_codes: List of area codes to query
            production_types: List of production types to query
            aggregation: Aggregation level ('raw', 'hourly', 'daily')

        Returns:
            List of dictionaries with generation data
        """

        if aggregation == "hourly":
            query = text(
                """
                SELECT 
                    hour as time,
                    area_code,
                    production_type,
                    avg_mw as value_mw,
                    min_mw,
                    max_mw,
                    data_points
                FROM generation_hourly_summary
                WHERE hour >= :start_date
                AND hour <= :end_date
                AND area_code = ANY(:area_codes)
                AND production_type = ANY(:production_types)
                ORDER BY hour DESC
                LIMIT 10000
            """
            )
        elif aggregation == "daily":
            query = text(
                """
                SELECT 
                    day as time,
                    area_code,
                    production_type,
                    avg_mw as value_mw,
                    min_mw,
                    max_mw,
                    total_mw,
                    data_points
                FROM generation_daily_summary
                WHERE day >= :start_date
                AND day <= :end_date
                AND area_code = ANY(:area_codes)
                AND production_type = ANY(:production_types)
                ORDER BY day DESC
                LIMIT 1000
            """
            )
        else:
            query = text(
                """
                SELECT 
                    time,
                    area_code,
                    production_type,
                    value_mw,
                    generation_unit_code,
                    data_quality_score
                FROM power_generation_data
                WHERE time >= :start_date
                AND time <= :end_date
                AND area_code = ANY(:area_codes)
                AND production_type = ANY(:production_types)
                ORDER BY time DESC
                LIMIT 50000
            """
            )

        result = await self.db.execute(
            query,
            {
                "start_date": start_date,
                "end_date": end_date,
                "area_codes": area_codes,
                "production_types": production_types,
            },
        )

        rows = result.fetchall()
        return [dict(row._mapping) for row in rows]

    async def get_data_availability(self, area_codes: Optional[List[str]] = None) -> Dict[str, any]:
        """
        Get information about available data in the database.

        Args:
            area_codes: Optional list of area codes to filter by

        Returns:
            Dictionary with data availability information
        """

        query = text(
            """
            SELECT 
                area_code,
                production_type,
                MIN(time) as earliest_data,
                MAX(time) as latest_data,
                COUNT(*) as record_count,
                COUNT(DISTINCT DATE(time)) as days_with_data
            FROM power_generation_data
            WHERE (:filter_areas = false OR area_code = ANY(:area_codes))
            GROUP BY area_code, production_type
            ORDER BY area_code, production_type
        """
        )

        result = await self.db.execute(
            query, {"filter_areas": area_codes is not None, "area_codes": area_codes or []}
        )

        rows = result.fetchall()

        availability = {}
        for row in rows:
            area = row.area_code
            if area not in availability:
                availability[area] = {}

            availability[area][row.production_type] = {
                "earliest_data": row.earliest_data.isoformat() if row.earliest_data else None,
                "latest_data": row.latest_data.isoformat() if row.latest_data else None,
                "record_count": row.record_count,
                "days_with_data": row.days_with_data,
            }

        return availability
