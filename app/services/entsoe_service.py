"""Service layer for ENTSOE data operations."""

from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd
import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import ValidationException
from app.models.entsoe_fetch_history import EntsoeFetchHistory
from app.models.user import User
from app.services.entsoe_client import ENTSOEClient

logger = structlog.get_logger()


class ENTSOEService:
    """Service layer for ENTSOE data operations."""

    def __init__(self, db: AsyncSession):
        self.db = db
        self.client = ENTSOEClient()

    async def fetch_real_time_generation(
        self,
        start_date: datetime,
        end_date: datetime,
        area_codes: List[str],
        production_types: List[str],
        current_user: User,
    ) -> Dict[str, any]:
        """
        Fetch real-time generation data from ENTSOE API.
        Track the fetch operation in history.

        Args:
            start_date: Start date for data fetch
            end_date: End date for data fetch
            area_codes: List of area codes to fetch
            production_types: List of production types (wind, solar)
            current_user: User making the request

        Returns:
            Dictionary with data and metadata
        """

        # Validate date range (max 1 month for real-time queries)
        if end_date - start_date > timedelta(days=31):
            raise ValidationException("Date range cannot exceed 31 days for real-time queries")

        if not area_codes:
            raise ValidationException("At least one area code must be specified")

        if not production_types:
            raise ValidationException("At least one production type must be specified")

        # Create fetch history record
        fetch_record = EntsoeFetchHistory(
            request_type="real_time",
            start_datetime=start_date,
            end_datetime=end_date,
            area_code=",".join(area_codes),
            production_type=",".join(production_types),
            status="pending",
            requested_by_user_id=current_user.id,
        )
        self.db.add(fetch_record)
        await self.db.commit()
        await self.db.refresh(fetch_record)

        results = {
            "data": [],
            "metadata": {
                "fetch_id": fetch_record.id,
                "areas": {},
                "total_records": 0,
                "errors": [],
            },
        }

        start_time = datetime.utcnow()

        try:
            # Fetch data for each area
            for area_code in area_codes:
                try:
                    df, metadata = await self.client.fetch_generation_data(
                        start_date, end_date, area_code, production_types
                    )

                    if not df.empty:
                        # Convert to JSON-serializable format
                        df_dict = df.to_dict("records")
                        # Ensure timestamps are ISO format strings
                        for record in df_dict:
                            timestamp = record.get("timestamp")
                            if pd.api.types.is_datetime64_any_dtype(type(timestamp)) or isinstance(
                                timestamp, (pd.Timestamp, datetime)
                            ):
                                record["timestamp"] = pd.Timestamp(timestamp).isoformat()

                        results["data"].extend(df_dict)
                        # Convert error dicts to strings
                        error_strings = []
                        for err in metadata.get("errors", []):
                            if isinstance(err, dict):
                                error_strings.append(err.get("error", str(err)))
                            else:
                                error_strings.append(str(err))

                        results["metadata"]["areas"][area_code] = {
                            "success": metadata["success"],
                            "records": metadata["records"],
                            "errors": error_strings,
                        }
                        results["metadata"]["total_records"] += metadata["records"]
                    else:
                        # Convert error dicts to strings
                        error_strings = []
                        for err in metadata.get("errors", []):
                            if isinstance(err, dict):
                                error_strings.append(err.get("error", str(err)))
                            else:
                                error_strings.append(str(err))

                        if not error_strings:
                            error_strings = ["No data available"]

                        results["metadata"]["areas"][area_code] = {
                            "success": False,
                            "records": 0,
                            "errors": error_strings,
                        }

                except Exception as e:
                    logger.error(f"Error fetching data for {area_code}: {str(e)}")
                    results["metadata"]["errors"].append({"area_code": area_code, "error": str(e)})
                    results["metadata"]["areas"][area_code] = {
                        "success": False,
                        "records": 0,
                        "errors": [str(e)],
                    }

            # Update fetch history
            fetch_record.status = "success" if results["data"] else "failed"
            fetch_record.records_fetched = results["metadata"]["total_records"]
            fetch_record.response_time_ms = int(
                (datetime.utcnow() - start_time).total_seconds() * 1000
            )
            fetch_record.completed_at = datetime.utcnow()

            if results["metadata"]["errors"]:
                fetch_record.error_message = str(results["metadata"]["errors"])
                if results["data"]:
                    fetch_record.status = "partial"

            await self.db.commit()

            return results

        except Exception as e:
            # Update fetch history with error
            fetch_record.status = "failed"
            fetch_record.error_message = str(e)
            fetch_record.completed_at = datetime.utcnow()
            await self.db.commit()
            raise

    async def get_fetch_history(
        self, limit: int = 100, offset: int = 0, status: Optional[str] = None
    ) -> List[EntsoeFetchHistory]:
        """
        Get fetch history records.

        Args:
            limit: Maximum number of records to return
            offset: Number of records to skip
            status: Filter by status (optional)

        Returns:
            List of fetch history records
        """
        query = select(EntsoeFetchHistory).order_by(EntsoeFetchHistory.created_at.desc())

        if status:
            query = query.where(EntsoeFetchHistory.status == status)

        query = query.limit(limit).offset(offset)
        result = await self.db.execute(query)
        return result.scalars().all()
