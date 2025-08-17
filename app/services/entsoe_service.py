"""Service layer for ENTSOE data operations."""

from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd
import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import ValidationException
from app.models.user import User
from app.services.entsoe_client import ENTSOEClient
from app.services.entsoe_storage_service import ENTSOEStorageService

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
        current_user: Optional[User] = None,
        store_data: bool = True,
    ) -> Dict[str, any]:
        """
        Fetch real-time generation data from ENTSOE API.

        Args:
            start_date: Start date for data fetch
            end_date: End date for data fetch
            area_codes: List of area codes to fetch
            production_types: List of production types (wind, solar)
            current_user: User making the request (optional)

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

        results = {
            "data": [],
            "metadata": {
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

            # Store data in database if requested
            if store_data and results["data"]:
                storage_service = ENTSOEStorageService(self.db)
                storage_result = await storage_service.store_generation_data(
                    data=results["data"],
                    user=current_user,
                )
                
                # Add storage metadata to results
                results["metadata"]["storage"] = storage_result

            return results

        except Exception as e:
            logger.error(f"Error fetching ENTSOE data: {str(e)}")
            raise

