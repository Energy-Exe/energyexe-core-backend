"""EIA API client service."""

from datetime import datetime
from typing import Dict, List, Optional, Tuple

import httpx
import pandas as pd
import structlog

from app.core.config import get_settings

logger = structlog.get_logger()


class EIAClient:
    """Client for interacting with EIA API v2."""

    BASE_URL = "https://api.eia.gov/v2/electricity/facility-fuel/data/"

    def __init__(self, api_key: str = None):
        settings = get_settings()
        self.api_key = api_key or settings.EIA_API_KEY
        if not self.api_key:
            logger.warning("EIA API key not configured")

    async def fetch_monthly_generation_data(
        self,
        plant_codes: List[str],
        start_year: int,
        start_month: int,
        end_year: int,
        end_month: int,
    ) -> Tuple[pd.DataFrame, Dict[str, any]]:
        """
        Fetch monthly generation data from EIA API.

        Args:
            plant_codes: List of plant codes (generation unit codes)
            start_year: Start year
            start_month: Start month (1-12)
            end_year: End year
            end_month: End month (1-12)

        Returns:
            Tuple of (DataFrame with generation data, metadata dict)
        """
        try:
            metadata = {
                "start_year": start_year,
                "start_month": start_month,
                "end_year": end_year,
                "end_month": end_month,
                "success": True,
                "errors": [],
                "plant_codes_requested": plant_codes,
                "plant_codes_found": set(),
            }

            # Build query parameters
            params = [
                ("frequency", "monthly"),
                ("data[0]", "generation"),
                ("facets[fuel2002][]", "WND"),  # Wind fuel type
                ("sort[0][column]", "period"),
                ("sort[0][direction]", "desc"),
                ("offset", "0"),
                ("length", "5000"),
            ]

            # Add plant codes as facets
            for code in plant_codes:
                params.append(("facets[plantCode][]", code))

            # Add API key if available
            if self.api_key:
                params.append(("api_key", self.api_key))

            logger.info(f"Fetching EIA data for plant codes: {plant_codes}")
            logger.info(f"Period: {start_year}-{start_month:02d} to {end_year}-{end_month:02d}")

            # Make the API request
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(
                    self.BASE_URL,
                    params=params,
                )

                if response.status_code != 200:
                    error_msg = f"EIA API error: {response.status_code} - {response.text}"
                    logger.error(error_msg)
                    metadata["success"] = False
                    metadata["errors"].append({"error": error_msg})
                    return pd.DataFrame(), metadata

                data = response.json()

                if "response" not in data or "data" not in data["response"]:
                    logger.warning("EIA API returned no data")
                    metadata["errors"].append(
                        {"error": "No data available for the specified parameters"}
                    )
                    metadata["success"] = False
                    return pd.DataFrame(), metadata

                # Extract the data array
                records = data["response"]["data"]

                if not records:
                    logger.warning("EIA API returned empty data array")
                    metadata["records"] = 0
                    return pd.DataFrame(), metadata

                # Convert to DataFrame
                df = pd.DataFrame(records)

                logger.info(f"Received {len(df)} records from EIA API")
                logger.info(f"Columns: {df.columns.tolist()}")

                # Process and standardize the data
                if not df.empty:
                    # Filter data by date range
                    if "period" in df.columns:
                        # Convert period to datetime for filtering
                        df["period_date"] = pd.to_datetime(df["period"], format="%Y-%m")

                        start_date = pd.Timestamp(year=start_year, month=start_month, day=1)
                        end_date = pd.Timestamp(year=end_year, month=end_month, day=1)

                        # Filter by date range
                        df = df[(df["period_date"] >= start_date) & (df["period_date"] <= end_date)]

                        # Drop the temporary column
                        df = df.drop("period_date", axis=1)

                    # Track which plant codes were found
                    if "plantCode" in df.columns:
                        # Convert plantCode to string to ensure consistency
                        df["plantCode"] = df["plantCode"].astype(str)
                        metadata["plant_codes_found"] = set(df["plantCode"].unique())

                    # Ensure generation is numeric
                    if "generation" in df.columns:
                        df["generation"] = pd.to_numeric(df["generation"], errors="coerce").fillna(
                            0
                        )

                    metadata["records"] = len(df)
                else:
                    metadata["records"] = 0

                return df, metadata

        except Exception as e:
            logger.error(f"EIA API error: {str(e)}")
            metadata = {
                "start_year": start_year,
                "start_month": start_month,
                "end_year": end_year,
                "end_month": end_month,
                "success": False,
                "errors": [{"general": str(e)}],
                "records": 0,
            }
            return pd.DataFrame(), metadata
