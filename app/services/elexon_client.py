"""Elexon API client service."""

from datetime import datetime
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import httpx
import pandas as pd
import structlog

from app.core.config import get_settings

logger = structlog.get_logger()


class ElexonClient:
    """Client for interacting with Elexon Insights API."""

    BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1"

    def __init__(self, api_key: str = None):
        settings = get_settings()
        self.api_key = api_key or settings.ELEXON_API_KEY
        self.headers = {"Accept": "application/json", "x-api-key": self.api_key}

    async def fetch_physical_data(
        self,
        start: datetime,
        end: datetime,
        settlement_period_from: Optional[int] = None,
        settlement_period_to: Optional[int] = None,
        bm_units: Optional[List[str]] = None,
    ) -> Tuple[pd.DataFrame, Dict[str, any]]:
        """
        Fetch physical generation data from Elexon B1610 API.

        Args:
            start: Start datetime
            end: End datetime
            settlement_period_from: Optional start settlement period (1-50)
            settlement_period_to: Optional end settlement period (1-50)
            bm_units: Optional list of BM Unit IDs to filter

        Returns:
            Tuple of (DataFrame with generation data, metadata dict)
        """
        try:
            metadata = {
                "start": start,
                "end": end,
                "success": True,
                "errors": [],
                "bm_units_requested": bm_units or [],
                "bm_units_found": set(),
            }

            # Format dates for API - Elexon expects ISO format with timezone
            from_date = start.strftime("%Y-%m-%dT%H:%MZ")
            to_date = end.strftime("%Y-%m-%dT%H:%MZ")

            # Build URL with query parameters
            url = f"{self.BASE_URL}/datasets/B1610/stream"
            params = {
                "from": from_date,
                "to": to_date,
            }

            # Add optional parameters
            if settlement_period_from:
                params["settlementPeriodFrom"] = settlement_period_from
            if settlement_period_to:
                params["settlementPeriodTo"] = settlement_period_to

            logger.info(f"Fetching Elexon data from {from_date} to {to_date}")
            logger.info(f"API URL: {url}")
            logger.info(f"BM Units: {bm_units}")
            logger.info(f"Parameters: {params}")

            # Build the full URL with BM units as separate parameters
            async with httpx.AsyncClient(timeout=30.0) as client:
                if bm_units:
                    # Create a list of tuples for multiple bmUnit parameters
                    params_list = [("from", from_date), ("to", to_date)]
                    if settlement_period_from:
                        params_list.append(("settlementPeriodFrom", settlement_period_from))
                    if settlement_period_to:
                        params_list.append(("settlementPeriodTo", settlement_period_to))
                    for bm_unit in bm_units:
                        params_list.append(("bmUnit", bm_unit))

                    response = await client.get(
                        url,
                        headers=self.headers,
                        params=params_list,
                    )
                else:
                    response = await client.get(
                        url,
                        headers=self.headers,
                        params=params,
                    )

                if response.status_code != 200:
                    error_msg = f"Elexon API error: {response.status_code} - {response.text}"
                    logger.error(error_msg)
                    metadata["success"] = False
                    metadata["errors"].append({"error": error_msg})
                    return pd.DataFrame(), metadata

                data = response.json()

                if not data:
                    logger.warning("Elexon API returned no data")
                    metadata["errors"].append(
                        {"error": "No data available for the specified parameters"}
                    )
                    metadata["success"] = False
                    return pd.DataFrame(), metadata

                # Convert to DataFrame
                df = pd.DataFrame(data)

                logger.info(f"Received {len(df)} records from Elexon API")
                logger.info(f"Columns: {df.columns.tolist()}")

                # Process and standardize the data
                if not df.empty:
                    # Rename columns to match our schema
                    column_mapping = {
                        "settlementDate": "settlement_date",
                        "settlementPeriod": "settlement_period",
                        "bmUnit": "bm_unit",
                        "quantity": "value",
                        "datasetType": "dataset_type",
                    }

                    df = df.rename(columns=column_mapping)

                    # Convert settlement date and period to UTC timestamp
                    # Handles UK DST correctly:
                    # - Normal days: 48 settlement periods
                    # - Spring forward (March): 46 periods (clock skips 01:00-02:00)
                    # - Fall back (October): 50 periods (clock repeats 01:00-02:00)
                    if "settlement_date" in df.columns and "settlement_period" in df.columns:
                        # Parse dates and localize to UK timezone (Europe/London)
                        # This correctly handles BST (UTC+1) vs GMT (UTC+0)
                        uk_dates = pd.to_datetime(df["settlement_date"]).dt.tz_localize(
                            "Europe/London", ambiguous="infer", nonexistent="shift_forward"
                        )
                        # Convert UK midnight to UTC (this gives the actual UTC time
                        # when the settlement day starts in UK)
                        utc_dates = uk_dates.dt.tz_convert("UTC")
                        # Add settlement period offset (period 1 = 00:00, each period is 30 min)
                        # Adding in UTC avoids DST complications
                        df["timestamp"] = utc_dates + pd.to_timedelta(
                            (df["settlement_period"] - 1) * 30, unit="minutes"
                        )
                        # Remove timezone info and format as ISO string
                        df["timestamp"] = df["timestamp"].dt.tz_localize(None)
                        df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S")

                    # Convert value to float and handle nulls
                    if "value" in df.columns:
                        df["value"] = pd.to_numeric(df["value"], errors="coerce").fillna(0)

                    # Add unit information (Elexon provides data in MW)
                    df["unit"] = "MW"

                    # Track which BM units were found
                    if "bm_unit" in df.columns:
                        metadata["bm_units_found"] = set(df["bm_unit"].unique())

                    # Select relevant columns
                    columns_to_keep = ["timestamp", "bm_unit", "value", "unit", "settlement_period"]
                    df = df[[col for col in columns_to_keep if col in df.columns]]

                    metadata["records"] = len(df)
                else:
                    metadata["records"] = 0

                return df, metadata

        except Exception as e:
            logger.error(f"Elexon API error: {str(e)}")
            metadata = {
                "start": start,
                "end": end,
                "success": False,
                "errors": [{"general": str(e)}],
                "records": 0,
            }
            return pd.DataFrame(), metadata

    async def get_bm_units_for_windfarm(self, generation_unit_codes: List[str]) -> List[str]:
        """
        Get BM Unit IDs that match the generation unit codes.

        In Elexon, the bmUnit field often corresponds to our generation unit codes.
        This method helps map between our system and Elexon's BM Units.

        Args:
            generation_unit_codes: List of generation unit codes from our system

        Returns:
            List of BM Unit IDs to query in Elexon
        """
        # For now, we'll assume direct mapping (code = bmUnit)
        # In a real system, you might need a mapping table or transformation logic
        return generation_unit_codes
