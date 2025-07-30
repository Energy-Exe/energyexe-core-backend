"""ENTSOE API client service."""

from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import httpx
import pandas as pd
import structlog
from entsoe import EntsoePandasClient

from app.core.config import get_settings

logger = structlog.get_logger()


class ENTSOEClient:
    """Client for interacting with ENTSOE Transparency Platform API."""

    PRODUCTION_TYPE_MAPPINGS = {
        "wind": ["Wind Offshore", "Wind Onshore"],  # Wind generation types
        "solar": ["Solar"],  # Solar generation
    }

    # European bidding zones
    AREA_CODES = {
        "DE_LU": "Germany/Luxembourg",
        "FR": "France",
        "ES": "Spain",
        "GB": "United Kingdom",
        "IT": "Italy",
        "NL": "Netherlands",
        "BE": "Belgium",
        "AT": "Austria",
        "CH": "Switzerland",
        "PL": "Poland",
        "DK_1": "Denmark West",
        "DK_2": "Denmark East",
        "NO_1": "Norway NO1",
        "SE_1": "Sweden SE1",
        "SE_2": "Sweden SE2",
        "SE_3": "Sweden SE3",
        "SE_4": "Sweden SE4",
    }

    def __init__(self, api_key: str = None):
        settings = get_settings()
        self.api_key = api_key or settings.ENTSOE_API_KEY
        self.client = EntsoePandasClient(api_key=self.api_key)

    async def fetch_generation_data(
        self,
        start: datetime,
        end: datetime,
        area_code: str,
        production_types: List[str] = ["wind", "solar"],
    ) -> Tuple[pd.DataFrame, Dict[str, any]]:
        """
        Fetch actual generation data from ENTSOE API.

        Args:
            start: Start datetime (UTC)
            end: End datetime (UTC)
            area_code: Area/bidding zone code
            production_types: List of production types to fetch

        Returns:
            Tuple of (DataFrame with generation data, metadata dict)
        """
        try:
            all_data = []
            metadata = {
                "area_code": area_code,
                "start": start,
                "end": end,
                "production_types": production_types,
                "success": True,
                "errors": [],
            }

            # Query generation data for the area
            try:
                logger.info(f"Querying ENTSOE API for {area_code} from {start} to {end}")

                # Check if dates are in the future
                now_utc = datetime.now(timezone.utc)
                # Ensure start is timezone-aware
                if start.tzinfo is None:
                    start_aware = start.replace(tzinfo=timezone.utc)
                else:
                    start_aware = start

                if start_aware > now_utc:
                    raise ValueError(
                        f"Start date {start} is in the future. ENTSOE only provides historical data."
                    )

                logger.info(f"ENTSOE API Key: {self.api_key[:10]}... (first 10 chars)")
                try:
                    df = self.client.query_generation(
                        area_code,
                        start=pd.Timestamp(start, tz="UTC"),
                        end=pd.Timestamp(end, tz="UTC"),
                        psr_type=None,  # Get all types
                    )
                except Exception as api_error:
                    logger.error(f"ENTSOE API error: {str(api_error)}")
                    raise

                if df is None or df.empty:
                    logger.warning(
                        f"ENTSOE API returned no data for {area_code} from {start} to {end}"
                    )
                    metadata["errors"].append(
                        {
                            "area_code": area_code,
                            "error": "No data available for the specified date range. Try a more recent date range.",
                        }
                    )
                    metadata["success"] = False
                else:
                    logger.info(f"ENTSOE API returned dataframe with shape: {df.shape}")

                    # Process the data based on production types
                    for prod_type in production_types:
                        type_codes = self.PRODUCTION_TYPE_MAPPINGS.get(prod_type, [])

                        if isinstance(df.columns, pd.MultiIndex):
                            # Extract columns for this production type
                            filtered_data = []
                            for col in df.columns:
                                # Check if any of the type names match the column
                                # Column is a tuple like ('Wind Offshore', 'Actual Aggregated')
                                col_name = col[0] if isinstance(col, tuple) else str(col)
                                for type_name in type_codes:
                                    if type_name == col_name and (
                                        isinstance(col, tuple) and "Actual Aggregated" in col[1]
                                    ):
                                        # Create a new dataframe with single column
                                        df_single = pd.DataFrame(df[col])
                                        df_single.columns = ["value"]
                                        df_single["production_type"] = prod_type
                                        df_single["area_code"] = area_code
                                        filtered_data.append(df_single)

                            if filtered_data:
                                combined_df = pd.concat(filtered_data)
                                # Group by timestamp and production type, summing values
                                combined_df = (
                                    combined_df.groupby(
                                        [combined_df.index, "production_type", "area_code"]
                                    )["value"]
                                    .sum()
                                    .reset_index()
                                )
                                # Rename the index column to timestamp
                                combined_df.rename(columns={"level_0": "timestamp"}, inplace=True)
                                all_data.append(combined_df)
                        else:
                            # Handle single column case
                            df["production_type"] = prod_type
                            df["area_code"] = area_code
                            df.columns = ["value", "production_type", "area_code"]
                            all_data.append(df)

            except Exception as e:
                logger.error(f"Error fetching data for {area_code}: {str(e)}")
                metadata["errors"].append({"area_code": area_code, "error": str(e)})
                metadata["success"] = False

            if all_data:
                result_df = pd.concat(all_data, axis=0, ignore_index=True)
                # Ensure we don't have duplicate timestamp columns
                if "timestamp" not in result_df.columns and "index" in result_df.columns:
                    result_df.rename(columns={"index": "timestamp"}, inplace=True)
                metadata["records"] = len(result_df)
            else:
                result_df = pd.DataFrame()
                metadata["records"] = 0

            return result_df, metadata

        except Exception as e:
            logger.error(f"ENTSOE API error: {str(e)}")
            metadata = {
                "area_code": area_code,
                "start": start,
                "end": end,
                "production_types": production_types,
                "success": False,
                "errors": [{"general": str(e)}],
                "records": 0,
            }
            return pd.DataFrame(), metadata

    def get_available_areas(self) -> Dict[str, str]:
        """Return available area codes and their descriptions."""
        return self.AREA_CODES
