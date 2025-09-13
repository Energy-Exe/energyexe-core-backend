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
                    logger.info(f"DataFrame columns: {df.columns.tolist()}")
                    logger.info(f"Is MultiIndex: {isinstance(df.columns, pd.MultiIndex)}")

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
                            # First, ensure we're working with a copy
                            df_copy = df.copy()

                            # If df has multiple columns, we need to handle it differently
                            if len(df_copy.columns) > 1:
                                # Sum all columns to get total generation
                                df_copy["value"] = df_copy.sum(axis=1)
                                # Keep only the value column
                                df_copy = df_copy[["value"]]
                            else:
                                # Rename the single column to 'value'
                                df_copy.columns = ["value"]

                            df_copy["production_type"] = prod_type
                            df_copy["area_code"] = area_code
                            all_data.append(df_copy)

            except Exception as e:
                logger.error(f"Error fetching data for {area_code}: {str(e)}")
                metadata["errors"].append({"area_code": area_code, "error": str(e)})
                metadata["success"] = False

            if all_data:
                result_df = pd.concat(all_data, axis=0, ignore_index=False)

                # Reset index to make timestamp a column
                result_df = result_df.reset_index()

                # Handle different possible timestamp column names
                if "index" in result_df.columns and "timestamp" not in result_df.columns:
                    result_df.rename(columns={"index": "timestamp"}, inplace=True)
                elif "level_0" in result_df.columns and "timestamp" not in result_df.columns:
                    result_df.rename(columns={"level_0": "timestamp"}, inplace=True)

                # Ensure timestamp is in ISO format
                if "timestamp" in result_df.columns:
                    result_df["timestamp"] = pd.to_datetime(result_df["timestamp"]).dt.strftime(
                        "%Y-%m-%dT%H:%M:%S"
                    )

                # Add unit information
                result_df["unit"] = "MW"

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

    async def fetch_generation_per_unit(
        self,
        start: datetime,
        end: datetime,
        area_code: str,
        eic_codes: Optional[List[str]] = None,
        production_types: Optional[List[str]] = None,
    ) -> Tuple[pd.DataFrame, Dict[str, any]]:
        """
        Fetch generation data per individual unit from ENTSOE API.
        
        Args:
            start: Start datetime (UTC)
            end: End datetime (UTC)  
            area_code: Area/bidding zone code
            eic_codes: Optional list of EIC codes to filter specific units
            production_types: Optional list of production types to filter
            
        Returns:
            Tuple of (DataFrame with per-unit generation data, metadata dict)
        """
        metadata = {
            "area_code": area_code,
            "start": start,
            "end": end,
            "eic_codes": eic_codes,
            "production_types": production_types,
            "success": True,
            "errors": [],
            "units_found": [],
        }
        
        print(metadata)
        
        try:
            logger.info(f"Querying ENTSOE per-unit data for {area_code} from {start} to {end}")
            if eic_codes:
                logger.info(f"Filtering for EIC codes: {eic_codes}")
            
            # Convert production types to PSR types if provided
            # Note: We try both onshore and offshore wind types
            psr_type = None
            if production_types and len(production_types) == 1:
                if production_types[0].lower() == "wind":
                    # We'll try both B18 (offshore) and B19 (onshore)
                    psr_type = None  # Get all types, then filter
                elif production_types[0].lower() == "solar":
                    psr_type = "B16"  # Solar
            
            # Query generation per plant with EIC codes included
            df = self.client.query_generation_per_plant(
                area_code,
                start=pd.Timestamp(start, tz="UTC"),
                end=pd.Timestamp(end, tz="UTC"),
                psr_type=psr_type,
                include_eic=True,  # Include EIC codes in the output
            )
            
            if df is None or df.empty:
                logger.warning(f"No per-unit data available for {area_code}")
                metadata["success"] = False
                metadata["errors"].append("No data available for the specified parameters")
                return pd.DataFrame(), metadata
            
            logger.info(f"Received data with shape: {df.shape}")
            if isinstance(df.columns, pd.MultiIndex):
                logger.info(f"Column structure sample: {df.columns[0] if len(df.columns) > 0 else 'No columns'}")
            
            # Process the dataframe
            all_data = []
            
            # The dataframe has multi-level columns
            # Structure can be: (unit_name, type, aggregation, eic_code) for B18/B19
            # or different for other types
            if isinstance(df.columns, pd.MultiIndex):
                for col in df.columns:
                    # Extract unit info from column
                    unit_name = col[0] if isinstance(col, tuple) else str(col)
                    
                    # EIC code position varies:
                    # For wind offshore/onshore: position 3 in (name, type, aggregation, eic_code)
                    # For other types: might be at position 1
                    eic_code = None
                    if len(col) > 3 and isinstance(col[3], str) and "W" in col[3]:
                        eic_code = col[3]
                        # Also check if it's a wind type we want
                        if production_types and "wind" in production_types:
                            # Accept both offshore and onshore wind
                            if "Wind" not in str(col[1]):
                                continue
                    elif len(col) > 1 and isinstance(col[1], str) and "W" in col[1]:
                        eic_code = col[1]
                    
                    # If we have specific EIC codes to filter, skip others
                    if eic_codes and eic_code and eic_code not in eic_codes:
                        continue
                    
                    # Skip if no EIC code found
                    if not eic_code:
                        continue
                    
                    # Create record for this unit
                    unit_df = pd.DataFrame(df[col])
                    unit_df.columns = ["value"]
                    unit_df["unit_name"] = unit_name
                    unit_df["eic_code"] = eic_code
                    unit_df["area_code"] = area_code
                    unit_df = unit_df.reset_index()
                    unit_df.rename(columns={"index": "timestamp"}, inplace=True)
                    
                    all_data.append(unit_df)
                    metadata["units_found"].append({
                        "name": unit_name,
                        "eic_code": eic_code
                    })
            else:
                # Single unit case or different structure
                df_reset = df.reset_index()
                if "index" in df_reset.columns:
                    df_reset.rename(columns={"index": "timestamp"}, inplace=True)
                df_reset["area_code"] = area_code
                all_data.append(df_reset)
            
            if all_data:
                result_df = pd.concat(all_data, ignore_index=True)
                
                # Ensure timestamp is in ISO format
                if "timestamp" in result_df.columns:
                    result_df["timestamp"] = pd.to_datetime(result_df["timestamp"]).dt.strftime(
                        "%Y-%m-%dT%H:%M:%S"
                    )
                
                # Add unit column
                result_df["unit"] = "MW"
                
                metadata["records"] = len(result_df)
                metadata["units_count"] = len(metadata["units_found"])
                
                logger.info(f"Processed {len(metadata['units_found'])} units with {metadata['records']} total records")
                for unit in metadata["units_found"]:
                    logger.info(f"  Found unit: {unit['name']} (EIC: {unit['eic_code']})")
                
                return result_df, metadata
            else:
                metadata["success"] = False
                metadata["errors"].append("No matching units found")
                return pd.DataFrame(), metadata
                
        except Exception as e:
            logger.error(f"Error fetching per-unit data: {str(e)}")
            print(e)
            metadata["success"] = False
            metadata["errors"].append(str(e))
            return pd.DataFrame(), metadata

    def get_available_areas(self) -> Dict[str, str]:
        """Return available area codes and their descriptions."""
        return self.AREA_CODES
