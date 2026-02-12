"""Elexon API client service."""

from datetime import date, datetime
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
                    # NOTE: settlement_date is crucial for correct UTC hour calculation in aggregation
                    columns_to_keep = ["timestamp", "bm_unit", "value", "unit", "settlement_period", "settlement_date"]
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

    async def fetch_market_index_prices(
        self,
        start: datetime,
        end: datetime,
    ) -> Tuple[pd.DataFrame, Dict]:
        """
        Fetch Market Index Data (MID) prices from Elexon BMRS API.

        MID provides half-hourly electricity prices for GB.
        Only APXMIDP data provider has real prices (N2EXMIDP is zeros).

        Half-hourly data is aggregated to hourly (average price, sum volume).

        Args:
            start: Start datetime
            end: End datetime

        Returns:
            Tuple of (DataFrame with hourly price data, metadata dict)

        DataFrame columns:
            - timestamp: UTC hourly datetime
            - price: Average price in GBP/MWh
            - volume: Total volume in MWh
            - price_type: "day_ahead"
        """
        metadata = {
            "start": start,
            "end": end,
            "success": True,
            "errors": [],
            "records": 0,
            "api_calls": 0,
        }

        try:
            from_date = start.strftime("%Y-%m-%dT%H:%MZ")
            to_date = end.strftime("%Y-%m-%dT%H:%MZ")

            url = f"{self.BASE_URL}/datasets/MID/stream"
            params = {
                "from": from_date,
                "to": to_date,
            }

            logger.info(
                "Fetching Elexon MID price data",
                from_date=from_date,
                to_date=to_date,
            )

            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.get(
                    url,
                    headers=self.headers,
                    params=params,
                )
                metadata["api_calls"] = 1

                if response.status_code != 200:
                    error_msg = f"Elexon MID API error: {response.status_code} - {response.text}"
                    logger.error(error_msg)
                    metadata["success"] = False
                    metadata["errors"].append(error_msg)
                    return pd.DataFrame(), metadata

                data = response.json()

                if not data:
                    logger.warning("Elexon MID API returned no data")
                    metadata["success"] = False
                    metadata["errors"].append("No MID data available")
                    return pd.DataFrame(), metadata

                df = pd.DataFrame(data)
                logger.info(f"Received {len(df)} MID records from Elexon API")

                if df.empty:
                    metadata["records"] = 0
                    return pd.DataFrame(), metadata

                # Filter to APXMIDP only (N2EXMIDP is always zeros)
                if "dataProvider" in df.columns:
                    df = df[df["dataProvider"] == "APXMIDP"]
                    if df.empty:
                        logger.warning("No APXMIDP records in MID data")
                        metadata["records"] = 0
                        return pd.DataFrame(), metadata

                # Convert settlement date + period to UTC timestamp
                # Reuses same DST-aware logic as fetch_physical_data
                if "settlementDate" in df.columns and "settlementPeriod" in df.columns:
                    uk_dates = pd.to_datetime(df["settlementDate"]).dt.tz_localize(
                        "Europe/London", ambiguous="infer", nonexistent="shift_forward"
                    )
                    utc_dates = uk_dates.dt.tz_convert("UTC")
                    df["timestamp"] = utc_dates + pd.to_timedelta(
                        (df["settlementPeriod"] - 1) * 30, unit="minutes"
                    )
                    df["timestamp"] = df["timestamp"].dt.tz_localize(None)

                # Extract price and volume
                if "price" in df.columns:
                    df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0)
                if "volume" in df.columns:
                    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0)

                # Aggregate half-hourly to hourly: average price, sum volume
                df["hour"] = df["timestamp"].dt.floor("h")
                hourly = df.groupby("hour").agg(
                    price=("price", "mean"),
                    volume=("volume", "sum"),
                ).reset_index()

                hourly = hourly.rename(columns={"hour": "timestamp"})
                hourly["price_type"] = "day_ahead"
                hourly["currency"] = "GBP"
                hourly["unit"] = "GBP/MWh"

                metadata["records"] = len(hourly)
                logger.info(
                    f"Aggregated to {len(hourly)} hourly MID price records"
                )

                return hourly, metadata

        except Exception as e:
            logger.error(f"Elexon MID API error: {str(e)}")
            metadata["success"] = False
            metadata["errors"].append(str(e))
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

    async def fetch_acceptance_volumes(
        self,
        settlement_date: date,
        bid_offer: str,
        bm_units: Optional[List[str]] = None,
    ) -> Tuple[pd.DataFrame, Dict]:
        """
        Fetch bid-offer acceptance volumes from BOAV API.

        BOAV data captures:
        - Bids Accepted: Generator paid to REDUCE output (curtailment)
        - Offers Accepted: Generator paid to INCREASE output

        For calculating actual production:
        Actual Production = Metered Generation (B1610) + abs(Curtailed Volume from Bids)

        Args:
            settlement_date: Date to fetch (YYYY-MM-DD)
            bid_offer: 'bid' (curtailment) or 'offer' (increase)
            bm_units: Optional list of BM units to filter

        Returns:
            Tuple of (DataFrame with acceptance data, metadata dict)

        DataFrame columns:
            - timestamp: UTC timestamp (DST-aware converted)
            - settlement_date: Original UK date
            - settlement_period: 1-50
            - bm_unit: BM Unit identifier
            - acceptance_id: Unique acceptance ID
            - total_volume_accepted: Volume in MWh (negative for bids)
            - acceptance_duration: 'S' (short) or 'L' (long)
            - pair_volumes: JSONB with detailed volume breakdown
        """
        try:
            metadata = {
                "settlement_date": settlement_date,
                "bid_offer": bid_offer,
                "success": True,
                "errors": [],
                "bm_units_requested": bm_units or [],
                "bm_units_found": set(),
            }

            # Format date for API
            date_str = (
                settlement_date.strftime("%Y-%m-%d")
                if isinstance(settlement_date, date)
                else settlement_date
            )

            # Build URL
            url = f"{self.BASE_URL}/balancing/settlement/acceptance/volumes/all/{bid_offer}/{date_str}"

            logger.info(
                f"Fetching BOAV {bid_offer} data for {date_str}",
                url=url,
                bm_units=bm_units,
            )

            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.get(url, headers=self.headers)

                if response.status_code != 200:
                    error_msg = (
                        f"Elexon BOAV API error: {response.status_code} - {response.text}"
                    )
                    logger.error(error_msg)
                    metadata["success"] = False
                    metadata["errors"].append({"error": error_msg})
                    return pd.DataFrame(), metadata

                data = response.json()

                # Handle response structure - data may be in 'data' key
                if isinstance(data, dict) and "data" in data:
                    records = data["data"]
                elif isinstance(data, list):
                    records = data
                else:
                    logger.warning("Unexpected BOAV API response structure")
                    metadata["errors"].append({"error": "Unexpected response structure"})
                    metadata["success"] = False
                    return pd.DataFrame(), metadata

                if not records:
                    logger.warning(f"No BOAV {bid_offer} data for {date_str}")
                    metadata["records"] = 0
                    return pd.DataFrame(), metadata

                # Convert to DataFrame
                df = pd.DataFrame(records)

                logger.info(f"Received {len(df)} BOAV records from Elexon API")

                # Filter by BM units if specified
                if bm_units and "bmUnit" in df.columns:
                    original_count = len(df)
                    df = df[df["bmUnit"].isin(bm_units)]
                    if df.empty:
                        logger.info(
                            f"No BOAV {bid_offer} data for specified BM units on {date_str} "
                            f"(API returned {original_count} records for other units)"
                        )
                        metadata["records"] = 0
                        metadata["total_api_records"] = original_count
                        return pd.DataFrame(), metadata
                    logger.info(
                        f"Filtered to {len(df)} records matching our BM units "
                        f"(from {original_count} total)"
                    )

                # Process and standardize the data
                if not df.empty:
                    # Rename columns to match our schema
                    column_mapping = {
                        "settlementDate": "settlement_date",
                        "settlementPeriod": "settlement_period",
                        "bmUnit": "bm_unit",
                        "acceptanceId": "acceptance_id",
                        "totalVolumeAccepted": "total_volume_accepted",
                        "acceptanceDuration": "acceptance_duration",
                        "pairVolumes": "pair_volumes",
                    }

                    df = df.rename(columns=column_mapping)

                    # Convert settlement date and period to UTC timestamp
                    # Uses same DST-aware logic as fetch_physical_data (B1610)
                    if (
                        "settlement_date" in df.columns
                        and "settlement_period" in df.columns
                    ):
                        # Parse dates and localize to UK timezone (Europe/London)
                        # This correctly handles BST (UTC+1) vs GMT (UTC+0)
                        uk_dates = pd.to_datetime(df["settlement_date"]).dt.tz_localize(
                            "Europe/London",
                            ambiguous="infer",
                            nonexistent="shift_forward",
                        )
                        # Convert UK midnight to UTC
                        utc_dates = uk_dates.dt.tz_convert("UTC")
                        # Add settlement period offset (period 1 = 00:00, each period is 30 min)
                        df["timestamp"] = utc_dates + pd.to_timedelta(
                            (df["settlement_period"] - 1) * 30, unit="minutes"
                        )
                        # Remove timezone info and format as ISO string
                        df["timestamp"] = df["timestamp"].dt.tz_localize(None)
                        df["timestamp"] = df["timestamp"].dt.strftime(
                            "%Y-%m-%dT%H:%M:%S"
                        )

                    # Ensure numeric fields are properly typed
                    if "total_volume_accepted" in df.columns:
                        df["total_volume_accepted"] = pd.to_numeric(
                            df["total_volume_accepted"], errors="coerce"
                        ).fillna(0)

                    if "settlement_period" in df.columns:
                        df["settlement_period"] = pd.to_numeric(
                            df["settlement_period"], errors="coerce"
                        ).fillna(0).astype(int)

                    # Track which BM units were found
                    if "bm_unit" in df.columns:
                        metadata["bm_units_found"] = set(df["bm_unit"].unique())

                    metadata["records"] = len(df)
                else:
                    metadata["records"] = 0

                return df, metadata

        except Exception as e:
            logger.error(f"Elexon BOAV API error: {str(e)}")
            metadata = {
                "settlement_date": settlement_date,
                "bid_offer": bid_offer,
                "success": False,
                "errors": [{"general": str(e)}],
                "records": 0,
            }
            return pd.DataFrame(), metadata
