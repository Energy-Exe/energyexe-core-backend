"""ENTSOE API client for fetching power price data."""

from datetime import datetime, timezone
from typing import Dict, Tuple, Optional

import pandas as pd
import structlog
from entsoe import EntsoePandasClient

from app.core.config import get_settings

logger = structlog.get_logger()


class ENTSOEPriceClient:
    """Client for fetching power price data from ENTSOE Transparency Platform API."""

    # European bidding zones
    AREA_CODES = {
        "DE_LU": "Germany/Luxembourg",
        "FR": "France",
        "ES": "Spain",
        "GB": "United Kingdom",
        "IT_NORD": "Italy North",
        "IT_CNOR": "Italy Central North",
        "IT_CSUD": "Italy Central South",
        "IT_SUD": "Italy South",
        "IT_SICI": "Italy Sicily",
        "IT_SARD": "Italy Sardinia",
        "NL": "Netherlands",
        "BE": "Belgium",
        "AT": "Austria",
        "CH": "Switzerland",
        "PL": "Poland",
        "DK_1": "Denmark West",
        "DK_2": "Denmark East",
        "NO_1": "Norway NO1",
        "NO_2": "Norway NO2",
        "NO_3": "Norway NO3",
        "NO_4": "Norway NO4",
        "NO_5": "Norway NO5",
        "SE_1": "Sweden SE1",
        "SE_2": "Sweden SE2",
        "SE_3": "Sweden SE3",
        "SE_4": "Sweden SE4",
        "FI": "Finland",
        "EE": "Estonia",
        "LV": "Latvia",
        "LT": "Lithuania",
        "PT": "Portugal",
        "GR": "Greece",
        "HU": "Hungary",
        "CZ": "Czech Republic",
        "SK": "Slovakia",
        "SI": "Slovenia",
        "HR": "Croatia",
        "RO": "Romania",
        "BG": "Bulgaria",
        "RS": "Serbia",
        "BA": "Bosnia Herzegovina",
        "ME": "Montenegro",
        "MK": "North Macedonia",
        "AL": "Albania",
    }

    def __init__(self, api_key: str = None):
        settings = get_settings()
        self.api_key = api_key or settings.ENTSOE_API_KEY
        self.client = EntsoePandasClient(api_key=self.api_key)

    async def fetch_day_ahead_prices(
        self,
        start: datetime,
        end: datetime,
        area_code: str,
    ) -> Tuple[pd.DataFrame, Dict]:
        """
        Fetch day-ahead prices from ENTSOE API.

        Args:
            start: Start datetime (UTC)
            end: End datetime (UTC)
            area_code: Bidding zone code (e.g., DE_LU, FR, DK_1)

        Returns:
            Tuple of (DataFrame with price data, metadata dict)
        """
        metadata = {
            "area_code": area_code,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "price_type": "day_ahead",
            "success": True,
            "errors": [],
            "records": 0,
        }

        try:
            logger.info(f"Fetching ENTSOE day-ahead prices for {area_code} from {start} to {end}")

            # Ensure start and end are timezone-naive before creating pd.Timestamp
            start_dt = start.replace(tzinfo=None) if start.tzinfo else start
            end_dt = end.replace(tzinfo=None) if end.tzinfo else end

            # DE_LU and AT return only 15-minute resolution data (EXAA block auctions).
            # entsoe-py skips the 15min→60min fallback for these zones, so we must
            # request 15min explicitly and resample to hourly ourselves.
            resolution = '15min' if area_code in ('DE_LU', 'AT') else '60min'

            # Query day-ahead prices
            prices = self.client.query_day_ahead_prices(
                area_code,
                start=pd.Timestamp(start_dt, tz="UTC"),
                end=pd.Timestamp(end_dt, tz="UTC"),
                resolution=resolution,
            )

            # Resample 15-minute data to hourly (average price per hour)
            if resolution == '15min' and prices is not None and not prices.empty:
                prices = prices.resample('h').mean()

            if prices is None or (isinstance(prices, pd.Series) and prices.empty):
                logger.warning(f"No day-ahead price data returned for {area_code}")
                metadata["success"] = False
                metadata["errors"].append("No data available for the specified date range")
                return pd.DataFrame(), metadata

            # Convert Series to DataFrame
            if isinstance(prices, pd.Series):
                df = prices.to_frame(name="price")
            else:
                df = prices

            df = df.reset_index()
            df.columns = ["timestamp", "price"]

            # Ensure timestamp is timezone-aware UTC
            df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.tz_localize(None).dt.tz_localize("UTC")

            # Add metadata columns
            df["area_code"] = area_code
            df["price_type"] = "day_ahead"
            df["currency"] = "EUR"
            df["unit"] = "EUR/MWh"

            metadata["records"] = len(df)
            logger.info(f"Fetched {len(df)} day-ahead price records for {area_code}")

            return df, metadata

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error fetching day-ahead prices for {area_code}: {error_msg}")
            metadata["success"] = False
            metadata["errors"].append(error_msg)
            return pd.DataFrame(), metadata

    async def fetch_intraday_prices(
        self,
        start: datetime,
        end: datetime,
        area_code: str,
    ) -> Tuple[pd.DataFrame, Dict]:
        """
        Fetch intraday prices from ENTSOE API.

        Note: Intraday prices may not be available for all bidding zones.

        Args:
            start: Start datetime (UTC)
            end: End datetime (UTC)
            area_code: Bidding zone code

        Returns:
            Tuple of (DataFrame with price data, metadata dict)
        """
        metadata = {
            "area_code": area_code,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "price_type": "intraday",
            "success": True,
            "errors": [],
            "records": 0,
        }

        try:
            logger.info(f"Fetching ENTSOE intraday prices for {area_code} from {start} to {end}")

            # Ensure start and end are timezone-naive before creating pd.Timestamp
            start_dt = start.replace(tzinfo=None) if start.tzinfo else start
            end_dt = end.replace(tzinfo=None) if end.tzinfo else end

            # Query imbalance prices (closest to intraday settlement prices)
            try:
                prices = self.client.query_imbalance_prices(
                    area_code,
                    start=pd.Timestamp(start_dt, tz="UTC"),
                    end=pd.Timestamp(end_dt, tz="UTC"),
                )
            except Exception as imbalance_error:
                # Imbalance prices may not be available for all zones
                logger.warning(f"Imbalance prices not available for {area_code}: {imbalance_error}")
                metadata["success"] = False
                metadata["errors"].append(f"Intraday/imbalance prices not available: {str(imbalance_error)}")
                return pd.DataFrame(), metadata

            if prices is None or (isinstance(prices, pd.DataFrame) and prices.empty):
                logger.warning(f"No intraday price data returned for {area_code}")
                metadata["success"] = False
                metadata["errors"].append("No intraday data available for the specified date range")
                return pd.DataFrame(), metadata

            # Handle different return types
            if isinstance(prices, pd.DataFrame):
                # Take the first price column if multiple exist
                price_col = prices.columns[0] if len(prices.columns) > 0 else None
                if price_col:
                    df = prices[[price_col]].copy()
                    df.columns = ["price"]
                else:
                    return pd.DataFrame(), metadata
            elif isinstance(prices, pd.Series):
                df = prices.to_frame(name="price")
            else:
                return pd.DataFrame(), metadata

            df = df.reset_index()
            df.columns = ["timestamp", "price"]

            # Ensure timestamp is timezone-aware UTC
            df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.tz_localize(None).dt.tz_localize("UTC")

            # Add metadata columns
            df["area_code"] = area_code
            df["price_type"] = "intraday"
            df["currency"] = "EUR"
            df["unit"] = "EUR/MWh"

            metadata["records"] = len(df)
            logger.info(f"Fetched {len(df)} intraday price records for {area_code}")

            return df, metadata

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error fetching intraday prices for {area_code}: {error_msg}")
            metadata["success"] = False
            metadata["errors"].append(error_msg)
            return pd.DataFrame(), metadata

    async def fetch_prices(
        self,
        start: datetime,
        end: datetime,
        area_code: str,
        price_types: Optional[list] = None,
    ) -> Tuple[pd.DataFrame, Dict]:
        """
        Fetch both day-ahead and intraday prices from ENTSOE API.

        Args:
            start: Start datetime (UTC)
            end: End datetime (UTC)
            area_code: Bidding zone code
            price_types: List of price types to fetch (default: ["day_ahead", "intraday"])

        Returns:
            Tuple of (combined DataFrame with all price data, metadata dict)
        """
        if price_types is None:
            price_types = ["day_ahead", "intraday"]

        all_data = []
        all_metadata = {
            "area_code": area_code,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "price_types": price_types,
            "success": True,
            "errors": [],
            "records": 0,
            "by_type": {},
        }

        if "day_ahead" in price_types:
            da_df, da_metadata = await self.fetch_day_ahead_prices(start, end, area_code)
            all_metadata["by_type"]["day_ahead"] = da_metadata
            if not da_df.empty:
                all_data.append(da_df)
            if not da_metadata["success"]:
                all_metadata["errors"].extend(da_metadata["errors"])

        if "intraday" in price_types:
            id_df, id_metadata = await self.fetch_intraday_prices(start, end, area_code)
            all_metadata["by_type"]["intraday"] = id_metadata
            if not id_df.empty:
                all_data.append(id_df)
            # Don't mark as failure if only intraday fails (it's optional)
            if not id_metadata["success"] and "day_ahead" not in price_types:
                all_metadata["errors"].extend(id_metadata["errors"])

        if all_data:
            result_df = pd.concat(all_data, ignore_index=True)
            all_metadata["records"] = len(result_df)
        else:
            result_df = pd.DataFrame()
            all_metadata["success"] = False

        return result_df, all_metadata

    def get_available_areas(self) -> Dict[str, str]:
        """Return available area codes and their descriptions."""
        return self.AREA_CODES
