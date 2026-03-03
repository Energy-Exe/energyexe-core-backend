"""ECB Exchange Rate API client.

Fetches daily exchange rates from the ECB Statistical Data Warehouse.
ECB publishes rates at 2:15 PM CET each business day (no weekends/holidays).
"""

import io
from datetime import date
from typing import Dict, List, Optional, Tuple

import httpx
import pandas as pd
import structlog

logger = structlog.get_logger()


class ECBExchangeRateClient:
    BASE_URL = "https://data-api.ecb.europa.eu/service/data/EXR"
    SUPPORTED_CURRENCIES = ["NOK", "GBP", "DKK", "USD"]

    def __init__(self, timeout: float = 60.0):
        self.timeout = timeout

    async def fetch_daily_rates(
        self, currency: str, start_date: date, end_date: date
    ) -> Tuple[Optional[pd.DataFrame], Dict]:
        """
        Fetch daily exchange rates for a single currency from ECB.

        Args:
            currency: ISO 4217 code (e.g., "NOK")
            start_date: Start date (inclusive)
            end_date: End date (inclusive)

        Returns:
            Tuple of (DataFrame with columns [rate_date, rate], metadata dict)
        """
        url = (
            f"{self.BASE_URL}/D.{currency}.EUR.SP00.A"
            f"?format=csvdata"
            f"&startPeriod={start_date.isoformat()}"
            f"&endPeriod={end_date.isoformat()}"
        )

        metadata = {"currency": currency, "url": url}

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(url)
                response.raise_for_status()

            csv_text = response.text
            if not csv_text.strip():
                logger.warning("Empty response from ECB", currency=currency)
                return None, {**metadata, "error": "empty_response"}

            df = pd.read_csv(io.StringIO(csv_text))

            if "TIME_PERIOD" not in df.columns or "OBS_VALUE" not in df.columns:
                logger.warning(
                    "Unexpected CSV columns from ECB",
                    currency=currency,
                    columns=list(df.columns),
                )
                return None, {**metadata, "error": "unexpected_columns"}

            result = df[["TIME_PERIOD", "OBS_VALUE"]].copy()
            result.columns = ["rate_date", "rate"]
            result["rate_date"] = pd.to_datetime(result["rate_date"]).dt.date
            result["rate"] = pd.to_numeric(result["rate"], errors="coerce")
            result = result.dropna(subset=["rate"])

            logger.info(
                "Fetched ECB rates",
                currency=currency,
                records=len(result),
                start=str(result["rate_date"].min()) if len(result) > 0 else None,
                end=str(result["rate_date"].max()) if len(result) > 0 else None,
            )

            return result, {**metadata, "records": len(result)}

        except httpx.HTTPStatusError as e:
            logger.error(
                "ECB API HTTP error",
                currency=currency,
                status=e.response.status_code,
            )
            return None, {**metadata, "error": f"http_{e.response.status_code}"}
        except Exception as e:
            logger.error("ECB API error", currency=currency, error=str(e))
            return None, {**metadata, "error": str(e)}

    async def fetch_all_rates(
        self, start_date: date, end_date: date
    ) -> Tuple[Optional[pd.DataFrame], List[Dict]]:
        """
        Fetch daily exchange rates for all supported currencies.

        Returns:
            Tuple of (combined DataFrame with columns [currency, rate_date, rate], list of metadata dicts)
        """
        all_frames = []
        all_metadata = []

        for currency in self.SUPPORTED_CURRENCIES:
            df, meta = await self.fetch_daily_rates(currency, start_date, end_date)
            all_metadata.append(meta)
            if df is not None and len(df) > 0:
                df["currency"] = currency
                all_frames.append(df)

        if not all_frames:
            return None, all_metadata

        combined = pd.concat(all_frames, ignore_index=True)
        logger.info("Fetched all ECB rates", total_records=len(combined))
        return combined, all_metadata
