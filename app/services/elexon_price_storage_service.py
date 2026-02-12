"""Service for fetching and storing Elexon MID price data."""

from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Dict, List, Any, Optional, Tuple

import pandas as pd
import structlog
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.price_data import PriceDataRaw
from app.services.elexon_client import ElexonClient

logger = structlog.get_logger()

# GB EIC code (same as used by ENTSOE for GB bidzone)
GB_EIC_CODE = "10YGB----------A"


class ElexonPriceStorageService:
    """Service for fetching Elexon MID prices and storing in price_data_raw."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def fetch_and_store_prices(
        self,
        start_date: datetime,
        end_date: datetime,
        user_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Fetch MID price data from Elexon API and store in price_data_raw.

        Args:
            start_date: Start datetime (UTC)
            end_date: End datetime (UTC)
            user_id: User triggering the fetch

        Returns:
            Summary of the fetch operation
        """
        start_time = datetime.now()
        client = ElexonClient()

        results = {
            "success": True,
            "source": "ELEXON",
            "identifier": GB_EIC_CODE,
            "date_range": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat(),
            },
            "total_records_stored": 0,
            "total_records_updated": 0,
            "api_calls": 0,
            "errors": [],
        }

        try:
            df, metadata = await client.fetch_market_index_prices(
                start=start_date,
                end=end_date,
            )
            results["api_calls"] = metadata.get("api_calls", 0)

            if df.empty:
                results["errors"].append("No MID data returned from Elexon API")
                if metadata.get("errors"):
                    results["errors"].extend(
                        e if isinstance(e, str) else str(e)
                        for e in metadata["errors"]
                    )
                return results

            stored, updated = await self._store_price_records(
                df, user_id, metadata
            )
            results["total_records_stored"] = stored
            results["total_records_updated"] = updated

        except Exception as e:
            error_msg = f"Error fetching Elexon MID prices: {str(e)}"
            logger.error(error_msg)
            results["errors"].append(error_msg)
            results["success"] = False

        end_time = datetime.now()
        results["duration_seconds"] = round(
            (end_time - start_time).total_seconds(), 2
        )

        return results

    async def _store_price_records(
        self,
        df: pd.DataFrame,
        user_id: Optional[int],
        api_metadata: Dict,
    ) -> Tuple[int, int]:
        """Store Elexon MID price records in price_data_raw using bulk upsert."""
        if df.empty:
            return 0, 0

        records_to_insert = []
        now = datetime.now(timezone.utc)

        for _, row in df.iterrows():
            timestamp = row.get("timestamp")
            if not isinstance(timestamp, datetime):
                timestamp = pd.to_datetime(timestamp)
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)

            period_end = timestamp + timedelta(hours=1)
            price = float(row.get("price", 0))
            volume = float(row.get("volume", 0))

            data = {
                "price": price,
                "volume": volume,
                "currency": "GBP",
                "unit": "GBP/MWh",
                "data_provider": "APXMIDP",
                "fetch_metadata": {
                    "fetched_by_user_id": user_id,
                    "fetch_timestamp": now.isoformat(),
                    "fetch_method": "api",
                },
            }

            records_to_insert.append({
                "source": "ELEXON",
                "source_type": "api",
                "price_type": "day_ahead",
                "identifier": GB_EIC_CODE,
                "period_start": timestamp,
                "period_end": period_end,
                "period_type": "PT60M",
                "value_extracted": Decimal(str(price)),
                "unit": "GBP/MWh",
                "currency": "GBP",
                "data": data,
                "created_at": now,
                "updated_at": now,
            })

        if not records_to_insert:
            return 0, 0

        try:
            stmt = insert(PriceDataRaw).values(records_to_insert)

            stmt = stmt.on_conflict_do_update(
                constraint='uq_price_raw_source_identifier_period_type',
                set_={
                    'value_extracted': stmt.excluded.value_extracted,
                    'data': stmt.excluded.data,
                    'updated_at': datetime.now(timezone.utc),
                    'period_end': stmt.excluded.period_end,
                    'period_type': stmt.excluded.period_type,
                    'unit': stmt.excluded.unit,
                    'currency': stmt.excluded.currency,
                }
            )

            await self.db.execute(stmt)
            await self.db.commit()

            logger.info(
                f"Bulk upserted {len(records_to_insert)} Elexon MID price records"
            )
            return len(records_to_insert), 0

        except Exception as e:
            logger.error(f"Error storing Elexon price records: {str(e)}")
            await self.db.rollback()
            return 0, 0
