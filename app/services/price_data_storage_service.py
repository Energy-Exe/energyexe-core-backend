"""Service for storing and fetching price data from ENTSOE API."""

from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Dict, List, Any, Optional, Tuple

import pandas as pd
import structlog
from sqlalchemy import select, and_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.price_data import PriceDataRaw
from app.models.bidzone import Bidzone
from app.services.entsoe_price_client import ENTSOEPriceClient

logger = structlog.get_logger()


class PriceDataStorageService:
    """Service for fetching price data from ENTSOE API and storing in price_data_raw."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def fetch_and_store_prices(
        self,
        bidzone_codes: List[str],
        start_date: datetime,
        end_date: datetime,
        price_types: Optional[List[str]] = None,
        user_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Fetch price data from ENTSOE API for given bidzones and store in price_data_raw.

        Args:
            bidzone_codes: List of bidzone codes (e.g., ["DE_LU", "FR"])
            start_date: Start datetime (UTC)
            end_date: End datetime (UTC)
            price_types: List of price types to fetch (default: ["day_ahead", "intraday"])
            user_id: User triggering the fetch

        Returns:
            Summary of the fetch operation
        """
        if price_types is None:
            price_types = ["day_ahead", "intraday"]

        start_time = datetime.now()
        client = ENTSOEPriceClient()

        results = {
            "success": True,
            "bidzone_codes": bidzone_codes,
            "date_range": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat(),
            },
            "price_types": price_types,
            "total_records_stored": 0,
            "total_records_updated": 0,
            "by_bidzone": {},
            "errors": [],
        }

        for bidzone_code in bidzone_codes:
            bidzone_result = {
                "records_stored": 0,
                "records_updated": 0,
                "by_price_type": {},
                "errors": [],
            }

            try:
                # Fetch prices from ENTSOE API
                df, metadata = await client.fetch_prices(
                    start=start_date,
                    end=end_date,
                    area_code=bidzone_code,
                    price_types=price_types,
                )

                if df.empty:
                    bidzone_result["errors"].append(f"No data returned for {bidzone_code}")
                    if metadata.get("errors"):
                        bidzone_result["errors"].extend(metadata["errors"])
                    results["by_bidzone"][bidzone_code] = bidzone_result
                    continue

                # Store the data by price type
                for price_type in price_types:
                    type_df = df[df["price_type"] == price_type]
                    if type_df.empty:
                        continue

                    stored, updated = await self._store_price_records(
                        type_df,
                        bidzone_code,
                        price_type,
                        user_id,
                        metadata,
                    )

                    bidzone_result["by_price_type"][price_type] = {
                        "records_stored": stored,
                        "records_updated": updated,
                    }
                    bidzone_result["records_stored"] += stored
                    bidzone_result["records_updated"] += updated

                results["total_records_stored"] += bidzone_result["records_stored"]
                results["total_records_updated"] += bidzone_result["records_updated"]

            except Exception as e:
                error_msg = f"Error fetching prices for {bidzone_code}: {str(e)}"
                logger.error(error_msg)
                bidzone_result["errors"].append(error_msg)
                results["success"] = False

            results["by_bidzone"][bidzone_code] = bidzone_result

        # Calculate response time
        end_time = datetime.now()
        results["duration_seconds"] = round((end_time - start_time).total_seconds(), 2)

        return results

    async def _store_price_records(
        self,
        df: pd.DataFrame,
        bidzone_code: str,
        price_type: str,
        user_id: Optional[int],
        api_metadata: Dict,
    ) -> Tuple[int, int]:
        """Store price records in price_data_raw using bulk upsert."""
        if df.empty:
            return 0, 0

        # Prepare all records for bulk insert
        records_to_insert = []
        now = datetime.now(timezone.utc)

        for idx, row in df.iterrows():
            # Extract timestamp
            timestamp = row.get("timestamp")
            if not isinstance(timestamp, datetime):
                timestamp = pd.to_datetime(timestamp)

            # Ensure timezone-aware
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)

            # Price data is typically hourly
            period_end = timestamp + timedelta(hours=1)
            period_type = "PT60M"

            # Extract price value
            price = float(row.get("price", 0))
            currency = row.get("currency", "EUR")
            unit = row.get("unit", "EUR/MWh")

            # Build data JSONB
            data = {
                "area_code": bidzone_code,
                "price": price,
                "currency": currency,
                "unit": unit,
                "fetch_metadata": {
                    "fetched_by_user_id": user_id,
                    "fetch_timestamp": now.isoformat(),
                    "fetch_method": "api",
                    "api_metadata": self._make_json_serializable(api_metadata),
                },
            }

            records_to_insert.append({
                "source": "ENTSOE",
                "source_type": "api",
                "price_type": price_type,
                "identifier": bidzone_code,
                "period_start": timestamp,
                "period_end": period_end,
                "period_type": period_type,
                "value_extracted": Decimal(str(price)),
                "unit": unit,
                "currency": currency,
                "data": data,
                "created_at": now,
                "updated_at": now,
            })

        if not records_to_insert:
            return 0, 0

        # Use PostgreSQL bulk upsert
        try:
            stmt = insert(PriceDataRaw).values(records_to_insert)

            # Use upsert to handle existing records
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

            logger.info(f"Bulk upserted {len(records_to_insert)} {price_type} price records for {bidzone_code}")

            return len(records_to_insert), 0

        except Exception as e:
            logger.error(f"Error storing price records for {bidzone_code}: {str(e)}")
            await self.db.rollback()
            return 0, 0

    async def store_csv_price_records(
        self,
        records: List[Dict[str, Any]],
        source_file: str,
    ) -> Tuple[int, int]:
        """
        Store price records from CSV import in price_data_raw.

        Args:
            records: List of price record dicts with keys:
                - period_start: datetime
                - identifier: bidzone code
                - price_type: "day_ahead" or "intraday"
                - value: price value
                - currency: currency code
                - data: raw CSV row data as dict
            source_file: Name of the source CSV file

        Returns:
            Tuple of (records_stored, records_updated)
        """
        if not records:
            return 0, 0

        now = datetime.now(timezone.utc)
        records_to_insert = []

        for record in records:
            period_start = record["period_start"]
            if period_start.tzinfo is None:
                period_start = period_start.replace(tzinfo=timezone.utc)

            period_end = period_start + timedelta(hours=1)

            # Determine price type from contract type
            contract_type = record.get("data", {}).get("ContractType", "")
            if "Intraday" in contract_type:
                price_type = "intraday"
            else:
                price_type = "day_ahead"

            records_to_insert.append({
                "source": "ENTSOE",
                "source_type": "csv",
                "price_type": price_type,
                "identifier": record["identifier"],
                "period_start": period_start,
                "period_end": period_end,
                "period_type": "PT60M",
                "value_extracted": Decimal(str(record["value"])),
                "unit": "EUR/MWh",
                "currency": record.get("currency", "EUR"),
                "data": {
                    **record.get("data", {}),
                    "source_file": source_file,
                    "import_timestamp": now.isoformat(),
                },
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
                    'currency': stmt.excluded.currency,
                }
            )

            await self.db.execute(stmt)
            await self.db.commit()

            logger.info(f"Bulk upserted {len(records_to_insert)} CSV price records from {source_file}")
            return len(records_to_insert), 0

        except Exception as e:
            logger.error(f"Error storing CSV price records: {str(e)}")
            await self.db.rollback()
            return 0, 0

    async def get_raw_prices(
        self,
        bidzone_codes: Optional[List[str]] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        price_type: Optional[str] = None,
        limit: int = 1000,
        offset: int = 0,
    ) -> List[PriceDataRaw]:
        """
        Query raw price data from price_data_raw table.

        Args:
            bidzone_codes: Optional list of bidzone codes to filter
            start_date: Optional start date filter
            end_date: Optional end date filter
            price_type: Optional price type filter ("day_ahead" or "intraday")
            limit: Maximum records to return
            offset: Pagination offset

        Returns:
            List of PriceDataRaw records
        """
        stmt = select(PriceDataRaw).where(PriceDataRaw.source == "ENTSOE")

        if bidzone_codes:
            stmt = stmt.where(PriceDataRaw.identifier.in_(bidzone_codes))

        if start_date:
            stmt = stmt.where(PriceDataRaw.period_start >= start_date)

        if end_date:
            stmt = stmt.where(PriceDataRaw.period_start < end_date)

        if price_type:
            stmt = stmt.where(PriceDataRaw.price_type == price_type)

        stmt = stmt.order_by(PriceDataRaw.period_start.desc())
        stmt = stmt.limit(limit).offset(offset)

        result = await self.db.execute(stmt)
        return list(result.scalars().all())

    async def get_available_bidzones(self) -> List[Dict[str, Any]]:
        """
        Get list of bidzones that have price data available.

        Returns:
            List of dicts with bidzone info and data availability
        """
        from sqlalchemy import func, distinct

        # Get distinct bidzone codes from raw data
        stmt = select(
            PriceDataRaw.identifier,
            func.min(PriceDataRaw.period_start).label("earliest_date"),
            func.max(PriceDataRaw.period_start).label("latest_date"),
            func.count(PriceDataRaw.id).label("record_count"),
        ).where(
            PriceDataRaw.source == "ENTSOE"
        ).group_by(
            PriceDataRaw.identifier
        )

        result = await self.db.execute(stmt)
        raw_bidzones = result.all()

        # Get bidzone details
        bidzones_info = []
        for row in raw_bidzones:
            bidzone_code = row.identifier

            # Try to get bidzone from database
            bidzone_stmt = select(Bidzone).where(Bidzone.code == bidzone_code)
            bidzone_result = await self.db.execute(bidzone_stmt)
            bidzone = bidzone_result.scalar_one_or_none()

            bidzones_info.append({
                "code": bidzone_code,
                "name": bidzone.name if bidzone else bidzone_code,
                "earliest_date": row.earliest_date.isoformat() if row.earliest_date else None,
                "latest_date": row.latest_date.isoformat() if row.latest_date else None,
                "record_count": row.record_count,
            })

        return bidzones_info

    def _make_json_serializable(self, obj: Any) -> Any:
        """Convert non-JSON-serializable objects to serializable format."""
        if isinstance(obj, set):
            return list(obj)
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, dict):
            return {k: self._make_json_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._make_json_serializable(item) for item in obj]
        else:
            return obj

    async def get_price_availability(
        self,
        year: int,
        month: int,
        bidzone_codes: Optional[List[str]] = None,
        price_type: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Get price data availability for a specific month.

        Args:
            year: Year for availability check
            month: Month (1-12) for availability check
            bidzone_codes: Optional list of bidzone codes to filter
            price_type: Optional price type filter ("day_ahead" or "intraday")

        Returns:
            Dict with availability by day and summary statistics
        """
        from calendar import monthrange
        from sqlalchemy import func

        # Get days in month
        days_in_month = monthrange(year, month)[1]
        start_date = datetime(year, month, 1, tzinfo=timezone.utc)
        end_date = datetime(year, month, days_in_month, 23, 59, 59, tzinfo=timezone.utc)

        # Build query to get daily aggregates
        query = select(
            func.date(PriceDataRaw.period_start).label('date'),
            PriceDataRaw.identifier.label('bidzone'),
            PriceDataRaw.price_type,
            func.count(PriceDataRaw.id).label('count')
        ).where(
            and_(
                PriceDataRaw.source == "ENTSOE",
                PriceDataRaw.period_start >= start_date,
                PriceDataRaw.period_start <= end_date,
            )
        )

        # Apply filters
        if bidzone_codes:
            query = query.where(PriceDataRaw.identifier.in_(bidzone_codes))

        if price_type:
            query = query.where(PriceDataRaw.price_type == price_type)

        query = query.group_by(
            func.date(PriceDataRaw.period_start),
            PriceDataRaw.identifier,
            PriceDataRaw.price_type
        )

        result = await self.db.execute(query)
        rows = result.all()

        # Process results into availability structure
        availability = {}
        all_bidzones = set()
        all_price_types = set()

        for row in rows:
            date_str = row.date.strftime('%Y-%m-%d')

            if date_str not in availability:
                availability[date_str] = {
                    'bidzones': [],
                    'recordCount': 0,
                    'priceTypes': []
                }

            if row.bidzone not in availability[date_str]['bidzones']:
                availability[date_str]['bidzones'].append(row.bidzone)

            if row.price_type not in availability[date_str]['priceTypes']:
                availability[date_str]['priceTypes'].append(row.price_type)

            availability[date_str]['recordCount'] += row.count

            all_bidzones.add(row.bidzone)
            all_price_types.add(row.price_type)

        # Calculate summary
        days_with_data = len(availability)
        coverage = (days_with_data / days_in_month) * 100 if days_in_month > 0 else 0

        return {
            'availability': availability,
            'summary': {
                'totalDays': days_in_month,
                'daysWithData': days_with_data,
                'coverage': round(coverage, 1),
                'bidzones': sorted(list(all_bidzones)),
                'priceTypes': sorted(list(all_price_types))
            }
        }

    async def fetch_and_store_prices_for_dates(
        self,
        dates: List,
        bidzone_codes: List[str],
        price_types: Optional[List[str]] = None,
        user_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Fetch and store price data for specific dates and bidzones.

        Args:
            dates: List of dates to fetch
            bidzone_codes: List of bidzone codes (e.g., NO_1, SE_1)
            price_types: List of price types to fetch (default: ["day_ahead"])
            user_id: User triggering the fetch

        Returns:
            Detailed results of the fetch operation by date and bidzone
        """
        from datetime import date as date_type

        if price_types is None:
            price_types = ["day_ahead"]

        start_time = datetime.now()
        client = ENTSOEPriceClient()

        results = {
            "success": True,
            "dates_requested": [d.isoformat() if hasattr(d, 'isoformat') else str(d) for d in dates],
            "bidzone_codes": bidzone_codes,
            "price_types": price_types,
            "results": [],
            "total_records_stored": 0,
            "total_records_updated": 0,
            "errors": [],
        }

        for fetch_date in dates:
            # Ensure fetch_date is a date object
            if isinstance(fetch_date, datetime):
                fetch_date = fetch_date.date()

            # Convert date to datetime range (full day in UTC)
            start_dt = datetime.combine(fetch_date, datetime.min.time()).replace(tzinfo=timezone.utc)
            end_dt = datetime.combine(fetch_date + timedelta(days=1), datetime.min.time()).replace(tzinfo=timezone.utc)

            date_result = {
                "date": fetch_date.isoformat(),
                "success": True,
                "by_bidzone": {},
                "total_records": 0,
                "errors": [],
            }

            for bidzone_code in bidzone_codes:
                bidzone_result = {
                    "bidzone_code": bidzone_code,
                    "records_stored": 0,
                    "records_updated": 0,
                    "by_price_type": {},
                    "errors": [],
                }

                try:
                    # Fetch prices from ENTSOE API
                    df, metadata = await client.fetch_prices(
                        start=start_dt,
                        end=end_dt,
                        area_code=bidzone_code,
                        price_types=price_types,
                    )

                    if df.empty:
                        bidzone_result["errors"].append(f"No data returned for {bidzone_code} on {fetch_date}")
                        if metadata.get("errors"):
                            bidzone_result["errors"].extend(metadata["errors"])
                        date_result["by_bidzone"][bidzone_code] = bidzone_result
                        continue

                    # Store the data by price type
                    for pt in price_types:
                        type_df = df[df["price_type"] == pt]
                        if type_df.empty:
                            continue

                        stored, updated = await self._store_price_records(
                            type_df,
                            bidzone_code,
                            pt,
                            user_id,
                            metadata,
                        )

                        bidzone_result["by_price_type"][pt] = {
                            "records_stored": stored,
                            "records_updated": updated,
                        }
                        bidzone_result["records_stored"] += stored
                        bidzone_result["records_updated"] += updated

                    date_result["total_records"] += bidzone_result["records_stored"]
                    results["total_records_stored"] += bidzone_result["records_stored"]
                    results["total_records_updated"] += bidzone_result["records_updated"]

                except Exception as e:
                    error_msg = f"Error fetching prices for {bidzone_code} on {fetch_date}: {str(e)}"
                    logger.error(error_msg)
                    bidzone_result["errors"].append(error_msg)
                    date_result["success"] = False
                    results["success"] = False

                date_result["by_bidzone"][bidzone_code] = bidzone_result

            if date_result["errors"] or not date_result["success"]:
                results["errors"].extend(date_result["errors"])

            results["results"].append(date_result)

        # Calculate response time
        end_time = datetime.now()
        results["duration_seconds"] = round((end_time - start_time).total_seconds(), 2)

        return results
