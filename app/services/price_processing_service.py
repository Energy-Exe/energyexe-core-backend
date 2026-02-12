"""Service for processing raw price data to windfarm-level hourly data."""

from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Dict, List, Any, Optional, Tuple
from uuid import uuid4

import structlog
from sqlalchemy import select, and_, func, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models.price_data import PriceDataRaw, PriceData
from app.models.windfarm import Windfarm
from app.models.bidzone import Bidzone

logger = structlog.get_logger()


class PriceProcessingService:
    """Service for processing raw price data into windfarm-level hourly data."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def process_raw_to_hourly(
        self,
        windfarm_ids: Optional[List[int]] = None,
        bidzone_codes: Optional[List[str]] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        force_reprocess: bool = False,
        source: str = "ENTSOE",
    ) -> Dict[str, Any]:
        """
        Process raw price data from price_data_raw to windfarm-level price_data.

        This maps bidzone prices to individual windfarms based on their bidzone_id.

        Args:
            windfarm_ids: Optional list of windfarm IDs to process
            bidzone_codes: Optional list of bidzone codes to process
            start_date: Optional start date filter
            end_date: Optional end date filter
            force_reprocess: If True, reprocess even if data exists
            source: Price data source ("ENTSOE" or "ELEXON")

        Returns:
            Summary of the processing operation
        """
        start_time = datetime.now()

        results = {
            "success": True,
            "windfarms_processed": 0,
            "records_created": 0,
            "records_updated": 0,
            "errors": [],
            "by_windfarm": {},
        }

        try:
            # Get windfarms to process
            windfarms = await self._get_windfarms_with_bidzones(
                windfarm_ids=windfarm_ids,
                bidzone_codes=bidzone_codes,
                force_reprocess=force_reprocess,
                source=source,
            )

            if not windfarms:
                results["errors"].append("No windfarms found with bidzones configured")
                results["success"] = False
                return results

            logger.info(f"Processing price data for {len(windfarms)} windfarms")

            # Group windfarms by bidzone for efficient processing
            bidzone_windfarms: Dict[int, List[Windfarm]] = {}
            for windfarm in windfarms:
                if windfarm.bidzone_id:
                    if windfarm.bidzone_id not in bidzone_windfarms:
                        bidzone_windfarms[windfarm.bidzone_id] = []
                    bidzone_windfarms[windfarm.bidzone_id].append(windfarm)

            # Process each bidzone's windfarms
            for bidzone_id, bidzone_windfarms_list in bidzone_windfarms.items():
                # Get bidzone code
                bidzone_stmt = select(Bidzone).where(Bidzone.id == bidzone_id)
                bidzone_result = await self.db.execute(bidzone_stmt)
                bidzone = bidzone_result.scalar_one_or_none()

                if not bidzone:
                    continue

                bidzone_code = bidzone.code

                # Get raw price data for this bidzone
                raw_prices = await self._get_raw_prices_for_bidzone(
                    bidzone_code=bidzone_code,
                    start_date=start_date,
                    end_date=end_date,
                    source=source,
                )

                if not raw_prices:
                    logger.info(f"No raw price data for bidzone {bidzone_code}")
                    continue

                # Process for each windfarm in this bidzone
                for windfarm in bidzone_windfarms_list:
                    try:
                        created, updated = await self._process_windfarm_prices(
                            windfarm=windfarm,
                            bidzone=bidzone,
                            raw_prices=raw_prices,
                            force_reprocess=force_reprocess,
                            source=source,
                        )

                        results["windfarms_processed"] += 1
                        results["records_created"] += created
                        results["records_updated"] += updated

                        results["by_windfarm"][windfarm.id] = {
                            "name": windfarm.name,
                            "bidzone": bidzone_code,
                            "records_created": created,
                            "records_updated": updated,
                        }

                        logger.info(f"Processed {created + updated} price records for {windfarm.name}")

                        # Commit after each windfarm to prevent data loss on errors
                        await self.db.commit()

                    except Exception as e:
                        error_msg = f"Error processing prices for {windfarm.name}: {str(e)}"
                        logger.error(error_msg)
                        results["errors"].append(error_msg)
                        # Rollback the failed transaction
                        await self.db.rollback()
                        # Continue processing next windfarm instead of crashing
                        continue

        except Exception as e:
            error_msg = f"Error processing price data: {str(e)}"
            logger.error(error_msg)
            results["errors"].append(error_msg)
            results["success"] = False

        # Calculate duration
        end_time = datetime.now()
        results["duration_seconds"] = round((end_time - start_time).total_seconds(), 2)

        return results

    async def _get_windfarms_with_bidzones(
        self,
        windfarm_ids: Optional[List[int]] = None,
        bidzone_codes: Optional[List[str]] = None,
        force_reprocess: bool = False,
        source: str = "ENTSOE",
    ) -> List[Windfarm]:
        """Get windfarms that have bidzones configured and need price data processing."""
        stmt = select(Windfarm).where(Windfarm.bidzone_id.isnot(None))

        if not force_reprocess:
            # Only get windfarms that don't have price_data for this source yet
            stmt = stmt.where(
                ~Windfarm.id.in_(
                    select(PriceData.windfarm_id).where(
                        PriceData.source == source
                    ).distinct()
                )
            )

        if windfarm_ids:
            stmt = stmt.where(Windfarm.id.in_(windfarm_ids))

        if bidzone_codes:
            # Join with bidzones to filter by code
            stmt = stmt.join(Bidzone).where(Bidzone.code.in_(bidzone_codes))

        result = await self.db.execute(stmt)
        return list(result.scalars().all())

    async def _get_raw_prices_for_bidzone(
        self,
        bidzone_code: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        source: str = "ENTSOE",
    ) -> Dict[datetime, Dict[str, Decimal]]:
        """
        Get raw prices for a bidzone, organized by hour.

        Returns:
            Dict mapping hour -> {"day_ahead": price, "intraday": price}
        """
        stmt = select(PriceDataRaw).where(
            and_(
                PriceDataRaw.source == source,
                PriceDataRaw.identifier == bidzone_code,
            )
        )

        if start_date:
            stmt = stmt.where(PriceDataRaw.period_start >= start_date)

        if end_date:
            stmt = stmt.where(PriceDataRaw.period_start < end_date)

        stmt = stmt.order_by(PriceDataRaw.period_start)

        result = await self.db.execute(stmt)
        raw_records = result.scalars().all()

        # Organize by hour - round period_start to hour boundary for aggregation
        # This handles both PT60M (hourly) and PT15M (15-minute) data
        prices_by_hour: Dict[datetime, Dict[str, Any]] = {}

        for record in raw_records:
            # Round to hour boundary (00:15, 00:30, 00:45 → 00:00)
            hour = record.period_start.replace(minute=0, second=0, microsecond=0)

            if hour not in prices_by_hour:
                prices_by_hour[hour] = {
                    "day_ahead": [],  # Store as list for averaging
                    "intraday": [],   # Store as list for averaging
                    "currency": record.currency or "EUR",
                    "raw_ids": [],
                }

            # Collect all values for this hour (for averaging 15-min data)
            if record.price_type == "day_ahead" and record.value_extracted is not None:
                prices_by_hour[hour]["day_ahead"].append(record.value_extracted)
            elif record.price_type == "intraday" and record.value_extracted is not None:
                prices_by_hour[hour]["intraday"].append(record.value_extracted)

            prices_by_hour[hour]["raw_ids"].append(record.id)

        # Average multiple values per hour (for PT15M data)
        # Use Decimal arithmetic to avoid async context issues
        aggregated_prices = {}
        for hour, data in prices_by_hour.items():
            # Calculate averages using Decimal for precision
            day_ahead_avg = None
            if data["day_ahead"]:
                total = Decimal("0")
                for val in data["day_ahead"]:
                    total += val
                day_ahead_avg = total / Decimal(len(data["day_ahead"]))

            intraday_avg = None
            if data["intraday"]:
                total = Decimal("0")
                for val in data["intraday"]:
                    total += val
                intraday_avg = total / Decimal(len(data["intraday"]))

            aggregated_prices[hour] = {
                "day_ahead": day_ahead_avg,
                "intraday": intraday_avg,
                "currency": data["currency"],
                "raw_ids": data["raw_ids"],
            }

        return aggregated_prices

    async def _process_windfarm_prices(
        self,
        windfarm: Windfarm,
        bidzone: Bidzone,
        raw_prices: Dict[datetime, Dict[str, Any]],
        force_reprocess: bool = False,
        batch_size: int = 2000,  # PostgreSQL has 32767 param limit; 12 params * 2000 = 24000
        source: str = "ENTSOE",
    ) -> Tuple[int, int]:
        """
        Process raw prices for a single windfarm.

        Returns:
            Tuple of (records_created, records_updated)
        """
        now = datetime.now(timezone.utc)
        records_to_insert = []

        for hour, price_data in raw_prices.items():
            # Skip hours with no price data
            if price_data["day_ahead"] is None and price_data["intraday"] is None:
                continue

            records_to_insert.append({
                "id": uuid4(),
                "hour": hour,
                "windfarm_id": windfarm.id,
                "bidzone_id": bidzone.id,
                "day_ahead_price": price_data["day_ahead"],
                "intraday_price": price_data["intraday"],
                "currency": price_data["currency"],
                "source": source,
                "raw_data_ids": price_data["raw_ids"],
                "quality_flag": "good" if price_data["day_ahead"] is not None else "partial",
                "created_at": now,
                "updated_at": now,
            })

        if not records_to_insert:
            return 0, 0

        total_inserted = 0

        try:
            # Process in batches to avoid PostgreSQL parameter limit
            for i in range(0, len(records_to_insert), batch_size):
                batch = records_to_insert[i:i + batch_size]

                stmt = insert(PriceData).values(batch)

                # Use upsert to handle existing records
                stmt = stmt.on_conflict_do_update(
                    constraint='uq_price_hour_windfarm_source',
                    set_={
                        'day_ahead_price': stmt.excluded.day_ahead_price,
                        'intraday_price': stmt.excluded.intraday_price,
                        'currency': stmt.excluded.currency,
                        'raw_data_ids': stmt.excluded.raw_data_ids,
                        'quality_flag': stmt.excluded.quality_flag,
                        'updated_at': now,
                    }
                )

                await self.db.execute(stmt)
                await self.db.commit()
                total_inserted += len(batch)

            logger.info(f"Processed {total_inserted} price records for {windfarm.name}")

            return total_inserted, 0

        except Exception as e:
            logger.error(f"Error processing prices for {windfarm.name}: {str(e)}")
            await self.db.rollback()
            return 0, 0

    async def get_processed_prices(
        self,
        windfarm_ids: Optional[List[int]] = None,
        bidzone_ids: Optional[List[int]] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 1000,
        offset: int = 0,
    ) -> List[PriceData]:
        """
        Query processed price data from price_data table.

        Args:
            windfarm_ids: Optional list of windfarm IDs to filter
            bidzone_ids: Optional list of bidzone IDs to filter
            start_date: Optional start date filter
            end_date: Optional end date filter
            limit: Maximum records to return
            offset: Pagination offset

        Returns:
            List of PriceData records
        """
        stmt = select(PriceData)

        if windfarm_ids:
            stmt = stmt.where(PriceData.windfarm_id.in_(windfarm_ids))

        if bidzone_ids:
            stmt = stmt.where(PriceData.bidzone_id.in_(bidzone_ids))

        if start_date:
            stmt = stmt.where(PriceData.hour >= start_date)

        if end_date:
            stmt = stmt.where(PriceData.hour < end_date)

        stmt = stmt.order_by(PriceData.hour.desc())
        stmt = stmt.limit(limit).offset(offset)

        result = await self.db.execute(stmt)
        return list(result.scalars().all())

    async def get_price_statistics(
        self,
        windfarm_id: int,
        start_date: datetime,
        end_date: datetime,
    ) -> Dict[str, Any]:
        """
        Get price statistics for a windfarm over a period.

        Returns statistics like average, min, max prices.
        """
        stmt = text("""
            SELECT
                COUNT(*) as hours_with_data,
                AVG(day_ahead_price) as avg_day_ahead,
                MIN(day_ahead_price) as min_day_ahead,
                MAX(day_ahead_price) as max_day_ahead,
                AVG(intraday_price) as avg_intraday,
                MIN(intraday_price) as min_intraday,
                MAX(intraday_price) as max_intraday
            FROM price_data
            WHERE windfarm_id = :windfarm_id
              AND hour >= :start_date
              AND hour < :end_date
        """)

        result = await self.db.execute(
            stmt,
            {
                "windfarm_id": windfarm_id,
                "start_date": start_date,
                "end_date": end_date,
            }
        )
        row = result.fetchone()

        if not row:
            return {}

        return {
            "hours_with_data": row.hours_with_data,
            "day_ahead": {
                "average": float(row.avg_day_ahead) if row.avg_day_ahead else None,
                "min": float(row.min_day_ahead) if row.min_day_ahead else None,
                "max": float(row.max_day_ahead) if row.max_day_ahead else None,
            },
            "intraday": {
                "average": float(row.avg_intraday) if row.avg_intraday else None,
                "min": float(row.min_intraday) if row.min_intraday else None,
                "max": float(row.max_intraday) if row.max_intraday else None,
            },
        }

    async def get_windfarm_coverage(
        self,
        windfarm_id: int,
        start_date: datetime,
        end_date: datetime,
    ) -> Dict[str, Any]:
        """
        Get price data coverage for a windfarm over a period.

        Returns information about data completeness.
        """
        # Calculate expected hours
        total_hours = int((end_date - start_date).total_seconds() / 3600)

        # Count actual hours with data
        stmt = text("""
            SELECT
                COUNT(*) as hours_with_data,
                COUNT(day_ahead_price) as hours_with_day_ahead,
                COUNT(intraday_price) as hours_with_intraday
            FROM price_data
            WHERE windfarm_id = :windfarm_id
              AND hour >= :start_date
              AND hour < :end_date
        """)

        result = await self.db.execute(
            stmt,
            {
                "windfarm_id": windfarm_id,
                "start_date": start_date,
                "end_date": end_date,
            }
        )
        row = result.fetchone()

        if not row:
            return {
                "total_hours": total_hours,
                "hours_with_data": 0,
                "coverage_percent": 0.0,
            }

        return {
            "total_hours": total_hours,
            "hours_with_data": row.hours_with_data,
            "hours_with_day_ahead": row.hours_with_day_ahead,
            "hours_with_intraday": row.hours_with_intraday,
            "coverage_percent": round(row.hours_with_data / total_hours * 100, 2) if total_hours > 0 else 0.0,
            "day_ahead_coverage_percent": round(row.hours_with_day_ahead / total_hours * 100, 2) if total_hours > 0 else 0.0,
            "intraday_coverage_percent": round(row.hours_with_intraday / total_hours * 100, 2) if total_hours > 0 else 0.0,
        }
