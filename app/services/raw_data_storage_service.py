"""Service for fetching data from external APIs and storing in generation_data_raw."""

import json
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional, Tuple
from decimal import Decimal

import pandas as pd
import structlog
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.generation_data import GenerationDataRaw
from app.models.generation_unit import GenerationUnit
from app.models.windfarm import Windfarm
from app.schemas.raw_data_fetch import (
    RawDataFetchRequest,
    RawDataFetchResponse,
    GenerationUnitSummary,
)
from app.services.entsoe_client import ENTSOEClient
from app.services.elexon_client import ElexonClient
from app.services.eia_client import EIAClient
from app.services.taipower_client import TaipowerClient

logger = structlog.get_logger()


class RawDataStorageService:
    """Service for fetching from external APIs and storing in generation_data_raw."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def fetch_and_store_all_sources(
        self,
        windfarm_ids: List[int],
        start_date: datetime,
        end_date: datetime,
        user_id: int,
    ) -> Dict[str, Any]:
        """
        Fetch data from all available sources for the given windfarms.

        Auto-detects which sources have generation units configured for the windfarms
        and fetches from each source automatically.
        """
        from sqlalchemy.orm import selectinload

        # Get all windfarms with their generation units
        stmt = (
            select(Windfarm)
            .options(selectinload(Windfarm.generation_units))
            .where(Windfarm.id.in_(windfarm_ids))
        )
        result = await self.db.execute(stmt)
        windfarms = result.scalars().all()

        if not windfarms:
            from app.schemas.raw_data_fetch import UnifiedRawDataFetchResponse
            return UnifiedRawDataFetchResponse(
                success=False,
                windfarm_ids=windfarm_ids,
                windfarm_names=[],
                date_range={
                    "start": start_date.isoformat(),
                    "end": end_date.isoformat(),
                },
                total_records_stored=0,
                total_records_updated=0,
                sources_processed=[],
                by_source={},
                overall_summary={},
                errors=["No windfarms found with the provided IDs"],
            )

        # Collect all generation units and group by source
        sources_map = {}
        for windfarm in windfarms:
            for unit in windfarm.generation_units:
                source = unit.source
                if source not in sources_map:
                    sources_map[source] = set()
                sources_map[source].add(windfarm.id)

        # Build request for each source
        request_base = RawDataFetchRequest(
            windfarm_ids=windfarm_ids,
            start_date=start_date,
            end_date=end_date,
        )

        # Fetch from each detected source
        results_by_source = {}
        all_errors = []
        total_stored = 0
        total_updated = 0

        for source, wf_ids in sources_map.items():
            # Create request with only windfarms that have this source
            source_request = RawDataFetchRequest(
                windfarm_ids=list(wf_ids),
                start_date=start_date,
                end_date=end_date,
            )

            try:
                if source == "ENTSOE":
                    result = await self.fetch_and_store_entsoe(source_request, user_id)
                elif source == "ELEXON":
                    result = await self.fetch_and_store_elexon(source_request, user_id)
                elif source == "EIA":
                    result = await self.fetch_and_store_eia(source_request, user_id)
                elif source == "TAIPOWER":
                    result = await self.fetch_and_store_taipower(source_request, user_id)
                elif source == "NVE":
                    result = await self.fetch_and_store_nve(source_request, user_id)
                elif source == "ENERGISTYRELSEN":
                    result = await self.fetch_and_store_energistyrelsen(source_request, user_id)
                else:
                    logger.warning(f"Unknown source: {source}")
                    continue

                results_by_source[source] = result
                total_stored += result.records_stored
                total_updated += result.records_updated

                if result.errors:
                    all_errors.extend([f"{source}: {e}" for e in result.errors])

            except Exception as e:
                error_msg = f"{source}: {str(e)}"
                logger.error(f"Error fetching {source} data: {str(e)}")
                all_errors.append(error_msg)

        from app.schemas.raw_data_fetch import UnifiedRawDataFetchResponse
        return UnifiedRawDataFetchResponse(
            success=len(all_errors) == 0,
            windfarm_ids=windfarm_ids,
            windfarm_names=[w.name for w in windfarms],
            date_range={
                "start": start_date.isoformat(),
                "end": end_date.isoformat(),
            },
            total_records_stored=total_stored,
            total_records_updated=total_updated,
            sources_processed=list(sources_map.keys()),
            by_source=results_by_source,
            overall_summary={
                "sources_detected": len(sources_map),
                "sources_with_data": len([r for r in results_by_source.values() if r.records_stored > 0 or r.records_updated > 0]),
            },
            errors=all_errors,
        )

    async def fetch_and_store_entsoe(
        self,
        request: RawDataFetchRequest,
        user_id: int,
    ) -> RawDataFetchResponse:
        """Fetch ENTSOE data and store in generation_data_raw."""
        start_time = datetime.now()
        source = "ENTSOE"

        # Get windfarms with generation units
        windfarms = await self._get_windfarms_with_units(request.windfarm_ids, source)

        if not windfarms:
            return RawDataFetchResponse(
                success=False,
                source=source,
                windfarm_ids=request.windfarm_ids,
                windfarm_names=[],
                date_range={
                    "start": request.start_date.isoformat(),
                    "end": request.end_date.isoformat(),
                },
                records_stored=0,
                records_updated=0,
                generation_units_processed=[],
                errors=[f"No windfarms found with ENTSOE generation units"],
            )

        client = ENTSOEClient()
        all_generation_units = []
        all_errors = []
        total_records_stored = 0
        total_records_updated = 0
        total_api_calls = 0

        # Process each windfarm
        for windfarm in windfarms:
            # Get ENTSOE units for this windfarm
            entsoe_units = [u for u in windfarm.generation_units if u.source == "ENTSOE"]

            if not entsoe_units:
                continue

            # Get bidzone for this windfarm (for area_code)
            bidzone_code = None
            if windfarm.bidzone_id:
                from app.models.bidzone import Bidzone
                stmt = select(Bidzone).where(Bidzone.id == windfarm.bidzone_id)
                result = await self.db.execute(stmt)
                bidzone = result.scalar_one_or_none()
                if bidzone:
                    bidzone_code = bidzone.name  # e.g., 'DK_1', 'DK_2'

            if not bidzone_code:
                all_errors.append(f"Windfarm {windfarm.name} has no bidzone configured")
                continue

            # Extract EIC codes from units
            eic_codes = [u.code for u in entsoe_units if u.code and u.code != 'nan']

            if not eic_codes:
                all_errors.append(f"No EIC codes found for windfarm {windfarm.name}")
                continue

            try:
                # Fetch per-unit data from ENTSOE
                logger.info(f"Fetching ENTSOE data for {windfarm.name} ({len(eic_codes)} units)")

                # Convert dates to naive UTC for ENTSOE client
                start_naive = request.start_date.replace(tzinfo=None) if request.start_date.tzinfo else request.start_date
                end_naive = request.end_date.replace(tzinfo=None) if request.end_date.tzinfo else request.end_date

                df, metadata = await client.fetch_generation_per_unit(
                    start=start_naive,
                    end=end_naive,
                    area_code=bidzone_code,
                    eic_codes=eic_codes,
                    production_types=["wind"],
                )

                total_api_calls += 1

                if df.empty:
                    logger.warning(f"No data returned for windfarm {windfarm.name}")
                    continue

                # Transform and store data for each unit
                for unit in entsoe_units:
                    # Filter dataframe for this unit's EIC code
                    unit_df = df[df.get('eic_code', df.get('unit_code', '')) == unit.code]

                    if unit_df.empty:
                        logger.warning(f"No data for unit {unit.code}")
                        continue

                    # Transform to generation_data_raw format
                    records_stored, records_updated = await self._store_entsoe_records(
                        unit_df,
                        unit,
                        bidzone_code,
                        user_id,
                        metadata,
                    )

                    total_records_stored += records_stored
                    total_records_updated += records_updated

                    all_generation_units.append(
                        GenerationUnitSummary(
                            id=unit.id,
                            code=unit.code,
                            name=unit.name,
                            records_stored=records_stored,
                            records_updated=records_updated,
                        )
                    )

            except Exception as e:
                error_msg = f"Error fetching ENTSOE data for {windfarm.name}: {str(e)}"
                logger.error(error_msg)
                all_errors.append(error_msg)

        # Calculate response time
        end_time = datetime.now()
        response_time_seconds = (end_time - start_time).total_seconds()

        return RawDataFetchResponse(
            success=len(all_errors) == 0,
            source=source,
            windfarm_ids=request.windfarm_ids,
            windfarm_names=[w.name for w in windfarms],
            date_range={
                "start": request.start_date.isoformat(),
                "end": request.end_date.isoformat(),
            },
            records_stored=total_records_stored,
            records_updated=total_records_updated,
            generation_units_processed=all_generation_units,
            summary={
                "total_api_calls": total_api_calls,
                "api_response_time_seconds": round(response_time_seconds, 2),
            },
            errors=all_errors,
        )

    async def _store_entsoe_records(
        self,
        df: pd.DataFrame,
        unit: GenerationUnit,
        bidzone_code: str,
        user_id: int,
        api_metadata: Dict,
    ) -> Tuple[int, int]:
        """Store ENTSOE records in generation_data_raw."""
        records_stored = 0
        records_updated = 0

        # Convert api_metadata to JSON-serializable format
        serializable_metadata = self._make_json_serializable(api_metadata)

        for idx, row in df.iterrows():
            # Extract timestamp
            timestamp = row.get("timestamp", idx)
            if not isinstance(timestamp, datetime):
                timestamp = pd.to_datetime(timestamp)

            # Ensure timezone-aware
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)

            # Determine period type and end time
            resolution = row.get("resolution_code", "PT60M")
            if resolution == "PT15M":
                period_end = timestamp + timedelta(minutes=15)
                period_type = "PT15M"
            elif resolution == "PT60M":
                period_end = timestamp + timedelta(hours=1)
                period_type = "PT60M"
            else:
                # Default to hourly
                period_end = timestamp + timedelta(hours=1)
                period_type = "PT60M"

            # Extract value
            value = float(row.get("value", 0))

            # Build data JSONB
            data = {
                "eic_code": unit.code,
                "area_code": bidzone_code,
                "production_type": row.get("production_type", "wind"),
                "resolution_code": resolution,
                "installed_capacity_mw": float(row["installed_capacity_mw"]) if "installed_capacity_mw" in row and pd.notna(row["installed_capacity_mw"]) else None,
                "fetch_metadata": {
                    "fetched_by_user_id": user_id,
                    "fetch_timestamp": datetime.now(timezone.utc).isoformat(),
                    "fetch_method": "api",
                    "api_metadata": serializable_metadata,
                },
            }

            # Check if record exists
            stmt = select(GenerationDataRaw).where(
                and_(
                    GenerationDataRaw.source == "ENTSOE",
                    GenerationDataRaw.identifier == unit.code,
                    GenerationDataRaw.period_start == timestamp,
                )
            )
            result = await self.db.execute(stmt)
            existing_record = result.scalar_one_or_none()

            if existing_record:
                # Update existing record
                existing_record.value_extracted = Decimal(str(value))
                existing_record.data = data
                existing_record.updated_at = datetime.now(timezone.utc)
                records_updated += 1
            else:
                # Create new record
                new_record = GenerationDataRaw(
                    source="ENTSOE",
                    source_type="api",
                    identifier=unit.code,
                    period_start=timestamp,
                    period_end=period_end,
                    period_type=period_type,
                    value_extracted=Decimal(str(value)),
                    unit="MW",
                    data=data,
                )
                self.db.add(new_record)
                records_stored += 1

        await self.db.commit()
        return records_stored, records_updated

    async def fetch_and_store_elexon(
        self,
        request: RawDataFetchRequest,
        user_id: int,
    ) -> RawDataFetchResponse:
        """Fetch ELEXON data and store in generation_data_raw."""
        start_time = datetime.now()
        source = "ELEXON"

        # Get windfarms with generation units
        windfarms = await self._get_windfarms_with_units(request.windfarm_ids, source)

        if not windfarms:
            return RawDataFetchResponse(
                success=False,
                source=source,
                windfarm_ids=request.windfarm_ids,
                windfarm_names=[],
                date_range={
                    "start": request.start_date.isoformat(),
                    "end": request.end_date.isoformat(),
                },
                records_stored=0,
                records_updated=0,
                generation_units_processed=[],
                errors=[f"No windfarms found with ELEXON generation units"],
            )

        client = ElexonClient()
        all_generation_units = []
        all_errors = []
        total_records_stored = 0
        total_records_updated = 0
        total_api_calls = 0

        # Process each windfarm
        for windfarm in windfarms:
            # Get ELEXON units (BM Units) for this windfarm
            elexon_units = [u for u in windfarm.generation_units if u.source == "ELEXON"]

            if not elexon_units:
                continue

            # Extract BM Unit codes
            bm_units = [u.code for u in elexon_units if u.code]

            if not bm_units:
                all_errors.append(f"No BM Unit codes found for windfarm {windfarm.name}")
                continue

            try:
                # Fetch physical data from ELEXON
                logger.info(f"Fetching ELEXON data for {windfarm.name} ({len(bm_units)} units)")

                df, metadata = await client.fetch_physical_data(
                    start=request.start_date,
                    end=request.end_date,
                    bm_units=bm_units,
                )

                total_api_calls += 1

                if df.empty:
                    logger.warning(f"No data returned for windfarm {windfarm.name}")
                    continue

                # Transform and store data for each unit
                for unit in elexon_units:
                    # Filter dataframe for this BM Unit
                    unit_df = df[df.get('bm_unit', '') == unit.code]

                    if unit_df.empty:
                        logger.warning(f"No data for BM Unit {unit.code}")
                        continue

                    # Transform to generation_data_raw format
                    records_stored, records_updated = await self._store_elexon_records(
                        unit_df,
                        unit,
                        user_id,
                        metadata,
                    )

                    total_records_stored += records_stored
                    total_records_updated += records_updated

                    all_generation_units.append(
                        GenerationUnitSummary(
                            id=unit.id,
                            code=unit.code,
                            name=unit.name,
                            records_stored=records_stored,
                            records_updated=records_updated,
                        )
                    )

            except Exception as e:
                error_msg = f"Error fetching ELEXON data for {windfarm.name}: {str(e)}"
                logger.error(error_msg)
                all_errors.append(error_msg)

        # Calculate response time
        end_time = datetime.now()
        response_time_seconds = (end_time - start_time).total_seconds()

        return RawDataFetchResponse(
            success=len(all_errors) == 0,
            source=source,
            windfarm_ids=request.windfarm_ids,
            windfarm_names=[w.name for w in windfarms],
            date_range={
                "start": request.start_date.isoformat(),
                "end": request.end_date.isoformat(),
            },
            records_stored=total_records_stored,
            records_updated=total_records_updated,
            generation_units_processed=all_generation_units,
            summary={
                "total_api_calls": total_api_calls,
                "api_response_time_seconds": round(response_time_seconds, 2),
            },
            errors=all_errors,
        )

    async def _store_elexon_records(
        self,
        df: pd.DataFrame,
        unit: GenerationUnit,
        user_id: int,
        api_metadata: Dict,
    ) -> Tuple[int, int]:
        """Store ELEXON records in generation_data_raw."""
        records_stored = 0
        records_updated = 0

        # Convert api_metadata to JSON-serializable format
        serializable_metadata = self._make_json_serializable(api_metadata)

        for idx, row in df.iterrows():
            # Extract timestamp
            timestamp = row.get("timestamp", idx)
            if not isinstance(timestamp, datetime):
                timestamp = pd.to_datetime(timestamp)

            # Ensure timezone-aware
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)

            # ELEXON uses 30-minute settlement periods
            period_end = timestamp + timedelta(minutes=30)
            period_type = "PT30M"

            # Extract value (MWh for 30-min period)
            value = float(row.get("value", row.get("levelFrom", 0)))

            # Build data JSONB
            settlement_date = row.get("settlementDate")
            if isinstance(settlement_date, datetime):
                settlement_date = settlement_date.isoformat()

            data = {
                "bm_unit": unit.code,
                "level_from": float(row["levelFrom"]) if "levelFrom" in row else None,
                "level_to": float(row["levelTo"]) if "levelTo" in row else None,
                "settlement_period": int(row["settlementPeriod"]) if "settlementPeriod" in row else None,
                "settlement_date": settlement_date,
                "fetch_metadata": {
                    "fetched_by_user_id": user_id,
                    "fetch_timestamp": datetime.now(timezone.utc).isoformat(),
                    "fetch_method": "api",
                    "api_metadata": serializable_metadata,
                },
            }

            # Check if record exists
            stmt = select(GenerationDataRaw).where(
                and_(
                    GenerationDataRaw.source == "ELEXON",
                    GenerationDataRaw.identifier == unit.code,
                    GenerationDataRaw.period_start == timestamp,
                )
            )
            result = await self.db.execute(stmt)
            existing_record = result.scalar_one_or_none()

            if existing_record:
                # Update existing record
                existing_record.value_extracted = Decimal(str(value))
                existing_record.data = data
                existing_record.updated_at = datetime.now(timezone.utc)
                records_updated += 1
            else:
                # Create new record
                new_record = GenerationDataRaw(
                    source="ELEXON",
                    source_type="api",
                    identifier=unit.code,
                    period_start=timestamp,
                    period_end=period_end,
                    period_type=period_type,
                    value_extracted=Decimal(str(value)),
                    unit="MWh",
                    data=data,
                )
                self.db.add(new_record)
                records_stored += 1

        await self.db.commit()
        return records_stored, records_updated

    async def fetch_and_store_eia(
        self,
        request: RawDataFetchRequest,
        user_id: int,
    ) -> RawDataFetchResponse:
        """Fetch EIA data (monthly) and store in generation_data_raw."""
        start_time = datetime.now()
        source = "EIA"

        # Get windfarms with generation units
        windfarms = await self._get_windfarms_with_units(request.windfarm_ids, source)

        if not windfarms:
            return RawDataFetchResponse(
                success=False,
                source=source,
                windfarm_ids=request.windfarm_ids,
                windfarm_names=[],
                date_range={
                    "start": request.start_date.isoformat(),
                    "end": request.end_date.isoformat(),
                },
                records_stored=0,
                records_updated=0,
                generation_units_processed=[],
                errors=[f"No windfarms found with EIA generation units"],
            )

        client = EIAClient()
        all_generation_units = []
        all_errors = []
        total_records_stored = 0
        total_records_updated = 0
        total_api_calls = 0

        # Process each windfarm
        for windfarm in windfarms:
            # Get EIA units (plant codes) for this windfarm
            eia_units = [u for u in windfarm.generation_units if u.source == "EIA"]

            if not eia_units:
                continue

            # Extract plant codes
            plant_codes = [u.code for u in eia_units if u.code]

            if not plant_codes:
                all_errors.append(f"No plant codes found for windfarm {windfarm.name}")
                continue

            try:
                # Fetch monthly data from EIA
                logger.info(f"Fetching EIA data for {windfarm.name} ({len(plant_codes)} plants)")

                # EIA data is monthly, so extract year/month from date range
                start_year = request.start_date.year
                start_month = request.start_date.month
                end_year = request.end_date.year
                end_month = request.end_date.month

                df, metadata = await client.fetch_monthly_generation_data(
                    plant_codes=plant_codes,
                    start_year=start_year,
                    start_month=start_month,
                    end_year=end_year,
                    end_month=end_month,
                )

                total_api_calls += 1

                if df.empty:
                    logger.warning(f"No data returned for windfarm {windfarm.name}")
                    if metadata.get("errors"):
                        all_errors.extend([str(e) for e in metadata["errors"]])
                    continue

                # Transform and store data for each unit
                for unit in eia_units:
                    # Filter dataframe for this plant code
                    unit_df = df[df.get('plantCode', '').astype(str) == str(unit.code)]

                    if unit_df.empty:
                        logger.warning(f"No data for plant {unit.code}")
                        continue

                    # Transform to generation_data_raw format
                    records_stored, records_updated = await self._store_eia_records(
                        unit_df,
                        unit,
                        user_id,
                        metadata,
                    )

                    total_records_stored += records_stored
                    total_records_updated += records_updated

                    all_generation_units.append(
                        GenerationUnitSummary(
                            id=unit.id,
                            code=unit.code,
                            name=unit.name,
                            records_stored=records_stored,
                            records_updated=records_updated,
                        )
                    )

            except Exception as e:
                error_msg = f"Error fetching EIA data for {windfarm.name}: {str(e)}"
                logger.error(error_msg)
                all_errors.append(error_msg)

        # Calculate response time
        end_time = datetime.now()
        response_time_seconds = (end_time - start_time).total_seconds()

        return RawDataFetchResponse(
            success=len(all_errors) == 0,
            source=source,
            windfarm_ids=request.windfarm_ids,
            windfarm_names=[w.name for w in windfarms],
            date_range={
                "start": request.start_date.isoformat(),
                "end": request.end_date.isoformat(),
            },
            records_stored=total_records_stored,
            records_updated=total_records_updated,
            generation_units_processed=all_generation_units,
            summary={
                "total_api_calls": total_api_calls,
                "api_response_time_seconds": round(response_time_seconds, 2),
            },
            errors=all_errors,
        )

    async def _store_eia_records(
        self,
        df: pd.DataFrame,
        unit: GenerationUnit,
        user_id: int,
        api_metadata: Dict,
    ) -> Tuple[int, int]:
        """Store EIA records in generation_data_raw."""
        records_stored = 0
        records_updated = 0

        # Convert api_metadata to JSON-serializable format
        serializable_metadata = self._make_json_serializable(api_metadata)

        for idx, row in df.iterrows():
            # Extract period (YYYY-MM format)
            period_str = row.get("period")
            if not period_str:
                continue

            # Parse period to get start and end dates
            period_date = pd.to_datetime(period_str, format="%Y-%m")
            period_start = period_date.replace(tzinfo=timezone.utc)

            # End of month
            if period_date.month == 12:
                period_end = period_date.replace(year=period_date.year + 1, month=1, day=1, tzinfo=timezone.utc)
            else:
                period_end = period_date.replace(month=period_date.month + 1, day=1, tzinfo=timezone.utc)

            # Extract value (monthly generation in MWh)
            value = float(row.get("generation", 0))

            # Build data JSONB
            data = {
                "plant_code": str(row.get("plantCode", unit.code)),
                "plant_name": row.get("plantName", unit.name),
                "state": row.get("state"),
                "fuel_type": row.get("fuel2002", "WND"),
                "period": period_str,
                "generation_mwh": value,
                "fetch_metadata": {
                    "fetched_by_user_id": user_id,
                    "fetch_timestamp": datetime.now(timezone.utc).isoformat(),
                    "fetch_method": "api",
                    "api_metadata": serializable_metadata,
                },
            }

            # Check if record exists
            stmt = select(GenerationDataRaw).where(
                and_(
                    GenerationDataRaw.source == "EIA",
                    GenerationDataRaw.identifier == str(unit.code),
                    GenerationDataRaw.period_start == period_start,
                )
            )
            result = await self.db.execute(stmt)
            existing_record = result.scalar_one_or_none()

            if existing_record:
                # Update existing record
                existing_record.value_extracted = Decimal(str(value))
                existing_record.data = data
                existing_record.updated_at = datetime.now(timezone.utc)
                records_updated += 1
            else:
                # Create new record
                new_record = GenerationDataRaw(
                    source="EIA",
                    source_type="api",
                    identifier=str(unit.code),
                    period_start=period_start,
                    period_end=period_end,
                    period_type="month",
                    value_extracted=Decimal(str(value)),
                    unit="MWh",
                    data=data,
                )
                self.db.add(new_record)
                records_stored += 1

        await self.db.commit()
        return records_stored, records_updated

    async def fetch_and_store_taipower(
        self,
        request: RawDataFetchRequest,
        user_id: int,
    ) -> RawDataFetchResponse:
        """Fetch TAIPOWER data (live/10-minute) and store in generation_data_raw.

        Note: TAIPOWER API returns live data for ALL wind units in Taiwan in one call.
        We filter for the selected windfarms after fetching.
        """
        start_time = datetime.now()
        source = "TAIPOWER"

        # Get windfarms with generation units
        windfarms = await self._get_windfarms_with_units(request.windfarm_ids, source)

        if not windfarms:
            return RawDataFetchResponse(
                success=False,
                source=source,
                windfarm_ids=request.windfarm_ids,
                windfarm_names=[],
                date_range={
                    "start": request.start_date.isoformat(),
                    "end": request.end_date.isoformat(),
                },
                records_stored=0,
                records_updated=0,
                generation_units_processed=[],
                errors=[f"No windfarms found with TAIPOWER generation units"],
            )

        client = TaipowerClient()
        all_generation_units = []
        all_errors = []
        total_records_stored = 0
        total_records_updated = 0
        total_api_calls = 0

        try:
            # Fetch live data from TAIPOWER (returns ALL units)
            logger.info(f"Fetching TAIPOWER live data for {len(windfarms)} windfarms")

            taipower_response, metadata = await client.fetch_live_data()

            total_api_calls += 1

            if not taipower_response or not taipower_response.generation_units:
                logger.warning(f"No data returned from TAIPOWER")
                if metadata.get("errors"):
                    all_errors.extend([str(e) for e in metadata["errors"]])
                return RawDataFetchResponse(
                    success=False,
                    source=source,
                    windfarm_ids=request.windfarm_ids,
                    windfarm_names=[w.name for w in windfarms],
                    date_range={
                        "start": request.start_date.isoformat(),
                        "end": request.end_date.isoformat(),
                    },
                    records_stored=0,
                    records_updated=0,
                    generation_units_processed=[],
                    errors=all_errors,
                )

            # Get all TAIPOWER units across all selected windfarms
            all_taipower_units = []
            for windfarm in windfarms:
                taipower_units = [u for u in windfarm.generation_units if u.source == "TAIPOWER"]
                all_taipower_units.extend(taipower_units)

            # Create a map of unit codes to GenerationUnit objects
            unit_code_map = {u.code: u for u in all_taipower_units if u.code}

            # Transform and store data for each unit found in the API response
            for api_unit in taipower_response.generation_units:
                unit_name = api_unit.unit_name

                # Find matching generation unit by code
                generation_unit = unit_code_map.get(unit_name)

                if not generation_unit:
                    # This unit is not in our selected windfarms, skip it
                    continue

                # Store this unit's data
                records_stored, records_updated = await self._store_taipower_record(
                    api_unit,
                    taipower_response.datetime,
                    generation_unit,
                    user_id,
                    metadata,
                )

                total_records_stored += records_stored
                total_records_updated += records_updated

                all_generation_units.append(
                    GenerationUnitSummary(
                        id=generation_unit.id,
                        code=generation_unit.code,
                        name=generation_unit.name,
                        records_stored=records_stored,
                        records_updated=records_updated,
                    )
                )

        except Exception as e:
            error_msg = f"Error fetching TAIPOWER data: {str(e)}"
            logger.error(error_msg)
            all_errors.append(error_msg)

        # Calculate response time
        end_time = datetime.now()
        response_time_seconds = (end_time - start_time).total_seconds()

        return RawDataFetchResponse(
            success=len(all_errors) == 0,
            source=source,
            windfarm_ids=request.windfarm_ids,
            windfarm_names=[w.name for w in windfarms],
            date_range={
                "start": request.start_date.isoformat(),
                "end": request.end_date.isoformat(),
            },
            records_stored=total_records_stored,
            records_updated=total_records_updated,
            generation_units_processed=all_generation_units,
            summary={
                "total_api_calls": total_api_calls,
                "api_response_time_seconds": round(response_time_seconds, 2),
                "note": "TAIPOWER returns live data for all units in one call",
            },
            errors=all_errors,
        )

    async def _store_taipower_record(
        self,
        api_unit: Any,  # TaipowerGenerationUnit
        timestamp: datetime,
        unit: GenerationUnit,
        user_id: int,
        api_metadata: Dict,
    ) -> Tuple[int, int]:
        """Store TAIPOWER record in generation_data_raw."""
        records_stored = 0
        records_updated = 0

        # Convert api_metadata to JSON-serializable format
        serializable_metadata = self._make_json_serializable(api_metadata)

        # Ensure timezone-aware
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)

        # TAIPOWER is live/current data, so period is the timestamp itself
        # We can treat it as a 10-minute snapshot
        period_start = timestamp
        period_end = timestamp + timedelta(minutes=10)

        # Extract values
        generation_mw = float(api_unit.net_generation_mw) if api_unit.net_generation_mw else 0
        capacity_mw = float(api_unit.installed_capacity_mw) if api_unit.installed_capacity_mw else None
        capacity_factor = float(api_unit.capacity_utilization_percent) if api_unit.capacity_utilization_percent else None

        # Build data JSONB
        data = {
            "unit_name": api_unit.unit_name,
            "generation_type": api_unit.generation_type,
            "installed_capacity_mw": capacity_mw,
            "net_generation_mw": generation_mw,
            "capacity_utilization_percent": capacity_factor,
            "notes": api_unit.notes if hasattr(api_unit, 'notes') else None,
            "fetch_metadata": {
                "fetched_by_user_id": user_id,
                "fetch_timestamp": datetime.now(timezone.utc).isoformat(),
                "fetch_method": "api",
                "api_metadata": serializable_metadata,
            },
        }

        # Check if record exists (by timestamp)
        stmt = select(GenerationDataRaw).where(
            and_(
                GenerationDataRaw.source == "TAIPOWER",
                GenerationDataRaw.identifier == unit.code,
                GenerationDataRaw.period_start == period_start,
            )
        )
        result = await self.db.execute(stmt)
        existing_record = result.scalar_one_or_none()

        if existing_record:
            # Update existing record
            existing_record.value_extracted = Decimal(str(generation_mw))
            existing_record.data = data
            existing_record.updated_at = datetime.now(timezone.utc)
            records_updated += 1
        else:
            # Create new record
            new_record = GenerationDataRaw(
                source="TAIPOWER",
                source_type="api",
                identifier=unit.code,
                period_start=period_start,
                period_end=period_end,
                period_type="PT10M",  # 10-minute data
                value_extracted=Decimal(str(generation_mw)),
                unit="MW",
                data=data,
            )
            self.db.add(new_record)
            records_stored += 1

        await self.db.commit()
        return records_stored, records_updated

    async def fetch_and_store_nve(
        self,
        request: RawDataFetchRequest,
        user_id: int,
    ) -> RawDataFetchResponse:
        """Fetch NVE data and store in generation_data_raw."""
        return RawDataFetchResponse(
            success=False,
            source="NVE",
            windfarm_ids=request.windfarm_ids,
            windfarm_names=[],
            date_range={
                "start": request.start_date.isoformat(),
                "end": request.end_date.isoformat(),
            },
            records_stored=0,
            records_updated=0,
            generation_units_processed=[],
            errors=[
                "NVE does not provide a public API for historical data.",
                "Data must be imported from Excel files provided by NVE.",
                "Use the Excel import functionality instead."
            ],
        )

    async def fetch_and_store_energistyrelsen(
        self,
        request: RawDataFetchRequest,
        user_id: int,
    ) -> RawDataFetchResponse:
        """Fetch ENERGISTYRELSEN data and store in generation_data_raw."""
        return RawDataFetchResponse(
            success=False,
            source="ENERGISTYRELSEN",
            windfarm_ids=request.windfarm_ids,
            windfarm_names=[],
            date_range={
                "start": request.start_date.isoformat(),
                "end": request.end_date.isoformat(),
            },
            records_stored=0,
            records_updated=0,
            generation_units_processed=[],
            errors=[
                "ENERGISTYRELSEN (Danish Energy Agency) does not provide a public API.",
                "Data must be imported from Excel files provided by the agency.",
                "Use the Excel import functionality instead."
            ],
        )

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

    async def _get_windfarms_with_units(
        self,
        windfarm_ids: List[int],
        source: str,
    ) -> List[Windfarm]:
        """Get windfarms with their generation units filtered by source."""
        from sqlalchemy.orm import selectinload

        stmt = (
            select(Windfarm)
            .options(selectinload(Windfarm.generation_units))
            .where(Windfarm.id.in_(windfarm_ids))
        )
        result = await self.db.execute(stmt)
        windfarms = result.scalars().all()

        # Filter to only windfarms that have units for this source
        filtered_windfarms = []
        for windfarm in windfarms:
            has_source_units = any(u.source == source for u in windfarm.generation_units)
            if has_source_units:
                filtered_windfarms.append(windfarm)

        return filtered_windfarms
