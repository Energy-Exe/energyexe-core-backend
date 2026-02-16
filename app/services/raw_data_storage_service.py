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


def _detect_entsoe_resolution(df: pd.DataFrame) -> str:
    """Detect resolution from timestamp spacing (entsoe-py doesn't provide it as a column).

    Analyzes the minimum time delta between consecutive timestamps to determine
    whether data is PT15M, PT30M, or PT60M.
    """
    if df is None or df.empty or len(df) < 2:
        return "PT60M"
    timestamps = pd.to_datetime(
        df.get("timestamp", df.index) if not isinstance(df.index, pd.RangeIndex) else df.get("timestamp", df.index)
    )
    diffs = timestamps.diff().dropna()
    if diffs.empty:
        return "PT60M"
    min_delta = diffs.min().total_seconds()
    if min_delta <= 900:
        return "PT15M"
    elif min_delta <= 1800:
        return "PT30M"
    return "PT60M"


class RawDataStorageService:
    """Service for fetching from external APIs and storing in generation_data_raw."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def fetch_and_store_all_sources(
        self,
        windfarm_ids: Optional[List[int]],
        start_date: datetime,
        end_date: datetime,
        user_id: int,
        source_filter: Optional[str] = None,
        process_to_hourly: bool = False,
    ) -> Dict[str, Any]:
        """
        Fetch data from all available sources for the given windfarms.

        Auto-detects which sources have generation units configured for the windfarms
        and fetches from each source automatically.

        Args:
            windfarm_ids: Optional list of windfarm IDs. If None and source_filter is provided,
                         fetches all windfarms for that source.
            start_date: Start date for fetch
            end_date: End date for fetch
            user_id: User triggering the fetch
            source_filter: Optional source name (e.g., 'ENTSOE'). If provided without windfarm_ids,
                          fetches all windfarms for this source.
            process_to_hourly: If True, aggregate raw data to hourly resolution after fetching.
        """
        from sqlalchemy.orm import selectinload

        # If source_filter is provided and no specific windfarm_ids, get all windfarms for that source
        if source_filter and not windfarm_ids:
            logger.info(f"Fetching all windfarms for source: {source_filter}")

            # Get all windfarms that have generation units for this source
            stmt = (
                select(Windfarm)
                .options(selectinload(Windfarm.generation_units))
                .join(GenerationUnit, GenerationUnit.windfarm_id == Windfarm.id)
                .where(GenerationUnit.source == source_filter)
                .distinct()
            )
            result = await self.db.execute(stmt)
            windfarms = result.scalars().all()

            if windfarms:
                windfarm_ids = [w.id for w in windfarms]
                logger.info(f"Found {len(windfarm_ids)} windfarms for {source_filter}")
            else:
                logger.warning(f"No windfarms found for source {source_filter}")

        # Get all windfarms with their generation units
        if not windfarm_ids:
            from app.schemas.raw_data_fetch import UnifiedRawDataFetchResponse
            return UnifiedRawDataFetchResponse(
                success=False,
                windfarm_ids=[],
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
                errors=["No windfarms found for the specified criteria"],
            )

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
                # If source_filter is specified, only include that source
                if source_filter and source != source_filter:
                    continue
                if source not in sources_map:
                    sources_map[source] = set()
                sources_map[source].add(windfarm.id)

        # Log what sources were detected
        logger.info(f"Detected sources: {list(sources_map.keys())}")
        if source_filter:
            logger.info(f"Filtered to source: {source_filter}")

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

        # Process to hourly if requested
        aggregation_results = None
        if process_to_hourly and total_stored > 0:
            from app.services.unified_generation_service import UnifiedGenerationService
            from app.schemas.raw_data_fetch import AggregationResult

            aggregation_results = []
            generation_service = UnifiedGenerationService(self.db)

            for source in sources_map.keys():
                try:
                    logger.info(f"Aggregating {source} data to hourly resolution")
                    agg_result = await generation_service.process_to_hourly(
                        source=source,
                        start_date=start_date,
                        end_date=end_date,
                    )

                    aggregation_results.append(AggregationResult(
                        success=agg_result.get('success', False),
                        source=source,
                        raw_records_processed=agg_result.get('raw_records_processed', 0),
                        hourly_records_created=agg_result.get('hourly_records_created', 0),
                        errors=[],
                    ))
                    logger.info(
                        f"Aggregated {source}: {agg_result.get('raw_records_processed', 0)} raw -> "
                        f"{agg_result.get('hourly_records_created', 0)} hourly records"
                    )

                except Exception as e:
                    error_msg = f"Aggregation error for {source}: {str(e)}"
                    logger.error(error_msg)
                    aggregation_results.append(AggregationResult(
                        success=False,
                        source=source,
                        raw_records_processed=0,
                        hourly_records_created=0,
                        errors=[str(e)],
                    ))
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
            aggregation_results=aggregation_results,
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

        # OPTIMIZATION: Group windfarms by bidding zone
        # This allows us to make ONE API call per zone instead of one per windfarm
        from app.models.bidzone import Bidzone

        bidzone_groups = {}  # {bidzone_code: {windfarms: [...], units: [...], eic_codes: [...]}}

        for windfarm in windfarms:
            # Get ENTSOE units for this windfarm
            entsoe_units = [u for u in windfarm.generation_units if u.source == "ENTSOE"]

            if not entsoe_units:
                continue

            # Get bidzone
            if not windfarm.bidzone_id:
                all_errors.append(f"Windfarm {windfarm.name} has no bidzone configured")
                continue

            stmt = select(Bidzone).where(Bidzone.id == windfarm.bidzone_id)
            result = await self.db.execute(stmt)
            bidzone = result.scalar_one_or_none()

            if not bidzone or not bidzone.code:
                all_errors.append(f"Windfarm {windfarm.name} has no bidzone configured")
                continue

            bidzone_code = bidzone.code

            # Initialize bidzone group if needed
            if bidzone_code not in bidzone_groups:
                bidzone_groups[bidzone_code] = {
                    'bidzone_name': bidzone.name,
                    'windfarms': [],
                    'units': [],
                    'eic_codes': []
                }

            # Add windfarm and its units to the bidzone group
            bidzone_groups[bidzone_code]['windfarms'].append(windfarm)
            bidzone_groups[bidzone_code]['units'].extend(entsoe_units)
            bidzone_groups[bidzone_code]['eic_codes'].extend([
                u.code for u in entsoe_units if u.code and u.code != 'nan'
            ])

        logger.info(f"Grouped {len(windfarms)} windfarms into {len(bidzone_groups)} bidding zones")
        for zone_code, zone_data in bidzone_groups.items():
            logger.info(f"  {zone_data['bidzone_name']} ({zone_code}): {len(zone_data['windfarms'])} windfarms, {len(zone_data['units'])} units")

        # Convert dates to naive UTC for ENTSOE client
        start_naive = request.start_date.replace(tzinfo=None) if request.start_date.tzinfo else request.start_date
        end_naive = request.end_date.replace(tzinfo=None) if request.end_date.tzinfo else request.end_date

        # Process each bidding zone (ONE API call per zone!)
        for bidzone_code, zone_data in bidzone_groups.items():
            try:
                logger.info(
                    f"Fetching ENTSOE data for {zone_data['bidzone_name']} "
                    f"({len(zone_data['windfarms'])} windfarms, {len(zone_data['units'])} units)"
                )

                # Make ONE API call for the entire bidding zone
                # The API returns ALL wind units in the zone, we filter locally
                df, metadata = await client.fetch_generation_per_unit(
                    start=start_naive,
                    end=end_naive,
                    area_code=bidzone_code,
                    eic_codes=zone_data['eic_codes'],  # Filter for our EIC codes
                    production_types=["wind"],
                )

                total_api_calls += 1

                if df.empty:
                    logger.warning(
                        f"No data returned for bidzone {zone_data['bidzone_name']}",
                        bidzone_code=bidzone_code,
                        start=start_naive.isoformat(),
                        end=end_naive.isoformat(),
                        eic_codes=zone_data['eic_codes'],
                        metadata_errors=metadata.get("errors", []),
                    )
                    if metadata.get("errors"):
                        all_errors.extend([
                            f"Bidzone {zone_data['bidzone_name']}: {err}"
                            for err in metadata["errors"]
                            if isinstance(err, str)
                        ])
                    continue

                logger.info(f"Received {len(df)} records for {zone_data['bidzone_name']}")

                # Process each unit in this zone
                for unit in zone_data['units']:
                    # Filter dataframe for this unit's EIC code
                    unit_df = df[df.get('eic_code', df.get('unit_code', '')) == unit.code]

                    if unit_df.empty:
                        logger.debug(f"No data for unit {unit.code} in bidzone response")
                        continue

                    # Split into generation and consumption subsets
                    if 'data_direction' in unit_df.columns:
                        gen_df = unit_df[unit_df['data_direction'] != 'consumption']
                        consumption_df = unit_df[unit_df['data_direction'] == 'consumption']
                    else:
                        gen_df = unit_df
                        consumption_df = pd.DataFrame()

                    unit_stored = 0
                    unit_updated = 0

                    # Store generation records (source_type='api')
                    if not gen_df.empty:
                        rs, ru = await self._store_entsoe_records(
                            gen_df, unit, bidzone_code, user_id, metadata,
                        )
                        unit_stored += rs
                        unit_updated += ru

                    # Store consumption records (source_type='api_consumption')
                    if not consumption_df.empty:
                        rs, ru = await self._store_entsoe_records(
                            consumption_df, unit, bidzone_code, user_id, metadata,
                            source_type_override='api_consumption',
                        )
                        unit_stored += rs
                        unit_updated += ru
                        logger.info(f"Stored {rs} consumption records for unit {unit.code}")

                    total_records_stored += unit_stored
                    total_records_updated += unit_updated

                    all_generation_units.append(
                        GenerationUnitSummary(
                            id=unit.id,
                            code=unit.code,
                            name=unit.name,
                            records_stored=unit_stored,
                            records_updated=unit_updated,
                        )
                    )

            except Exception as e:
                error_msg = f"Error fetching ENTSOE data for bidzone {zone_data['bidzone_name']}: {str(e)}"
                logger.error(error_msg)
                all_errors.append(error_msg)

        # Post-import completeness check
        total_days = max(1, (request.end_date - request.start_date).days)
        for unit_summary in all_generation_units:
            if unit_summary.records_stored == 0:
                logger.warning(
                    f"Completeness: unit {unit_summary.code} ({unit_summary.name}) "
                    f"stored 0 records for {total_days}-day period"
                )
            else:
                # For hourly data expect ~24 * days; for PT15M expect ~96 * days
                expected_min = total_days * 20  # conservative lower bound
                if unit_summary.records_stored < expected_min:
                    logger.warning(
                        f"Completeness: unit {unit_summary.code} ({unit_summary.name}) "
                        f"stored only {unit_summary.records_stored} records "
                        f"(expected >={expected_min} for {total_days} days)"
                    )

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
        source_type_override: str = "api",
    ) -> Tuple[int, int]:
        """Store ENTSOE records in generation_data_raw using bulk upsert.

        Records are inserted in batches to avoid PostgreSQL's parameter limit (65,535).

        Args:
            source_type_override: Use 'api' for generation, 'api_consumption' for consumption.
        """
        from sqlalchemy.dialects.postgresql import insert
        from decimal import Decimal

        if df.empty:
            return 0, 0

        # Batch size to avoid PostgreSQL parameter limit
        # Each record has ~10 columns, so 1000 records = ~10,000 parameters (well under 65,535)
        BATCH_SIZE = 1000

        # Convert api_metadata to JSON-serializable format
        serializable_metadata = self._make_json_serializable(api_metadata)

        # Detect resolution from timestamp spacing (entsoe-py doesn't provide resolution_code column)
        detected_resolution = _detect_entsoe_resolution(df)
        logger.info(f"Detected ENTSOE resolution for unit {unit.code}: {detected_resolution}")

        # Prepare all records for bulk insert
        records_to_insert = []

        for idx, row in df.iterrows():
            # Extract timestamp
            timestamp = row.get("timestamp", idx)
            if not isinstance(timestamp, datetime):
                timestamp = pd.to_datetime(timestamp)

            # Ensure timezone-aware
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)

            # Use detected resolution (not row-level, since entsoe-py doesn't provide it)
            resolution = detected_resolution
            if resolution == "PT15M":
                period_end = timestamp + timedelta(minutes=15)
                period_type = "PT15M"
            elif resolution == "PT30M":
                period_end = timestamp + timedelta(minutes=30)
                period_type = "PT30M"
            else:
                period_end = timestamp + timedelta(hours=1)
                period_type = "PT60M"

            # Extract value with precision tracking
            raw_value = row.get("value", 0)
            value = float(raw_value)

            # Skip records with NaN values (ENTSOE reports NaN when unit isn't
            # generating/consuming in that period — not useful data)
            if pd.isna(value):
                continue

            # Build data JSONB
            data_direction = row.get("data_direction", "generation")
            data = {
                "source_value_type": type(raw_value).__name__,
                "source_value_raw": str(raw_value),
                "eic_code": unit.code,
                "area_code": bidzone_code,
                "production_type": row.get("production_type", "wind"),
                "resolution_code": resolution,
                "data_direction": data_direction,
                "installed_capacity_mw": float(row["installed_capacity_mw"]) if "installed_capacity_mw" in row and pd.notna(row["installed_capacity_mw"]) else None,
                "fetch_metadata": {
                    "fetched_by_user_id": user_id,
                    "fetch_timestamp": datetime.now(timezone.utc).isoformat(),
                    "fetch_method": "api",
                    "api_metadata": serializable_metadata,
                },
            }

            # Outlier detection: flag values exceeding unit capacity or absolute limits
            if unit.capacity_mw and value > float(unit.capacity_mw) * 1.1:
                data["outlier_flag"] = True
                data["outlier_ratio"] = round(value / float(unit.capacity_mw), 4)
            if value > 10000:  # >10 GW impossible for single unit
                data["outlier_flag"] = True
                data["outlier_severity"] = "critical"

            # Add to bulk insert list
            records_to_insert.append({
                "source": "ENTSOE",
                "source_type": source_type_override,
                "identifier": unit.code,
                "period_start": timestamp,
                "period_end": period_end,
                "period_type": period_type,
                "value_extracted": Decimal(str(value)),
                "unit": "MW",
                "data": data,
                "created_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc),
            })

        if not records_to_insert:
            return 0, 0

        # Use PostgreSQL bulk upsert in batches to avoid parameter limit
        total_records = len(records_to_insert)
        records_stored = 0

        try:
            for i in range(0, total_records, BATCH_SIZE):
                batch = records_to_insert[i:i + BATCH_SIZE]

                stmt = insert(GenerationDataRaw).values(batch)

                # Use upsert to handle existing records
                # On conflict: store previous value in JSONB for revision tracking
                from sqlalchemy import func
                stmt = stmt.on_conflict_do_update(
                    index_elements=['source', 'source_type', 'identifier', 'period_start'],
                    set_={
                        'value_extracted': stmt.excluded.value_extracted,
                        'data': func.jsonb_set(
                            stmt.excluded.data,
                            '{previous_value}',
                            func.to_jsonb(GenerationDataRaw.value_extracted),
                        ),
                        'updated_at': datetime.now(timezone.utc),
                        'period_end': stmt.excluded.period_end,
                        'period_type': stmt.excluded.period_type,
                        'unit': stmt.excluded.unit,
                    }
                )

                await self.db.execute(stmt)
                records_stored += len(batch)

                logger.debug(f"Batch {i // BATCH_SIZE + 1}: upserted {len(batch)} records for unit {unit.code}")

            await self.db.commit()
            logger.info(f"Bulk upserted {total_records} records for unit {unit.code} in {(total_records + BATCH_SIZE - 1) // BATCH_SIZE} batches")

            return records_stored, 0

        except Exception as e:
            logger.error(f"Error storing records for unit {unit.code}: {str(e)}")
            await self.db.rollback()
            return 0, 0

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
        """Store ELEXON records in generation_data_raw using bulk upsert.

        Records are inserted in batches to avoid PostgreSQL's parameter limit (65,535).
        """
        from sqlalchemy.dialects.postgresql import insert
        from decimal import Decimal

        if df.empty:
            return 0, 0

        # Batch size to avoid PostgreSQL parameter limit
        # Each record has ~10 columns, so 1000 records = ~10,000 parameters (well under 65,535)
        BATCH_SIZE = 1000

        # Convert api_metadata to JSON-serializable format
        serializable_metadata = self._make_json_serializable(api_metadata)

        # Prepare all records for bulk insert
        records_to_insert = []

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
            elif pd.isna(settlement_date):
                settlement_date = None

            # Safely extract values, handling NaN/None properly
            level_from = None
            if "levelFrom" in row and pd.notna(row["levelFrom"]):
                level_from = float(row["levelFrom"])

            level_to = None
            if "levelTo" in row and pd.notna(row["levelTo"]):
                level_to = float(row["levelTo"])

            settlement_period = None
            if "settlementPeriod" in row and pd.notna(row["settlementPeriod"]):
                settlement_period = int(row["settlementPeriod"])

            data = {
                "bm_unit": unit.code,
                "level_from": level_from,
                "level_to": level_to,
                "settlement_period": settlement_period,
                "settlement_date": settlement_date,
                "fetch_metadata": {
                    "fetched_by_user_id": user_id,
                    "fetch_timestamp": datetime.now(timezone.utc).isoformat(),
                    "fetch_method": "api",
                    "api_metadata": serializable_metadata,
                },
            }

            # Add to bulk insert list
            records_to_insert.append({
                "source": "ELEXON",
                "source_type": "api",
                "identifier": unit.code,
                "period_start": timestamp,
                "period_end": period_end,
                "period_type": period_type,
                "value_extracted": Decimal(str(value)),
                "unit": "MWh",
                "data": data,
                "created_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc),
            })

        if not records_to_insert:
            return 0, 0

        # Use PostgreSQL bulk upsert in batches to avoid parameter limit
        total_records = len(records_to_insert)
        records_stored = 0

        try:
            for i in range(0, total_records, BATCH_SIZE):
                batch = records_to_insert[i:i + BATCH_SIZE]

                stmt = insert(GenerationDataRaw).values(batch)

                # Use upsert to handle existing records
                # On conflict: store previous value in JSONB for revision tracking
                from sqlalchemy import func
                stmt = stmt.on_conflict_do_update(
                    index_elements=['source', 'source_type', 'identifier', 'period_start'],
                    set_={
                        'value_extracted': stmt.excluded.value_extracted,
                        'data': func.jsonb_set(
                            stmt.excluded.data,
                            '{previous_value}',
                            func.to_jsonb(GenerationDataRaw.value_extracted),
                        ),
                        'updated_at': datetime.now(timezone.utc),
                        'period_end': stmt.excluded.period_end,
                        'period_type': stmt.excluded.period_type,
                        'unit': stmt.excluded.unit,
                    }
                )

                await self.db.execute(stmt)
                records_stored += len(batch)

                logger.debug(f"Batch {i // BATCH_SIZE + 1}: upserted {len(batch)} records for unit {unit.code}")

            await self.db.commit()
            logger.info(f"Bulk upserted {total_records} records for unit {unit.code} in {(total_records + BATCH_SIZE - 1) // BATCH_SIZE} batches")

            return records_stored, 0

        except Exception as e:
            logger.error(f"Error storing records for unit {unit.code}: {str(e)}")
            await self.db.rollback()
            return 0, 0

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
