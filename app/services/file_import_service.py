"""Service for importing raw generation data from uploaded Excel files."""

import asyncio
import json
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import AsyncGenerator, Callable, Dict, List, Optional
from decimal import Decimal

import pandas as pd
import structlog
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.generation_data import GenerationDataRaw
from app.models.generation_unit import GenerationUnit
from app.models.turbine_unit import TurbineUnit
from app.schemas.raw_data_fetch import (
    FileUploadProgressUpdate,
    FileUploadResponse,
    GenerationUnitSummary,
)

logger = structlog.get_logger()


class FileImportService:
    """Service for importing NVE and Energistyrelsen data from Excel files."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def import_nve_file(
        self,
        file_content: bytes,
        filename: str,
        start_date: datetime,
        end_date: datetime,
        clean_first: bool = True,
        progress_callback: Optional[Callable[[FileUploadProgressUpdate], None]] = None,
    ) -> FileUploadResponse:
        """Import NVE data from uploaded Excel file with date range filtering."""
        start_time = datetime.now(timezone.utc)
        errors = []
        warnings = []

        try:
            # Send progress: Validating
            if progress_callback:
                await progress_callback(
                    FileUploadProgressUpdate(
                        status="validating",
                        message="Validating NVE file structure...",
                        progress_percent=5,
                    )
                )

            # Save to temp file
            with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp_file:
                tmp_file.write(file_content)
                tmp_path = Path(tmp_file.name)

            try:
                # Read Excel file
                df = pd.read_excel(tmp_path)

                # Validate structure
                if len(df) < 7:
                    raise ValueError("Invalid NVE file: Too few rows")

                if progress_callback:
                    await progress_callback(
                        FileUploadProgressUpdate(
                            status="validating",
                            message=f"File validated: {len(df):,} rows, {len(df.columns)} columns",
                            progress_percent=10,
                        )
                    )

                # Get NVE unit mapping
                unit_mapping = await self._get_nve_unit_mapping()

                if not unit_mapping:
                    raise ValueError("No NVE generation units found in database")

                # Clear existing data if requested
                if clean_first:
                    if progress_callback:
                        await progress_callback(
                            FileUploadProgressUpdate(
                                status="processing",
                                message="Clearing existing NVE data...",
                                progress_percent=15,
                            )
                        )
                    await self.db.execute(
                        text("DELETE FROM generation_data_raw WHERE source = 'NVE'")
                    )
                    await self.db.commit()

                # Process data with date filtering
                if progress_callback:
                    await progress_callback(
                        FileUploadProgressUpdate(
                            status="processing",
                            message="Processing NVE data...",
                            progress_percent=20,
                        )
                    )

                records = await self._process_nve_data(
                    df, unit_mapping, start_date, end_date, progress_callback
                )

                # Insert records
                if progress_callback:
                    await progress_callback(
                        FileUploadProgressUpdate(
                            status="inserting",
                            message=f"Inserting {len(records):,} records...",
                            progress_percent=80,
                        )
                    )

                records_stored, records_updated, units_summary = await self._insert_records(
                    records, progress_callback
                )

                # Calculate actual date range from processed data
                actual_min_date = min(r["period_start"] for r in records) if records else start_date
                actual_max_date = max(r["period_start"] for r in records) if records else end_date

                end_time = datetime.now(timezone.utc)
                duration = (end_time - start_time).total_seconds()

                if progress_callback:
                    await progress_callback(
                        FileUploadProgressUpdate(
                            status="completed",
                            message=f"Completed: {len(records):,} records processed",
                            progress_percent=100,
                        )
                    )

                return FileUploadResponse(
                    success=True,
                    source="NVE",
                    file_info={
                        "filename": filename,
                        "size_bytes": len(file_content),
                        "rows": len(df),
                        "columns": len(df.columns),
                    },
                    date_range_requested={
                        "start": start_date.isoformat(),
                        "end": end_date.isoformat(),
                    },
                    date_range_processed={
                        "start": actual_min_date.isoformat() if isinstance(actual_min_date, datetime) else actual_min_date,
                        "end": actual_max_date.isoformat() if isinstance(actual_max_date, datetime) else actual_max_date,
                    },
                    records_stored=records_stored,
                    records_updated=records_updated,
                    generation_units_processed=units_summary,
                    summary={
                        "duration_seconds": duration,
                        "processing_rate": len(records) / duration if duration > 0 else 0,
                        "total_records_processed": len(records),
                    },
                    errors=errors,
                    warnings=warnings,
                )

            finally:
                # Clean up temp file
                tmp_path.unlink(missing_ok=True)

        except Exception as e:
            logger.error(f"Error importing NVE file: {str(e)}")
            if progress_callback:
                await progress_callback(
                    FileUploadProgressUpdate(
                        status="error",
                        message=f"Error: {str(e)}",
                        progress_percent=0,
                    )
                )
            raise

    async def import_energistyrelsen_file(
        self,
        file_content: bytes,
        filename: str,
        start_date: datetime,
        end_date: datetime,
        clean_first: bool = True,
        progress_callback: Optional[Callable[[FileUploadProgressUpdate], None]] = None,
    ) -> FileUploadResponse:
        """Import Energistyrelsen data from uploaded Excel file with date range filtering."""
        start_time = datetime.now(timezone.utc)
        errors = []
        warnings = []

        try:
            # Send progress: Validating
            if progress_callback:
                await progress_callback(
                    FileUploadProgressUpdate(
                        status="validating",
                        message="Validating Energistyrelsen file structure...",
                        progress_percent=5,
                    )
                )

            # Save to temp file
            with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp_file:
                tmp_file.write(file_content)
                tmp_path = Path(tmp_file.name)

            try:
                # Read Excel file
                df = pd.read_excel(tmp_path, sheet_name="kWh")

                # Validate structure
                if len(df) < 7:
                    raise ValueError("Invalid Energistyrelsen file: Too few rows")

                if progress_callback:
                    await progress_callback(
                        FileUploadProgressUpdate(
                            status="validating",
                            message=f"File validated: {len(df):,} rows, {len(df.columns)} columns",
                            progress_percent=10,
                        )
                    )

                # Get turbine mapping
                turbine_mapping = await self._get_energistyrelsen_turbine_mapping()

                if not turbine_mapping:
                    raise ValueError("No Energistyrelsen turbine units found in database")

                # Clear existing data if requested
                if clean_first:
                    if progress_callback:
                        await progress_callback(
                            FileUploadProgressUpdate(
                                status="processing",
                                message="Clearing existing Energistyrelsen data...",
                                progress_percent=15,
                            )
                        )
                    await self.db.execute(
                        text("DELETE FROM generation_data_raw WHERE source = 'ENERGISTYRELSEN'")
                    )
                    await self.db.commit()

                # Process data with date filtering
                if progress_callback:
                    await progress_callback(
                        FileUploadProgressUpdate(
                            status="processing",
                            message="Processing Energistyrelsen data...",
                            progress_percent=20,
                        )
                    )

                records = await self._process_energistyrelsen_data(
                    df, turbine_mapping, start_date, end_date, progress_callback
                )

                # Insert records
                if progress_callback:
                    await progress_callback(
                        FileUploadProgressUpdate(
                            status="inserting",
                            message=f"Inserting {len(records):,} records...",
                            progress_percent=80,
                        )
                    )

                records_stored, records_updated, units_summary = await self._insert_records(
                    records, progress_callback
                )

                # Calculate actual date range from processed data
                actual_min_date = min(r["period_start"] for r in records) if records else start_date
                actual_max_date = max(r["period_start"] for r in records) if records else end_date

                end_time = datetime.now(timezone.utc)
                duration = (end_time - start_time).total_seconds()

                if progress_callback:
                    await progress_callback(
                        FileUploadProgressUpdate(
                            status="completed",
                            message=f"Completed: {len(records):,} records processed",
                            progress_percent=100,
                        )
                    )

                return FileUploadResponse(
                    success=True,
                    source="ENERGISTYRELSEN",
                    file_info={
                        "filename": filename,
                        "size_bytes": len(file_content),
                        "rows": len(df),
                        "columns": len(df.columns),
                    },
                    date_range_requested={
                        "start": start_date.isoformat(),
                        "end": end_date.isoformat(),
                    },
                    date_range_processed={
                        "start": actual_min_date.isoformat() if isinstance(actual_min_date, datetime) else actual_min_date,
                        "end": actual_max_date.isoformat() if isinstance(actual_max_date, datetime) else actual_max_date,
                    },
                    records_stored=records_stored,
                    records_updated=records_updated,
                    generation_units_processed=units_summary,
                    summary={
                        "duration_seconds": duration,
                        "processing_rate": len(records) / duration if duration > 0 else 0,
                        "total_records_processed": len(records),
                    },
                    errors=errors,
                    warnings=warnings,
                )

            finally:
                # Clean up temp file
                tmp_path.unlink(missing_ok=True)

        except Exception as e:
            logger.error(f"Error importing Energistyrelsen file: {str(e)}")
            if progress_callback:
                await progress_callback(
                    FileUploadProgressUpdate(
                        status="error",
                        message=f"Error: {str(e)}",
                        progress_percent=0,
                    )
                )
            raise

    async def _get_nve_unit_mapping(self) -> Dict[str, List]:
        """Get mapping of NVE generation units grouped by code."""
        result = await self.db.execute(
            select(GenerationUnit)
            .where(GenerationUnit.source == "NVE")
            .order_by(GenerationUnit.code, GenerationUnit.start_date)
        )
        units = result.scalars().all()

        # Group by code (multiple phases can have same code)
        units_by_code = {}
        for unit in units:
            if unit.code not in units_by_code:
                units_by_code[unit.code] = []
            units_by_code[unit.code].append(unit)

        logger.info(f"Found {len(units)} NVE units across {len(units_by_code)} unique codes")
        return units_by_code

    async def _get_energistyrelsen_turbine_mapping(self) -> Dict[str, any]:
        """Get mapping of Energistyrelsen turbine units."""
        result = await self.db.execute(select(TurbineUnit))
        turbines = result.scalars().all()

        turbines_by_gsrn = {str(turbine.code): turbine for turbine in turbines}
        logger.info(f"Found {len(turbines)} turbine units in database")

        return {
            "by_code": turbines_by_gsrn,
            "turbines": {turbine.id: turbine for turbine in turbines},
        }

    def _find_operational_unit(self, units_list: List, timestamp: datetime):
        """Find which phase/unit was operational at the given timestamp."""
        check_date = timestamp.date() if isinstance(timestamp, datetime) else timestamp

        for unit in units_list:
            if unit.start_date and check_date < unit.start_date:
                continue
            if unit.end_date and check_date > unit.end_date:
                continue
            return unit

        return None

    async def _process_nve_data(
        self,
        df: pd.DataFrame,
        unit_mapping: Dict,
        start_date: datetime,
        end_date: datetime,
        progress_callback: Optional[Callable] = None,
    ) -> List[Dict]:
        """Process NVE data from DataFrame with date filtering."""
        records = []

        # Create column-to-code mapping from first row
        first_row = df.iloc[0] if len(df) > 0 else None
        if not first_row:
            return records

        column_to_code = {}
        for col in df.columns[1:]:  # Skip first column (timestamp)
            code_value = first_row[col]
            if pd.notna(code_value):
                code_str = str(int(code_value)) if isinstance(code_value, (int, float)) else str(code_value)
                if code_str in unit_mapping:
                    column_to_code[col] = code_str

        logger.info(f"Mapped {len(column_to_code)} columns to NVE codes")

        # Process data rows (skip header rows 0-1)
        total_rows = len(df) - 2
        processed_rows = 0

        for idx in range(2, len(df)):
            row = df.iloc[idx]
            timestamp_value = row.iloc[0]

            if pd.isna(timestamp_value):
                continue

            try:
                # Parse timestamp
                if isinstance(timestamp_value, str):
                    timestamp = pd.to_datetime(timestamp_value)
                else:
                    timestamp = pd.to_datetime(timestamp_value)

                # Localize to Europe/Oslo then convert to UTC
                # Use ambiguous="infer" for fall-back DST (when 02:00-02:59 occurs twice)
                # Use nonexistent="shift_forward" for spring-forward DST (when 02:00-02:59 doesn't exist)
                if timestamp.tzinfo is None:
                    timestamp = timestamp.tz_localize(
                        "Europe/Oslo",
                        ambiguous="infer",
                        nonexistent="shift_forward"
                    ).tz_convert("UTC")
                elif timestamp.tzinfo != pd.Timestamp.now(tz="UTC").tzinfo:
                    timestamp = timestamp.tz_convert("UTC")

                # Filter by date range
                if timestamp < start_date or timestamp > end_date:
                    continue

                # Process each wind farm column
                for col, code in column_to_code.items():
                    value = row[col]

                    if pd.isna(value):
                        continue

                    units_list = unit_mapping.get(code, [])
                    if not units_list:
                        continue

                    operational_unit = self._find_operational_unit(units_list, timestamp)
                    if not operational_unit:
                        continue

                    record = {
                        "period_start": timestamp.isoformat(),
                        "period_end": (timestamp + pd.Timedelta(hours=1)).isoformat(),
                        "period_type": "hour",
                        "source": "NVE",
                        "source_type": "file",
                        "identifier": code,
                        "value_extracted": float(value),
                        "unit": "MWh",
                        "data": {
                            "generation_mwh": float(value),
                            "unit_code": code,
                            "unit_name": operational_unit.name,
                            "generation_unit_id": operational_unit.id,
                            "windfarm_id": operational_unit.windfarm_id,
                            "timestamp": timestamp.isoformat(),
                        },
                    }
                    records.append(record)

                processed_rows += 1
                if progress_callback and processed_rows % 1000 == 0:
                    progress_percent = 20 + int((processed_rows / total_rows) * 60)
                    await progress_callback(
                        FileUploadProgressUpdate(
                            status="processing",
                            message=f"Processing rows... {processed_rows:,}/{total_rows:,}",
                            progress_percent=progress_percent,
                            records_processed=len(records),
                        )
                    )

            except Exception as e:
                logger.debug(f"Error processing row {idx}: {e}")
                continue

        logger.info(f"Processed {len(records):,} NVE records")
        return records

    async def _process_energistyrelsen_data(
        self,
        df: pd.DataFrame,
        turbine_mapping: Dict,
        start_date: datetime,
        end_date: datetime,
        progress_callback: Optional[Callable] = None,
    ) -> List[Dict]:
        """Process Energistyrelsen data from DataFrame with date filtering."""
        records = []
        turbines_by_gsrn = turbine_mapping["by_code"]

        # Extract month columns (from column 17 onwards)
        month_columns = df.columns[17:]

        # Get month dates from row 1
        month_dates = {}
        for col_idx, col in enumerate(month_columns):
            try:
                month_str = df.iloc[1, col_idx + 17]
                if pd.notna(month_str) and "Note" not in str(month_str):
                    month_date = pd.to_datetime(month_str)
                    # Filter by date range
                    if start_date <= month_date <= end_date:
                        month_dates[col_idx + 17] = month_date
            except Exception:
                continue

        logger.info(f"Found {len(month_dates)} months in date range")

        # Process turbine rows (skip header rows 0-6)
        data_start_row = 7
        total_rows = len(df) - data_start_row
        processed_rows = 0

        for idx in range(data_start_row, len(df)):
            row = df.iloc[idx]
            gsrn = row.iloc[1]  # Column 'Turbine data' contains GSRN

            if pd.isna(gsrn) or str(gsrn).lower() in ["turbine identifier (gsrn)", "nan"]:
                continue

            gsrn_str = str(int(gsrn)) if isinstance(gsrn, (int, float)) else str(gsrn)
            turbine = turbines_by_gsrn.get(gsrn_str)

            if not turbine:
                continue

            # Process each month in date range
            for col_idx, month_date in month_dates.items():
                try:
                    value = row.iloc[col_idx]

                    if pd.isna(value) or str(value).lower() in ["nan", "n/a", "-", ""]:
                        continue

                    # Convert to float
                    if isinstance(value, str):
                        value = value.replace(",", "").replace(" ", "")
                    generation_kwh = float(value)

                    if generation_kwh <= 0:
                        continue

                    # Convert kWh to MWh
                    generation_mwh = generation_kwh / 1000.0

                    # Calculate period end
                    if month_date.month == 12:
                        period_end = datetime(month_date.year + 1, 1, 1) - timedelta(seconds=1)
                    else:
                        period_end = datetime(month_date.year, month_date.month + 1, 1) - timedelta(seconds=1)

                    record = {
                        "period_start": month_date.isoformat(),
                        "period_end": period_end.isoformat(),
                        "period_type": "month",
                        "source": "ENERGISTYRELSEN",
                        "source_type": "file",
                        "identifier": turbine.code,
                        "value_extracted": generation_mwh,
                        "unit": "MWh",
                        "data": {
                            "generation_mwh": generation_mwh,
                            "generation_kwh": generation_kwh,
                            "turbine_unit_id": turbine.id,
                            "gsrn": gsrn_str,
                            "month": month_date.strftime("%Y-%m"),
                            "period_type": "monthly_total",
                        },
                    }
                    records.append(record)

                except Exception as e:
                    logger.debug(f"Error processing month column for turbine {gsrn_str}: {e}")
                    continue

            processed_rows += 1
            if progress_callback and processed_rows % 100 == 0:
                progress_percent = 20 + int((processed_rows / total_rows) * 60)
                await progress_callback(
                    FileUploadProgressUpdate(
                        status="processing",
                        message=f"Processing turbines... {processed_rows:,}/{total_rows:,}",
                        progress_percent=progress_percent,
                        records_processed=len(records),
                    )
                )

        logger.info(f"Processed {len(records):,} Energistyrelsen records")
        return records

    async def _insert_records(
        self,
        records: List[Dict],
        progress_callback: Optional[Callable] = None,
    ) -> tuple[int, int, List[GenerationUnitSummary]]:
        """Insert records into database and return counts."""
        if not records:
            return 0, 0, []

        records_stored = 0
        records_updated = 0
        units_map = {}

        # Get initial count
        result = await self.db.execute(
            select(func.count(GenerationDataRaw.id)).where(
                GenerationDataRaw.source == records[0]["source"]
            )
        )
        initial_count = result.scalar() or 0

        # Batch insert
        batch_size = 10000
        total_batches = (len(records) + batch_size - 1) // batch_size

        for batch_num, i in enumerate(range(0, len(records), batch_size), 1):
            batch = records[i : i + batch_size]

            try:
                # Convert to GenerationDataRaw objects
                db_records = []
                for record in batch:
                    db_record = GenerationDataRaw(
                        period_start=datetime.fromisoformat(record["period_start"]),
                        period_end=datetime.fromisoformat(record["period_end"]),
                        period_type=record["period_type"],
                        source=record["source"],
                        source_type=record["source_type"],
                        identifier=record["identifier"],
                        value_extracted=Decimal(str(record["value_extracted"])),
                        unit=record["unit"],
                        data=record["data"],
                    )
                    db_records.append(db_record)

                    # Track units
                    unit_id = record["data"].get("generation_unit_id") or record["data"].get("turbine_unit_id")
                    if unit_id:
                        if unit_id not in units_map:
                            units_map[unit_id] = {
                                "id": unit_id,
                                "code": record["identifier"],
                                "name": record["data"].get("unit_name", ""),
                                "records": 0,
                            }
                        units_map[unit_id]["records"] += 1

                self.db.add_all(db_records)
                await self.db.commit()

                if progress_callback:
                    progress_percent = 80 + int((batch_num / total_batches) * 15)
                    await progress_callback(
                        FileUploadProgressUpdate(
                            status="inserting",
                            message=f"Inserted batch {batch_num}/{total_batches}",
                            progress_percent=progress_percent,
                        )
                    )

            except Exception as e:
                logger.error(f"Error inserting batch {batch_num}: {e}")
                await self.db.rollback()

        # Get final count
        result = await self.db.execute(
            select(func.count(GenerationDataRaw.id)).where(
                GenerationDataRaw.source == records[0]["source"]
            )
        )
        final_count = result.scalar() or 0

        records_stored = final_count - initial_count

        # Build units summary
        units_summary = [
            GenerationUnitSummary(
                id=unit["id"],
                code=unit["code"],
                name=unit["name"],
                records_stored=unit["records"],
                records_updated=0,
            )
            for unit in units_map.values()
        ]

        return records_stored, records_updated, units_summary

    async def import_taipower_file(
        self,
        file_content: bytes,
        filename: str,
        unit_code: str,
        start_date: datetime,
        end_date: datetime,
        clean_first: bool = True,
        progress_callback: Optional[Callable[[FileUploadProgressUpdate], None]] = None,
    ) -> FileUploadResponse:
        """Import Taipower data from uploaded Excel file with date range filtering."""
        start_time = datetime.now(timezone.utc)
        errors = []
        warnings = []

        try:
            # Send progress: Validating
            if progress_callback:
                await progress_callback(
                    FileUploadProgressUpdate(
                        status="validating",
                        message="Validating Taipower file structure...",
                        progress_percent=5,
                    )
                )

            # Save to temp file
            with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp_file:
                tmp_file.write(file_content)
                tmp_path = Path(tmp_file.name)

            try:
                # Read Excel file
                df = pd.read_excel(tmp_path)

                # Validate structure - check required columns
                required_columns = ['Timestamp', 'Power generation']
                missing_columns = [col for col in required_columns if col not in df.columns]
                if missing_columns:
                    raise ValueError(f"Invalid Taipower file: Missing columns {missing_columns}")

                # Get generation unit
                result = await self.db.execute(
                    select(GenerationUnit)
                    .where(GenerationUnit.source == "TAIPOWER")
                    .where(GenerationUnit.code == unit_code)
                )
                generation_unit = result.scalar_one_or_none()

                if not generation_unit:
                    raise ValueError(f"No Taipower generation unit found with code '{unit_code}'")

                if progress_callback:
                    await progress_callback(
                        FileUploadProgressUpdate(
                            status="validating",
                            message=f"File validated: {len(df):,} rows for {generation_unit.name}",
                            progress_percent=10,
                        )
                    )

                # Clear existing data for this unit if requested
                if clean_first:
                    if progress_callback:
                        await progress_callback(
                            FileUploadProgressUpdate(
                                status="processing",
                                message=f"Clearing existing data for {generation_unit.name}...",
                                progress_percent=15,
                            )
                        )
                    await self.db.execute(
                        text("DELETE FROM generation_data_raw WHERE source = 'TAIPOWER' AND identifier = :code"),
                        {"code": unit_code}
                    )
                    await self.db.commit()

                # Process data with date filtering
                if progress_callback:
                    await progress_callback(
                        FileUploadProgressUpdate(
                            status="processing",
                            message="Processing Taipower data...",
                            progress_percent=20,
                        )
                    )

                records = await self._process_taipower_data(
                    df, generation_unit, start_date, end_date, progress_callback, filename
                )

                # Insert records
                if progress_callback:
                    await progress_callback(
                        FileUploadProgressUpdate(
                            status="inserting",
                            message=f"Inserting {len(records):,} records...",
                            progress_percent=80,
                        )
                    )

                records_stored, records_updated, units_summary = await self._insert_records(
                    records, progress_callback
                )

                # Calculate actual date range from processed data
                actual_min_date = min(r["period_start"] for r in records) if records else start_date
                actual_max_date = max(r["period_start"] for r in records) if records else end_date

                end_time = datetime.now(timezone.utc)
                duration = (end_time - start_time).total_seconds()

                if progress_callback:
                    await progress_callback(
                        FileUploadProgressUpdate(
                            status="completed",
                            message=f"Completed: {len(records):,} records processed",
                            progress_percent=100,
                        )
                    )

                return FileUploadResponse(
                    success=True,
                    source="TAIPOWER",
                    file_info={
                        "filename": filename,
                        "size_bytes": len(file_content),
                        "rows": len(df),
                        "columns": len(df.columns),
                        "unit_code": unit_code,
                        "unit_name": generation_unit.name,
                    },
                    date_range_requested={
                        "start": start_date.isoformat(),
                        "end": end_date.isoformat(),
                    },
                    date_range_processed={
                        "start": actual_min_date.isoformat() if isinstance(actual_min_date, datetime) else actual_min_date,
                        "end": actual_max_date.isoformat() if isinstance(actual_max_date, datetime) else actual_max_date,
                    },
                    records_stored=records_stored,
                    records_updated=records_updated,
                    generation_units_processed=units_summary,
                    summary={
                        "duration_seconds": duration,
                        "processing_rate": len(records) / duration if duration > 0 else 0,
                        "total_records_processed": len(records),
                    },
                    errors=errors,
                    warnings=warnings,
                )

            finally:
                # Clean up temp file
                tmp_path.unlink(missing_ok=True)

        except Exception as e:
            logger.error(f"Error importing Taipower file: {str(e)}")
            if progress_callback:
                await progress_callback(
                    FileUploadProgressUpdate(
                        status="error",
                        message=f"Error: {str(e)}",
                        progress_percent=0,
                    )
                )
            raise

    async def _process_taipower_data(
        self,
        df: pd.DataFrame,
        generation_unit: GenerationUnit,
        start_date: datetime,
        end_date: datetime,
        progress_callback: Optional[Callable] = None,
        filename: str = "uploaded_file.xlsx",
    ) -> List[Dict]:
        """Process Taipower data from DataFrame with date filtering."""
        records = []
        total_rows = len(df)
        processed_rows = 0

        for idx, row in df.iterrows():
            try:
                # Parse timestamp
                timestamp_str = row.get('Timestamp', '')
                if pd.isna(timestamp_str) or timestamp_str == '':
                    continue

                # Parse datetime (format: YYYY/M/D HH:MM)
                timestamp = pd.to_datetime(timestamp_str, format='%Y/%m/%d %H:%M')

                # Make timezone aware (assume Taiwan time UTC+8)
                if timestamp.tzinfo is None:
                    timestamp = timestamp.tz_localize('Asia/Taipei').tz_convert('UTC')

                # Filter by date range
                if timestamp < start_date or timestamp > end_date:
                    continue

                # Get generation value
                generation = row.get('Power generation', 0)
                if pd.isna(generation):
                    generation = 0

                # Get capacity and capacity factor
                capacity = row.get('Installed capacity', None)
                if pd.isna(capacity):
                    capacity = None

                capacity_factor = row.get('Capacity factor', None)
                if pd.isna(capacity_factor):
                    capacity_factor = None

                # Create record
                record = {
                    "period_start": timestamp.isoformat(),
                    "period_end": (timestamp + pd.Timedelta(hours=1)).isoformat(),
                    "period_type": "hour",
                    "source": "TAIPOWER",
                    "source_type": "file",
                    "identifier": generation_unit.code,
                    "value_extracted": float(generation),
                    "unit": "MW",
                    "data": {
                        "generation_mw": float(generation),
                        "installed_capacity_mw": float(capacity) if capacity else None,
                        "capacity_factor": float(capacity_factor) if capacity_factor else None,
                        "unit_code": generation_unit.code,
                        "unit_name": generation_unit.name,
                        "generation_unit_id": generation_unit.id,
                        "windfarm_id": generation_unit.windfarm_id,
                        "file_source": filename,
                    },
                }
                records.append(record)

                processed_rows += 1
                if progress_callback and processed_rows % 1000 == 0:
                    progress_percent = 20 + int((processed_rows / total_rows) * 60)
                    await progress_callback(
                        FileUploadProgressUpdate(
                            status="processing",
                            message=f"Processing rows... {processed_rows:,}/{total_rows:,}",
                            progress_percent=progress_percent,
                            records_processed=len(records),
                        )
                    )

            except Exception as e:
                logger.debug(f"Error processing row {idx}: {e}")
                continue

        logger.info(f"Processed {len(records):,} Taipower records")
        return records
