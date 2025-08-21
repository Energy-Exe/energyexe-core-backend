"""Service layer for ENTSOE data operations."""

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

import pandas as pd
import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import ValidationException
from app.models.user import User
from app.models.generation_unit import GenerationUnit
from app.services.entsoe_client import ENTSOEClient
from app.services.entsoe_storage_service import ENTSOEStorageService

logger = structlog.get_logger()


class ENTSOEService:
    """Service layer for ENTSOE data operations."""

    def __init__(self, db: AsyncSession):
        self.db = db
        self.client = ENTSOEClient()

    async def fetch_real_time_generation(
        self,
        start_date: datetime,
        end_date: datetime,
        area_codes: List[str],
        production_types: List[str],
        current_user: Optional[User] = None,
        store_data: bool = True,
    ) -> Dict[str, any]:
        """
        Fetch real-time generation data from ENTSOE API.

        Args:
            start_date: Start date for data fetch
            end_date: End date for data fetch
            area_codes: List of area codes to fetch
            production_types: List of production types (wind, solar)
            current_user: User making the request (optional)

        Returns:
            Dictionary with data and metadata
        """

        # Validate date range (max 1 month for real-time queries)
        if end_date - start_date > timedelta(days=31):
            raise ValidationException("Date range cannot exceed 31 days for real-time queries")

        if not area_codes:
            raise ValidationException("At least one area code must be specified")

        if not production_types:
            raise ValidationException("At least one production type must be specified")

        results = {
            "data": [],
            "metadata": {
                "areas": {},
                "total_records": 0,
                "errors": [],
            },
        }

        start_time = datetime.utcnow()

        try:
            # Fetch data for each area
            for area_code in area_codes:
                try:
                    df, metadata = await self.client.fetch_generation_data(
                        start_date, end_date, area_code, production_types
                    )

                    if not df.empty:
                        # Convert to JSON-serializable format
                        df_dict = df.to_dict("records")
                        # Ensure timestamps are ISO format strings
                        for record in df_dict:
                            timestamp = record.get("timestamp")
                            if pd.api.types.is_datetime64_any_dtype(type(timestamp)) or isinstance(
                                timestamp, (pd.Timestamp, datetime)
                            ):
                                record["timestamp"] = pd.Timestamp(timestamp).isoformat()

                        results["data"].extend(df_dict)
                        # Convert error dicts to strings
                        error_strings = []
                        for err in metadata.get("errors", []):
                            if isinstance(err, dict):
                                error_strings.append(err.get("error", str(err)))
                            else:
                                error_strings.append(str(err))

                        results["metadata"]["areas"][area_code] = {
                            "success": metadata["success"],
                            "records": metadata["records"],
                            "errors": error_strings,
                        }
                        results["metadata"]["total_records"] += metadata["records"]
                    else:
                        # Convert error dicts to strings
                        error_strings = []
                        for err in metadata.get("errors", []):
                            if isinstance(err, dict):
                                error_strings.append(err.get("error", str(err)))
                            else:
                                error_strings.append(str(err))

                        if not error_strings:
                            error_strings = ["No data available"]

                        results["metadata"]["areas"][area_code] = {
                            "success": False,
                            "records": 0,
                            "errors": error_strings,
                        }

                except Exception as e:
                    logger.error(f"Error fetching data for {area_code}: {str(e)}")
                    results["metadata"]["errors"].append({"area_code": area_code, "error": str(e)})
                    results["metadata"]["areas"][area_code] = {
                        "success": False,
                        "records": 0,
                        "errors": [str(e)],
                    }

            # Store data in database if requested
            if store_data and results["data"]:
                storage_service = ENTSOEStorageService(self.db)
                storage_result = await storage_service.store_generation_data(
                    data=results["data"],
                    user=current_user,
                )
                
                # Add storage metadata to results
                results["metadata"]["storage"] = storage_result

            return results

        except Exception as e:
            logger.error(f"Error fetching ENTSOE data: {str(e)}")
            raise
    
    async def fetch_generation_per_unit(
        self,
        start_date: datetime,
        end_date: datetime,
        area_code: str,
        generation_units: List[GenerationUnit],
        current_user: Optional[User] = None,
        store_data: bool = True,
    ) -> Dict[str, Any]:
        """
        Fetch generation data per individual unit from ENTSOE API.
        
        Args:
            start_date: Start date for data fetch
            end_date: End date for data fetch
            area_code: Area code for the control area
            generation_units: List of generation units with EIC codes
            current_user: User making the request
            store_data: Whether to store the fetched data
            
        Returns:
            Dictionary with per-unit data and metadata
        """
        # Validate date range
        if end_date - start_date > timedelta(days=31):
            raise ValidationException("Date range cannot exceed 31 days for real-time queries")
        
        if not generation_units:
            raise ValidationException("At least one generation unit must be specified")
        
        # Extract EIC codes from generation units
        eic_codes = [unit.code for unit in generation_units if unit.code]
        
        if not eic_codes:
            raise ValidationException("No valid EIC codes found in generation units")
        
        # Determine production types from generation units
        production_types = set()
        for unit in generation_units:
            if unit.fuel_type and unit.fuel_type.lower() in ["wind", "wind power"]:
                production_types.add("wind")
            elif unit.fuel_type and unit.fuel_type.lower() in ["solar", "solar power", "photovoltaic"]:
                production_types.add("solar")
        
        if not production_types:
            production_types = {"wind"}  # Default to wind
        
        results = {
            "data": [],
            "units_data": {},  # Data grouped by unit
            "metadata": {
                "area_code": area_code,
                "total_records": 0,
                "units_requested": len(generation_units),
                "units_found": 0,
                "errors": [],
            },
        }
        
        try:
            # Fetch per-unit data from ENTSOE
            df, metadata = await self.client.fetch_generation_per_unit(
                start_date,
                end_date,
                area_code,
                eic_codes=eic_codes,
                production_types=list(production_types),
            )
            
            if not df.empty:
                # Convert to JSON-serializable format
                df_dict = df.to_dict("records")
                
                # Group data by EIC code
                for record in df_dict:
                    eic_code = record.get("eic_code")
                    if eic_code:
                        if eic_code not in results["units_data"]:
                            results["units_data"][eic_code] = []
                        results["units_data"][eic_code].append(record)
                
                # Add generation unit metadata to results
                for unit in generation_units:
                    if unit.code in results["units_data"]:
                        # Add unit info to each record for this unit
                        for record in results["units_data"][unit.code]:
                            record["generation_unit_id"] = unit.id
                            record["generation_unit_name"] = unit.name
                            record["generation_unit_capacity"] = float(unit.capacity_mw) if unit.capacity_mw else None
                
                # Flatten all unit data into single list
                for unit_data in results["units_data"].values():
                    results["data"].extend(unit_data)
                
                results["metadata"]["total_records"] = len(results["data"])
                results["metadata"]["units_found"] = len(results["units_data"])
                results["metadata"]["units_found_list"] = list(results["units_data"].keys())
                
                # Store data if requested
                if store_data and results["data"]:
                    storage_service = ENTSOEStorageService(self.db)
                    storage_result = await storage_service.store_generation_data_with_units(
                        data=results["data"],
                        generation_units=generation_units,
                        user=current_user,
                    )
                    results["metadata"]["storage"] = storage_result
            else:
                results["metadata"]["errors"].append("No data available for the specified units")
            
            return results
            
        except Exception as e:
            logger.error(f"Error fetching per-unit ENTSOE data: {str(e)}")
            results["metadata"]["errors"].append(str(e))
            return results

