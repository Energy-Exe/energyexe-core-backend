"""Service layer for storing ENTSOE data in database."""

from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional
from uuid import uuid4

import pandas as pd
import structlog
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.entsoe_generation_data import ENTSOEGenerationData
from app.models.user import User

logger = structlog.get_logger()


class ENTSOEStorageService:
    """Service for storing ENTSOE data in database."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def store_generation_data(
        self,
        data: List[Dict[str, Any]],
        user: Optional[User] = None,
    ) -> Dict[str, Any]:
        """
        Store generation data in database with duplicate handling.

        Args:
            data: List of data points from ENTSOE API
            user: User who initiated the storage

        Returns:
            Dict with operation results
        """
        fetch_id = uuid4()
        
        try:
            if not data:
                return {
                    "success": True,
                    "fetch_id": str(fetch_id),
                    "records_inserted": 0,
                    "records_updated": 0,
                    "message": "No data to store",
                }

            # Prepare data for bulk insert
            records_to_insert = []
            for item in data:
                # Convert timestamp if needed and ensure timezone awareness
                timestamp = item.get("timestamp")
                if isinstance(timestamp, str):
                    # Try to parse as timezone-aware
                    try:
                        timestamp = pd.to_datetime(timestamp, utc=True)
                    except:
                        # If fails, parse without timezone and localize
                        timestamp = pd.to_datetime(timestamp)
                        if timestamp.tzinfo is None:
                            timestamp = timestamp.tz_localize('UTC')
                elif pd.api.types.is_datetime64_any_dtype(type(timestamp)):
                    timestamp = pd.Timestamp(timestamp)
                    if timestamp.tzinfo is None:
                        timestamp = timestamp.tz_localize('UTC')
                elif isinstance(timestamp, pd.Timestamp):
                    if timestamp.tzinfo is None:
                        timestamp = timestamp.tz_localize('UTC')
                
                # Convert to Python datetime
                if hasattr(timestamp, 'to_pydatetime'):
                    timestamp = timestamp.to_pydatetime()

                records_to_insert.append({
                    "timestamp": timestamp,
                    "area_code": str(item.get("area_code", ""))[:50],  # Ensure max 50 chars
                    "production_type": str(item.get("production_type", ""))[:50],  # Ensure max 50 chars
                    "value": Decimal(str(item.get("value", 0))),
                    "unit": item.get("unit", "MW")[:10],  # Ensure max 10 chars
                    "fetch_id": fetch_id,
                })

            # Perform bulk upsert
            if records_to_insert:
                # Use PostgreSQL's ON CONFLICT to handle duplicates
                stmt = insert(ENTSOEGenerationData).values(records_to_insert)
                
                # On conflict, update the value and fetch_id
                stmt = stmt.on_conflict_do_update(
                    index_elements=["timestamp", "area_code", "production_type"],
                    set_={
                        "value": stmt.excluded.value,
                        "fetch_id": stmt.excluded.fetch_id,
                        "unit": stmt.excluded.unit,
                    }
                )
                
                await self.db.execute(stmt)
                await self.db.commit()

            logger.info(
                f"Stored {len(records_to_insert)} ENTSOE records",
                fetch_id=str(fetch_id),
            )

            return {
                "success": True,
                "fetch_id": str(fetch_id),
                "records_inserted": len(records_to_insert),
                "records_updated": 0,  # We don't track this separately for simplicity
                "message": f"Successfully stored {len(records_to_insert)} records",
            }

        except Exception as e:
            logger.error(f"Error storing ENTSOE data: {str(e)}", fetch_id=str(fetch_id))
            await self.db.rollback()
            
            return {
                "success": False,
                "fetch_id": str(fetch_id),
                "records_inserted": 0,
                "records_updated": 0,
                "message": f"Error storing data: {str(e)}",
                "error": str(e),
            }

    async def get_stored_data(
        self,
        start_date: datetime,
        end_date: datetime,
        area_codes: Optional[List[str]] = None,
        production_types: Optional[List[str]] = None,
        limit: int = 10000,
    ) -> List[Dict[str, Any]]:
        """
        Retrieve stored generation data from database.

        Args:
            start_date: Start date for query
            end_date: End date for query
            area_codes: Filter by area codes
            production_types: Filter by production types
            limit: Maximum number of records to return

        Returns:
            List of generation data records
        """
        stmt = select(ENTSOEGenerationData).where(
            ENTSOEGenerationData.timestamp >= start_date,
            ENTSOEGenerationData.timestamp <= end_date,
        )

        if area_codes:
            stmt = stmt.where(ENTSOEGenerationData.area_code.in_(area_codes))

        if production_types:
            stmt = stmt.where(ENTSOEGenerationData.production_type.in_(production_types))

        stmt = stmt.order_by(ENTSOEGenerationData.timestamp).limit(limit)

        result = await self.db.execute(stmt)
        records = result.scalars().all()

        return [
            {
                "timestamp": record.timestamp.isoformat(),
                "area_code": record.area_code,
                "production_type": record.production_type,
                "value": float(record.value),
                "unit": record.unit,
                "fetch_id": str(record.fetch_id) if record.fetch_id else None,
                "created_at": record.created_at.isoformat(),
            }
            for record in records
        ]
    
    async def store_generation_data_with_units(
        self,
        data: List[Dict[str, Any]],
        generation_units: List[Any],
        user: Optional[User] = None,
    ) -> Dict[str, Any]:
        """
        Store generation data with generation unit associations.
        
        This stores data in the regular ENTSOE table but tracks which generation
        units were involved in the fetch.
        
        Args:
            data: List of data points from ENTSOE API with unit info
            generation_units: List of generation units involved
            user: User who initiated the storage
            
        Returns:
            Dict with operation results
        """
        fetch_id = uuid4()
        
        try:
            if not data:
                return {
                    "success": True,
                    "fetch_id": str(fetch_id),
                    "records_inserted": 0,
                    "units_tracked": 0,
                    "message": "No data to store",
                }
            
            # Prepare data for bulk insert
            records_to_insert = []
            units_tracked = set()
            
            for item in data:
                # Convert timestamp if needed
                timestamp = item.get("timestamp")
                if isinstance(timestamp, str):
                    timestamp = pd.to_datetime(timestamp, utc=True)
                elif pd.api.types.is_datetime64_any_dtype(type(timestamp)):
                    timestamp = pd.Timestamp(timestamp)
                    if timestamp.tzinfo is None:
                        timestamp = timestamp.tz_localize('UTC')
                
                if hasattr(timestamp, 'to_pydatetime'):
                    timestamp = timestamp.to_pydatetime()
                
                # Track which unit this data is for
                if item.get("generation_unit_id"):
                    units_tracked.add(item["generation_unit_id"])
                
                # Store with EIC code as identifier
                eic_code = item.get("eic_code", "")
                
                records_to_insert.append({
                    "timestamp": timestamp,
                    "area_code": str(item.get("area_code", ""))[:50],
                    "production_type": f"unit_{eic_code}"[:50] if eic_code else "unknown",
                    "value": Decimal(str(item.get("value", 0))),
                    "unit": item.get("unit", "MW")[:10],
                    "fetch_id": fetch_id,
                })
            
            # Perform bulk upsert
            if records_to_insert:
                stmt = insert(ENTSOEGenerationData).values(records_to_insert)
                
                # On conflict, update the value and fetch_id
                stmt = stmt.on_conflict_do_update(
                    index_elements=["timestamp", "area_code", "production_type"],
                    set_={
                        "value": stmt.excluded.value,
                        "fetch_id": stmt.excluded.fetch_id,
                        "unit": stmt.excluded.unit,
                    }
                )
                
                await self.db.execute(stmt)
                await self.db.commit()
            
            logger.info(
                f"Stored {len(records_to_insert)} ENTSOE per-unit records for {len(units_tracked)} units",
                fetch_id=str(fetch_id),
            )
            
            return {
                "success": True,
                "fetch_id": str(fetch_id),
                "records_inserted": len(records_to_insert),
                "units_tracked": len(units_tracked),
                "message": f"Successfully stored {len(records_to_insert)} records for {len(units_tracked)} units",
            }
            
        except Exception as e:
            logger.error(f"Error storing ENTSOE per-unit data: {str(e)}", fetch_id=str(fetch_id))
            await self.db.rollback()
            
            return {
                "success": False,
                "fetch_id": str(fetch_id),
                "records_inserted": 0,
                "units_tracked": 0,
                "message": f"Error storing data: {str(e)}",
                "error": str(e),
            }