"""Taipower API client service."""

import json
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
import httpx
import structlog
from dateutil import parser
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_
from app.models.generation_unit import GenerationUnit
from app.schemas.taipower import (
    TaipowerDataResponse,
    TaipowerGenerationUnit,
    TaipowerGenerationDataPoint,
)

logger = structlog.get_logger()


class TaipowerClient:
    """Client for interacting with Taipower API."""

    API_URL = "https://service.taipower.com.tw/data/opendata/apply/file/d006001/001.json"

    def __init__(self):
        self.timeout = httpx.Timeout(30.0)

    async def fetch_live_data(self) -> Tuple[Optional[TaipowerDataResponse], Dict[str, Any]]:
        """
        Fetch live generation data from Taipower API.

        Returns:
            Tuple of (TaipowerDataResponse or None, metadata dict)
        """
        metadata = {
            "success": True,
            "errors": [],
            "fetch_time": datetime.utcnow(),
        }

        try:
            logger.info("Fetching live data from Taipower API")
            
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(self.API_URL)
                
                if response.status_code != 200:
                    error_msg = f"Taipower API error: {response.status_code}"
                    logger.error(error_msg)
                    metadata["success"] = False
                    metadata["errors"].append({"error": error_msg})
                    return None, metadata
                
                # Handle BOM in response
                text = response.text
                if text.startswith('\ufeff'):
                    text = text[1:]
                
                data = json.loads(text)
                
                # Parse the response - handle the actual field name "DateTime"
                timestamp_str = data.get("DateTime", datetime.utcnow().isoformat())
                
                # Parse timestamp if it's a string
                if isinstance(timestamp_str, str):
                    timestamp = parser.parse(timestamp_str)
                else:
                    timestamp = datetime.utcnow()
                
                # Parse the response
                taipower_response = TaipowerDataResponse(
                    datetime=timestamp,
                    generation_units=[
                        TaipowerGenerationUnit(**unit) for unit in data.get("aaData", [])
                    ]
                )
                
                metadata["timestamp"] = taipower_response.datetime
                metadata["unit_count"] = len(taipower_response.generation_units)
                
                logger.info(
                    f"Successfully fetched {metadata['unit_count']} generation units from Taipower"
                )
                
                return taipower_response, metadata
                
        except httpx.RequestError as e:
            error_msg = f"Network error fetching Taipower data: {str(e)}"
            logger.error(error_msg)
            metadata["success"] = False
            metadata["errors"].append({"error": error_msg})
            return None, metadata
        except Exception as e:
            error_msg = f"Unexpected error fetching Taipower data: {str(e)}"
            logger.error(error_msg)
            metadata["success"] = False
            metadata["errors"].append({"error": error_msg})
            return None, metadata


    def transform_to_data_points(
        self,
        data: TaipowerDataResponse,
        generation_units_map: Optional[Dict[str, GenerationUnit]] = None
    ) -> List[TaipowerGenerationDataPoint]:
        """
        Transform Taipower API response to data points.

        Args:
            data: Taipower API response
            generation_units_map: Optional map of unit names to GenerationUnit objects

        Returns:
            List of TaipowerGenerationDataPoint objects
        """
        data_points = []
        
        for unit in data.generation_units:
            generation_unit = generation_units_map.get(unit.unit_name) if generation_units_map else None
            
            data_point = TaipowerGenerationDataPoint(
                timestamp=data.datetime,
                generation_type=unit.generation_type,
                unit_name=unit.unit_name,
                installed_capacity_mw=unit.installed_capacity_mw,
                net_generation_mw=unit.net_generation_mw,
                capacity_utilization_percent=unit.capacity_utilization_percent,
                notes=unit.notes,
                generation_unit_id=generation_unit.id if generation_unit else None,
                generation_unit_code=generation_unit.code if generation_unit else unit.unit_name,
            )
            data_points.append(data_point)
            
        return data_points

    def calculate_summary_statistics(
        self, 
        data: TaipowerDataResponse
    ) -> Dict[str, Any]:
        """
        Calculate summary statistics from Taipower data.

        Args:
            data: Taipower API response

        Returns:
            Dictionary with summary statistics
        """
        total_generation = sum(unit.net_generation_mw for unit in data.generation_units)
        total_capacity = sum(unit.installed_capacity_mw for unit in data.generation_units)
        
        generation_by_type = {}
        capacity_by_type = {}
        
        for unit in data.generation_units:
            gen_type = unit.generation_type
            if gen_type not in generation_by_type:
                generation_by_type[gen_type] = 0
                capacity_by_type[gen_type] = 0
            generation_by_type[gen_type] += unit.net_generation_mw
            capacity_by_type[gen_type] += unit.installed_capacity_mw
        
        return {
            "total_generation_mw": total_generation,
            "total_capacity_mw": total_capacity,
            "overall_utilization_percent": (total_generation / total_capacity * 100) if total_capacity > 0 else 0,
            "generation_by_type": generation_by_type,
            "capacity_by_type": capacity_by_type,
            "unit_count": len(data.generation_units),
            "timestamp": data.datetime,
        }