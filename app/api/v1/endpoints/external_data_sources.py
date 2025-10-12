"""API endpoints for external data sources."""

from datetime import datetime
from typing import List
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
import structlog

from app.core.deps import get_current_active_user, get_db
from app.models.user import User
from app.models.windfarm import Windfarm
from app.models.generation_unit import GenerationUnit
from app.services.eia_client import EIAClient
from app.services.entsoe_client import ENTSOEClient
from app.services.elexon_client import ElexonClient
from app.services.taipower_client import TaipowerClient
from app.schemas.external_sources import (
    EIAFetchRequest,
    EIAFetchResponse,
    ENTSOEFetchRequest,
    ENTSOEFetchResponse,
    ELEXONFetchRequest,
    ELEXONFetchResponse,
    TAIPOWERFetchRequest,
    TAIPOWERFetchResponse,
    NVEFetchRequest,
    NVEFetchResponse,
    ENERGISTYRELSENFetchRequest,
    ENERGISTYRELSENFetchResponse,
    SourceInfo,
    SourcesListResponse,
)

logger = structlog.get_logger()

router = APIRouter()


async def get_windfarm_generation_units(
    db: AsyncSession, windfarm_ids: List[int]
) -> dict:
    """Get generation units for windfarms."""
    result = await db.execute(
        select(GenerationUnit)
        .where(GenerationUnit.windfarm_id.in_(windfarm_ids))
        .where(GenerationUnit.is_active == True)
    )
    units = result.scalars().all()

    # Get windfarms
    windfarms_result = await db.execute(
        select(Windfarm)
        .where(Windfarm.id.in_(windfarm_ids))
    )
    windfarms = windfarms_result.scalars().all()

    return {
        "units": units,
        "windfarms": windfarms,
        "units_by_windfarm": {wf.id: [u for u in units if u.windfarm_id == wf.id] for wf in windfarms}
    }


@router.post("/eia/fetch", response_model=EIAFetchResponse)
async def fetch_eia_data(
    request: EIAFetchRequest,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Fetch data from EIA API."""
    try:
        logger.info(f"Fetching EIA data for windfarms: {request.windfarm_ids}")

        # Get generation units for the windfarms
        windfarm_data = await get_windfarm_generation_units(db, request.windfarm_ids)

        if not windfarm_data["units"]:
            return EIAFetchResponse(
                success=False,
                data=[],
                metadata={"error": "No generation units found for specified windfarms"},
                message="No generation units found"
            )

        # Get plant codes from generation units
        plant_codes = [unit.code for unit in windfarm_data["units"] if unit.code]

        if not plant_codes:
            return EIAFetchResponse(
                success=False,
                data=[],
                metadata={"error": "No EIA plant codes configured for these windfarms"},
                message="No EIA plant codes found"
            )

        # Fetch data from EIA
        client = EIAClient()
        df, metadata = await client.fetch_monthly_generation_data(
            plant_codes=plant_codes,
            start_year=request.start_year,
            start_month=request.start_month,
            end_year=request.end_year,
            end_month=request.end_month,
        )

        # Convert DataFrame to list of dicts
        data = df.to_dict(orient="records") if not df.empty else []

        # Add windfarm information
        metadata["windfarms"] = [
            {"id": wf.id, "name": wf.name, "code": wf.code}
            for wf in windfarm_data["windfarms"]
        ]

        return EIAFetchResponse(
            success=metadata.get("success", True),
            data=data,
            metadata=metadata,
            message=f"Successfully fetched {len(data)} records" if data else "No data found"
        )

    except Exception as e:
        logger.error(f"Error fetching EIA data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/entsoe/fetch", response_model=ENTSOEFetchResponse)
async def fetch_entsoe_data(
    request: ENTSOEFetchRequest,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Fetch data from ENTSOE API."""
    try:
        logger.info(f"Fetching ENTSOE data for windfarms: {request.windfarm_ids}")

        # Get windfarm data with bidzones
        windfarm_data = await get_windfarm_generation_units(db, request.windfarm_ids)

        if not windfarm_data["windfarms"]:
            return ENTSOEFetchResponse(
                success=False,
                data=[],
                metadata={"error": "No windfarms found"},
                message="No windfarms found"
            )

        # Get unique bidzones from windfarms
        from app.models.bidzone import Bidzone
        bidzone_ids = [wf.bidzone_id for wf in windfarm_data["windfarms"] if wf.bidzone_id]

        if not bidzone_ids:
            return ENTSOEFetchResponse(
                success=False,
                data=[],
                metadata={"error": "No bidding zones configured for selected windfarms"},
                message="No bidding zones found"
            )

        # Get bidzone details
        bidzone_result = await db.execute(
            select(Bidzone).where(Bidzone.id.in_(bidzone_ids))
        )
        bidzones = bidzone_result.scalars().all()

        # Map bidzone names to ENTSOE area codes
        BIDZONE_TO_AREA_CODE = {
            'DE-LU': 'DE_LU', 'FR': 'FR', 'ES': 'ES', 'GB': 'GB', 'IT': 'IT',
            'NL': 'NL', 'BE': 'BE', 'AT': 'AT', 'CH': 'CH', 'PL': 'PL',
            'DK1': 'DK_1', 'DK2': 'DK_2',
            'NO1': 'NO_1', 'NO2': 'NO_2', 'NO3': 'NO_3', 'NO4': 'NO_4', 'NO5': 'NO_5',
            'SE1': 'SE_1', 'SE2': 'SE_2', 'SE3': 'SE_3', 'SE4': 'SE_4',
        }

        area_codes = [BIDZONE_TO_AREA_CODE.get(bz.name, bz.name) for bz in bidzones]
        unique_area_codes = list(set(area_codes))

        logger.info(f"Detected bidding zones: {[bz.name for bz in bidzones]}")
        logger.info(f"Mapped to area codes: {unique_area_codes}")

        # Get EIC codes from generation units
        # Filter generation units where source='ENTSOE' - their codes ARE EIC codes!
        entsoe_units = [u for u in windfarm_data["units"] if u.source == "ENTSOE"]
        eic_codes_from_units = [u.code for u in entsoe_units if u.code and u.code != 'nan']

        logger.info(f"Found {len(entsoe_units)} ENTSOE generation units")
        logger.info(f"Extracted EIC codes: {eic_codes_from_units[:5]}...")  # Log first 5

        # Group windfarms by bidding zone
        windfarms_by_zone = {}
        units_by_zone = {}
        for wf in windfarm_data["windfarms"]:
            bidzone = next((bz for bz in bidzones if bz.id == wf.bidzone_id), None)
            if bidzone:
                area_code = BIDZONE_TO_AREA_CODE.get(bidzone.name, bidzone.name)
                if area_code not in windfarms_by_zone:
                    windfarms_by_zone[area_code] = []
                    units_by_zone[area_code] = []
                windfarms_by_zone[area_code].append(wf)
                # Add units for this windfarm
                wf_units = [u for u in entsoe_units if u.windfarm_id == wf.id]
                units_by_zone[area_code].extend(wf_units)

        # Fetch data for each area code
        all_data = []
        combined_metadata = {
            "area_codes_queried": unique_area_codes,
            "bidzones": [{"id": bz.id, "name": bz.name, "code": bz.code} for bz in bidzones],
            "success": True,
            "errors": [],
            "using_eic_codes": len(eic_codes_from_units) > 0,
            "eic_codes_count": len(eic_codes_from_units),
        }

        client = ENTSOEClient()

        for area_code in unique_area_codes:
            logger.info(f"Fetching ENTSOE data for area code: {area_code}")

            # Convert timezone-aware datetime to naive datetime for ENTSOE client
            start_naive = request.start_date.replace(tzinfo=None) if request.start_date.tzinfo else request.start_date
            end_naive = request.end_date.replace(tzinfo=None) if request.end_date.tzinfo else request.end_date

            # Get EIC codes for units in this zone
            zone_eic_codes = [u.code for u in units_by_zone.get(area_code, []) if u.code and u.code != 'nan']

            # Use manual EIC codes if provided, otherwise use codes from generation units
            eic_codes_to_use = request.eic_codes if request.eic_codes else (zone_eic_codes if zone_eic_codes else None)

            if eic_codes_to_use:
                logger.info(f"Fetching per-unit data for {len(eic_codes_to_use)} EIC codes in {area_code}")
                # Fetch per-unit data with EIC codes
                df, metadata = await client.fetch_generation_per_unit(
                    start=start_naive,
                    end=end_naive,
                    area_code=area_code,
                    eic_codes=eic_codes_to_use,
                    production_types=request.production_types,
                )
            else:
                logger.info(f"No EIC codes found, fetching aggregated data for {area_code}")
                # Fetch aggregated data for the zone
                df, metadata = await client.fetch_generation_data(
                    start=start_naive,
                    end=end_naive,
                    area_code=area_code,
                    production_types=request.production_types,
                )

            # Add area code AND windfarm mapping to each record
            if not df.empty:
                records = df.to_dict(orient="records")
                zone_windfarms = windfarms_by_zone.get(area_code, [])

                for record in records:
                    record["area_code"] = area_code

                    # If we have EIC code in the record, try to match it to a specific windfarm
                    eic_code = record.get("eic_code")
                    if eic_code:
                        # Find which windfarm this EIC code belongs to
                        matching_unit = next((u for u in units_by_zone.get(area_code, []) if u.code == eic_code), None)
                        if matching_unit and matching_unit.windfarm_id:
                            matching_wf = next((wf for wf in zone_windfarms if wf.id == matching_unit.windfarm_id), None)
                            if matching_wf:
                                record["windfarm_id"] = matching_wf.id
                                record["windfarm_name"] = matching_wf.name
                                record["generation_unit_id"] = matching_unit.id
                                record["generation_unit_code"] = matching_unit.code

                    # Fallback: if no specific windfarm match, add all windfarms in zone
                    if "windfarm_id" not in record:
                        record["windfarm_ids"] = [wf.id for wf in zone_windfarms]
                        record["windfarm_names"] = [wf.name for wf in zone_windfarms]

                all_data.extend(records)

            if not metadata.get("success"):
                combined_metadata["errors"].append({
                    "area_code": area_code,
                    "errors": metadata.get("errors", [])
                })

        # Add windfarm information
        combined_metadata["windfarms"] = [
            {"id": wf.id, "name": wf.name, "code": wf.code, "bidzone_id": wf.bidzone_id}
            for wf in windfarm_data["windfarms"]
        ]
        combined_metadata["total_records"] = len(all_data)

        return ENTSOEFetchResponse(
            success=len(all_data) > 0,
            data=all_data,
            metadata=combined_metadata,
            message=f"Successfully fetched {len(all_data)} records from {len(unique_area_codes)} area(s)" if all_data else "No data found"
        )

    except Exception as e:
        logger.error(f"Error fetching ENTSOE data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/elexon/fetch", response_model=ELEXONFetchResponse)
async def fetch_elexon_data(
    request: ELEXONFetchRequest,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Fetch data from ELEXON API."""
    try:
        logger.info(f"Fetching ELEXON data for windfarms: {request.windfarm_ids}")

        # Get generation units for the windfarms
        windfarm_data = await get_windfarm_generation_units(db, request.windfarm_ids)

        if not windfarm_data["units"]:
            return ELEXONFetchResponse(
                success=False,
                data=[],
                metadata={"error": "No generation units found for specified windfarms"},
                message="No generation units found"
            )

        # Get BM Unit codes from generation units
        bm_units = [unit.code for unit in windfarm_data["units"] if unit.code]

        if not bm_units:
            return ELEXONFetchResponse(
                success=False,
                data=[],
                metadata={"error": "No ELEXON BM Unit codes configured for these windfarms"},
                message="No BM Unit codes found"
            )

        # Fetch data from ELEXON
        client = ElexonClient()
        df, metadata = await client.fetch_physical_data(
            start=request.start_date,
            end=request.end_date,
            bm_units=bm_units,
        )

        # Convert DataFrame to list of dicts
        data = df.to_dict(orient="records") if not df.empty else []

        # Add windfarm information
        metadata["windfarms"] = [
            {"id": wf.id, "name": wf.name, "code": wf.code}
            for wf in windfarm_data["windfarms"]
        ]

        return ELEXONFetchResponse(
            success=metadata.get("success", True),
            data=data,
            metadata=metadata,
            message=f"Successfully fetched {len(data)} records" if data else "No data found"
        )

    except Exception as e:
        logger.error(f"Error fetching ELEXON data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/taipower/fetch", response_model=TAIPOWERFetchResponse)
async def fetch_taipower_data(
    request: TAIPOWERFetchRequest,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Fetch data from TAIPOWER API."""
    try:
        logger.info(f"Fetching TAIPOWER data for windfarms: {request.windfarm_ids}")

        # Get windfarm data
        windfarm_data = await get_windfarm_generation_units(db, request.windfarm_ids)

        if not windfarm_data["windfarms"]:
            return TAIPOWERFetchResponse(
                success=False,
                data=[],
                metadata={"error": "No windfarms found"},
                message="No windfarms found"
            )

        # Fetch live data from TAIPOWER
        # Note: TAIPOWER API provides live/current data, not historical ranges
        client = TaipowerClient()
        taipower_response, metadata = await client.fetch_live_data()

        if not taipower_response:
            return TAIPOWERFetchResponse(
                success=False,
                data=[],
                metadata=metadata,
                message="Failed to fetch TAIPOWER data"
            )

        # Convert to list of dicts
        data = [
            {
                "timestamp": metadata["timestamp"].isoformat(),
                "generation_type": unit.generation_type,
                "unit_name": unit.unit_name,
                "installed_capacity_mw": unit.installed_capacity_mw,
                "net_generation_mw": unit.net_generation_mw,
                "capacity_utilization_percent": unit.capacity_utilization_percent,
                "notes": unit.notes,
            }
            for unit in taipower_response.generation_units
        ]

        # Add windfarm information
        metadata["windfarms"] = [
            {"id": wf.id, "name": wf.name, "code": wf.code}
            for wf in windfarm_data["windfarms"]
        ]

        # Add summary statistics
        summary = client.calculate_summary_statistics(taipower_response)
        metadata["summary"] = summary

        return TAIPOWERFetchResponse(
            success=True,
            data=data,
            metadata=metadata,
            message=f"Successfully fetched {len(data)} generation units"
        )

    except Exception as e:
        logger.error(f"Error fetching TAIPOWER data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/nve/fetch", response_model=NVEFetchResponse)
async def fetch_nve_data(
    request: NVEFetchRequest,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Fetch data from NVE API."""
    # NVE client not implemented yet
    return NVEFetchResponse(
        success=False,
        data=[],
        metadata={"error": "NVE API client not implemented yet"},
        message="NVE API not available"
    )


@router.post("/energistyrelsen/fetch", response_model=ENERGISTYRELSENFetchResponse)
async def fetch_energistyrelsen_data(
    request: ENERGISTYRELSENFetchRequest,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Fetch data from ENERGISTYRELSEN API."""
    # ENERGISTYRELSEN client not implemented yet
    return ENERGISTYRELSENFetchResponse(
        success=False,
        data=[],
        metadata={"error": "ENERGISTYRELSEN API client not implemented yet"},
        message="ENERGISTYRELSEN API not available"
    )


@router.get("/sources", response_model=SourcesListResponse)
async def list_sources(
    current_user: User = Depends(get_current_active_user),
):
    """List all available external data sources."""
    sources = [
        SourceInfo(
            code="eia",
            name="EIA",
            description="US Energy Information Administration - Monthly generation data for US wind farms",
            country="US",
            status="active",
            requires_api_key=True,
        ),
        SourceInfo(
            code="entsoe",
            name="ENTSOE",
            description="European Network of Transmission System Operators - Hourly generation data across Europe",
            country="EU",
            status="active",
            requires_api_key=True,
        ),
        SourceInfo(
            code="elexon",
            name="ELEXON",
            description="UK Electricity System Operator - Real-time and historical generation data for UK",
            country="GB",
            status="active",
            requires_api_key=True,
        ),
        SourceInfo(
            code="taipower",
            name="TAIPOWER",
            description="Taiwan Power Company - Live generation data for Taiwan power plants",
            country="TW",
            status="active",
            requires_api_key=False,
        ),
        SourceInfo(
            code="nve",
            name="NVE",
            description="Norwegian Water Resources and Energy Directorate - Generation data for Norway",
            country="NO",
            status="pending",
            requires_api_key=False,
        ),
        SourceInfo(
            code="energistyrelsen",
            name="ENERGISTYRELSEN",
            description="Danish Energy Agency - Generation data for Denmark",
            country="DK",
            status="pending",
            requires_api_key=False,
        ),
    ]

    return SourcesListResponse(sources=sources)
