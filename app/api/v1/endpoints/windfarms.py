from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.schemas.windfarm import (
    Windfarm,
    WindfarmCreate,
    WindfarmCreateWithOwners,
    WindfarmUpdate,
    WindfarmWithOwners,
)
from app.schemas.windfarm_owner import WindfarmOwner, WindfarmOwnerCreate, WindfarmOwnerUpdate
from app.services.windfarm import WindfarmService
from app.services.windfarm_owner import WindfarmOwnerService

router = APIRouter()


@router.get("/", response_model=List[Windfarm])
async def get_windfarms(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
):
    """Get all windfarms with pagination"""
    return await WindfarmService.get_windfarms(db, skip=skip, limit=limit)


@router.get("/search", response_model=List[Windfarm])
async def search_windfarms(
    q: str = Query(..., min_length=1),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
):
    """Search windfarms by name"""
    return await WindfarmService.search_windfarms(db, query=q, skip=skip, limit=limit)


@router.get("/{windfarm_id}", response_model=Windfarm)
async def get_windfarm(windfarm_id: int, db: AsyncSession = Depends(get_db)):
    """Get a specific windfarm by ID"""
    windfarm = await WindfarmService.get_windfarm(db, windfarm_id)
    if not windfarm:
        raise HTTPException(status_code=404, detail="Windfarm not found")
    return windfarm


@router.get("/{windfarm_id}/with-owners")
async def get_windfarm_with_owners(windfarm_id: int, db: AsyncSession = Depends(get_db)):
    """Get a specific windfarm by ID with owners"""
    windfarm = await WindfarmService.get_windfarm_with_owners(db, windfarm_id)
    if not windfarm:
        raise HTTPException(status_code=404, detail="Windfarm not found")

    # Convert ORM objects to dictionaries for JSON serialization
    windfarm_dict = {
        "id": windfarm.id,
        "code": windfarm.code,
        "name": windfarm.name,
        "country_id": windfarm.country_id,
        "state_id": windfarm.state_id,
        "region_id": windfarm.region_id,
        "bidzone_id": windfarm.bidzone_id,
        "market_balance_area_id": windfarm.market_balance_area_id,
        "control_area_id": windfarm.control_area_id,
        "nameplate_capacity_mw": windfarm.nameplate_capacity_mw,
        "project_id": windfarm.project_id,
        "commercial_operational_date": windfarm.commercial_operational_date.isoformat()
        if windfarm.commercial_operational_date
        else None,
        "first_power_date": windfarm.first_power_date.isoformat()
        if windfarm.first_power_date
        else None,
        "lat": windfarm.lat,
        "lng": windfarm.lng,
        "polygon_wkt": windfarm.polygon_wkt,
        "foundation_type": windfarm.foundation_type,
        "location_type": windfarm.location_type,
        "status": windfarm.status,
        "notes": windfarm.notes,
        "alternate_name": windfarm.alternate_name,
        "environmental_assessment_status": windfarm.environmental_assessment_status,
        "permits_obtained": windfarm.permits_obtained,
        "grid_connection_status": windfarm.grid_connection_status,
        "total_investment_amount": str(windfarm.total_investment_amount)
        if windfarm.total_investment_amount
        else None,
        "investment_currency": windfarm.investment_currency,
        "address": windfarm.address,
        "postal_code": windfarm.postal_code,
        "created_at": windfarm.created_at.isoformat(),
        "updated_at": windfarm.updated_at.isoformat(),
        "country": {
            "id": windfarm.country.id,
            "code": windfarm.country.code,
            "name": windfarm.country.name,
        }
        if windfarm.country
        else None,
        "state": {
            "id": windfarm.state.id,
            "code": windfarm.state.code,
            "name": windfarm.state.name,
        }
        if windfarm.state
        else None,
        "region": {
            "id": windfarm.region.id,
            "code": windfarm.region.code,
            "name": windfarm.region.name,
        }
        if windfarm.region
        else None,
        "bidzone": {
            "id": windfarm.bidzone.id,
            "code": windfarm.bidzone.code,
            "name": windfarm.bidzone.name,
        }
        if windfarm.bidzone
        else None,
        "market_balance_area": {
            "id": windfarm.market_balance_area.id,
            "code": windfarm.market_balance_area.code,
            "name": windfarm.market_balance_area.name,
        }
        if windfarm.market_balance_area
        else None,
        "control_area": {
            "id": windfarm.control_area.id,
            "code": windfarm.control_area.code,
            "name": windfarm.control_area.name,
        }
        if windfarm.control_area
        else None,
        "project": {
            "id": windfarm.project.id,
            "code": windfarm.project.code,
            "name": windfarm.project.name,
        }
        if windfarm.project
        else None,
        "windfarm_owners": [
            {
                "id": wo.id,
                "windfarm_id": wo.windfarm_id,
                "owner_id": wo.owner_id,
                "ownership_percentage": str(wo.ownership_percentage),
                "created_at": wo.created_at.isoformat(),
                "updated_at": wo.updated_at.isoformat(),
                "owner": {
                    "id": wo.owner.id,
                    "code": wo.owner.code,
                    "name": wo.owner.name,
                    "created_at": wo.owner.created_at.isoformat(),
                    "updated_at": wo.owner.updated_at.isoformat(),
                }
                if wo.owner
                else None,
            }
            for wo in windfarm.windfarm_owners
        ],
    }

    return windfarm_dict


@router.get("/code/{code}", response_model=Windfarm)
async def get_windfarm_by_code(code: str, db: AsyncSession = Depends(get_db)):
    """Get a windfarm by its code"""
    windfarm = await WindfarmService.get_windfarm_by_code(db, code)
    if not windfarm:
        raise HTTPException(status_code=404, detail="Windfarm not found")
    return windfarm


@router.post("/", response_model=Windfarm, status_code=201)
async def create_windfarm(windfarm: WindfarmCreate, db: AsyncSession = Depends(get_db)):
    """Create a new windfarm"""
    # Check if windfarm with same code already exists
    existing_windfarm = await WindfarmService.get_windfarm_by_code(db, windfarm.code)
    if existing_windfarm:
        raise HTTPException(status_code=400, detail="Windfarm with this code already exists")

    return await WindfarmService.create_windfarm(db, windfarm)


@router.post("/with-owners", status_code=201)
async def create_windfarm_with_owners(
    windfarm_data: WindfarmCreateWithOwners, db: AsyncSession = Depends(get_db)
):
    """Create a new windfarm with owners"""
    # Check if windfarm with same code already exists
    existing_windfarm = await WindfarmService.get_windfarm_by_code(db, windfarm_data.windfarm.code)
    if existing_windfarm:
        raise HTTPException(status_code=400, detail="Windfarm with this code already exists")

    # Validate that ownership percentages sum to 100%
    if not await WindfarmOwnerService.validate_ownership_percentages(windfarm_data.owners):
        raise HTTPException(
            status_code=400, detail="Ownership percentages must sum to exactly 100%"
        )

    # Create the windfarm first
    windfarm = await WindfarmService.create_windfarm(db, windfarm_data.windfarm)

    # Then create the ownership relationships
    await WindfarmOwnerService.create_windfarm_owners(db, windfarm.id, windfarm_data.owners)

    # Return the windfarm with owners - delegate to the get endpoint logic
    windfarm_with_owners = await WindfarmService.get_windfarm_with_owners(db, windfarm.id)

    # Convert ORM objects to dictionaries for JSON serialization
    windfarm_dict = {
        "id": windfarm_with_owners.id,
        "code": windfarm_with_owners.code,
        "name": windfarm_with_owners.name,
        "country_id": windfarm_with_owners.country_id,
        "state_id": windfarm_with_owners.state_id,
        "region_id": windfarm_with_owners.region_id,
        "bidzone_id": windfarm_with_owners.bidzone_id,
        "market_balance_area_id": windfarm_with_owners.market_balance_area_id,
        "control_area_id": windfarm_with_owners.control_area_id,
        "nameplate_capacity_mw": windfarm_with_owners.nameplate_capacity_mw,
        "project_id": windfarm_with_owners.project_id,
        "commercial_operational_date": windfarm_with_owners.commercial_operational_date.isoformat()
        if windfarm_with_owners.commercial_operational_date
        else None,
        "first_power_date": windfarm_with_owners.first_power_date.isoformat()
        if windfarm_with_owners.first_power_date
        else None,
        "lat": windfarm_with_owners.lat,
        "lng": windfarm_with_owners.lng,
        "polygon_wkt": windfarm_with_owners.polygon_wkt,
        "foundation_type": windfarm_with_owners.foundation_type,
        "location_type": windfarm_with_owners.location_type,
        "status": windfarm_with_owners.status,
        "notes": windfarm_with_owners.notes,
        "alternate_name": windfarm_with_owners.alternate_name,
        "environmental_assessment_status": windfarm_with_owners.environmental_assessment_status,
        "permits_obtained": windfarm_with_owners.permits_obtained,
        "grid_connection_status": windfarm_with_owners.grid_connection_status,
        "total_investment_amount": str(windfarm_with_owners.total_investment_amount)
        if windfarm_with_owners.total_investment_amount
        else None,
        "investment_currency": windfarm_with_owners.investment_currency,
        "address": windfarm_with_owners.address,
        "postal_code": windfarm_with_owners.postal_code,
        "created_at": windfarm_with_owners.created_at.isoformat(),
        "updated_at": windfarm_with_owners.updated_at.isoformat(),
        "country": {
            "id": windfarm_with_owners.country.id,
            "code": windfarm_with_owners.country.code,
            "name": windfarm_with_owners.country.name,
        }
        if windfarm_with_owners.country
        else None,
        "state": {
            "id": windfarm_with_owners.state.id,
            "code": windfarm_with_owners.state.code,
            "name": windfarm_with_owners.state.name,
        }
        if windfarm_with_owners.state
        else None,
        "region": {
            "id": windfarm_with_owners.region.id,
            "code": windfarm_with_owners.region.code,
            "name": windfarm_with_owners.region.name,
        }
        if windfarm_with_owners.region
        else None,
        "bidzone": {
            "id": windfarm_with_owners.bidzone.id,
            "code": windfarm_with_owners.bidzone.code,
            "name": windfarm_with_owners.bidzone.name,
        }
        if windfarm_with_owners.bidzone
        else None,
        "market_balance_area": {
            "id": windfarm_with_owners.market_balance_area.id,
            "code": windfarm_with_owners.market_balance_area.code,
            "name": windfarm_with_owners.market_balance_area.name,
        }
        if windfarm_with_owners.market_balance_area
        else None,
        "control_area": {
            "id": windfarm_with_owners.control_area.id,
            "code": windfarm_with_owners.control_area.code,
            "name": windfarm_with_owners.control_area.name,
        }
        if windfarm_with_owners.control_area
        else None,
        "project": {
            "id": windfarm_with_owners.project.id,
            "code": windfarm_with_owners.project.code,
            "name": windfarm_with_owners.project.name,
        }
        if windfarm_with_owners.project
        else None,
        "windfarm_owners": [
            {
                "id": wo.id,
                "windfarm_id": wo.windfarm_id,
                "owner_id": wo.owner_id,
                "ownership_percentage": str(wo.ownership_percentage),
                "created_at": wo.created_at.isoformat(),
                "updated_at": wo.updated_at.isoformat(),
                "owner": {
                    "id": wo.owner.id,
                    "code": wo.owner.code,
                    "name": wo.owner.name,
                    "created_at": wo.owner.created_at.isoformat(),
                    "updated_at": wo.owner.updated_at.isoformat(),
                }
                if wo.owner
                else None,
            }
            for wo in windfarm_with_owners.windfarm_owners
        ],
    }

    return windfarm_dict


@router.put("/{windfarm_id}", response_model=Windfarm)
async def update_windfarm(
    windfarm_id: int, windfarm_update: WindfarmUpdate, db: AsyncSession = Depends(get_db)
):
    """Update a windfarm"""
    # Check if windfarm with same code already exists (excluding current windfarm)
    if windfarm_update.code:
        existing_windfarm = await WindfarmService.get_windfarm_by_code(db, windfarm_update.code)
        if existing_windfarm and existing_windfarm.id != windfarm_id:
            raise HTTPException(status_code=400, detail="Windfarm with this code already exists")

    updated_windfarm = await WindfarmService.update_windfarm(db, windfarm_id, windfarm_update)
    if not updated_windfarm:
        raise HTTPException(status_code=404, detail="Windfarm not found")
    return updated_windfarm


@router.delete("/{windfarm_id}", response_model=Windfarm)
async def delete_windfarm(windfarm_id: int, db: AsyncSession = Depends(get_db)):
    """Delete a windfarm"""
    try:
        deleted_windfarm = await WindfarmService.delete_windfarm(db, windfarm_id)
        if not deleted_windfarm:
            raise HTTPException(status_code=404, detail="Windfarm not found")
        return deleted_windfarm
    except Exception as e:
        # Log the error for debugging
        import logging

        logging.error(f"Error deleting windfarm {windfarm_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to delete windfarm: {str(e)}")


# Windfarm Owner endpoints
@router.get("/{windfarm_id}/owners", response_model=List[WindfarmOwner])
async def get_windfarm_owners(windfarm_id: int, db: AsyncSession = Depends(get_db)):
    """Get all owners of a windfarm"""
    # Check if windfarm exists
    windfarm = await WindfarmService.get_windfarm(db, windfarm_id)
    if not windfarm:
        raise HTTPException(status_code=404, detail="Windfarm not found")

    return await WindfarmOwnerService.get_windfarm_owners(db, windfarm_id)


@router.post("/{windfarm_id}/owners", response_model=List[WindfarmOwner], status_code=201)
async def add_windfarm_owners(
    windfarm_id: int, owners_data: List[dict], db: AsyncSession = Depends(get_db)
):
    """Add owners to a windfarm (replaces existing owners)"""
    # Check if windfarm exists
    windfarm = await WindfarmService.get_windfarm(db, windfarm_id)
    if not windfarm:
        raise HTTPException(status_code=404, detail="Windfarm not found")

    # Validate that ownership percentages sum to 100%
    if not await WindfarmOwnerService.validate_ownership_percentages(owners_data):
        raise HTTPException(
            status_code=400, detail="Ownership percentages must sum to exactly 100%"
        )

    # Delete existing owners
    await WindfarmOwnerService.delete_all_windfarm_owners(db, windfarm_id)

    # Create new owners
    return await WindfarmOwnerService.create_windfarm_owners(db, windfarm_id, owners_data)


@router.put("/{windfarm_id}/owners/{owner_id}", response_model=WindfarmOwner)
async def update_windfarm_owner(
    windfarm_id: int,
    owner_id: int,
    owner_update: WindfarmOwnerUpdate,
    db: AsyncSession = Depends(get_db),
):
    """Update a specific owner's ownership percentage"""
    updated_owner = await WindfarmOwnerService.update_windfarm_owner(db, owner_id, owner_update)
    if not updated_owner:
        raise HTTPException(status_code=404, detail="Windfarm owner relationship not found")
    return updated_owner


@router.delete("/{windfarm_id}/owners/{owner_id}", response_model=WindfarmOwner)
async def remove_windfarm_owner(
    windfarm_id: int, owner_id: int, db: AsyncSession = Depends(get_db)
):
    """Remove an owner from a windfarm"""
    deleted_owner = await WindfarmOwnerService.delete_windfarm_owner(db, owner_id)
    if not deleted_owner:
        raise HTTPException(status_code=404, detail="Windfarm owner relationship not found")
    return deleted_owner
