from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.schemas.substation import (
    Substation,
    SubstationCreate,
    SubstationCreateWithOwners,
    SubstationUpdate,
)
from app.schemas.substation_owner import (
    SubstationOwner,
    SubstationOwnerUpdate,
    SubstationOwnerWithDetails,
)
from app.services.substation import SubstationService
from app.services.substation_owner import SubstationOwnerService

router = APIRouter()


@router.get("/", response_model=List[Substation])
async def get_substations(
    skip: int = Query(0, ge=0),
    limit: int = Query(1000, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
):
    """Get all substations with pagination"""
    return await SubstationService.get_substations(db, skip=skip, limit=limit)


@router.get("/search", response_model=List[Substation])
async def search_substations(
    q: str = Query(..., min_length=1),
    skip: int = Query(0, ge=0),
    limit: int = Query(1000, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
):
    """Search substations by name"""
    return await SubstationService.search_substations(db, query=q, skip=skip, limit=limit)


@router.get("/{substation_id}", response_model=Substation)
async def get_substation(substation_id: int, db: AsyncSession = Depends(get_db)):
    """Get a specific substation by ID"""
    substation = await SubstationService.get_substation(db, substation_id)
    if not substation:
        raise HTTPException(status_code=404, detail="Substation not found")
    return substation


@router.get("/code/{code}", response_model=Substation)
async def get_substation_by_code(code: str, db: AsyncSession = Depends(get_db)):
    """Get a substation by its code"""
    substation = await SubstationService.get_substation_by_code(db, code)
    if not substation:
        raise HTTPException(status_code=404, detail="Substation not found")
    return substation


@router.post("/", response_model=Substation, status_code=201)
async def create_substation(substation: SubstationCreate, db: AsyncSession = Depends(get_db)):
    """Create a new substation"""
    # Check if substation with same code already exists
    existing_substation = await SubstationService.get_substation_by_code(db, substation.code)
    if existing_substation:
        raise HTTPException(status_code=400, detail="Substation with this code already exists")

    return await SubstationService.create_substation(db, substation)


@router.put("/{substation_id}", response_model=Substation)
async def update_substation(
    substation_id: int, substation_update: SubstationUpdate, db: AsyncSession = Depends(get_db)
):
    """Update a substation"""
    # Check if substation with same code already exists (excluding current substation)
    if substation_update.code:
        existing_substation = await SubstationService.get_substation_by_code(
            db, substation_update.code
        )
        if existing_substation and existing_substation.id != substation_id:
            raise HTTPException(status_code=400, detail="Substation with this code already exists")

    updated_substation = await SubstationService.update_substation(
        db, substation_id, substation_update
    )
    if not updated_substation:
        raise HTTPException(status_code=404, detail="Substation not found")
    return updated_substation


@router.get("/{substation_id}/with-owners")
async def get_substation_with_owners(substation_id: int, db: AsyncSession = Depends(get_db)):
    """Get a specific substation by ID with owners"""
    substation = await SubstationService.get_substation_with_owners(db, substation_id)
    if not substation:
        raise HTTPException(status_code=404, detail="Substation not found")

    # Convert ORM objects to dictionaries for JSON serialization
    substation_dict = {
        "id": substation.id,
        "code": substation.code,
        "name": substation.name,
        "substation_type": substation.substation_type,
        "lat": substation.lat,
        "lng": substation.lng,
        "current_type": substation.current_type,
        "array_cable_voltage_kv": substation.array_cable_voltage_kv,
        "export_cable_voltage_kv": substation.export_cable_voltage_kv,
        "transformer_capacity_mva": substation.transformer_capacity_mva,
        "commissioning_date": substation.commissioning_date.isoformat()
        if substation.commissioning_date
        else None,
        "operational_date": substation.operational_date.isoformat()
        if substation.operational_date
        else None,
        "notes": substation.notes,
        "address": substation.address,
        "postal_code": substation.postal_code,
        "created_at": substation.created_at.isoformat(),
        "updated_at": substation.updated_at.isoformat(),
        "substation_owners": [
            {
                "id": so.id,
                "substation_id": so.substation_id,
                "owner_id": so.owner_id,
                "ownership_percentage": str(so.ownership_percentage),
                "created_at": so.created_at.isoformat(),
                "updated_at": so.updated_at.isoformat(),
                "owner": {
                    "id": so.owner.id,
                    "code": so.owner.code,
                    "name": so.owner.name,
                    "created_at": so.owner.created_at.isoformat(),
                    "updated_at": so.owner.updated_at.isoformat(),
                }
                if so.owner
                else None,
            }
            for so in substation.substation_owners
        ],
    }

    return substation_dict


@router.post("/with-owners", status_code=201)
async def create_substation_with_owners(
    substation_data: SubstationCreateWithOwners, db: AsyncSession = Depends(get_db)
):
    """Create a new substation with owners"""
    # Check if substation with same code already exists
    existing_substation = await SubstationService.get_substation_by_code(
        db, substation_data.substation.code
    )
    if existing_substation:
        raise HTTPException(status_code=400, detail="Substation with this code already exists")

    # Create the substation first
    substation = await SubstationService.create_substation(db, substation_data.substation)

    # Then create the ownership relationships
    await SubstationOwnerService.create_substation_owners(
        db, substation.id, substation_data.owners
    )

    # Return the substation with owners
    substation_with_owners = await SubstationService.get_substation_with_owners(db, substation.id)

    # Convert ORM objects to dictionaries for JSON serialization
    substation_dict = {
        "id": substation_with_owners.id,
        "code": substation_with_owners.code,
        "name": substation_with_owners.name,
        "substation_type": substation_with_owners.substation_type,
        "lat": substation_with_owners.lat,
        "lng": substation_with_owners.lng,
        "current_type": substation_with_owners.current_type,
        "array_cable_voltage_kv": substation_with_owners.array_cable_voltage_kv,
        "export_cable_voltage_kv": substation_with_owners.export_cable_voltage_kv,
        "transformer_capacity_mva": substation_with_owners.transformer_capacity_mva,
        "commissioning_date": substation_with_owners.commissioning_date.isoformat()
        if substation_with_owners.commissioning_date
        else None,
        "operational_date": substation_with_owners.operational_date.isoformat()
        if substation_with_owners.operational_date
        else None,
        "notes": substation_with_owners.notes,
        "address": substation_with_owners.address,
        "postal_code": substation_with_owners.postal_code,
        "created_at": substation_with_owners.created_at.isoformat(),
        "updated_at": substation_with_owners.updated_at.isoformat(),
        "substation_owners": [
            {
                "id": so.id,
                "substation_id": so.substation_id,
                "owner_id": so.owner_id,
                "ownership_percentage": str(so.ownership_percentage),
                "created_at": so.created_at.isoformat(),
                "updated_at": so.updated_at.isoformat(),
                "owner": {
                    "id": so.owner.id,
                    "code": so.owner.code,
                    "name": so.owner.name,
                    "created_at": so.owner.created_at.isoformat(),
                    "updated_at": so.owner.updated_at.isoformat(),
                }
                if so.owner
                else None,
            }
            for so in substation_with_owners.substation_owners
        ],
    }

    return substation_dict


@router.delete("/{substation_id}", response_model=Substation)
async def delete_substation(substation_id: int, db: AsyncSession = Depends(get_db)):
    """Delete a substation"""
    deleted_substation = await SubstationService.delete_substation(db, substation_id)
    if not deleted_substation:
        raise HTTPException(status_code=404, detail="Substation not found")
    return deleted_substation


# Substation Owner endpoints
@router.get("/{substation_id}/owners", response_model=List[SubstationOwnerWithDetails])
async def get_substation_owners(substation_id: int, db: AsyncSession = Depends(get_db)):
    """Get all owners of a substation"""
    # Check if substation exists
    substation = await SubstationService.get_substation(db, substation_id)
    if not substation:
        raise HTTPException(status_code=404, detail="Substation not found")

    return await SubstationOwnerService.get_substation_owners(db, substation_id)


@router.post("/{substation_id}/owners", response_model=List[SubstationOwnerWithDetails], status_code=201)
async def add_substation_owners(
    substation_id: int, owners_data: List[dict], db: AsyncSession = Depends(get_db)
):
    """Add owners to a substation (replaces existing owners)"""
    # Check if substation exists
    substation = await SubstationService.get_substation(db, substation_id)
    if not substation:
        raise HTTPException(status_code=404, detail="Substation not found")

    # Delete existing owners
    await SubstationOwnerService.delete_all_substation_owners(db, substation_id)

    # Create new owners
    await SubstationOwnerService.create_substation_owners(db, substation_id, owners_data)

    # Return owners with details
    return await SubstationOwnerService.get_substation_owners(db, substation_id)


@router.put("/{substation_id}/owners/{owner_id}", response_model=SubstationOwnerWithDetails)
async def update_substation_owner(
    substation_id: int,
    owner_id: int,
    owner_update: SubstationOwnerUpdate,
    db: AsyncSession = Depends(get_db),
):
    """Update a specific owner's ownership percentage"""
    updated_owner = await SubstationOwnerService.update_substation_owner(db, owner_id, owner_update)
    if not updated_owner:
        raise HTTPException(status_code=404, detail="Substation owner relationship not found")
    return updated_owner


@router.delete("/{substation_id}/owners/{owner_id}", response_model=SubstationOwner)
async def remove_substation_owner(
    substation_id: int, owner_id: int, db: AsyncSession = Depends(get_db)
):
    """Remove an owner from a substation"""
    deleted_owner = await SubstationOwnerService.delete_substation_owner(db, owner_id)
    if not deleted_owner:
        raise HTTPException(status_code=404, detail="Substation owner relationship not found")
    return deleted_owner
