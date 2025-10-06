from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.deps import get_current_active_user, get_db
from app.models.user import User
from app.schemas.country import Country, CountryCreate, CountryUpdate
from app.services.country import country as country_service

router = APIRouter()


@router.get("/", response_model=List[Country])
async def read_countries(
    db: AsyncSession = Depends(get_db),
    skip: int = Query(0, ge=0),
    limit: int = Query(1000, ge=1, le=1000),
    current_user: User = Depends(get_current_active_user),
) -> List[Country]:
    """
    Retrieve countries.
    """
    countries = await country_service.get_multi(db, skip=skip, limit=limit)
    return countries


@router.get("/search", response_model=List[Country])
async def search_countries(
    q: str = Query(..., min_length=1),
    db: AsyncSession = Depends(get_db),
    skip: int = Query(0, ge=0),
    limit: int = Query(1000, ge=1, le=1000),
    current_user: User = Depends(get_current_active_user),
) -> List[Country]:
    """
    Search countries by name or code.
    """
    countries = await country_service.search(db, query=q, skip=skip, limit=limit)
    return countries


@router.post("/", response_model=Country)
async def create_country(
    *,
    db: AsyncSession = Depends(get_db),
    country_in: CountryCreate,
    current_user: User = Depends(get_current_active_user),
) -> Country:
    """
    Create new country.
    """
    # Check if country with same code already exists
    existing = await country_service.get_by_code(db, code=country_in.code)
    if existing:
        raise HTTPException(
            status_code=400, detail=f"Country with code {country_in.code} already exists"
        )

    country = await country_service.create(db, obj_in=country_in)
    return country


@router.get("/{country_id}", response_model=Country)
async def read_country(
    *,
    db: AsyncSession = Depends(get_db),
    country_id: int,
    current_user: User = Depends(get_current_active_user),
) -> Country:
    """
    Get country by ID.
    """
    country = await country_service.get(db, id=country_id)
    if not country:
        raise HTTPException(status_code=404, detail="Country not found")
    return country


@router.get("/code/{country_code}", response_model=Country)
async def read_country_by_code(
    *,
    db: AsyncSession = Depends(get_db),
    country_code: str,
    current_user: User = Depends(get_current_active_user),
) -> Country:
    """
    Get country by code.
    """
    country = await country_service.get_by_code(db, code=country_code)
    if not country:
        raise HTTPException(status_code=404, detail="Country not found")
    return country


@router.put("/{country_id}", response_model=Country)
async def update_country(
    *,
    db: AsyncSession = Depends(get_db),
    country_id: int,
    country_in: CountryUpdate,
    current_user: User = Depends(get_current_active_user),
) -> Country:
    """
    Update a country.
    """
    country = await country_service.get(db, id=country_id)
    if not country:
        raise HTTPException(status_code=404, detail="Country not found")

    # Check if updating code to one that already exists
    if country_in.code and country_in.code != country.code:
        existing = await country_service.get_by_code(db, code=country_in.code)
        if existing:
            raise HTTPException(
                status_code=400, detail=f"Country with code {country_in.code} already exists"
            )

    country = await country_service.update(db, db_obj=country, obj_in=country_in)
    return country


@router.delete("/{country_id}", response_model=Country)
async def delete_country(
    *,
    db: AsyncSession = Depends(get_db),
    country_id: int,
    current_user: User = Depends(get_current_active_user),
) -> Country:
    """
    Delete a country.
    """
    country = await country_service.get(db, id=country_id)
    if not country:
        raise HTTPException(status_code=404, detail="Country not found")
    country = await country_service.delete(db, id=country_id)
    return country
