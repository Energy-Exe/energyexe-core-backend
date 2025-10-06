from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.deps import get_current_active_user, get_db
from app.models.user import User
from app.schemas.state import State, StateCreate, StateUpdate, StateWithCountry
from app.services.country import country as country_service
from app.services.state import state as state_service

router = APIRouter()


@router.get("/", response_model=List[StateWithCountry])
async def read_states(
    db: AsyncSession = Depends(get_db),
    skip: int = Query(0, ge=0),
    limit: int = Query(1000, ge=1, le=1000),
    current_user: User = Depends(get_current_active_user),
) -> List[StateWithCountry]:
    """
    Retrieve states.
    """
    states = await state_service.get_multi(db, skip=skip, limit=limit)
    return states


@router.get("/search", response_model=List[StateWithCountry])
async def search_states(
    q: str = Query(..., min_length=1),
    db: AsyncSession = Depends(get_db),
    skip: int = Query(0, ge=0),
    limit: int = Query(1000, ge=1, le=1000),
    current_user: User = Depends(get_current_active_user),
) -> List[StateWithCountry]:
    """
    Search states by name or code.
    """
    states = await state_service.search(db, query=q, skip=skip, limit=limit)
    return states


@router.get("/country/{country_id}", response_model=List[StateWithCountry])
async def read_states_by_country(
    *,
    db: AsyncSession = Depends(get_db),
    country_id: int,
    skip: int = Query(0, ge=0),
    limit: int = Query(1000, ge=1, le=1000),
    current_user: User = Depends(get_current_active_user),
) -> List[StateWithCountry]:
    """
    Get states by country ID.
    """
    # Verify country exists
    country = await country_service.get(db, id=country_id)
    if not country:
        raise HTTPException(status_code=404, detail="Country not found")

    states = await state_service.get_by_country(db, country_id=country_id, skip=skip, limit=limit)
    return states


@router.post("/", response_model=StateWithCountry)
async def create_state(
    *,
    db: AsyncSession = Depends(get_db),
    state_in: StateCreate,
    current_user: User = Depends(get_current_active_user),
) -> StateWithCountry:
    """
    Create new state.
    """
    # Check if country exists
    country = await country_service.get(db, id=state_in.country_id)
    if not country:
        raise HTTPException(status_code=400, detail="Country not found")

    # Check if state with same code already exists
    existing = await state_service.get_by_code(db, code=state_in.code)
    if existing:
        raise HTTPException(
            status_code=400, detail=f"State with code {state_in.code} already exists"
        )

    state = await state_service.create(db, obj_in=state_in)
    return await state_service.get_with_country(db, id=state.id)


@router.get("/{state_id}", response_model=StateWithCountry)
async def read_state(
    *,
    db: AsyncSession = Depends(get_db),
    state_id: int,
    current_user: User = Depends(get_current_active_user),
) -> StateWithCountry:
    """
    Get state by ID.
    """
    state = await state_service.get_with_country(db, id=state_id)
    if not state:
        raise HTTPException(status_code=404, detail="State not found")
    return state


@router.get("/code/{state_code}", response_model=StateWithCountry)
async def read_state_by_code(
    *,
    db: AsyncSession = Depends(get_db),
    state_code: str,
    current_user: User = Depends(get_current_active_user),
) -> StateWithCountry:
    """
    Get state by code.
    """
    state = await state_service.get_by_code(db, code=state_code)
    if not state:
        raise HTTPException(status_code=404, detail="State not found")
    return await state_service.get_with_country(db, id=state.id)


@router.put("/{state_id}", response_model=StateWithCountry)
async def update_state(
    *,
    db: AsyncSession = Depends(get_db),
    state_id: int,
    state_in: StateUpdate,
    current_user: User = Depends(get_current_active_user),
) -> StateWithCountry:
    """
    Update a state.
    """
    state = await state_service.get(db, id=state_id)
    if not state:
        raise HTTPException(status_code=404, detail="State not found")

    # Check if updating country to one that exists
    if state_in.country_id and state_in.country_id != state.country_id:
        country = await country_service.get(db, id=state_in.country_id)
        if not country:
            raise HTTPException(status_code=400, detail="Country not found")

    # Check if updating code to one that already exists
    if state_in.code and state_in.code != state.code:
        existing = await state_service.get_by_code(db, code=state_in.code)
        if existing:
            raise HTTPException(
                status_code=400, detail=f"State with code {state_in.code} already exists"
            )

    state = await state_service.update(db, db_obj=state, obj_in=state_in)
    return await state_service.get_with_country(db, id=state.id)


@router.delete("/{state_id}", response_model=StateWithCountry)
async def delete_state(
    *,
    db: AsyncSession = Depends(get_db),
    state_id: int,
    current_user: User = Depends(get_current_active_user),
) -> StateWithCountry:
    """
    Delete a state.
    """
    state = await state_service.get_with_country(db, id=state_id)
    if not state:
        raise HTTPException(status_code=404, detail="State not found")
    await state_service.delete(db, id=state_id)
    return state
