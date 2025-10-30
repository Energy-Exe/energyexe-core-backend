"""API endpoints for windfarm timeline and evolution data."""

from datetime import datetime, date
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, or_
from decimal import Decimal

from app.core.database import get_db
from app.models.generation_unit import GenerationUnit
from app.models.turbine_unit import TurbineUnit
from app.models.turbine_model import TurbineModel
from app.models.generation_data import GenerationData

router = APIRouter()


@router.get("/{windfarm_id}/timeline")
async def get_windfarm_timeline(
    windfarm_id: int,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    db: AsyncSession = Depends(get_db)
) -> Dict[str, Any]:
    """
    Get windfarm capacity evolution timeline showing additions and removals of units over time.

    Returns:
    - Timeline events (unit additions/removals)
    - Capacity snapshots over time
    - Current capacity breakdown
    """

    # Get all generation units for this windfarm
    gen_units_query = await db.execute(
        select(GenerationUnit)
        .where(GenerationUnit.windfarm_id == windfarm_id)
    )
    gen_units = gen_units_query.scalars().all()

    # Get all turbine units with their models for this windfarm
    turbine_units_query = await db.execute(
        select(TurbineUnit, TurbineModel)
        .join(TurbineModel, TurbineUnit.turbine_model_id == TurbineModel.id)
        .where(TurbineUnit.windfarm_id == windfarm_id)
    )
    turbine_units_data = turbine_units_query.all()

    # Collect all timeline events
    events = []

    # Process generation units
    for unit in gen_units:
        if unit.start_date:
            events.append({
                'date': unit.start_date.isoformat(),
                'type': 'addition',
                'unit_type': 'generation_unit',
                'unit_id': unit.id,
                'unit_code': unit.code,
                'unit_name': unit.name,
                'capacity_mw': float(unit.capacity_mw) if unit.capacity_mw else 0,
            })

        if unit.end_date:
            events.append({
                'date': unit.end_date.isoformat(),
                'type': 'removal',
                'unit_type': 'generation_unit',
                'unit_id': unit.id,
                'unit_code': unit.code,
                'unit_name': unit.name,
                'capacity_mw': float(unit.capacity_mw) if unit.capacity_mw else 0,
            })

    # Process turbine units
    for turbine_unit, turbine_model in turbine_units_data:
        capacity_kw = turbine_model.rated_power_kw if turbine_model.rated_power_kw else 0
        capacity_mw = float(capacity_kw) / 1000.0 if capacity_kw else 0

        if turbine_unit.start_date:
            events.append({
                'date': turbine_unit.start_date.isoformat(),
                'type': 'addition',
                'unit_type': 'turbine_unit',
                'unit_id': turbine_unit.id,
                'unit_code': turbine_unit.code,
                'turbine_model': turbine_model.model,
                'capacity_mw': capacity_mw,
            })

        if turbine_unit.end_date:
            events.append({
                'date': turbine_unit.end_date.isoformat(),
                'type': 'removal',
                'unit_type': 'turbine_unit',
                'unit_id': turbine_unit.id,
                'unit_code': turbine_unit.code,
                'turbine_model': turbine_model.model,
                'capacity_mw': capacity_mw,
            })

    # Sort events by date
    events.sort(key=lambda x: x['date'])

    # Calculate capacity snapshots over time
    # Track active units by their code to handle replacements correctly
    # Key insight: Same code = replacement, different code = addition/removal
    capacity_by_date = {}
    active_gen_units = {}  # Maps unit_code -> capacity_mw
    active_turbine_units = {}  # Maps unit_code -> capacity_mw

    for event in events:
        event_date = event['date']
        unit_code = event['unit_code']

        if event['type'] == 'addition':
            # If unit with same code exists, this is a replacement (not an addition)
            # Remove old capacity first, then add new capacity
            if event['unit_type'] == 'generation_unit':
                if unit_code in active_gen_units:
                    # This is a replacement - remove old capacity first
                    pass  # Old capacity will be overwritten below
                active_gen_units[unit_code] = event['capacity_mw']
            else:  # turbine_unit
                if unit_code in active_turbine_units:
                    # This is a replacement - remove old capacity first
                    pass  # Old capacity will be overwritten below
                active_turbine_units[unit_code] = event['capacity_mw']
        else:  # removal
            # Remove the unit from active tracking
            if event['unit_type'] == 'generation_unit':
                active_gen_units.pop(unit_code, None)
            else:  # turbine_unit
                active_turbine_units.pop(unit_code, None)

        # Calculate total capacity from active units
        gen_unit_capacity = sum(active_gen_units.values())
        turbine_capacity = sum(active_turbine_units.values())
        current_capacity = gen_unit_capacity + turbine_capacity

        # Store the final capacity for this date (will overwrite if multiple events on same day)
        capacity_by_date[event_date] = {
            'date': event_date,
            'total_capacity_mw': round(current_capacity, 2),
            'generation_unit_capacity_mw': round(gen_unit_capacity, 2),
            'turbine_capacity_mw': round(turbine_capacity, 2),
        }

    # Convert to list and sort by date
    capacity_timeline = sorted(capacity_by_date.values(), key=lambda x: x['date'])

    # Add "carry forward" points to create flat lines between events
    # This makes step charts display properly with plateaus instead of spikes
    enhanced_timeline = []
    for i, snapshot in enumerate(capacity_timeline):
        # Add the actual event point
        enhanced_timeline.append(snapshot)

        # If there's a next event, add a point right before it with current capacity
        # This creates the "flat line" effect
        if i < len(capacity_timeline) - 1:
            next_date = capacity_timeline[i + 1]['date']
            # Parse the date and subtract one day to create the carry-forward point
            from datetime import datetime, timedelta
            current_date = datetime.fromisoformat(snapshot['date'])
            next_date_obj = datetime.fromisoformat(next_date)

            # Only add carry-forward if there's a gap of more than 1 day
            if (next_date_obj - current_date).days > 1:
                carry_forward_date = (next_date_obj - timedelta(days=1)).date().isoformat()
                enhanced_timeline.append({
                    'date': carry_forward_date,
                    'total_capacity_mw': snapshot['total_capacity_mw'],
                    'generation_unit_capacity_mw': snapshot['generation_unit_capacity_mw'],
                    'turbine_capacity_mw': snapshot['turbine_capacity_mw'],
                })

    capacity_timeline = enhanced_timeline

    # Calculate current capacity breakdown
    current_gen_capacity = sum(
        float(u.capacity_mw) if u.capacity_mw else 0
        for u in gen_units
        if not u.end_date or u.end_date > date.today()
    )

    current_turbine_capacity = sum(
        float(tm.rated_power_kw / 1000.0) if tm.rated_power_kw else 0
        for tu, tm in turbine_units_data
        if not tu.end_date or tu.end_date > date.today()
    )

    return {
        'windfarm_id': windfarm_id,
        'events': events,
        'capacity_timeline': capacity_timeline,
        'current_capacity': {
            'total_mw': round(current_gen_capacity + current_turbine_capacity, 2),
            'generation_units_mw': round(current_gen_capacity, 2),
            'turbine_units_mw': round(current_turbine_capacity, 2),
            'active_generation_units': sum(1 for u in gen_units if not u.end_date or u.end_date > date.today()),
            'active_turbine_units': sum(1 for tu, _ in turbine_units_data if not tu.end_date or tu.end_date > date.today()),
        }
    }


@router.get("/{windfarm_id}/generation-timeline")
async def get_windfarm_generation_timeline(
    windfarm_id: int,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    aggregation: str = 'daily',  # 'hourly', 'daily', 'monthly'
    db: AsyncSession = Depends(get_db)
) -> Dict[str, Any]:
    """
    Get windfarm power generation data over time with capacity comparison.

    Args:
        windfarm_id: ID of the windfarm
        start_date: Start datetime for data (default: 30 days ago)
        end_date: End datetime for data (default: now)
        aggregation: Time aggregation level ('hourly', 'daily', 'monthly')
    """
    from datetime import timedelta

    if not start_date:
        start_date = datetime.now() - timedelta(days=365)  # Default to 1 year of data
    if not end_date:
        end_date = datetime.now()

    # Get all generation units for this windfarm
    gen_units_query = await db.execute(
        select(GenerationUnit.id)
        .where(GenerationUnit.windfarm_id == windfarm_id)
    )
    gen_unit_ids = [row[0] for row in gen_units_query.all()]

    # Get all turbine units for this windfarm
    turbine_units_query = await db.execute(
        select(TurbineUnit.id)
        .where(TurbineUnit.windfarm_id == windfarm_id)
    )
    turbine_unit_ids = [row[0] for row in turbine_units_query.all()]

    # Build query conditions
    # Query for data linked via generation_unit_id OR turbine_unit_id OR windfarm_id directly
    # We include windfarm_id to catch data that's linked directly to the windfarm
    unit_conditions = []

    # Add condition for generation_unit_id if we have any
    if gen_unit_ids:
        unit_conditions.append(GenerationData.generation_unit_id.in_(gen_unit_ids))

    # Add condition for turbine_unit_id if we have any
    if turbine_unit_ids:
        unit_conditions.append(GenerationData.turbine_unit_id.in_(turbine_unit_ids))

    # Also include data linked directly to windfarm (this is important!)
    unit_conditions.append(GenerationData.windfarm_id == windfarm_id)

    # Get generation data - match by generation_unit_id OR turbine_unit_id OR windfarm_id
    query = select(GenerationData).where(
        and_(
            GenerationData.hour >= start_date,
            GenerationData.hour <= end_date,
            or_(*unit_conditions) if unit_conditions else GenerationData.windfarm_id == windfarm_id
        )
    ).order_by(GenerationData.hour).limit(50000)  # Limit to prevent huge queries

    result = await db.execute(query)
    generation_records = result.scalars().all()

    # Aggregate data based on aggregation level
    aggregated_data = []

    if aggregation == 'hourly':
        # Return hourly data directly
        aggregated_data = [
            {
                'timestamp': record.hour.isoformat(),
                'generation_mwh': float(record.generation_mwh),
                'capacity_mw': float(record.capacity_mw) if record.capacity_mw else None,
                'capacity_factor': float(record.capacity_factor) if record.capacity_factor else None,
            }
            for record in generation_records
        ]

    elif aggregation == 'daily':
        # Group by day
        from collections import defaultdict
        daily_data = defaultdict(lambda: {'generation': 0, 'count': 0, 'capacity': None})

        for record in generation_records:
            day_key = record.hour.date().isoformat()
            daily_data[day_key]['generation'] += float(record.generation_mwh)
            daily_data[day_key]['count'] += 1
            if record.capacity_mw:
                daily_data[day_key]['capacity'] = float(record.capacity_mw)

        aggregated_data = [
            {
                'timestamp': day,
                'generation_mwh': round(data['generation'], 2),
                'capacity_mw': data['capacity'],
                'hours_count': data['count'],
            }
            for day, data in sorted(daily_data.items())
        ]

    elif aggregation == 'monthly':
        # Group by month
        from collections import defaultdict
        monthly_data = defaultdict(lambda: {'generation': 0, 'count': 0, 'capacity': None})

        for record in generation_records:
            month_key = record.hour.strftime('%Y-%m')
            monthly_data[month_key]['generation'] += float(record.generation_mwh)
            monthly_data[month_key]['count'] += 1
            if record.capacity_mw:
                monthly_data[month_key]['capacity'] = float(record.capacity_mw)

        aggregated_data = [
            {
                'timestamp': month,
                'generation_mwh': round(data['generation'], 2),
                'capacity_mw': data['capacity'],
                'hours_count': data['count'],
            }
            for month, data in sorted(monthly_data.items())
        ]

    return {
        'windfarm_id': windfarm_id,
        'start_date': start_date.isoformat(),
        'end_date': end_date.isoformat(),
        'aggregation': aggregation,
        'data': aggregated_data,
        'total_records': len(generation_records),
    }
