"""API endpoints for unified generation data management."""

from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import Integer

from app.core.deps import get_current_active_user, get_db
from app.models.user import User
from app.services.unified_generation_service import UnifiedGenerationService
from app.schemas.generation import (
    GenerationDataResponse,
    ProcessingRequest,
    ProcessingResponse,
    ManualOverrideRequest
)

router = APIRouter()


@router.post("/raw/import-csv")
async def import_csv_data(
    source: str = Query(..., description="Data source: ELEXON, ENTSOE, EIA, TAIPOWER"),
    file: UploadFile = File(...),
    limit: Optional[int] = Query(None, description="Limit number of rows to import"),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
) -> Dict[str, Any]:
    """Import generation data from CSV file."""
    
    # Save uploaded file temporarily
    import tempfile
    import os
    
    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp:
        content = await file.read()
        tmp.write(content)
        tmp_path = tmp.name
    
    try:
        service = UnifiedGenerationService(db)
        
        if source == 'ELEXON':
            result = await service.import_elexon_csv(tmp_path, limit)
        else:
            return {
                'success': False,
                'message': f'CSV import not yet implemented for {source}'
            }
        
        return result
        
    finally:
        # Clean up temp file
        os.unlink(tmp_path)


@router.post("/raw/store")
async def store_raw_data(
    source: str,
    data: List[Dict],
    source_type: str = 'api',
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
) -> Dict[str, Any]:
    """Store raw generation data from API."""
    
    service = UnifiedGenerationService(db)
    result = await service.store_raw_data(source, data, source_type)
    return result


@router.post("/process")
async def process_to_hourly(
    source: str,
    identifier: Optional[str] = None,
    generation_unit_id: Optional[int] = None,
    windfarm_id: Optional[int] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
) -> Dict[str, Any]:
    """Process raw data to hourly resolution."""
    
    service = UnifiedGenerationService(db)
    result = await service.process_to_hourly(
        source=source,
        identifier=identifier,
        start_date=start_date,
        end_date=end_date,
        generation_unit_id=generation_unit_id,
        windfarm_id=windfarm_id
    )
    return result


@router.get("/hourly")
async def get_hourly_data(
    generation_unit_id: Optional[int] = None,
    windfarm_id: Optional[int] = None,
    source: Optional[str] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    min_quality_score: float = 0.0,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
) -> List[Dict[str, Any]]:
    """Get hourly generation data."""
    
    service = UnifiedGenerationService(db)
    data = await service.get_hourly_data(
        generation_unit_id=generation_unit_id,
        windfarm_id=windfarm_id,
        source=source,
        start_date=start_date,
        end_date=end_date,
        min_quality_score=min_quality_score
    )
    
    # Convert to dict for response
    return [
        {
            'hour': record.hour.isoformat(),
            'generation_mwh': float(record.generation_mwh),
            'generation_unit_id': record.generation_unit_id,
            'windfarm_id': record.windfarm_id,
            'source': record.source,
            'quality_score': float(record.quality_score) if record.quality_score else None,
            'quality_flag': record.quality_flag,
            'is_manual_override': record.is_manual_override
        }
        for record in data
    ]


@router.post("/override")
async def manual_override(
    hour: datetime,
    generation_unit_id: int,
    source: str,
    new_value: float,
    reason: str,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
) -> Dict[str, Any]:
    """Manually override a generation value."""
    
    service = UnifiedGenerationService(db)
    result = await service.manual_override(
        hour=hour,
        generation_unit_id=generation_unit_id,
        source=source,
        new_value=new_value,
        reason=reason,
        user=current_user
    )
    
    if not result['success']:
        raise HTTPException(status_code=404, detail=result['message'])
    
    return result


@router.get("/raw")
async def get_raw_data(
    source: Optional[str] = None,
    identifier: Optional[str] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    limit: int = Query(50, le=1000),
    offset: int = Query(0, ge=0),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
) -> Dict[str, Any]:
    """Get raw generation data with pagination."""
    
    from app.models.generation_data import GenerationDataRaw
    from sqlalchemy import select, func
    
    # Build query
    query = select(GenerationDataRaw)
    count_query = select(func.count(GenerationDataRaw.id))
    
    # Apply filters
    if source:
        query = query.where(GenerationDataRaw.source == source)
        count_query = count_query.where(GenerationDataRaw.source == source)
    if identifier:
        query = query.where(GenerationDataRaw.identifier == identifier)
        count_query = count_query.where(GenerationDataRaw.identifier == identifier)
    if start_date:
        query = query.where(GenerationDataRaw.period_start >= start_date)
        count_query = count_query.where(GenerationDataRaw.period_start >= start_date)
    if end_date:
        query = query.where(GenerationDataRaw.period_end <= end_date)
        count_query = count_query.where(GenerationDataRaw.period_end <= end_date)
    
    # Get total count
    total_result = await db.execute(count_query)
    total = total_result.scalar()
    
    # Apply pagination
    query = query.order_by(GenerationDataRaw.period_start.desc()).limit(limit).offset(offset)
    
    result = await db.execute(query)
    records = result.scalars().all()
    
    return {
        'data': [
            {
                'id': record.id,
                'source': record.source,
                'identifier': record.identifier,
                'period_start': record.period_start.isoformat() if record.period_start else None,
                'period_end': record.period_end.isoformat() if record.period_end else None,
                'period_type': record.period_type,
                'value_extracted': float(record.value_extracted) if record.value_extracted else None,
                'unit': record.unit,
                'data': record.data,
                'created_at': record.created_at.isoformat()
            }
            for record in records
        ],
        'pagination': {
            'total': total,
            'limit': limit,
            'offset': offset,
            'hasMore': offset + limit < total
        }
    }


@router.get("/stats")
async def get_generation_stats(
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
) -> Dict[str, Any]:
    """Get generation data statistics for all sources."""
    
    from app.models.generation_data import GenerationDataRaw, GenerationData
    from sqlalchemy import select, func
    
    sources = ['ENTSOE', 'ELEXON', 'EIA', 'TAIPOWER', 'NVE', 'ENERGISTYRELSEN']
    source_stats = []
    
    for source in sources:
        # Get raw data stats
        raw_stats = await db.execute(
            select(
                func.count(GenerationDataRaw.id).label('total_records'),
                func.min(GenerationDataRaw.period_start).label('min_date'),
                func.max(GenerationDataRaw.period_end).label('max_date'),
                func.count(func.distinct(GenerationDataRaw.identifier)).label('identifiers')
            ).where(GenerationDataRaw.source == source)
        )
        raw_data = raw_stats.first()
        
        # Get processed data quality stats
        quality_stats = await db.execute(
            select(
                func.avg(GenerationData.quality_score).label('avg_quality')
            ).where(GenerationData.source == source)
        )
        quality_data = quality_stats.first()
        
        # Calculate coverage (simplified - days with data / total days in range)
        coverage = 100.0  # Default to 100% if no data
        if raw_data.min_date and raw_data.max_date:
            total_days = (raw_data.max_date - raw_data.min_date).days + 1
            days_with_data = await db.execute(
                select(func.count(func.distinct(func.date(GenerationDataRaw.period_start))))
                .where(GenerationDataRaw.source == source)
            )
            days_count = days_with_data.scalar()
            if total_days > 0:
                coverage = (days_count / total_days) * 100
        
        source_stats.append({
            'source': source,
            'totalRecords': raw_data.total_records or 0,
            'dateRange': {
                'start': raw_data.min_date.isoformat() if raw_data.min_date else None,
                'end': raw_data.max_date.isoformat() if raw_data.max_date else None
            },
            'coverage': min(coverage, 100.0),
            'avgQuality': float(quality_data.avg_quality or 0.8),  # Default to 0.8 if no data
            'lastUpdate': datetime.utcnow().isoformat(),
            'identifiers': raw_data.identifiers or 0
        })
    
    return {
        'sources': source_stats
    }


@router.get("/availability")
async def get_availability(
    year: Optional[int] = Query(None, description="Year for availability check"),
    month: Optional[int] = Query(None, description="Month for availability check (1-12)"),
    sources: Optional[str] = Query(None, description="Comma-separated list of sources or single source"),
    windfarm_id: Optional[int] = None,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
) -> Dict[str, Any]:
    """Get data availability for specified month."""

    from app.models.generation_data import GenerationDataRaw, GenerationData
    from sqlalchemy import select, func, and_
    from calendar import monthrange

    # Default to current month if not provided
    if year is None or month is None:
        now = datetime.utcnow()
        year = year or now.year
        month = month or now.month

    # Validate month range
    if month < 1 or month > 12:
        raise HTTPException(status_code=400, detail="Month must be between 1 and 12")

    # Parse sources - handle both comma-separated string and list
    if sources:
        # If it's a comma-separated string, split it
        if isinstance(sources, str):
            sources_list = [s.strip() for s in sources.split(',') if s.strip()]
        else:
            sources_list = sources
    else:
        # Default to all sources if none specified
        sources_list = ['ENTSOE', 'ELEXON', 'EIA', 'TAIPOWER', 'NVE', 'ENERGISTYRELSEN']
    
    # Get start and end dates for the month
    from datetime import date
    import logging

    logger = logging.getLogger(__name__)

    days_in_month = monthrange(year, month)[1]
    start_date = date(year, month, 1)
    end_date = date(year, month, days_in_month)

    logger.info(f"Availability query: year={year}, month={month}, start={start_date}, end={end_date}, sources={sources_list}")

    availability = {}

    # Optimized: Get all data for the month in one query per source
    for source in sources_list:
        # Get daily counts for the entire month in one query
        daily_query = select(
            func.date(GenerationDataRaw.period_start).label('date'),
            func.count(GenerationDataRaw.id).label('count')
        ).where(
            and_(
                GenerationDataRaw.source == source,
                GenerationDataRaw.period_start >= start_date,
                GenerationDataRaw.period_start <= end_date
            )
        ).group_by(func.date(GenerationDataRaw.period_start))

        result = await db.execute(daily_query)
        daily_data = result.all()

        logger.info(f"{source}: Found {len(daily_data)} days of data")

        # Process results
        for row in daily_data:
            date_str = row.date.strftime('%Y-%m-%d')

            if date_str not in availability:
                availability[date_str] = {
                    'sources': [],
                    'recordCount': 0,
                    'quality': None
                }

            availability[date_str]['sources'].append(source)
            availability[date_str]['recordCount'] += row.count
    
    # Calculate summary
    days_with_data = len(availability)
    coverage = (days_with_data / days_in_month) * 100 if days_in_month > 0 else 0

    logger.info(f"Final result: {days_with_data} days with data out of {days_in_month} ({coverage:.1f}% coverage)")

    return {
        'availability': availability,
        'summary': {
            'totalDays': days_in_month,
            'daysWithData': days_with_data,
            'coverage': coverage,
            'sources': sources_list
        }
    }


@router.get("/quality-stats")
async def get_quality_stats(
    source: Optional[str] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    group_by: str = Query('daily', regex='^(hourly|daily|monthly)$'),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
) -> Dict[str, Any]:
    """Get quality statistics for generation data."""
    
    from app.models.generation_data import GenerationData
    from sqlalchemy import select, func
    
    # Build base query
    query = select(
        func.date_trunc(group_by, GenerationData.hour).label('period'),
        func.avg(GenerationData.quality_score).label('avg_quality'),
        func.min(GenerationData.quality_score).label('min_quality'),
        func.max(GenerationData.quality_score).label('max_quality'),
        func.avg(GenerationData.completeness).label('completeness'),
        func.count(GenerationData.id).label('record_count'),
        func.sum(func.cast(GenerationData.is_manual_override, Integer)).label('manual_overrides')
    )
    
    # Apply filters
    if source:
        query = query.where(GenerationData.source == source)
    if start_date:
        query = query.where(GenerationData.hour >= start_date)
    if end_date:
        query = query.where(GenerationData.hour <= end_date)
    
    # Group by period
    query = query.group_by('period').order_by('period')
    
    result = await db.execute(query)
    stats = result.all()
    
    # Calculate overall summary
    summary_query = select(
        func.avg(GenerationData.quality_score).label('overall_avg_quality'),
        func.count(GenerationData.id).label('total_records'),
        func.sum(func.cast(GenerationData.is_manual_override, Integer)).label('total_manual_overrides')
    )
    
    if source:
        summary_query = summary_query.where(GenerationData.source == source)
    if start_date:
        summary_query = summary_query.where(GenerationData.hour >= start_date)
    if end_date:
        summary_query = summary_query.where(GenerationData.hour <= end_date)
    
    summary_result = await db.execute(summary_query)
    summary = summary_result.first()
    
    return {
        'stats': [
            {
                'period': stat.period.isoformat(),
                'avgQuality': float(stat.avg_quality) if stat.avg_quality else 0,
                'minQuality': float(stat.min_quality) if stat.min_quality else 0,
                'maxQuality': float(stat.max_quality) if stat.max_quality else 0,
                'completeness': float(stat.completeness) if stat.completeness else 0,
                'recordCount': stat.record_count,
                'manualOverrides': stat.manual_overrides or 0
            }
            for stat in stats
        ],
        'summary': {
            'overallAvgQuality': float(summary.overall_avg_quality) if summary.overall_avg_quality else 0,
            'totalRecords': summary.total_records or 0,
            'totalManualOverrides': summary.total_manual_overrides or 0
        }
    }


# Portfolio-level generation endpoints

@router.get("/portfolio/stats")
async def get_portfolio_generation_stats(
    start_date: datetime = Query(..., description="Start date for statistics"),
    end_date: datetime = Query(..., description="End date for statistics"),
    portfolio_id: Optional[int] = Query(None, description="Filter by portfolio ID"),
    country_id: Optional[int] = Query(None, description="Filter by country ID"),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
) -> Dict[str, Any]:
    """
    Get aggregated generation statistics across all accessible wind farms.

    Returns total MWh, average capacity factor, farm count, and top performers.
    """
    from app.models.generation_data import GenerationData
    from app.models.windfarm import Windfarm
    from app.models.portfolio import PortfolioItem
    from sqlalchemy import select, func, and_, desc
    from sqlalchemy.orm import selectinload

    # Build base query for generation aggregation
    conditions = [
        GenerationData.hour >= start_date,
        GenerationData.hour < end_date + timedelta(days=1),
    ]

    # If portfolio_id is specified, filter by portfolio items
    if portfolio_id:
        windfarm_ids_query = select(PortfolioItem.windfarm_id).where(
            PortfolioItem.portfolio_id == portfolio_id
        )
        windfarm_ids_result = await db.execute(windfarm_ids_query)
        windfarm_ids = [row[0] for row in windfarm_ids_result.fetchall()]
        if windfarm_ids:
            conditions.append(GenerationData.windfarm_id.in_(windfarm_ids))
        else:
            # Empty portfolio - return zeros
            return {
                'total_mwh': 0,
                'avg_capacity_factor': 0,
                'farm_count': 0,
                'record_count': 0,
                'avg_quality_score': 0,
                'top_performers': [],
                'bottom_performers': [],
            }

    # If country_id is specified, filter windfarms by country
    if country_id:
        windfarm_ids_query = select(Windfarm.id).where(Windfarm.country_id == country_id)
        windfarm_ids_result = await db.execute(windfarm_ids_query)
        country_windfarm_ids = [row[0] for row in windfarm_ids_result.fetchall()]
        if country_windfarm_ids:
            conditions.append(GenerationData.windfarm_id.in_(country_windfarm_ids))

    # Get total generation stats
    stats_query = select(
        func.sum(GenerationData.generation_mwh).label('total_mwh'),
        func.avg(GenerationData.quality_score).label('avg_quality'),
        func.count(func.distinct(GenerationData.windfarm_id)).label('farm_count'),
        func.count(GenerationData.id).label('record_count'),
    ).where(and_(*conditions))

    stats_result = await db.execute(stats_query)
    stats = stats_result.first()

    # Get per-farm stats for ranking
    farm_stats_query = select(
        GenerationData.windfarm_id,
        func.sum(GenerationData.generation_mwh).label('total_mwh'),
        func.avg(GenerationData.quality_score).label('avg_quality'),
    ).where(
        and_(*conditions)
    ).group_by(
        GenerationData.windfarm_id
    ).order_by(
        desc('total_mwh')
    )

    farm_stats_result = await db.execute(farm_stats_query)
    farm_stats = farm_stats_result.fetchall()

    # Get windfarm details for top/bottom performers
    windfarm_ids = [row.windfarm_id for row in farm_stats]
    windfarm_details = {}
    if windfarm_ids:
        windfarms_query = select(Windfarm).where(Windfarm.id.in_(windfarm_ids))
        windfarms_result = await db.execute(windfarms_query)
        for wf in windfarms_result.scalars().all():
            windfarm_details[wf.id] = {
                'id': wf.id,
                'name': wf.name,
                'code': wf.code,
                'capacity_mw': float(wf.nameplate_capacity_mw) if wf.nameplate_capacity_mw else 0,
            }

    # Calculate capacity factors
    hours_in_period = (end_date - start_date).total_seconds() / 3600

    top_performers = []
    bottom_performers = []

    for row in farm_stats[:10]:  # Top 10
        wf = windfarm_details.get(row.windfarm_id, {})
        capacity = wf.get('capacity_mw', 0)
        cf = (float(row.total_mwh) / (capacity * hours_in_period) * 100) if capacity and hours_in_period else 0
        top_performers.append({
            'windfarm_id': row.windfarm_id,
            'name': wf.get('name', f'Farm {row.windfarm_id}'),
            'total_mwh': round(float(row.total_mwh), 2),
            'capacity_factor': round(cf, 1),
            'avg_quality': round(float(row.avg_quality or 0), 2),
        })

    for row in farm_stats[-10:] if len(farm_stats) > 10 else []:  # Bottom 10
        wf = windfarm_details.get(row.windfarm_id, {})
        capacity = wf.get('capacity_mw', 0)
        cf = (float(row.total_mwh) / (capacity * hours_in_period) * 100) if capacity and hours_in_period else 0
        bottom_performers.append({
            'windfarm_id': row.windfarm_id,
            'name': wf.get('name', f'Farm {row.windfarm_id}'),
            'total_mwh': round(float(row.total_mwh), 2),
            'capacity_factor': round(cf, 1),
            'avg_quality': round(float(row.avg_quality or 0), 2),
        })

    # Calculate portfolio-level capacity factor
    total_capacity_mw = sum(wf.get('capacity_mw', 0) for wf in windfarm_details.values())
    avg_capacity_factor = 0
    if total_capacity_mw and hours_in_period and stats.total_mwh:
        avg_capacity_factor = (float(stats.total_mwh) / (total_capacity_mw * hours_in_period)) * 100

    return {
        'total_mwh': round(float(stats.total_mwh or 0), 2),
        'avg_capacity_factor': round(avg_capacity_factor, 1),
        'farm_count': stats.farm_count or 0,
        'record_count': stats.record_count or 0,
        'avg_quality_score': round(float(stats.avg_quality or 0), 2),
        'total_capacity_mw': round(total_capacity_mw, 2),
        'top_performers': top_performers,
        'bottom_performers': bottom_performers,
    }


@router.get("/portfolio/timeseries")
async def get_portfolio_generation_timeseries(
    start_date: datetime = Query(..., description="Start date"),
    end_date: datetime = Query(..., description="End date"),
    aggregation: str = Query("daily", regex="^(hourly|daily|weekly|monthly)$"),
    portfolio_id: Optional[int] = Query(None, description="Filter by portfolio ID"),
    country_id: Optional[int] = Query(None, description="Filter by country ID"),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
) -> Dict[str, Any]:
    """
    Get portfolio generation timeseries with breakdown by wind farm.

    Returns timeseries data aggregated by the specified period (hourly/daily/weekly/monthly).
    """
    from app.models.generation_data import GenerationData
    from app.models.windfarm import Windfarm
    from app.models.portfolio import PortfolioItem
    from sqlalchemy import select, func, and_, text

    # Build base conditions
    conditions = [
        GenerationData.hour >= start_date,
        GenerationData.hour < end_date + timedelta(days=1),
    ]

    # Filter by portfolio or country
    windfarm_filter_ids = None
    if portfolio_id:
        windfarm_ids_query = select(PortfolioItem.windfarm_id).where(
            PortfolioItem.portfolio_id == portfolio_id
        )
        windfarm_ids_result = await db.execute(windfarm_ids_query)
        windfarm_filter_ids = [row[0] for row in windfarm_ids_result.fetchall()]
        if windfarm_filter_ids:
            conditions.append(GenerationData.windfarm_id.in_(windfarm_filter_ids))

    if country_id:
        windfarm_ids_query = select(Windfarm.id).where(Windfarm.country_id == country_id)
        windfarm_ids_result = await db.execute(windfarm_ids_query)
        country_windfarm_ids = [row[0] for row in windfarm_ids_result.fetchall()]
        if country_windfarm_ids:
            conditions.append(GenerationData.windfarm_id.in_(country_windfarm_ids))

    # Map aggregation to PostgreSQL date_trunc
    agg_map = {
        'hourly': 'hour',
        'daily': 'day',
        'weekly': 'week',
        'monthly': 'month',
    }
    trunc_period = agg_map.get(aggregation, 'day')

    # Get total timeseries
    total_query = select(
        func.date_trunc(trunc_period, GenerationData.hour).label('period'),
        func.sum(GenerationData.generation_mwh).label('total_mwh'),
        func.avg(GenerationData.quality_score).label('avg_quality'),
        func.count(func.distinct(GenerationData.windfarm_id)).label('farm_count'),
    ).where(
        and_(*conditions)
    ).group_by(
        func.date_trunc(trunc_period, GenerationData.hour)
    ).order_by('period')

    total_result = await db.execute(total_query)
    total_data = total_result.fetchall()

    # Get per-farm breakdown
    farm_query = select(
        func.date_trunc(trunc_period, GenerationData.hour).label('period'),
        GenerationData.windfarm_id,
        func.sum(GenerationData.generation_mwh).label('total_mwh'),
    ).where(
        and_(*conditions)
    ).group_by(
        func.date_trunc(trunc_period, GenerationData.hour),
        GenerationData.windfarm_id,
    ).order_by('period', GenerationData.windfarm_id)

    farm_result = await db.execute(farm_query)
    farm_data = farm_result.fetchall()

    # Get windfarm names
    unique_windfarm_ids = set(row.windfarm_id for row in farm_data)
    windfarm_names = {}
    if unique_windfarm_ids:
        windfarms_query = select(Windfarm.id, Windfarm.name).where(
            Windfarm.id.in_(unique_windfarm_ids)
        )
        windfarms_result = await db.execute(windfarms_query)
        for row in windfarms_result.fetchall():
            windfarm_names[row.id] = row.name

    # Build response
    timeseries = []
    for row in total_data:
        period_str = row.period.isoformat() if row.period else None
        timeseries.append({
            'period': period_str,
            'total_mwh': round(float(row.total_mwh or 0), 2),
            'avg_quality': round(float(row.avg_quality or 0), 2),
            'farm_count': row.farm_count or 0,
        })

    # Build per-farm breakdown
    farm_breakdown = {}
    for row in farm_data:
        period_str = row.period.isoformat() if row.period else None
        wf_name = windfarm_names.get(row.windfarm_id, f'Farm {row.windfarm_id}')

        if wf_name not in farm_breakdown:
            farm_breakdown[wf_name] = []

        farm_breakdown[wf_name].append({
            'period': period_str,
            'mwh': round(float(row.total_mwh or 0), 2),
        })

    return {
        'aggregation': aggregation,
        'start_date': start_date.isoformat(),
        'end_date': end_date.isoformat(),
        'timeseries': timeseries,
        'by_farm': farm_breakdown,
    }