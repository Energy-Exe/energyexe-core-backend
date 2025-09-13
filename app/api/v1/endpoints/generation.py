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
    
    sources = ['ENTSOE', 'ELEXON', 'EIA', 'TAIPOWER']
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
    sources: Optional[List[str]] = Query(None),
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
    
    # Default to all sources if none specified
    if not sources:
        sources = ['ENTSOE', 'ELEXON', 'EIA', 'TAIPOWER']
    
    # Get start and end dates for the month
    days_in_month = monthrange(year, month)[1]
    start_date = datetime(year, month, 1)
    end_date = datetime(year, month, days_in_month, 23, 59, 59)
    
    availability = {}
    
    # For each day in the month
    for day in range(1, days_in_month + 1):
        date_str = f"{year:04d}-{month:02d}-{day:02d}"
        day_start = datetime(year, month, day)
        day_end = datetime(year, month, day, 23, 59, 59)
        
        day_sources = []
        total_records = 0
        total_quality = 0.0
        quality_count = 0
        
        for source in sources:
            # Check if raw data exists for this day and source
            query = select(func.count(GenerationDataRaw.id)).where(
                and_(
                    GenerationDataRaw.source == source,
                    GenerationDataRaw.period_start >= day_start,
                    GenerationDataRaw.period_start < day_end + timedelta(days=1)
                )
            )
            
            result = await db.execute(query)
            count = result.scalar()
            
            if count > 0:
                day_sources.append(source)
                total_records += count
                
                # Get quality scores for processed data
                quality_query = select(func.avg(GenerationData.quality_score)).where(
                    and_(
                        GenerationData.source == source,
                        GenerationData.hour >= day_start,
                        GenerationData.hour < day_end + timedelta(days=1)
                    )
                )
                quality_result = await db.execute(quality_query)
                avg_quality = quality_result.scalar()
                if avg_quality:
                    total_quality += float(avg_quality)
                    quality_count += 1
        
        if day_sources:
            availability[date_str] = {
                'sources': day_sources,
                'recordCount': total_records,
                'quality': (total_quality / quality_count) if quality_count > 0 else None
            }
    
    # Calculate summary
    days_with_data = len(availability)
    coverage = (days_with_data / days_in_month) * 100 if days_in_month > 0 else 0
    
    return {
        'availability': availability,
        'summary': {
            'totalDays': days_in_month,
            'daysWithData': days_with_data,
            'coverage': coverage,
            'sources': sources
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