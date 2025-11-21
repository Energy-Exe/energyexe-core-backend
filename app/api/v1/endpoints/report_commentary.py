"""API endpoints for LLM-generated report commentary."""

from datetime import datetime
from typing import List, Optional
from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, func, and_
from sqlalchemy.ext.asyncio import AsyncSession
import structlog

from app.core.database import get_db
from app.core.deps import get_current_user
from app.models.user import User
from app.models.report_commentary import ReportCommentary
from app.schemas.llm_commentary import (
    CommentaryGenerationRequest,
    CommentaryResponse,
    CommentarySummary,
    BulkCommentaryGenerationRequest,
    BulkCommentaryGenerationResponse,
    CommentaryUpdateRequest,
    LLMUsageStats
)
from app.services.llm_commentary_service import LLMCommentaryService

logger = structlog.get_logger(__name__)

router = APIRouter()


@router.post("/windfarms/{windfarm_id}/generate-commentary", response_model=CommentaryResponse)
async def generate_commentary(
    windfarm_id: int,
    request: CommentaryGenerationRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Generate commentary for a specific section.

    This endpoint generates LLM commentary for a single report section.
    If a cached version exists (within 24 hours by default), it will be returned
    unless regenerate=true is specified.
    """
    try:
        service = LLMCommentaryService(db)

        # For now, we'll pass minimal data - should be enhanced to fetch actual report data
        data = {
            'windfarm_id': windfarm_id,
            'section_type': request.section_type,
            'start_date': request.start_date.strftime('%Y-%m-%d'),
            'end_date': request.end_date.strftime('%Y-%m-%d'),
        }

        commentary = await service.generate_commentary(
            windfarm_id=windfarm_id,
            section_type=request.section_type,
            data=data,
            date_range=(request.start_date, request.end_date),
            regenerate=request.regenerate,
            temperature=request.temperature,
            max_tokens=request.max_tokens
        )

        return commentary

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(
            "commentary_generation_failed",
            windfarm_id=windfarm_id,
            section_type=request.section_type,
            error=str(e)
        )
        raise HTTPException(status_code=500, detail="Commentary generation failed")


@router.post("/windfarms/{windfarm_id}/generate-all-commentary", response_model=BulkCommentaryGenerationResponse)
async def generate_all_commentary(
    windfarm_id: int,
    request: BulkCommentaryGenerationRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Generate commentary for multiple sections in parallel.

    This is more efficient than calling the single-section endpoint multiple times.
    """
    try:
        service = LLMCommentaryService(db)

        # Prepare report data (simplified for now)
        report_data = {
            'windfarm_id': windfarm_id,
            'date_range_start': request.start_date,
            'date_range_end': request.end_date,
        }

        start_time = datetime.utcnow()
        commentaries_dict = await service.generate_all_sections(
            windfarm_id=windfarm_id,
            report_data=report_data,
            selected_sections=request.section_types,
            regenerate=request.regenerate
        )
        duration = (datetime.utcnow() - start_time).total_seconds()

        # Calculate totals
        commentaries = list(commentaries_dict.values())
        total_cost = sum(c.generation_cost_usd for c in commentaries)

        # Track any failures
        errors = {}
        for section in request.section_types:
            if section not in commentaries_dict:
                errors[section] = "Generation failed"

        return BulkCommentaryGenerationResponse(
            windfarm_id=windfarm_id,
            total_sections=len(request.section_types),
            successful=len(commentaries),
            failed=len(errors),
            total_cost_usd=total_cost,
            total_duration_seconds=Decimal(str(duration)),
            commentaries=commentaries,
            errors=errors if errors else None
        )

    except Exception as e:
        logger.error(
            "bulk_commentary_generation_failed",
            windfarm_id=windfarm_id,
            error=str(e)
        )
        raise HTTPException(status_code=500, detail="Bulk commentary generation failed")


@router.get("/windfarms/{windfarm_id}/commentary/{section_type}", response_model=CommentaryResponse)
async def get_commentary(
    windfarm_id: int,
    section_type: str,
    start_date: datetime = Query(...),
    end_date: datetime = Query(...),
    db: AsyncSession = Depends(get_db)
):
    """
    Get existing commentary for a section.

    Returns the most recent commentary for the specified section and date range.
    """
    stmt = select(ReportCommentary).where(
        and_(
            ReportCommentary.windfarm_id == windfarm_id,
            ReportCommentary.section_type == section_type,
            ReportCommentary.date_range_start == start_date,
            ReportCommentary.date_range_end == end_date,
            ReportCommentary.is_current == True
        )
    ).order_by(ReportCommentary.created_at.desc())

    result = await db.execute(stmt)
    commentary = result.scalar_one_or_none()

    if not commentary:
        raise HTTPException(status_code=404, detail="Commentary not found")

    return commentary


@router.get("/windfarms/{windfarm_id}/commentaries", response_model=List[CommentarySummary])
async def list_commentaries(
    windfarm_id: int,
    current_only: bool = Query(True, description="Only return current versions"),
    db: AsyncSession = Depends(get_db)
):
    """
    List all commentaries for a windfarm.

    Returns a lightweight summary of each commentary.
    """
    stmt = select(ReportCommentary).where(
        ReportCommentary.windfarm_id == windfarm_id
    )

    if current_only:
        stmt = stmt.where(ReportCommentary.is_current == True)

    stmt = stmt.order_by(
        ReportCommentary.section_type,
        ReportCommentary.created_at.desc()
    )

    result = await db.execute(stmt)
    commentaries = result.scalars().all()

    # Convert to summaries
    summaries = []
    for c in commentaries:
        word_count = len(c.commentary_text.split())
        summaries.append(
            CommentarySummary(
                id=c.id,
                section_type=c.section_type,
                status=c.status,
                created_at=c.created_at,
                word_count=word_count,
                generation_cost_usd=c.generation_cost_usd
            )
        )

    return summaries


@router.patch("/windfarms/{windfarm_id}/commentary/{commentary_id}", response_model=CommentaryResponse)
async def update_commentary(
    windfarm_id: int,
    commentary_id: int,
    request: CommentaryUpdateRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Update existing commentary.

    Allows manual editing of commentary text or changing status.
    """
    stmt = select(ReportCommentary).where(
        and_(
            ReportCommentary.id == commentary_id,
            ReportCommentary.windfarm_id == windfarm_id
        )
    )

    result = await db.execute(stmt)
    commentary = result.scalar_one_or_none()

    if not commentary:
        raise HTTPException(status_code=404, detail="Commentary not found")

    # Update fields
    commentary.commentary_text = request.commentary_text
    if request.status:
        commentary.status = request.status
    commentary.updated_at = datetime.utcnow()

    await db.commit()
    await db.refresh(commentary)

    logger.info(
        "commentary_updated",
        commentary_id=commentary_id,
        windfarm_id=windfarm_id,
        updated_by=current_user.id
    )

    return commentary


@router.delete("/windfarms/{windfarm_id}/commentary/{commentary_id}")
async def delete_commentary(
    windfarm_id: int,
    commentary_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Delete a commentary.
    """
    stmt = select(ReportCommentary).where(
        and_(
            ReportCommentary.id == commentary_id,
            ReportCommentary.windfarm_id == windfarm_id
        )
    )

    result = await db.execute(stmt)
    commentary = result.scalar_one_or_none()

    if not commentary:
        raise HTTPException(status_code=404, detail="Commentary not found")

    await db.delete(commentary)
    await db.commit()

    logger.info(
        "commentary_deleted",
        commentary_id=commentary_id,
        windfarm_id=windfarm_id,
        deleted_by=current_user.id
    )

    return {"message": "Commentary deleted successfully"}


@router.get("/usage-stats", response_model=LLMUsageStats)
async def get_usage_stats(
    start_date: Optional[datetime] = Query(None),
    end_date: Optional[datetime] = Query(None),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get LLM usage statistics.

    Returns aggregate statistics about LLM commentary generation costs and usage.
    """
    stmt = select(ReportCommentary)

    if start_date:
        stmt = stmt.where(ReportCommentary.created_at >= start_date)
    if end_date:
        stmt = stmt.where(ReportCommentary.created_at <= end_date)

    result = await db.execute(stmt)
    commentaries = result.scalars().all()

    if not commentaries:
        return LLMUsageStats(
            total_commentaries=0,
            total_cost_usd=Decimal('0'),
            total_tokens_input=0,
            total_tokens_output=0,
            avg_cost_per_commentary=Decimal('0'),
            cost_by_section_type={},
            commentaries_by_provider={}
        )

    # Calculate aggregates
    total_cost = sum(c.generation_cost_usd for c in commentaries)
    total_tokens_input = sum(c.token_count_input for c in commentaries)
    total_tokens_output = sum(c.token_count_output for c in commentaries)

    # Group by section type
    cost_by_section = {}
    for c in commentaries:
        if c.section_type not in cost_by_section:
            cost_by_section[c.section_type] = Decimal('0')
        cost_by_section[c.section_type] += c.generation_cost_usd

    # Group by provider
    by_provider = {}
    for c in commentaries:
        by_provider[c.llm_provider] = by_provider.get(c.llm_provider, 0) + 1

    avg_cost = total_cost / len(commentaries) if commentaries else Decimal('0')

    return LLMUsageStats(
        total_commentaries=len(commentaries),
        total_cost_usd=total_cost,
        total_tokens_input=total_tokens_input,
        total_tokens_output=total_tokens_output,
        avg_cost_per_commentary=avg_cost,
        cost_by_section_type=cost_by_section,
        commentaries_by_provider=by_provider
    )
