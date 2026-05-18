"""Map page endpoints for the client-ui #44 redesign.

Three endpoints power the map view:
- `GET /map/performance-scores` — per-WF 5-bucket commercial + generation scores
- `GET /map/financial-metrics`  — per-WF EBITDA margin / rev/MWh / opex/MWh batch
- `POST /map/interpret-view`    — SSE-streamed AI narrative of the current view

All three aggregate over existing tables (performance_summaries,
generation_concentration_summaries, peer_group_aggregates, financial_data) and
the existing `/brain-agent/chat` agent. No schema changes.
"""

import asyncio
import json
import time
from typing import List, Optional

import structlog
from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.deps import get_current_active_user, get_db
from app.models.user import User
from app.schemas.map import (
    MapFinancialMetricsResponse,
    MapPerformanceScoresResponse,
    MapStatePayload,
)
from app.services.map_performance_service import MapPerformanceService

logger = structlog.get_logger(__name__)
router = APIRouter()


def _parse_ids(raw: Optional[str]) -> Optional[List[int]]:
    if not raw:
        return None
    out: List[int] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.append(int(part))
        except ValueError:
            continue
    return out or None


@router.get("/performance-scores", response_model=MapPerformanceScoresResponse)
async def get_performance_scores(
    windfarm_ids: Optional[str] = Query(
        None,
        description="Comma-separated wind-farm IDs. If omitted, all operational wind farms are scored.",
    ),
    year: int = Query(
        ..., ge=2000, le=2100, description="Calendar year of the period being scored."
    ),
    month: Optional[int] = Query(
        None, ge=1, le=12, description="Optional month for monthly aggregation."
    ),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> MapPerformanceScoresResponse:
    """Return per-windfarm 5-bucket performance scores + coverage indicators.

    The map FE calls this once per (filter set, period) and recolours markers
    from the returned `commercial_bucket` / `generation_bucket`. Buckets are
    null when the underlying metric is missing or the peer aggregate has too
    few wind farms (<3) to be statistically meaningful.
    """
    ids = _parse_ids(windfarm_ids)
    service = MapPerformanceService(db)
    return await service.get_scores(windfarm_ids=ids, year=year, month=month)


@router.get("/financial-metrics", response_model=MapFinancialMetricsResponse)
async def get_financial_metrics(
    windfarm_ids: Optional[str] = Query(
        None,
        description="Comma-separated wind-farm IDs. If omitted, all operational wind farms are returned.",
    ),
    year: int = Query(..., ge=2000, le=2100),
    display_currency: str = Query("EUR", pattern="^[A-Z]{3}$"),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> MapFinancialMetricsResponse:
    """Return EBITDA margin / revenue-per-MWh / opex-per-MWh for each WF for `year`.

    Wind farms without reported financial data return `has_data=False` so the
    FE can render them as hollow dashed markers.
    """
    ids = _parse_ids(windfarm_ids)
    service = MapPerformanceService(db)
    return await service.get_financial_metrics(
        windfarm_ids=ids, year=year, display_currency=display_currency
    )


HEARTBEAT_INTERVAL = 5


async def _with_heartbeat(aiter, interval: float):
    """Mirror of brain_agent._with_heartbeat — yields None during inactivity."""
    ait = aiter.__aiter__()
    pending_next: Optional[asyncio.Task] = None
    try:
        while True:
            if pending_next is None:
                pending_next = asyncio.ensure_future(ait.__anext__())
            done, _ = await asyncio.wait({pending_next}, timeout=interval)
            if pending_next in done:
                try:
                    event = pending_next.result()
                except StopAsyncIteration:
                    return
                pending_next = None
                yield event
            else:
                yield None
    finally:
        if pending_next is not None and not pending_next.done():
            pending_next.cancel()


@router.post("/interpret-view")
async def interpret_map_view(
    payload: MapStatePayload,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Stream an AI interpretation of the current map state via brain-agent.

    The request body is a snapshot of the visible map state. The service
    builds a deterministic, grounded prompt over that state and delegates to
    `BrainAgentService.chat`, streaming the same SSE event shape as
    `/brain-agent/chat` so the FE can reuse its consumer logic.
    """
    from app.services.brain_agent_service import BrainAgentService

    service = MapPerformanceService(db)

    scores = None
    if payload.windfarm_ids:
        try:
            score_response = await service.get_scores(
                windfarm_ids=payload.windfarm_ids,
                year=payload.period_year,
                month=payload.period_month,
            )
            scores = score_response.scores
        except Exception as exc:
            logger.warning("map_interpret_score_load_failed", error=str(exc))

    prompt = service.build_interpretation_prompt(payload, scores=scores)

    logger.info(
        "map_interpret_view",
        user=current_user.email,
        windfarms=len(payload.windfarm_ids),
        view=payload.view,
        prompt_len=len(prompt),
    )

    brain_service = BrainAgentService(db)
    source = "client" if current_user.role == "client" else "admin"

    user_name = None
    if current_user.first_name:
        user_name = current_user.first_name
        if current_user.last_name:
            user_name += f" {current_user.last_name}"

    async def event_generator():
        try:
            async for event in _with_heartbeat(
                brain_service.chat(
                    user_id=current_user.id,
                    session_id=None,
                    prompt=prompt,
                    user_name=user_name,
                    user_first_name=current_user.first_name,
                    user_company_name=current_user.company_name,
                    model=None,
                    conversation_history=None,
                    source=source,
                ),
                interval=HEARTBEAT_INTERVAL,
            ):
                if event is None:
                    yield "event: heartbeat\ndata: {}\n\n"
                else:
                    yield f"event: {event.event_type}\ndata: {json.dumps(event.data, default=str)}\n\n"
        except Exception as exc:
            import traceback

            logger.error(
                "map_interpret_stream_error",
                error=str(exc),
                traceback=traceback.format_exc(),
            )
            yield (
                "event: error\ndata: "
                + json.dumps({"message": "Internal server error", "code": "internal_error"})
                + "\n\n"
            )

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )
