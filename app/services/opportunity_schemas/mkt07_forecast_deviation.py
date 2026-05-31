"""MKT-07 · Intraday forecast deviation — M4 INACTIVE no-op (issue #106).

**STATUS: INACTIVE — data-blocked.** This schema is registered so it is *known*
to the engine, but it is flipped to ``"INACTIVE"`` in ``registry.SCHEMA_STATUS``
and is therefore skipped wholesale by ``run_for_windfarm`` — it emits **no
per-windfarm rows**. ``detect`` is a documented no-op that returns ``None``; it
exists only so the schema has a callable home and a single place to capture the
activation criteria for when the blocking data lands.

Activation blocker
==================
MKT-07 flags windfarms with a persistent **gap between forecast and actual
generation** (poor day-ahead / intraday forecasting drives balancing-cost
exposure and imbalance penalties). Detecting that requires an ingested
generation-forecast series to compare against metered actuals, but **no forecast
data exists** in the platform. Without forecasts there is nothing to deviate
from, so the schema cannot fire and stays INACTIVE.

Activation criteria + placeholder thresholds (for when data lands)
==================================================================
Once a forecast series is ingested, compute the mean absolute percentage error
(MAPE) of forecast vs actual over the detection window and tier on it::

    MAPE >= 25%  → CONFIRMED
    MAPE >= 15%  → INDICATIVE
    MAPE >=  8%  → WATCH
    otherwise    → None

Activation is gated on forecast-data ingestion and is tracked in **issue #116**
(data backlog + accuracy caveats). Flip ``SCHEMA_STATUS[SchemaCode.MKT_07]`` back
to ``"ACTIVE"`` and replace this no-op with the real detector at that point.
"""

from __future__ import annotations

from typing import Optional

from app.services.opportunity_schemas.context import DetectionContext, DetectorResult


async def detect(ctx: DetectionContext) -> Optional[DetectorResult]:
    """MKT-07 (INACTIVE): always returns ``None`` — no finding, no row.

    The schema is data-blocked (no generation-forecast series in the platform —
    see the module docstring and tracking issue #116), so this detector is a
    no-op. It is also never invoked in practice:
    ``SCHEMA_STATUS[SchemaCode.MKT_07]`` is ``"INACTIVE"`` so ``run_for_windfarm``
    skips it before calling. Returning ``None`` is the belt-and-suspenders
    guarantee of "no per-windfarm rows" even if the schema were invoked directly.
    """
    return None


__all__ = ["detect"]
