"""MKT-05 · PPA underpricing — M4 INACTIVE no-op (issue #106).

**STATUS: INACTIVE — data-blocked.** This schema is registered so it is *known*
to the engine, but it is flipped to ``"INACTIVE"`` in ``registry.SCHEMA_STATUS``
and is therefore skipped wholesale by ``run_for_windfarm`` — it emits **no
per-windfarm rows**. ``detect`` is a documented no-op that returns ``None``; it
exists only so the schema has a callable home and a single place to capture the
activation criteria for when the blocking data lands.

Activation blocker
==================
MKT-05 flags windfarms whose **active PPA is priced below the prevailing spot /
day-ahead market** — i.e. the contract is leaving money on the table. Detecting
that requires a captured PPA strike price to compare against market prices, but
the ``ppa.ppa_price_eur_mwh`` column is **NULL across the entire fleet** (no PPA
prices have been ingested). Without a strike price there is nothing to compare,
so the schema cannot fire and stays INACTIVE.

Activation criteria + placeholder thresholds (for when data lands)
==================================================================
Once ``ppa_price_eur_mwh`` is populated, compute the spread of the windfarm's
realised spot/day-ahead capture price over its PPA strike price and tier on the
gap (spot − PPA, €/MWh)::

    spot - ppa >= 20  → CONFIRMED
    spot - ppa >= 15  → INDICATIVE
    spot - ppa >= 10  → WATCH
    otherwise         → None

Activation is gated on PPA-price ingestion and is tracked in **issue #116**
(data backlog + accuracy caveats). Flip ``SCHEMA_STATUS[SchemaCode.MKT_05]`` back
to ``"ACTIVE"`` and replace this no-op with the real detector at that point.
"""

from __future__ import annotations

from typing import Optional

from app.services.opportunity_schemas.context import DetectionContext, DetectorResult


async def detect(ctx: DetectionContext) -> Optional[DetectorResult]:
    """MKT-05 (INACTIVE): always returns ``None`` — no finding, no row.

    The schema is data-blocked (no PPA strike prices in the data model — see the
    module docstring and tracking issue #116), so this detector is a no-op. It is
    also never invoked in practice: ``SCHEMA_STATUS[SchemaCode.MKT_05]`` is
    ``"INACTIVE"`` so ``run_for_windfarm`` skips it before calling. Returning
    ``None`` is the belt-and-suspenders guarantee of "no per-windfarm rows" even
    if the schema were invoked directly.
    """
    return None


__all__ = ["detect"]
