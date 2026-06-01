#!/usr/bin/env python3
"""Resilient sharded opportunity-detection runner for parallel backfills.

Splits the operational windfarm fleet into ``--total-shards`` disjoint slices and
runs detection for exactly one slice (``--shard-index``). Several copies — one per
shard index — run concurrently against the same database safely: detection
supersedes/commits **per windfarm** and the opportunities unique index is the
partial ``(windfarm_id, schema_code) WHERE status=ACTIVE``, so disjoint slices
never collide.

Resilience (why this does NOT share one session across the slice)
================================================================
Each windfarm runs in its **own fresh session**. A long parallel run inevitably
hits the occasional dropped RDS connection; SQLAlchemy's async ``rollback()``
cannot recover a half-open connection (``greenlet_spawn``/"can't reconnect until
invalid transaction is rolled back"), which — with a single shared session —
takes down the shard's entire remaining tail. With a session per windfarm, a
dropped connection costs at most that one windfarm: the next windfarm checks out
a fresh connection (``pool_pre_ping`` discards the dead one) and the loop
continues. Every exception (including a failed rollback) is caught per windfarm.

Sharding is round-robin (``ids[index::total]``) so slow data-rich windfarms
spread evenly across shards.

``--only-missing-since TS`` (top-up mode): skip windfarms already refreshed since
the ISO timestamp ``TS`` (any opportunity row with ``updated_at > TS``). Used to
complete a fleet run that died partway without re-doing the windfarms already
done.

Usage:
    python scripts/jobs/run_detection_shard.py --total-shards 6 --shard-index 0
    python scripts/jobs/run_detection_shard.py --total-shards 1 --shard-index 0 \
        --only-missing-since '2026-05-31 21:10:00'
"""

import argparse
import asyncio
import sys
from datetime import datetime
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent))

from sqlalchemy import select, text

from app.core.database import get_session_factory
from app.models.windfarm import Windfarm
from app.services.opportunity_detection_service import OpportunityDetectionService


async def _operational_ids(SF) -> list:
    async with SF() as db:
        result = await db.execute(
            select(Windfarm.id).where(Windfarm.status == "operational").order_by(Windfarm.id)
        )
        return [r[0] for r in result.fetchall()]


async def _filter_already_done(SF, ids: list, since: str) -> list:
    """Drop ids that already have an opportunity row updated since ``since``.

    ``since`` is parsed to a ``datetime`` — asyncpg requires a datetime object for
    a timestamp bind param, not a string.
    """
    since_dt = datetime.fromisoformat(since)
    async with SF() as db:
        result = await db.execute(
            text(
                "SELECT DISTINCT windfarm_id FROM opportunities "
                "WHERE updated_at > :since AND windfarm_id = ANY(:ids)"
            ),
            {"since": since_dt, "ids": ids},
        )
        done = {r[0] for r in result.fetchall()}
    return [i for i in ids if i not in done]


async def run(total_shards: int, shard_index: int, period_months: int, only_missing_since) -> int:
    SF = get_session_factory()
    all_ids = await _operational_ids(SF)
    my_ids = all_ids[shard_index::total_shards]

    if only_missing_since:
        before = len(my_ids)
        my_ids = await _filter_already_done(SF, my_ids, only_missing_since)
        print(f"top-up: {before} in slice, {len(my_ids)} still missing since {only_missing_since}")

    print(
        f"=== shard {shard_index}/{total_shards}: {len(my_ids)} of {len(all_ids)} "
        f"operational windfarms (period_months={period_months}) ==="
    )
    if not my_ids:
        print("Nothing to do.")
        return 0

    succeeded = failed = 0
    failed_ids = []
    for n, wf_id in enumerate(my_ids, 1):
        # Fresh session per windfarm: isolates any dropped-connection failure to
        # this one windfarm instead of poisoning the rest of the slice.
        try:
            async with SF() as db:
                svc = OpportunityDetectionService(db)
                await svc.detect_all([wf_id], period_months=period_months)
            succeeded += 1
        except Exception as exc:  # noqa: BLE001 - never let one windfarm kill the run
            failed += 1
            failed_ids.append(wf_id)
            print(f"  [wf {wf_id}] FAILED: {type(exc).__name__}: {exc}")
        if n % 25 == 0:
            print(f"  progress: {n}/{len(my_ids)} (ok={succeeded} fail={failed})")

    print(
        f"✅ shard {shard_index} done: succeeded={succeeded} failed={failed}"
        + (f" failed_ids={failed_ids}" if failed_ids else "")
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run one resilient shard of opportunity detection."
    )
    parser.add_argument("--total-shards", type=int, required=True)
    parser.add_argument("--shard-index", type=int, required=True)
    parser.add_argument("--period-months", type=int, default=24)
    parser.add_argument(
        "--only-missing-since",
        type=str,
        default=None,
        help="Skip windfarms with an opportunity row updated since this ISO timestamp (top-up mode).",
    )
    args = parser.parse_args()
    if args.total_shards < 1:
        parser.error("--total-shards must be >= 1")
    if not (0 <= args.shard_index < args.total_shards):
        parser.error("--shard-index must be in [0, total_shards)")

    return asyncio.run(
        run(args.total_shards, args.shard_index, args.period_months, args.only_missing_since)
    )


if __name__ == "__main__":
    sys.exit(main())
