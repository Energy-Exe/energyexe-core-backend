"""Backfill script: runs performance pipeline + opportunity detection for all eligible windfarms.

Usage:
    poetry run python scripts/backfill_pipeline.py                    # All eligible windfarms
    poetry run python scripts/backfill_pipeline.py --batch-size 20    # Process 20 at a time
    poetry run python scripts/backfill_pipeline.py --pipeline-only    # Skip opportunity detection
    poetry run python scripts/backfill_pipeline.py --opportunities-only  # Skip pipeline, run opportunities only
    poetry run python scripts/backfill_pipeline.py --windfarm-ids 7176 7209  # Specific windfarms
"""

import argparse
import asyncio
import sys
import time
from datetime import datetime

# Ensure app is importable
sys.path.insert(0, ".")


async def get_eligible_windfarms(db):
    """Get windfarm IDs that have generation + weather data + capacity."""
    from sqlalchemy import text

    result = await db.execute(text("""
        SELECT w.id, w.name
        FROM windfarms w
        WHERE w.status = 'operational'
          AND w.nameplate_capacity_mw > 0
          AND EXISTS (SELECT 1 FROM weather_data wd WHERE wd.windfarm_id = w.id LIMIT 1)
          AND EXISTS (SELECT 1 FROM generation_data g WHERE g.windfarm_id = w.id AND g.is_ramp_up = false LIMIT 1)
        ORDER BY w.id
    """))
    return [(r[0], r[1]) for r in result.fetchall()]


async def run_backfill(args):
    from app.core.database import get_session_factory
    from app.services.performance_pipeline_service import PerformancePipelineService
    from app.services.opportunity_detection_service import OpportunityDetectionService

    factory = get_session_factory()

    # Determine windfarms
    if args.windfarm_ids:
        windfarm_ids = args.windfarm_ids
        print(f"[BACKFILL] Processing {len(windfarm_ids)} specified windfarms")
    else:
        async with factory() as db:
            eligible = await get_eligible_windfarms(db)
            windfarm_ids = [wf_id for wf_id, _ in eligible]
            print(f"[BACKFILL] Found {len(windfarm_ids)} eligible windfarms")

    total = len(windfarm_ids)
    batch_size = args.batch_size
    start_time = time.time()

    # ─── Phase 1: Performance Pipeline ─────────────────────────
    if not args.opportunities_only:
        print(f"\n{'='*60}")
        print(f"[PIPELINE] Starting performance pipeline for {total} windfarms (batch_size={batch_size})")
        print(f"{'='*60}")

        pipeline_ok = 0
        pipeline_fail = 0
        pipeline_errors = []

        for i in range(0, total, batch_size):
            batch = windfarm_ids[i : i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (total + batch_size - 1) // batch_size

            print(f"\n[PIPELINE] Batch {batch_num}/{total_batches} ({len(batch)} windfarms: {batch[0]}-{batch[-1]})")

            for wf_id in batch:
                wf_start = time.time()
                try:
                    async with factory() as db:
                        svc = PerformancePipelineService(db)
                        result = await svc.run_pipeline(windfarm_id=wf_id)
                        await db.commit()

                    elapsed = time.time() - wf_start
                    pc = result.get("power_curves", {})
                    years = pc.get("years", [])
                    bins = pc.get("bins_stored", 0)

                    if "error" in result or "error" in pc:
                        err = result.get("error") or pc.get("error", "unknown")
                        print(f"  [{wf_id}] SKIP ({elapsed:.1f}s): {err}")
                        pipeline_fail += 1
                        pipeline_errors.append((wf_id, err))
                    else:
                        # Summarize anomaly results
                        anom = result.get("anomaly_detection", {})
                        total_underperf = sum(
                            ar.get("underperf_hours", 0)
                            for ar in anom.values()
                            if isinstance(ar, dict) and "error" not in ar
                        )
                        total_lost = sum(
                            ar.get("lost_mwh", 0)
                            for ar in anom.values()
                            if isinstance(ar, dict) and "error" not in ar
                        )
                        print(
                            f"  [{wf_id}] OK ({elapsed:.1f}s): "
                            f"{len(years)} years, {bins} bins, "
                            f"{total_underperf} underperf hrs, {total_lost:.0f} lost MWh"
                        )
                        pipeline_ok += 1

                except Exception as e:
                    elapsed = time.time() - wf_start
                    print(f"  [{wf_id}] ERROR ({elapsed:.1f}s): {e}")
                    pipeline_fail += 1
                    pipeline_errors.append((wf_id, str(e)))

            # Batch progress
            done = min(i + batch_size, total)
            elapsed_total = time.time() - start_time
            rate = done / max(elapsed_total, 1) * 60  # per minute
            eta = (total - done) / max(rate, 0.01)
            print(
                f"  Progress: {done}/{total} ({done*100/total:.0f}%) | "
                f"{pipeline_ok} ok, {pipeline_fail} fail | "
                f"{rate:.1f}/min | ETA: {eta:.0f} min"
            )

        print(f"\n[PIPELINE] Complete: {pipeline_ok} succeeded, {pipeline_fail} failed")
        if pipeline_errors:
            print(f"[PIPELINE] Failed windfarms: {[e[0] for e in pipeline_errors[:20]]}")

    # ─── Phase 2: Opportunity Detection ────────────────────────
    if not args.pipeline_only:
        print(f"\n{'='*60}")
        print(f"[OPPORTUNITIES] Starting opportunity detection for {total} windfarms")
        print(f"{'='*60}")

        opp_start = time.time()
        total_opps = 0

        for i in range(0, total, batch_size):
            batch = windfarm_ids[i : i + batch_size]
            try:
                async with factory() as db:
                    svc = OpportunityDetectionService(db)
                    opps = await svc.detect_all(batch)
                    await db.commit()
                    total_opps += len(opps)

                done = min(i + batch_size, total)
                print(
                    f"  Batch {i//batch_size + 1}: {len(opps)} opportunities | "
                    f"Progress: {done}/{total} ({done*100/total:.0f}%)"
                )
            except Exception as e:
                print(f"  Batch {i//batch_size + 1}: ERROR: {e}")

        opp_elapsed = time.time() - opp_start
        print(f"\n[OPPORTUNITIES] Complete: {total_opps} opportunities detected in {opp_elapsed:.0f}s")

    # ─── Summary ───────────────────────────────────────────────
    total_elapsed = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"[BACKFILL] Total time: {total_elapsed/60:.1f} minutes")
    print(f"{'='*60}")


def main():
    parser = argparse.ArgumentParser(description="Backfill performance pipeline and opportunities")
    parser.add_argument("--batch-size", type=int, default=10, help="Windfarms per batch (default: 10)")
    parser.add_argument("--pipeline-only", action="store_true", help="Skip opportunity detection")
    parser.add_argument("--opportunities-only", action="store_true", help="Skip pipeline, run opportunities only")
    parser.add_argument("--windfarm-ids", type=int, nargs="+", help="Specific windfarm IDs")
    args = parser.parse_args()

    asyncio.run(run_backfill(args))


if __name__ == "__main__":
    main()
