"""Read-only dry run of the 2026-08 OPEX-per-MWh fix (FIN-02 / FIN-03).

For every ``primary_asset`` windfarm with OPEX filings, prints side by side:

* OLD — the pre-fix accessor formula (Σ OPEX over all filings ≤ as-of divided by
  Σ ``reported_generation_gwh`` over only the filings that carried one);
* NEW native — ``financial_opex_metrics`` in the farm's filing currency;
* NEW EUR — the same in EUR (what FIN-02/03 compare), plus years used, basis,
  coverage and every dropped filing's reason;
* the cohort median / peer count and the FIN-02/03 verdict through the real
  ``DetectionContext`` + detector (bare-int windfarm, exactly as the nightly).

Then a cross-check: for farms with ONE usable filing at >= 99% metered coverage,
the EUR value must equal ``FinancialDataService.calculate_financial_ratios``'s
``opex_per_mwh`` for that period (the Financial tab) within rounding. Finally it
times the largest cohort end-to-end.

Usage (DATABASE_URL from .env — point it at STAGING, never prod without the tunnel):

    poetry run python scripts/debug/compare_opex_metrics.py [--as-of 2025-12-31] [--farms 7197,7200]

No writes: everything runs on a session that is rolled back at the end.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from collections import Counter
from datetime import date, datetime, time as dtime

sys.path.insert(0, ".")

# Print as we go — a long run must not lose its output to block buffering.
sys.stdout.reconfigure(line_buffering=True)


def _fmt(v, nd=1):
    return "-" if v is None else f"{v:,.{nd}f}"


async def _old_formula(db, wf_id: int, as_of: date):
    """Replicates the pre-fix ``_compute_own_opex_financials`` arithmetic."""
    from sqlalchemy import text

    rows = (
        await db.execute(
            text(
                """
                SELECT SUM(fd.total_operating_expenses) AS opex,
                       SUM(fd.reported_generation_gwh) AS gwh,
                       COUNT(*) AS n
                FROM windfarm_financial_entities wfe
                JOIN financial_data fd ON fd.financial_entity_id = wfe.financial_entity_id
                WHERE wfe.windfarm_id = :wf AND wfe.relationship_type = 'primary_asset'
                  AND EXTRACT(YEAR FROM fd.period_start)::int <= :y
                """
            ),
            {"wf": wf_id, "y": as_of.year},
        )
    ).first()
    if rows is None or rows.opex is None or not rows.gwh:
        return None, int(rows.n) if rows is not None else 0
    return float(rows.opex) / (float(rows.gwh) * 1000.0), int(rows.n)


async def main(as_of: date, only: list[int] | None) -> int:
    from sqlalchemy import text

    from app.core.database import get_session_factory
    from app.services import financial_opex_metrics as fom
    from app.services.financial_data_service import FinancialDataService
    from app.services.opportunity_schemas import fin02_onshore_opex, fin03_offshore_opex
    from app.services.opportunity_schemas.context import DetectionContext

    Session = get_session_factory()
    async with Session() as db:
        farms = (
            await db.execute(
                text(
                    """
                    SELECT DISTINCT w.id, w.name, w.location_type, w.bidzone_id
                    FROM windfarms w
                    JOIN windfarm_financial_entities wfe
                      ON wfe.windfarm_id = w.id AND wfe.relationship_type = 'primary_asset'
                    JOIN financial_data fd ON fd.financial_entity_id = wfe.financial_entity_id
                    WHERE fd.total_operating_expenses IS NOT NULL
                    ORDER BY w.id
                    """
                )
            )
        ).fetchall()
        if only:
            farms = [f for f in farms if f.id in only]
        ids = [f.id for f in farms]
        print(f"as_of={as_of}  farms with primary_asset OPEX filings: {len(ids)}\n")

        t0 = time.perf_counter()
        native = await fom.opex_metrics_for_windfarms(
            db, windfarm_ids=ids, as_of=as_of, display_currency=None, include_synthetic=False
        )
        eur = await fom.opex_metrics_for_windfarms(
            db, windfarm_ids=ids, as_of=as_of, display_currency="EUR", include_synthetic=False
        )
        print(f"helper (native + EUR) over {len(ids)} farms: {time.perf_counter() - t0:.1f}s\n")

        header = (
            f"{'id':>5} {'name':<26} {'loc':<8} {'OLD':>9} {'NEW nat':>12} {'NEW EUR':>9} "
            f"{'years':<16} {'basis':<8} {'cov%':>5} {'median':>7} {'peers':>5} {'pct':>7} {'verdict':<10} dropped"
        )
        print(header)
        print("-" * len(header))

        period_start = datetime.combine(
            as_of.replace(year=as_of.year - 1, month=1, day=1), dtime.min
        )
        period_end = datetime.combine(as_of, dtime.max)
        verdict_counts: Counter = Counter()
        drop_counts: Counter = Counter()
        inflated_before = 0
        for f in farms:
            try:
                old, n_old = await _old_formula(db, f.id, as_of)
            except Exception as exc:  # noqa: BLE001 — keep the dry run going
                await db.rollback()
                print(f"  old formula failed for {f.id}: {exc}")
                old = None
            m_nat = native.get(f.id)
            m_eur = eur.get(f.id)
            for m in (m_nat, m_eur):
                if m:
                    for d in m.dropped:
                        drop_counts[d["reason"]] += 1

            ctx = DetectionContext(
                db=db, windfarm=f.id, period_start=period_start, period_end=period_end
            )
            detector = fin02_onshore_opex if f.location_type == "onshore" else fin03_offshore_opex
            try:
                result = await detector.detect(ctx)
            except Exception as exc:  # noqa: BLE001
                result, verdict = None, f"ERR {type(exc).__name__}"
            else:
                verdict = result.severity.value if result else "none"
            verdict_counts[verdict] += 1
            median = ctx.peek(f"zone_opex_median:{f.location_type}")
            peers = ctx.peek(f"zone_opex_peer_count:{f.location_type}")
            pct = result.data_slots.get("pct_over_median") if result else None

            if old is not None and m_nat is not None and old > m_nat.opex_per_mwh * 1.5:
                inflated_before += 1
            nat_txt = f"{_fmt(m_nat.opex_per_mwh)} {m_nat.currency}" if m_nat else "-"
            years = ",".join(str(y) for y in m_eur.years_used) if m_eur else "-"
            print(
                f"{f.id:>5} {f.name[:26]:<26} {str(f.location_type)[:8]:<8} {_fmt(old):>9} {nat_txt:>12} "
                f"{_fmt(m_eur.opex_per_mwh if m_eur else None):>9} {years:<16} "
                f"{(m_eur.generation_source if m_eur else '-'):<8} {_fmt(m_eur.min_coverage_pct if m_eur else None, 0):>5} "
                f"{_fmt(median):>7} {str(peers or '-'):>5} {_fmt(pct):>7} {verdict:<10} "
                f"{[d['reason'] for d in (m_eur.dropped if m_eur else [])]}"
            )

        print()
        print(f"farms with OLD > 1.5 × NEW (were inflated): {inflated_before}")
        print(f"verdicts: {dict(verdict_counts)}")
        print(f"dropped filings by reason: {dict(drop_counts)}")

        # Cross-check vs the Financial tab (calculate_financial_ratios, EUR).
        print(
            "\ncross-check vs calculate_financial_ratios(display_currency='EUR') "
            "for single-filing farms at >= 99% metered coverage:"
        )
        svc = FinancialDataService(db)
        checked = mismatched = 0
        for wf_id, m in eur.items():
            if (
                m.rows_used != 1
                or m.generation_source != "metered"
                or (m.min_coverage_pct or 0) < 99
            ):
                continue
            responses = await svc.calculate_financial_ratios(wf_id, display_currency="EUR")
            ref = None
            for r in responses:
                for p in r.periods:
                    if p.period_end == m.period_end and p.opex_per_mwh is not None:
                        ref = float(p.opex_per_mwh)
            if ref is None:
                continue
            checked += 1
            if abs(ref - m.opex_per_mwh) > max(0.05, 0.005 * ref):
                mismatched += 1
                print(f"  MISMATCH wf {wf_id}: helper {m.opex_per_mwh:.2f} vs ratios {ref:.2f}")
        print(f"  checked={checked} mismatched={mismatched}")

        # Time the largest cohort.
        big = (
            await db.execute(
                text(
                    """
                    SELECT w.bidzone_id, w.location_type, COUNT(DISTINCT w.id) AS n
                    FROM windfarms w
                    JOIN windfarm_financial_entities wfe
                      ON wfe.windfarm_id = w.id AND wfe.relationship_type = 'primary_asset'
                    JOIN financial_data fd ON fd.financial_entity_id = wfe.financial_entity_id
                    WHERE fd.total_operating_expenses IS NOT NULL
                      AND w.bidzone_id IS NOT NULL AND w.location_type IS NOT NULL
                    GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 1
                    """
                )
            )
        ).first()
        if big is not None:
            t0 = time.perf_counter()
            cohort = await fom.opex_metrics_for_cohort(
                db,
                bidzone_id=big.bidzone_id,
                location_type=big.location_type,
                as_of=as_of,
                exclude_windfarm_id=None,
            )
            elapsed = time.perf_counter() - t0
            med = fom.cohort_median(cohort)
            print(
                f"\nlargest cohort bidzone={big.bidzone_id} {big.location_type} ({big.n} farms): "
                f"{elapsed:.1f}s, usable={len(cohort)}, median={med}"
            )

        await db.rollback()
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--as-of", default=date.today().isoformat())
    ap.add_argument("--farms", default="", help="comma-separated windfarm ids to restrict to")
    args = ap.parse_args()
    only = [int(x) for x in args.farms.split(",") if x.strip()] or None
    sys.exit(asyncio.run(main(date.fromisoformat(args.as_of), only)))
