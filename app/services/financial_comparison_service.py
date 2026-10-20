"""
Financial page of the Performance Analytics section (EPR-189).

Revenue / MWh, OPEX / MWh and EBITDA margin per financial year for every
financial entity reporting wind farms in the compared markets (Norway, the UK
and Denmark). Filtering, averages and ranking happen in the browser, so one
cached payload serves every view.

- One entry per financial entity. A combined report (one entity, several
  farms, e.g. Fosen Vind DA) is one entry: combined MW, combined generation,
  one vote in every average.
- A filing counts when it covers a full 12-month year (calendar or not), is
  not synthetic, reports revenue and starts at least a year after the latest
  COD of the entity's farms (the ratios endpoint's ramp-up rule). It belongs
  to the financial year in which most of its months fall; when a year-end
  change puts two filings in one year the later-ending one wins.
- Per-MWh uses metered generation (best single source, pooled over every
  linked farm) when it covers half the period, else the filing's reported
  generation, else none. The margin needs no generation.
- Money converts at ECB period-average rates. A filing whose EUR revenue or
  OPEX per MWh falls outside 1–1,000 is a data-entry slip: both per-MWh values
  are dropped (the margin stays).

Known divergence: ``/financial-data/ratios`` sums hourly generation over every
source, so multi-source farms can differ there.
"""

from collections import Counter
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional, Sequence, Tuple

import structlog
from sqlalchemy import bindparam, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.comparison_analytics_service import COUNTRIES, ComparisonAnalyticsService
from app.services.exchange_rate_service import ExchangeRateService
from app.services.financial_opex_metrics import (
    OPEX_MIN_COVERAGE_PCT,
    OPEX_PLAUSIBLE_EUR_PER_MWH,
    RAMP_UP_DAYS,
    GenerationStat,
    expected_months,
    generation_for_filings,
)

logger = structlog.get_logger()

CURRENCIES = ("EUR", "NOK", "GBP")
GEN_METERED = "metered"
GEN_REPORTED = "reported"


@dataclass(frozen=True)
class Filing:
    fd_id: int
    entity_id: int
    period_start: date
    period_end: date
    currency: str
    revenue: Decimal
    opex: Optional[Decimal]
    ebitda: Optional[Decimal]
    reported_gwh: Optional[Decimal]


def financial_year(period_start: date) -> int:
    """The year holding most of a 12-month period's months (a Jul–Jun year ties to its start)."""
    return period_start.year if period_start.month <= 7 else period_start.year + 1


def is_full_year(period_start: date, period_end: date) -> bool:
    days = (period_end - period_start).days + 1
    return expected_months(period_start, period_end) == 12 and 360 <= days <= 371


def eligible_filings(
    filings: Sequence[Filing], latest_cod: Dict[int, Optional[date]]
) -> Tuple[Dict[Tuple[int, int], Filing], Counter]:
    """One filing per (entity, financial year), with the drop counts per reason."""
    dropped: Counter = Counter()
    out: Dict[Tuple[int, int], Filing] = {}
    for f in filings:
        if not is_full_year(f.period_start, f.period_end):
            dropped["not_12_months"] += 1
            continue
        cod = latest_cod.get(f.entity_id)
        if cod is not None and f.period_start < cod + timedelta(days=RAMP_UP_DAYS):
            dropped["ramp_up"] += 1
            continue
        key = (f.entity_id, financial_year(f.period_start))
        kept = out.get(key)
        if kept is not None:
            dropped["duplicate_year"] += 1
            if (kept.period_end, kept.fd_id) > (f.period_end, f.fd_id):
                continue
        out[key] = f
    return out, dropped


def generation_mwh(
    f: Filing, stat: Optional[GenerationStat]
) -> Tuple[Optional[Decimal], Optional[str]]:
    """Per-MWh denominator: metered with enough coverage, else reported, else none."""
    if stat is not None and stat.gen_mwh > 0 and stat.coverage_pct >= OPEX_MIN_COVERAGE_PCT:
        return stat.gen_mwh, GEN_METERED
    if f.reported_gwh is not None and f.reported_gwh > 0:
        return f.reported_gwh * 1000, GEN_REPORTED
    return None, None


def _r1(v: Optional[Decimal]) -> Optional[float]:
    return None if v is None else round(float(v), 1)


def filing_values(
    f: Filing,
    mwh: Optional[Decimal],
    rates: Dict[str, Optional[Decimal]],
) -> Tuple[Dict[str, Optional[float]], Dict[str, Optional[float]], Optional[float], bool]:
    """(revenue/MWh by currency, OPEX/MWh by currency, margin %, implausible).

    ``rates`` holds the filing's native → target rate per target currency.
    """
    margin = _r1(f.ebitda / f.revenue * 100) if f.ebitda is not None and f.revenue else None
    rev = {c: None for c in CURRENCIES}
    opex = {c: None for c in CURRENCIES}
    if mwh is None:
        return rev, opex, margin, False

    eur = rates.get("EUR")
    if eur is not None:
        lo, hi = OPEX_PLAUSIBLE_EUR_PER_MWH
        checks = [f.revenue] + ([f.opex] if f.opex is not None else [])
        if any(not lo <= float(v * eur / mwh) <= hi for v in checks):
            return rev, opex, margin, True

    for c in CURRENCIES:
        rate = rates.get(c)
        if rate is None:
            continue
        rev[c] = _r1(f.revenue * rate / mwh)
        opex[c] = _r1(f.opex * rate / mwh) if f.opex is not None else None
    return rev, opex, margin, False


def most_common(values: Sequence[Optional[str]]) -> Optional[str]:
    present = [v for v in values if v]
    return Counter(present).most_common(1)[0][0] if present else None


class FinancialComparisonService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def _farms(self) -> List[Any]:
        """(entity, farm) pairs for non-deleted farms in the compared markets."""
        result = await self.db.execute(
            text(
                """
                SELECT DISTINCT fe.id AS entity_id, fe.name AS entity_name,
                       w.id, w.name, w.nameplate_capacity_mw, w.location_type,
                       w.commercial_operational_date AS cod,
                       c.code AS country_code, b.name AS bidzone_name
                FROM windfarm_financial_entities wfe
                JOIN financial_entities fe ON fe.id = wfe.financial_entity_id
                JOIN windfarms w ON w.id = wfe.windfarm_id
                JOIN countries c ON c.id = w.country_id
                LEFT JOIN bidzones b ON b.id = w.bidzone_id
                WHERE w.is_deleted = false AND c.code IN :countries
                ORDER BY fe.id, w.name
                """
            ).bindparams(bindparam("countries", expanding=True)),
            {"countries": list(COUNTRIES)},
        )
        return list(result.fetchall())

    async def _linked_farm_counts(self, entity_ids: List[int]) -> Dict[int, int]:
        """Live farms linked to each entity, the coverage denominator. A deleted
        placeholder (e.g. Raggovidda 2, 0 MW, no data) would halve the coverage
        of the farm that does report."""
        result = await self.db.execute(
            text(
                """
                SELECT wfe.financial_entity_id, COUNT(DISTINCT wfe.windfarm_id) AS n
                FROM windfarm_financial_entities wfe
                JOIN windfarms w ON w.id = wfe.windfarm_id
                WHERE wfe.financial_entity_id IN :ids AND w.is_deleted = false
                GROUP BY wfe.financial_entity_id
                """
            ).bindparams(bindparam("ids", expanding=True)),
            {"ids": entity_ids},
        )
        return {r.financial_entity_id: int(r.n) for r in result.fetchall()}

    async def _filings(self, entity_ids: List[int]) -> List[Filing]:
        result = await self.db.execute(
            text(
                """
                SELECT id, financial_entity_id, period_start, period_end, currency,
                       total_revenue, total_operating_expenses, ebitda,
                       reported_generation_gwh
                FROM financial_data
                WHERE financial_entity_id IN :ids
                  AND NOT is_synthetic AND total_revenue IS NOT NULL
                """
            ).bindparams(bindparam("ids", expanding=True)),
            {"ids": entity_ids},
        )
        return [
            Filing(
                fd_id=r.id,
                entity_id=r.financial_entity_id,
                period_start=r.period_start,
                period_end=r.period_end,
                currency=str(r.currency).upper(),
                revenue=Decimal(r.total_revenue),
                opex=Decimal(r.total_operating_expenses)
                if r.total_operating_expenses is not None
                else None,
                ebitda=Decimal(r.ebitda) if r.ebitda is not None else None,
                reported_gwh=Decimal(r.reported_generation_gwh)
                if r.reported_generation_gwh is not None
                else None,
            )
            for r in result.fetchall()
        ]

    async def get_financial(self) -> Dict[str, Any]:
        await self.db.execute(text("SET LOCAL statement_timeout = '30s'"))
        rows = await self._farms()
        by_entity: Dict[int, List[Any]] = {}
        for r in rows:
            by_entity.setdefault(r.entity_id, []).append(r)
        entity_ids = list(by_entity)
        if not entity_ids:
            return {"years": [], "entries": []}

        latest_cod = {
            e: max((f.cod for f in farms if f.cod is not None), default=None)
            for e, farms in by_entity.items()
        }
        kept, dropped = eligible_filings(await self._filings(entity_ids), latest_cod)
        filings = {f.fd_id: f for f in kept.values()}

        linked = await self._linked_farm_counts(entity_ids)
        generation = await generation_for_filings(
            self.db, filings, {fd: linked.get(f.entity_id, 1) for fd, f in filings.items()}
        )
        rates = await ExchangeRateService(self.db).rates_for_periods(
            {(f.currency, f.period_start, f.period_end) for f in filings.values()}, CURRENCIES
        )
        owners = await ComparisonAnalyticsService(self.db).owners([r.id for r in rows])

        values: Dict[Tuple[int, int], Dict[str, Any]] = {}
        for key, f in kept.items():
            mwh, basis = generation_mwh(f, generation.get(f.fd_id))
            if mwh is None:
                dropped["no_generation"] += 1
            fx = {c: rates.get((f.currency, f.period_start, f.period_end, c)) for c in CURRENCIES}
            rev, opex, margin, implausible = filing_values(f, mwh, fx)
            if implausible:
                dropped["implausible"] += 1
                basis = None
            values[key] = {"f": f, "rev": rev, "opex": opex, "margin": margin, "basis": basis}

        used_years = [
            y
            for (_, y), v in values.items()
            if v["margin"] is not None or v["rev"]["EUR"] is not None
        ]
        if not used_years:
            return {"years": [], "entries": []}
        years = list(range(min(used_years), max(used_years) + 1))

        entries = []
        for entity_id, farms in by_entity.items():
            cells = [values.get((entity_id, y)) for y in years]
            if not any(c and (c["margin"] is not None or any(c["rev"].values())) for c in cells):
                continue
            combined = len(farms) > 1
            types = {f.location_type for f in farms if f.location_type}
            entries.append(
                {
                    "id": entity_id,
                    "name": farms[0].entity_name if combined else farms[0].name,
                    "entity_name": farms[0].entity_name,
                    "combined": combined,
                    "farms": [
                        {"id": f.id, "name": f.name, "capacity_mw": f.nameplate_capacity_mw}
                        for f in farms
                    ],
                    "capacity_mw": round(sum(f.nameplate_capacity_mw or 0 for f in farms), 1)
                    or None,
                    "country": most_common([f.country_code for f in farms]),
                    "zone": most_common([f.bidzone_name for f in farms]),
                    "type": types.pop() if len(types) == 1 else ("mixed" if types else None),
                    # Same shape as the Comparison payload, so the page reuses its filters.
                    "owners": [
                        {"name": n}
                        for n in sorted({o["name"] for f in farms for o in owners.get(f.id, [])})
                    ],
                    "rev": {
                        c: [cell["rev"][c] if cell else None for cell in cells] for c in CURRENCIES
                    },
                    "opex": {
                        c: [cell["opex"][c] if cell else None for cell in cells] for c in CURRENCIES
                    },
                    "margin": [cell["margin"] if cell else None for cell in cells],
                    "periods": [
                        f"{cell['f'].period_start:%Y-%m}/{cell['f'].period_end:%Y-%m}"
                        if cell
                        else None
                        for cell in cells
                    ],
                    "gen_basis": [cell["basis"] if cell else None for cell in cells],
                    "currency": [cell["f"].currency if cell else None for cell in cells],
                }
            )
        entries.sort(key=lambda e: e["name"])

        logger.info(
            "financial_comparison_built",
            entities=len(by_entity),
            entries=len(entries),
            filings_used=len(kept),
            dropped=dict(dropped),
        )
        return {"years": years, "entries": entries}
