"""OPEX-per-MWh metrics — the single definition shared by the FIN-02 / FIN-03
detectors and the report builders.

Why this module exists
======================
Before this module the FIN-02/03 detectors divided ``financial_data.
total_operating_expenses`` by ``financial_data.reported_generation_gwh`` — an
optional filing field that is NULL in ~60% of rows — and summed OPEX over every
fiscal year while summing generation only over the years where the field was
filled. Lutelandet (prod report 41) showed 682 NOK/MWh: five years of OPEX over
one year of generation. The canonical, correct definition already existed in
``FinancialDataService.calculate_financial_ratios`` (metered ``generation_data``
denominator, COD+365d ramp-up exclusion, ECB FX); this module applies that
definition to one farm *or a whole peer cohort* in three DB round-trips.

Definition
==========
For each ``primary_asset`` filing (newest first, ending on/before ``as_of``):

* ramp-up: ``period_start >= COD + 365 days`` (applied in SQL, like the ratios
  endpoint; pre-COD and commissioning-year filings never qualify);
* denominator: metered net generation ``Σ(generation_mwh − consumption_mwh)``
  over the filing's own period, read from the nightly-refreshed monthly
  aggregate ``mv_generation_monthly_by_windfarm`` (12 rows per filing instead of
  ~9,000 hourly rows), from the single best-coverage source (never summed
  across sources — multi-source farms would double count). Coverage is
  distinct-hours / expected-hours for hourly sources and distinct-months /
  expected-months for ``MONTHLY_SOURCES`` (EIA / ENERGISTYRELSEN store a month
  at one hour). Until the view's first refresh the query raises and callers
  degrade to "no metric" (logged) — never to a wrong number. Below
  ``OPEX_MIN_COVERAGE_PCT`` the filing falls back to its
  own ``reported_generation_gwh``; with neither it is dropped from BOTH the
  numerator and the denominator;
* currency: display mode converts every filing with the ECB period-average rate
  (``ExchangeRateService.get_rate_for_period``) and drops filings with no rate;
  native mode (``display_currency=None``) keeps the newest filing's currency and
  drops filings in any other currency — sums never mix currencies;
* plausibility (display mode): a filing outside ``OPEX_PLAUSIBLE_EUR_PER_MWH``
  (1–1,000 EUR/MWh) is dropped — that band catches ~1000x unit errors (a kNOK
  sheet tagged NOK) while leaving any real overrun, however extreme, untouched;
* pooling: the first ``max_rows`` usable filings are pooled as Σopex / Σgen and
  the ratios come from ``FinancialDataService._compute_ratios``.

Every rejected filing is recorded in ``OpexMetrics.dropped`` with one reason,
and each public call emits one ``opex_metrics_computed`` log line with the
per-reason counts, so silent shrinkage of the cohort is visible in CloudWatch.

Test contract
=============
``aggregate_opex_metrics`` / ``pick_best_generation`` / ``cohort_median`` are
pure. The async entry points call ``db.execute(text(sql), params)`` positionally
and consume results with ``.fetchall()`` only, so they work against the
``AsyncMock`` sessions used throughout ``tests/opportunity_schemas``.
"""

from __future__ import annotations

import statistics
import time as _time
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.generation_data import MONTHLY_SOURCES
from app.services.exchange_rate_service import SUPPORTED_CURRENCIES, ExchangeRateService

logger = structlog.get_logger()

# ─── Tunables ─────────────────────────────────────────────────────────────────

# Cross-farm comparisons (peer medians) are made in EUR — the platform convention
# for the peer-financials view and the map (ECB rates are EUR-based).
OPEX_DISPLAY_CURRENCY = "EUR"
# Pool up to this many of the most recent usable filings (user decision).
OPEX_MAX_ROWS = 3
# Fetch more candidates than we pool: overlaps / FX gaps / coverage can drop some.
OPEX_CANDIDATE_ROWS = 6
# Cohorts fetch fewer spares per farm — the generation join is the expensive part.
OPEX_COHORT_CANDIDATE_ROWS = 4
# Monthly aggregate the denominator is read from (see _GENERATION_SQL).
GENERATION_MONTHLY_VIEW = "mv_generation_monthly_by_windfarm"
# The generation query runs in bounded batches of filings — cheap against the
# monthly view, but it keeps any single statement's IN-list small.
GENERATION_QUERY_CHUNK = 200
# Metered generation must cover at least this share of the filing period.
OPEX_MIN_COVERAGE_PCT = 50.0
# A cohort median needs at least this many distinct peer entities.
OPEX_MIN_PEERS = 3
# Mirrors FinancialDataService.calculate_financial_ratios (COD + 365 days).
RAMP_UP_DAYS = 365
# Per-filing EUR/MWh outside this band is treated as a data-entry error. Unit
# slips are ~1000x off (a kNOK sheet tagged NOK reads ~0.16 or ~160,000), so the
# band is deliberately wide: Hywind Scotland's 2024 overhaul year is a genuine
# ~690 EUR/MWh and must survive.
OPEX_PLAUSIBLE_EUR_PER_MWH = (1.0, 1000.0)
# Metered vs reported generation disagreeing by more than this is logged.
GEN_MISMATCH_WARN_PCT = 30.0

_FX_TTL_SECONDS = 24 * 3600
_FX_NONE_TTL_SECONDS = 3600
_FX_MAX_ENTRIES = 512

# Drop reasons (stable strings — surfaced in logs and the debug script).
DROP_OVERLAP = "overlap"
DROP_NON_POSITIVE_OPEX = "non_positive_opex"
DROP_NO_DENOMINATOR = "no_denominator"
DROP_FX_UNAVAILABLE = "fx_unavailable"
DROP_CURRENCY_MISMATCH = "currency_mismatch"
DROP_IMPLAUSIBLE = "implausible"

GEN_SOURCE_METERED = "metered"
GEN_SOURCE_REPORTED = "reported"
GEN_SOURCE_MIXED = "mixed"


# ─── Row types ────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class FilingRow:
    """One candidate ``financial_data`` filing for one windfarm."""

    windfarm_id: int
    financial_entity_id: int
    fd_id: int
    period_start: date
    period_end: date
    currency: str
    total_opex: Decimal
    total_revenue: Optional[Decimal]
    ebitda: Optional[Decimal]
    reported_generation_gwh: Optional[Decimal]
    cod: Optional[date] = None
    is_synthetic: bool = False


@dataclass(frozen=True)
class GenerationStat:
    """Best-source metered generation for one filing period."""

    gen_mwh: Decimal
    coverage_pct: float
    source: str


@dataclass
class OpexMetrics:
    windfarm_id: int
    financial_entity_id: int
    currency: str
    total_opex: float
    total_revenue: Optional[float]
    ebitda: Optional[float]
    generation_mwh: float
    opex_per_mwh: float
    ebitda_margin_pct: Optional[float]
    rows_used: int
    years_used: List[int]
    period_start: date
    period_end: date
    native_currency: Optional[str]
    native_opex_per_mwh: Optional[float]
    generation_source: str
    min_coverage_pct: Optional[float]
    dropped: List[Dict[str, Any]] = field(default_factory=list)


# ─── SQL ──────────────────────────────────────────────────────────────────────

# Candidate filings, newest first per windfarm. Ramp-up is applied here so the
# ROW_NUMBER ranking only sees eligible filings. ``{scope}`` / ``{synthetic}``
# are module-controlled fragments (never user input); everything else is bound.
_FILINGS_SQL = f"""
WITH links AS (
    SELECT wfe.windfarm_id,
           wfe.financial_entity_id,
           w.commercial_operational_date AS cod
    FROM windfarm_financial_entities wfe
    JOIN windfarms w ON w.id = wfe.windfarm_id
    WHERE wfe.relationship_type = 'primary_asset'
      AND {{scope}}
),
ranked AS (
    SELECT l.windfarm_id,
           l.financial_entity_id,
           l.cod,
           fd.id AS fd_id,
           fd.period_start,
           fd.period_end,
           fd.currency,
           fd.is_synthetic,
           fd.total_operating_expenses,
           fd.total_revenue,
           fd.ebitda,
           fd.reported_generation_gwh,
           ROW_NUMBER() OVER (
               PARTITION BY l.windfarm_id
               ORDER BY fd.period_end DESC, fd.id DESC
           ) AS rn
    FROM links l
    JOIN financial_data fd ON fd.financial_entity_id = l.financial_entity_id
    WHERE fd.period_end <= :as_of
      AND fd.total_operating_expenses IS NOT NULL
      AND fd.total_operating_expenses > 0
      {{synthetic}}
      AND (l.cod IS NULL OR fd.period_start >= l.cod + INTERVAL '{RAMP_UP_DAYS} days')
)
SELECT windfarm_id, financial_entity_id, cod, fd_id, period_start, period_end,
       currency, is_synthetic, total_operating_expenses, total_revenue, ebitda,
       reported_generation_gwh
FROM ranked
WHERE rn <= :candidate_rows
ORDER BY windfarm_id, period_end DESC, fd_id DESC
"""

_SCOPE_BY_IDS = "wfe.windfarm_id = ANY(:wf_ids)"
_SCOPE_COHORT = (
    "w.bidzone_id = :bidzone_id AND w.location_type = :location_type "
    "AND wfe.windfarm_id <> :exclude_wf_id"
)
_SYNTHETIC_EXCLUDE = "AND NOT fd.is_synthetic"

# Metered generation per (filing, source) from the nightly-refreshed monthly
# aggregate ``mv_generation_monthly_by_windfarm`` (migration c4d8e1f2a3b5,
# refreshed by app/services/generation_monthly_view.py). Reading hourly
# ``generation_data`` instead costs ~9,000 random heap pages per filing-year —
# a 70-farm cohort would be millions of reads. Joins every link of the entity
# so a holdco's generation is pooled with its entity-level OPEX, exactly like
# FinancialDataService.get_peer_financial_summary. Filings are month-aligned
# (1,233 of 1,234 on staging); the aggregate includes whole months, so a
# mid-month filing boundary is rounded to its month (coverage is capped at 100%).
_GENERATION_SQL = f"""
SELECT fd.id AS fd_id,
       m.source,
       SUM(m.net_mwh) AS gen_mwh,
       SUM(m.hours_with_data) AS hours_with_data,
       COUNT(*) AS months_with_data
FROM financial_data fd
JOIN windfarm_financial_entities l
  ON l.financial_entity_id = fd.financial_entity_id
JOIN {GENERATION_MONTHLY_VIEW} m
  ON m.windfarm_id = l.windfarm_id
 AND m.month >= date_trunc('month', fd.period_start)::date
 AND m.month <= date_trunc('month', fd.period_end)::date
WHERE fd.id = ANY(:fd_ids)
GROUP BY fd.id, m.source
"""


# ─── Pure helpers ─────────────────────────────────────────────────────────────


def _as_date(v: Any) -> Optional[date]:
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    return None


def _dec(v: Any) -> Optional[Decimal]:
    if v is None:
        return None
    if isinstance(v, Decimal):
        return v
    return Decimal(str(v))


def expected_hours(period_start: date, period_end: date) -> int:
    return ((period_end - period_start).days + 1) * 24


def expected_months(period_start: date, period_end: date) -> int:
    return (period_end.year - period_start.year) * 12 + (period_end.month - period_start.month) + 1


def filing_from_row(row: Any) -> FilingRow:
    """Build a ``FilingRow`` from a SQL row (attribute access, ``.fetchall()`` shape)."""
    return FilingRow(
        windfarm_id=int(row.windfarm_id),
        financial_entity_id=int(row.financial_entity_id),
        fd_id=int(row.fd_id),
        period_start=_as_date(row.period_start),
        period_end=_as_date(row.period_end),
        currency=str(row.currency).upper(),
        total_opex=_dec(row.total_operating_expenses),
        total_revenue=_dec(getattr(row, "total_revenue", None)),
        ebitda=_dec(getattr(row, "ebitda", None)),
        reported_generation_gwh=_dec(getattr(row, "reported_generation_gwh", None)),
        cod=_as_date(getattr(row, "cod", None)),
        is_synthetic=bool(getattr(row, "is_synthetic", False)),
    )


def pick_best_generation(
    gen_rows: Iterable[Any], filings: Dict[int, FilingRow]
) -> Dict[int, GenerationStat]:
    """Per filing, choose the single source with the best coverage.

    ``gen_rows`` are ``(fd_id, source, gen_mwh, hours_with_data, months_with_data)``
    rows (attribute access). Sources are never summed together: a farm with
    ELEXON + ENTSOE (or NVE + OTHER) units would otherwise count twice.
    Coverage for ``MONTHLY_SOURCES`` is month-based.
    """
    best: Dict[int, GenerationStat] = {}
    for r in gen_rows:
        fd_id = int(r.fd_id)
        filing = filings.get(fd_id)
        if filing is None:
            continue
        gen = _dec(r.gen_mwh)
        if gen is None:
            continue
        source = str(r.source) if r.source is not None else ""
        if source in MONTHLY_SOURCES:
            have = int(r.months_with_data or 0)
            expected = expected_months(filing.period_start, filing.period_end)
        else:
            have = int(r.hours_with_data or 0)
            expected = expected_hours(filing.period_start, filing.period_end)
        coverage = min(100.0, have / expected * 100.0) if expected > 0 else 0.0
        candidate = GenerationStat(gen_mwh=gen, coverage_pct=round(coverage, 1), source=source)
        current = best.get(fd_id)
        if (
            current is None
            or candidate.coverage_pct > current.coverage_pct
            or (
                candidate.coverage_pct == current.coverage_pct
                and candidate.gen_mwh > current.gen_mwh
            )
        ):
            best[fd_id] = candidate
    return best


def _overlaps(a: FilingRow, b: FilingRow) -> bool:
    return a.period_start <= b.period_end and b.period_start <= a.period_end


def aggregate_opex_metrics(
    filings: Sequence[FilingRow],
    generation: Dict[int, GenerationStat],
    rates: Optional[Dict[Tuple[str, date, date], Optional[Decimal]]],
    *,
    display_currency: Optional[str],
    max_rows: int = OPEX_MAX_ROWS,
    min_coverage_pct: float = OPEX_MIN_COVERAGE_PCT,
) -> Dict[int, OpexMetrics]:
    """Pool the first ``max_rows`` usable filings per windfarm (see module doc).

    ``filings`` must be ordered newest-first within each windfarm (the SQL
    guarantees this; tests pass lists in that order). ``rates`` is keyed
    ``(currency, period_start, period_end)`` and is ignored in native mode.
    """
    from app.services.financial_data_service import FinancialDataService

    by_farm: Dict[int, List[FilingRow]] = {}
    for f in filings:
        by_farm.setdefault(f.windfarm_id, []).append(f)

    out: Dict[int, OpexMetrics] = {}
    for wf_id, rows in by_farm.items():
        rows = sorted(rows, key=lambda r: (r.period_end, r.fd_id), reverse=True)
        used: List[Tuple[FilingRow, Decimal, Decimal, str, Optional[float]]] = []
        dropped: List[Dict[str, Any]] = []
        native_ccy: Optional[str] = None

        for row in rows:
            if len(used) >= max_rows:
                break
            if row.total_opex is None or row.total_opex <= 0:
                dropped.append({"fd_id": row.fd_id, "reason": DROP_NON_POSITIVE_OPEX})
                continue
            if any(_overlaps(row, u[0]) for u in used):
                dropped.append({"fd_id": row.fd_id, "reason": DROP_OVERLAP})
                continue

            # Denominator: best-coverage metered generation, else the filing's own figure.
            stat = generation.get(row.fd_id)
            gen_mwh: Optional[Decimal] = None
            gen_kind = GEN_SOURCE_METERED
            coverage: Optional[float] = None
            if stat is not None and stat.gen_mwh > 0 and stat.coverage_pct >= min_coverage_pct:
                gen_mwh = stat.gen_mwh
                coverage = stat.coverage_pct
                reported = row.reported_generation_gwh
                if reported is not None and reported > 0:
                    reported_mwh = reported * 1000
                    diff_pct = abs(float(gen_mwh - reported_mwh)) / float(reported_mwh) * 100
                    if diff_pct > GEN_MISMATCH_WARN_PCT:
                        logger.warning(
                            "opex_generation_mismatch",
                            windfarm_id=wf_id,
                            fd_id=row.fd_id,
                            metered_mwh=float(gen_mwh),
                            reported_mwh=float(reported_mwh),
                            diff_pct=round(diff_pct, 1),
                        )
            elif row.reported_generation_gwh is not None and row.reported_generation_gwh > 0:
                gen_mwh = row.reported_generation_gwh * 1000
                gen_kind = GEN_SOURCE_REPORTED
            if gen_mwh is None or gen_mwh <= 0:
                dropped.append({"fd_id": row.fd_id, "reason": DROP_NO_DENOMINATOR})
                continue

            # Currency.
            if display_currency is None:
                if native_ccy is None:
                    native_ccy = row.currency
                if row.currency != native_ccy:
                    dropped.append({"fd_id": row.fd_id, "reason": DROP_CURRENCY_MISMATCH})
                    continue
                rate = Decimal("1")
            elif row.currency == display_currency:
                rate = Decimal("1")
            else:
                rate = (rates or {}).get((row.currency, row.period_start, row.period_end))
                if rate is None:
                    dropped.append({"fd_id": row.fd_id, "reason": DROP_FX_UNAVAILABLE})
                    continue

            # Plausibility (display mode only — the band is in EUR/MWh terms).
            if display_currency is not None:
                per_mwh = float(row.total_opex * rate / gen_mwh)
                lo, hi = OPEX_PLAUSIBLE_EUR_PER_MWH
                if per_mwh < lo or per_mwh > hi:
                    dropped.append(
                        {"fd_id": row.fd_id, "reason": DROP_IMPLAUSIBLE, "value": round(per_mwh, 2)}
                    )
                    continue

            used.append((row, rate, gen_mwh, gen_kind, coverage))

        if not used:
            if dropped:
                logger.debug("opex_metrics_no_usable_filings", windfarm_id=wf_id, dropped=dropped)
            continue

        currency = display_currency or native_ccy or used[0][0].currency
        sum_opex = sum((u[0].total_opex * u[1] for u in used), Decimal("0"))
        sum_gen = sum((u[2] for u in used), Decimal("0"))
        revenues = [u[0].total_revenue * u[1] for u in used if u[0].total_revenue is not None]
        ebitdas = [u[0].ebitda * u[1] for u in used if u[0].ebitda is not None]
        sum_rev = sum(revenues, Decimal("0")) if len(revenues) == len(used) else None
        sum_ebitda = sum(ebitdas, Decimal("0")) if len(ebitdas) == len(used) else None

        ratios = FinancialDataService._compute_ratios(
            total_revenue=sum_rev,
            total_opex=sum_opex,
            ebitda=sum_ebitda,
            generation_mwh=sum_gen,
        )
        opex_per_mwh = ratios["opex_per_mwh"]
        if opex_per_mwh is None:
            continue

        currencies = {u[0].currency for u in used}
        native_currency = next(iter(currencies)) if len(currencies) == 1 else None
        native_opex = None
        if native_currency is not None:
            native_opex = float(
                round(sum((u[0].total_opex for u in used), Decimal("0")) / sum_gen, 2)
            )
        kinds = {u[3] for u in used}
        gen_source = next(iter(kinds)) if len(kinds) == 1 else GEN_SOURCE_MIXED
        coverages = [u[4] for u in used if u[4] is not None]

        out[wf_id] = OpexMetrics(
            windfarm_id=wf_id,
            financial_entity_id=used[0][0].financial_entity_id,
            currency=currency,
            total_opex=float(round(sum_opex, 2)),
            total_revenue=float(round(sum_rev, 2)) if sum_rev is not None else None,
            ebitda=float(round(sum_ebitda, 2)) if sum_ebitda is not None else None,
            generation_mwh=float(round(sum_gen, 1)),
            opex_per_mwh=float(opex_per_mwh),
            ebitda_margin_pct=(
                float(ratios["ebitda_margin_pct"])
                if ratios["ebitda_margin_pct"] is not None
                else None
            ),
            rows_used=len(used),
            years_used=sorted(u[0].period_end.year for u in used),
            period_start=min(u[0].period_start for u in used),
            period_end=max(u[0].period_end for u in used),
            native_currency=native_currency,
            native_opex_per_mwh=native_opex,
            generation_source=gen_source,
            min_coverage_pct=min(coverages) if coverages else None,
            dropped=dropped,
        )
    return out


def cohort_median(
    metrics: Dict[int, OpexMetrics],
    *,
    display_currency: str = OPEX_DISPLAY_CURRENCY,
    min_peers: int = OPEX_MIN_PEERS,
) -> Optional[Tuple[float, int]]:
    """Median OPEX/MWh over a cohort — one vote per financial entity.

    Farms whose metrics are not in ``display_currency`` are ignored (they were
    never converted, so they are not comparable). Returns ``(median, n)`` or
    ``None`` when fewer than ``min_peers`` entities qualify.
    """
    seen_entities: set = set()
    values: List[float] = []
    for m in sorted(metrics.values(), key=lambda x: x.windfarm_id):
        if m.currency != display_currency:
            continue
        if m.financial_entity_id in seen_entities:
            continue
        seen_entities.add(m.financial_entity_id)
        values.append(m.opex_per_mwh)
    if len(values) < min_peers:
        return None
    return float(statistics.median(values)), len(values)


def dropped_by_reason(metrics: Iterable[OpexMetrics]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for m in metrics:
        for d in m.dropped:
            counts[d["reason"]] = counts.get(d["reason"], 0) + 1
    return counts


# ─── FX cache ─────────────────────────────────────────────────────────────────

_FX_CACHE: Dict[tuple, Tuple[float, Optional[Decimal]]] = {}


def _fx_cache_get(key: tuple) -> Tuple[bool, Optional[Decimal]]:
    entry = _FX_CACHE.get(key)
    if entry is None:
        return False, None
    computed_at, rate = entry
    ttl = _FX_TTL_SECONDS if rate is not None else _FX_NONE_TTL_SECONDS
    if _time.monotonic() - computed_at > ttl:
        _FX_CACHE.pop(key, None)
        return False, None
    return True, rate


def _fx_cache_put(key: tuple, rate: Optional[Decimal]) -> None:
    while len(_FX_CACHE) >= _FX_MAX_ENTRIES:
        oldest = min(_FX_CACHE, key=lambda k: _FX_CACHE[k][0])
        _FX_CACHE.pop(oldest, None)
    _FX_CACHE[key] = (_time.monotonic(), rate)


async def _fetch_rates(
    db: AsyncSession,
    filings: Sequence[FilingRow],
    display_currency: str,
) -> Dict[Tuple[str, date, date], Optional[Decimal]]:
    needed = {
        (f.currency, f.period_start, f.period_end)
        for f in filings
        if f.currency != display_currency
    }
    if not needed:
        return {}
    svc = ExchangeRateService(db)
    rates: Dict[Tuple[str, date, date], Optional[Decimal]] = {}
    convertible = SUPPORTED_CURRENCIES | {"EUR"}
    for ccy, start, end in sorted(needed):
        if ccy not in convertible or display_currency not in convertible:
            rates[(ccy, start, end)] = None
            continue
        key = (ccy, display_currency, start, end)
        hit, rate = _fx_cache_get(key)
        if not hit:
            rate = await svc.get_rate_for_period(ccy, display_currency, start, end)
            _fx_cache_put(key, rate)
        rates[(ccy, start, end)] = rate
    return rates


# ─── Async entry points ───────────────────────────────────────────────────────


async def _compute(
    db: AsyncSession,
    *,
    scope_sql: str,
    params: Dict[str, Any],
    as_of: date,
    display_currency: Optional[str],
    max_rows: int,
    include_synthetic: bool,
    scope_label: str,
    candidate_rows: int = OPEX_CANDIDATE_ROWS,
) -> Dict[int, OpexMetrics]:
    sql = _FILINGS_SQL.format(
        scope=scope_sql, synthetic="" if include_synthetic else _SYNTHETIC_EXCLUDE
    )
    bind = dict(params)
    bind["as_of"] = as_of
    bind["candidate_rows"] = max(candidate_rows, max_rows)

    result = await db.execute(text(sql), bind)
    filing_rows = result.fetchall()
    if not filing_rows:
        return {}
    filings = [filing_from_row(r) for r in filing_rows]
    by_id = {f.fd_id: f for f in filings}

    gen_rows: List[Any] = []
    fd_ids = list(by_id.keys())
    for i in range(0, len(fd_ids), GENERATION_QUERY_CHUNK):
        chunk = fd_ids[i:][:GENERATION_QUERY_CHUNK]
        gen_result = await db.execute(text(_GENERATION_SQL), {"fd_ids": chunk})
        gen_rows.extend(gen_result.fetchall())
    generation = pick_best_generation(gen_rows, by_id)

    rates = None
    if display_currency is not None:
        rates = await _fetch_rates(db, filings, display_currency)

    metrics = aggregate_opex_metrics(
        filings, generation, rates, display_currency=display_currency, max_rows=max_rows
    )
    logger.info(
        "opex_metrics_computed",
        scope=scope_label,
        as_of=str(as_of),
        display_currency=display_currency,
        farms_in=len({f.windfarm_id for f in filings}),
        farms_out=len(metrics),
        dropped_by_reason=dropped_by_reason(metrics.values()),
    )
    return metrics


async def opex_metrics_for_windfarms(
    db: AsyncSession,
    *,
    windfarm_ids: Sequence[int],
    as_of: date,
    display_currency: Optional[str] = OPEX_DISPLAY_CURRENCY,
    max_rows: int = OPEX_MAX_ROWS,
    include_synthetic: bool = False,
) -> Dict[int, OpexMetrics]:
    """OPEX/MWh for specific windfarms (``primary_asset`` links only).

    ``display_currency=None`` keeps each farm's own filing currency (single-farm
    views); a currency code converts every filing with ECB period rates
    (cross-farm comparisons).
    """
    ids = [int(i) for i in windfarm_ids]
    if not ids:
        return {}
    return await _compute(
        db,
        scope_sql=_SCOPE_BY_IDS,
        params={"wf_ids": ids},
        as_of=as_of,
        display_currency=display_currency,
        max_rows=max_rows,
        include_synthetic=include_synthetic,
        scope_label=f"windfarms:{len(ids)}",
    )


async def opex_metrics_for_cohort(
    db: AsyncSession,
    *,
    bidzone_id: int,
    location_type: str,
    as_of: date,
    exclude_windfarm_id: Optional[int],
    display_currency: str = OPEX_DISPLAY_CURRENCY,
    max_rows: int = OPEX_MAX_ROWS,
    include_synthetic: bool = False,
) -> Dict[int, OpexMetrics]:
    """OPEX/MWh for every peer in a (bidzone, location_type) cohort.

    The subject farm is excluded so it never benchmarks against itself.
    """
    return await _compute(
        db,
        scope_sql=_SCOPE_COHORT,
        params={
            "bidzone_id": int(bidzone_id),
            "location_type": location_type,
            "exclude_wf_id": int(exclude_windfarm_id) if exclude_windfarm_id is not None else -1,
        },
        as_of=as_of,
        display_currency=display_currency,
        max_rows=max_rows,
        include_synthetic=include_synthetic,
        scope_label=f"cohort:{bidzone_id}:{location_type}",
        candidate_rows=OPEX_COHORT_CANDIDATE_ROWS,
    )


__all__ = [
    "OPEX_DISPLAY_CURRENCY",
    "OPEX_MAX_ROWS",
    "OPEX_CANDIDATE_ROWS",
    "OPEX_COHORT_CANDIDATE_ROWS",
    "GENERATION_MONTHLY_VIEW",
    "GENERATION_QUERY_CHUNK",
    "OPEX_MIN_COVERAGE_PCT",
    "OPEX_MIN_PEERS",
    "OPEX_PLAUSIBLE_EUR_PER_MWH",
    "RAMP_UP_DAYS",
    "FilingRow",
    "GenerationStat",
    "OpexMetrics",
    "aggregate_opex_metrics",
    "cohort_median",
    "dropped_by_reason",
    "expected_hours",
    "expected_months",
    "filing_from_row",
    "opex_metrics_for_cohort",
    "opex_metrics_for_windfarms",
    "pick_best_generation",
]
