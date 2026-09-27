"""Private, read-only inputs for EPR-138's offline preparer; never a valuation API.

The caller owns a repeatable-read/read-only session with search_path=public. No
suite imports, application settings, lifecycle writes or shared persistence live here.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from sqlalchemy import inspect, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.exchange_rate import ExchangeRate
from app.services.exchange_rate_service import ALLOWED_DISPLAY_CURRENCIES, ExchangeRateService
from app.services.financial_data_service import FinancialDataService
from app.services.scada_ppa_service import ScadaPpaService

SCHEMA_VERSION = "scada-offtake-snapshot/v1"
PAGE_SIZE = 200


@dataclass(frozen=True)
class ExportRequest:
    """``owner_user_id`` is the EXPORTING user (recorded on the envelope as ``owner_user_id`` for
    the pipeline's private lane); since EPR-143 the register is shared per farm, so it no longer
    filters the terms. Name and position are kept for the positional callers."""

    farm: str
    owner_user_id: int
    period_start: date
    period_end: date
    reporting_currency: str | None = None

    def __post_init__(self):
        if not self.farm.strip() or self.owner_user_id < 1:
            raise ValueError("Farm and a positive owner user ID are required")
        if self.period_start > self.period_end or self.period_end == date.max:
            raise ValueError("Invalid inclusive analysis window")
        if self.reporting_currency and self.reporting_currency not in ALLOWED_DISPLAY_CURRENCIES:
            raise ValueError("Unsupported reporting currency")


def native(value: Any) -> Any:
    """Lossless JSON encoding: no float coercion of database Decimal values."""
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("Non-finite source amount")
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): native(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [native(v) for v in value]
    return value


def row_columns(row: Any) -> dict:
    return native({col.key: getattr(row, col.key) for col in inspect(type(row)).column_attrs})


def seal_snapshot(snapshot: dict) -> dict:
    """Shared v1 identity contract; extraction time/transaction are non-material."""
    result = native(snapshot)
    material = {
        k: v for k, v in result.items() if k not in {"exported_at", "content_sha256", "snapshot_id"}
    }
    material["source"] = {k: v for k, v in material["source"].items() if k != "transaction"}
    encoded = json.dumps(
        material, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")
    digest = hashlib.sha256(encoded).hexdigest()
    result.update(content_sha256=digest, snapshot_id="offtake-" + digest[:24])
    return result


def empty_snapshot(request: ExportRequest, source: dict, *, blocked_reason: str) -> dict:
    """A failed read is explicitly blocked, never evidence that there are no terms."""
    return seal_snapshot(
        {
            "schema_version": SCHEMA_VERSION,
            "kind": "private_offtake_snapshot",
            "visibility": "private",
            # EPR-143: explicit scope discriminator. v1 envelopes (exporter_version "1") were
            # owner-filtered; v2 exports every Active term on the farm regardless of who entered it.
            "scope": "farm",
            "owner_user_id": request.owner_user_id,
            "farm": {"slug": request.farm, "windfarm_id": None, "timezone": None},
            "analysis_window": {
                "from": request.period_start,
                "to": request.period_end,
                "date_boundary": "inclusive_local_dates",
            },
            "original_terms": [],
            "reporting_currency": {
                "currency": None,
                "selection": None,
                "provenance": {},
                "blocked_reasons": [],
            },
            "fx_schedule": [],
            "source": source,
            "suite": {"version": "activation-deferred", "runtime_injection": False},
            "readiness": {
                "status": "blocked",
                "blocked_reasons": [blocked_reason],
                "ppa_monetary_activation": False,
            },
            "exported_at": datetime.now(timezone.utc),
        }
    )


def select_reporting_currency(
    override: str | None, filings: list[dict], market_sources: list[dict], terms: list[dict]
) -> dict:
    """Use reported dates across *all* linked entities, then observed prices."""
    selected = override
    selection = "explicit_override" if override else None
    reasons: list[str] = []
    provenance: dict = {
        "override": override,
        "financial_filings": filings,
        "market_price_sources": market_sources,
    }
    if not override:
        if filings:
            if any(not f.get("period_end") or not f.get("period_start") for f in filings):
                reasons.append("financial_reporting_dates_missing")
            else:
                latest = max((f["period_end"], f["period_start"]) for f in filings)
                latest_rows = [f for f in filings if (f["period_end"], f["period_start"]) == latest]
                provenance["latest_financial_filing_ids"] = [f["id"] for f in latest_rows]
                currencies = {f.get("currency") for f in latest_rows}
                if len(currencies) > 1:
                    reasons.append("conflicting_latest_financial_currencies_override_required")
                elif not next(iter(currencies)):
                    reasons.append("reported_currency_missing")
                else:
                    reported = next(iter(currencies))
                    selected = reported if reported in ALLOWED_DISPLAY_CURRENCIES else "EUR"
                    selection = "latest_reported_financial_currency"
                    if selected != reported:
                        provenance["fallback_reason"] = "unsupported_reported_currency_uses_eur"
        else:
            currencies = {row.get("currency") for row in market_sources}
            if len(currencies) > 1:
                reasons.append("conflicting_market_price_currencies_override_required")
            elif not currencies or not next(iter(currencies)):
                reasons.append("market_price_currency_unavailable")
            elif next(iter(currencies)) not in ALLOWED_DISPLAY_CURRENCIES:
                reasons.append("unsupported_market_price_currency_override_required")
            else:
                selected = next(iter(currencies))
                selection = "observed_market_price_currency"
    if selected == "GBP" and any(t.get("currency") not in (None, "", "GBP") for t in terms):
        if override:
            reasons.append("non_gbp_contract_cannot_target_gbp")
            selected = None
        else:
            selected = "EUR"
            provenance["fallback_reason"] = "non_gbp_contract_cannot_target_gbp_uses_eur"
    return {
        "currency": selected,
        "selection": selection,
        "provenance": provenance,
        "blocked_reasons": reasons,
    }


def term_periods(term: dict, start: date, end: date) -> list[tuple[str, date, date]]:
    """Native contract dates are inclusive farm-local calendar dates."""
    if not term.get("effective_date") or not term.get("expiration_date"):
        return []
    first = max(start, date.fromisoformat(term["effective_date"]))
    last = min(end, date.fromisoformat(term["expiration_date"]))
    if first > last:
        return []
    periods = [
        ("calendar_year", max(first, date(year, 1, 1)), min(last, date(year, 12, 31)))
        for year in range(first.year, last.year + 1)
    ]
    # Kept separately even for a single year: decade-only references have their own provenance.
    periods.append(("full_period", first, last))
    return periods


class ScadaOfftakeExporter:
    def __init__(self, db: AsyncSession):
        self.db = db
        self.ppas = ScadaPpaService(db)
        self.financials = FinancialDataService(db)
        self.rates = ExchangeRateService(db)

    async def all_active_terms(self, windfarm_id: int) -> list[dict]:
        """Every Active term on the farm, whoever entered it (EPR-143: the register is shared)."""
        terms: list[dict] = []
        expected_total = None
        while expected_total is None or len(terms) < expected_total:
            page, total = await self.ppas.list_ppas(
                windfarm_id=windfarm_id,
                ppa_status="Active",
                limit=PAGE_SIZE,
                offset=len(terms),
            )
            if expected_total is not None and total != expected_total:
                raise ValueError("PPA pagination total changed during snapshot")
            expected_total = total
            if not page and len(terms) < total:
                raise ValueError("PPA pagination ended before the advertised count")
            for row in page:
                if row.windfarm_id != windfarm_id or row.ppa_status != "Active":
                    raise ValueError("PPA service returned a row outside the requested scope")
                terms.append(row_columns(row))
            if len(terms) > total or len({row["id"] for row in terms}) != len(terms):
                raise ValueError("PPA pagination returned duplicate or excess rows")
        return sorted(terms, key=lambda row: row["id"])

    async def observed_market_sources(
        self, windfarm_id: int, request: ExportRequest, farm_timezone: str
    ) -> list[dict]:
        local_zone = ZoneInfo(farm_timezone)
        first = datetime.combine(request.period_start, time.min, local_zone).astimezone(
            timezone.utc
        )
        last = datetime.combine(
            request.period_end + timedelta(days=1), time.min, local_zone
        ).astimezone(timezone.utc)
        result = await self.db.execute(
            text(
                """
            SELECT source, currency, count(*) AS observation_count,
                   min(hour) AS first_observation, max(hour) AS last_observation,
                   max(updated_at) AS last_updated_at
            FROM public.price_data
            WHERE windfarm_id = :windfarm_id AND hour >= :first AND hour < :last
              AND day_ahead_price IS NOT NULL
            GROUP BY source, currency ORDER BY source, currency
        """
            ),
            {"windfarm_id": windfarm_id, "first": first, "last": last},
        )
        return native([dict(row) for row in result.mappings().all()])

    async def fx_observations(
        self, from_currency: str, to_currency: str, start: date, end: date
    ) -> list[dict]:
        if from_currency == to_currency:
            return []
        quotes = {from_currency, to_currency} - {"EUR"}
        result = await self.db.execute(
            select(ExchangeRate)
            .where(
                ExchangeRate.base_currency == "EUR",
                ExchangeRate.quote_currency.in_(quotes),
                ExchangeRate.rate_date >= start,
                ExchangeRate.rate_date <= end,
            )
            .order_by(ExchangeRate.rate_date, ExchangeRate.quote_currency)
        )
        rows = [row_columns(row) for row in result.scalars().all()]
        if len(quotes) == 2:
            # Mirror the service's same-day join for provenance, never recompute the rate.
            dates_by_quote = [
                {row["rate_date"] for row in rows if row["quote_currency"] == q} for q in quotes
            ]
            matched_dates = set.intersection(*dates_by_quote)
            rows = [row for row in rows if row["rate_date"] in matched_dates]
        return rows

    async def fx_schedule(
        self, terms: list[dict], target: str | None, request: ExportRequest
    ) -> tuple[list[dict], list[str]]:
        schedule, reasons = [], []
        for term in terms:
            term_id = term["id"]
            if not term.get("currency"):
                reasons.append(f"term:{term_id}:currency_missing")
            if term.get("power_share_pct") is None:
                reasons.append(f"term:{term_id}:power_share_missing")
            if not term.get("effective_date") or not term.get("expiration_date"):
                reasons.append(f"term:{term_id}:contract_dates_missing")
                continue
            if term["effective_date"] > term["expiration_date"]:
                reasons.append(f"term:{term_id}:contract_dates_invalid")
                continue
            if not target or not term.get("currency"):
                continue
            if term["currency"] not in ALLOWED_DISPLAY_CURRENCIES:
                reasons.append(f"term:{term_id}:unsupported_contract_currency")
                continue
            for kind, first, last in term_periods(term, request.period_start, request.period_end):
                rate = await self.rates.get_rate_for_period(term["currency"], target, first, last)
                observations = await self.fx_observations(term["currency"], target, first, last)
                if rate is None:
                    reasons.append(f"term:{term_id}:fx_unavailable:{first}:{last}")
                elif term["currency"] != target and not observations:
                    raise ValueError("Rate has no supporting observations")
                references = []
                for field in ("strike_price", "floor_price", "cap_price", "index_spread"):
                    amount = term.get(field)
                    if amount is not None:
                        references.append(
                            {
                                "field": field,
                                "original_amount": amount,
                                "original_currency": term["currency"],
                                "reporting_amount": str(Decimal(amount) * rate)
                                if rate is not None
                                else None,
                                "reporting_currency": target,
                                "label": "reporting reference; not received PPA revenue",
                            }
                        )
                schedule.append(
                    {
                        "term_id": term_id,
                        "period_kind": kind,
                        "from": first,
                        "to": last,
                        "source_currency": term["currency"],
                        "target_currency": target,
                        "rate": rate,
                        "status": "available" if rate is not None else "unavailable",
                        "observations": observations,
                        "nominal_price_references": references,
                    }
                )
        return native(schedule), reasons

    async def build_snapshot(self, request: ExportRequest, source: dict) -> dict:
        snapshot = empty_snapshot(request, source, blocked_reason="farm_mapping_missing")
        result = await self.db.execute(
            text(
                """
            SELECT farm, windfarm_id, tz AS timezone, pipeline_version, computed_at
            FROM scada.dim_farm WHERE farm = :farm
        """
            ),
            {"farm": request.farm},
        )
        farm = result.mappings().one_or_none()
        if farm is None:
            return snapshot
        snapshot["farm"] = {
            "slug": farm["farm"],
            "windfarm_id": farm["windfarm_id"],
            "timezone": farm["timezone"],
        }
        snapshot["source"]["farm_dimension"] = native(dict(farm))
        if farm["windfarm_id"] is None:
            return seal_snapshot(snapshot)
        try:
            if not farm["timezone"]:
                raise ValueError("Missing timezone")
            ZoneInfo(farm["timezone"])
        except (ZoneInfoNotFoundError, ValueError):
            snapshot["readiness"]["blocked_reasons"] = ["farm_timezone_missing_or_invalid"]
            return seal_snapshot(snapshot)
        terms = await self.all_active_terms(farm["windfarm_id"])
        snapshot["original_terms"] = terms
        filings = await self.financials.get_by_windfarm(farm["windfarm_id"])
        filing_rows = native(
            [
                {
                    field: getattr(f, field)
                    for field in (
                        "id",
                        "financial_entity_id",
                        "period_start",
                        "period_end",
                        "currency",
                        "source",
                        "is_synthetic",
                        "created_at",
                        "updated_at",
                    )
                }
                for f in filings
                if not f.is_synthetic
            ]
        )
        filing_rows.sort(key=lambda row: row["id"])
        market_sources = await self.observed_market_sources(
            farm["windfarm_id"], request, farm["timezone"]
        )
        currency = select_reporting_currency(
            request.reporting_currency, filing_rows, market_sources, terms
        )
        snapshot["reporting_currency"] = currency
        schedule, reasons = await self.fx_schedule(terms, currency["currency"], request)
        reasons = currency["blocked_reasons"] + reasons
        snapshot["fx_schedule"] = schedule
        # Exact-date coverage, overlap and clause readiness are owned by the offline preparer.
        snapshot["readiness"] = {
            "status": "blocked" if reasons else ("prepared" if terms else "no_terms"),
            "blocked_reasons": sorted(set(reasons)),
            "ppa_monetary_activation": False,
            "next_step": "offline_prepare_offtake; corrected upstream valuation required",
        }
        return seal_snapshot(snapshot)
