# Module 6 — Commercial reporting

> **Status: partially implemented.** There is no dedicated commercial-reporting service. The pieces are spread across `PerformancePipelineService` (constraint proxy + PPA scenarios), `P50TargetService` (P50 baseline), `PPAService` (contracts), and `performance_summaries` columns. The spec's CSV exports and multi-year roll-up don't exist yet.

## Purpose

Translate operational performance from Modules 2–5 into the commercial metrics a board cares about:

- Actual revenue vs P50 financial baseline.
- Lost-value proxy from capacity headroom (`q90 - q50` gap).
- PPA price scenario analysis ("if we sign at €30/MWh, what changes?").

These connect technical performance to **financial close commitments** (P50 targets often back debt-service ratios) and to **contracting decisions** (PPA negotiation, merchant exposure).

## Concepts

### P50 target — the commercial baseline
A windfarm's financial model is built on an **annual energy production forecast** produced by wind-resource consultants at financial close. The "P50" is the 50th-percentile outcome — the level where, statistically, half of future years should exceed it and half should miss it. The P50 underwrites:

- Debt service ratios — covenants often kick in if production sits below P50 for too long.
- Equity return models — IRR assumptions live or die on P50 vs actual.
- Refinancing — lenders re-rate the asset based on production history vs P50.

P50 targets in our system are time-bounded (per-windfarm, with `start_date` and `end_date`). They are not computed from operational data; they're loaded from the consultant's report.

### PPA — Power Purchase Agreement
Long-term contract to sell the windfarm's output at a fixed (or indexed) price:

- **Fixed-price PPA** — revenue certainty; caps upside if spot prices rise.
- **Indexed PPA** — revenue tracks a market index plus/minus a spread.
- **Merchant** — no contract; all output sold at hourly spot. Highest volatility, highest upside.

For commercial reporting, the PPA price (when one covers the period) replaces hourly spot price in revenue and loss calculations. See `_get_ppa_price` in Module 3's service for the lookup.

### Lost-value proxy (`q90 - q50` gap)
Different from Module 3's lost MWh (which is `q50 - actual`, the gap between expected and observed). Module 6's proxy is `q90 - q50` — the gap between **upper-capability** and **median expectation**:

```
lost_mwh_proxy_per_hour = max(0, (q90_bin - q50_bin) × rated_mw)
```

It's a **conservative estimate of commercial headroom**: how much energy could be recovered if the turbine reliably operated at its upper-capability bound rather than its median. Multiplied by hourly price → `lost_value_eur`.

Use case: "even if we can't address every underperformance hour, what's the value of moving the median up?"

### PPA scenario analysis
Negotiation tool. For a set of candidate PPA prices `[23.2, 26.0, 30.0, 35.0, 40.0]` EUR/MWh, compute for each year:

- `contract_revenue = actual_mwh × scenario_price`
- `revenue_vs_p50_target = contract_revenue - (p50_target_mwh × scenario_price)`
- `value_of_1pct_per_year = 0.01 × actual_mwh × scenario_price` ← the EUR value of a 1 % production improvement at this price point

Answers: "if we're negotiating between €26 and €35, what's the cash impact?" and "how much does a 1 % efficiency gain matter at each price point?"

### How this differs from Module 3 commercial outputs
Both produce EUR numbers, but with different framings:

| Concept | Module 3 | Module 6 |
|---|---|---|
| Reference | `q50` (median expectation) | `q90` (upper-capability bound) |
| Question | "how much did we *actually* lose to underperformance?" | "how much *potential* sits between median and upper capability?" |
| Audience | operations / O&M | finance / commercial |
| Action implied | fix specific anomalies | argue for capex, derate review, or PPA renegotiation |

## What exists today

### 1. Constraint proxy timeseries + lost value (`performance_pipeline_service.py:233–334`)

Computed inline in `_compute_commercial_metrics(windfarm_id, year, pipeline_run_id)`:

- Loads the `overall_clean` power curve.
- For each hour, computes `gap = q90_bin - q50_bin` at that hour's wind bin.
- Sums `(gap × rated_mw)` → annual `total_constraint_proxy_mwh`.
- Multiplies hour-by-hour by `market_price` → annual `lost_value_eur`.
- Persists onto `performance_summaries` for the yearly row only (`month=NULL`).

DB columns (`app/models/performance_summary.py`):
- `constraint_proxy_mwh`
- `lost_value_eur`

Exposed in the response of `GET /api/v1/performance-pipeline/summary/{windfarm_id}` (`app/api/v1/endpoints/performance_pipeline.py:438–454`).

### 2. P50 targets

**Service:** `app/services/p50_target_service.py` — full CRUD.
**Model:** `app/models/p50_target.py:18–53` — `(windfarm_id, p50_target_volume_gwh, p50_target_start_date, p50_target_end_date)`.

Key methods:
- `get_active_target(windfarm_id, as_of_date)` — pick the live target for a date.
- `get_p50_analysis(windfarm_id, target_id)` — compare actual generation vs P50 monthly and yearly; returns cumulative gap in GWh.

API:
- `GET /api/v1/p50-targets/windfarms/{windfarm_id}/p50-targets`
- `GET /api/v1/p50-targets/windfarms/{windfarm_id}/p50-targets/active`
- Plus CRUD endpoints.

P50 imports / backfill: MEMORY.md note `project_p50_fallback_import.md` — 148 auto-computed P50 targets inserted; coverage 5.1 % → 14.3 %; remaining 1,392 windfarms blocked on ERA5 NaN cleanup.

### 3. PPA contracts

**Service:** `app/services/ppa_service.py` — CRUD plus Excel import.
**Model:** `app/models/ppa.py:18–55` — `(contract_type, ppa_price_eur_mwh, ppa_status, has_availability_penalties, …)`.

API: `app/api/v1/endpoints/ppas.py:1–80` — full CRUD by windfarm.

### 4. PPA scenario analysis

Inline in `PerformancePipelineService.run_ppa_scenarios(windfarm_id, year, price_scenarios)` (`app/services/performance_pipeline_service.py:337–411`).

Per scenario price:
- `revenue_eur = actual_mwh × price`
- `revenue_vs_p50_eur = actual_mwh × price - p50_target_mwh × price`
- `value_of_1pct_eur_per_year = 0.01 × actual_mwh × price`

Default scenarios: `[23.2, 26.0, 30.0, 35.0, 40.0]` EUR/MWh.

**Not persisted** — computed on-demand and returned in the response. API: `POST /api/v1/performance-pipeline/ppa-scenarios/{windfarm_id}` (`app/api/v1/endpoints/performance_pipeline.py:457–471`).

### 5. Unified performance summary view

`performance_summaries` is the single rollup table for Modules 3, 4, and 6 outputs. The schema response `PerformanceSummaryResponse` (`app/schemas/performance_pipeline.py`) exposes all three modules' columns side-by-side, so a single API call returns the full picture for a windfarm-period.

## Link to opportunity detection

`OpportunityDetectionService` (`app/services/opportunity_detection_service.py`) surfaces actionable findings to the client UI. Today it consumes:

- **Module 3 outputs** for OPS-01 (volatile disruptions — uses ODI, availability, PPA status).
- **`PriceAnalyticsService`** for MKT-01 (low capture rate — achieved vs market price).
- **`GenerationConcentrationService`** for MKT-03 (cannibalisation — concentration of generation in low-price hours).

**Module 6's commercial outputs (P50 gap, PPA scenarios, lost value) are NOT yet wired into opportunity detection.** Natural future extensions:

- "If `revenue_vs_p50 < -€5M` AND cannibalisation is high → recommend PPA renegotiation."
- "If `lost_value_eur` (q90-q50 gap) is consistently > €2M AND degradation slope is positive → recommend derate review."
- "If actual is sub-P50 for 3 consecutive years → flag covenant-watch alert."

## Gaps vs spec

| Spec ask | Status | Notes |
|---|---|---|
| `lost_mwh_proxy` per-hour timeseries | ✗ stored aggregate-only | Hourly proxy is computed but only the annual sum lands on `performance_summaries`. No hourly timeseries persisted. |
| Annual `Actual_MWh`, `Contract_Revenue_EUR`, `LostEnergyProxy_MWh`, `LostValue_EUR` | ✓ partial | `constraint_proxy_mwh` (= LostEnergyProxy_MWh) and `lost_value_eur` are stored. `Contract_Revenue_EUR` and `Contract_Revenue_vs_P50Target_EUR` are computed on-demand in scenario analysis but never persisted. |
| PPA scenario analysis | ✓ | On-demand only, not cached. |
| `value_of_1pct_per_year` | ✓ | Inside scenario response. |
| `commercial_summary_board_level_q90_vs_q50.csv` | ✗ | No CSV export endpoint. |
| `scenario_ppa_q90_vs_q50.csv` | ✗ | No CSV export endpoint. |
| Multi-year commercial roll-up (cumulative vs P50, year-on-year revenue trend) | ✗ | No service or API today. |
| Revenue uplift column when base PPA price is included in the scenario list | ✗ | Not computed today. |

### Greenfield opportunity (suggested by exploration)

A future `CommercialReportService` would:

1. Aggregate P50 vs actual across multi-year windows ("cumulative gap since financial close").
2. Persist PPA scenario results for dashboarding (avoid recomputation on every page load).
3. Expose `/export/commercial-summary.csv` with rows `(windfarm, year, actual_mwh, p50_target_mwh, gap_gwh, avg_market_price, lost_value_proxy_eur, ppa_price, ppa_revenue, ppa_revenue_vs_p50)`.
4. Wire P50 gap + lost-value into `OpportunityDetectionService` for client-facing recommendations.

## File reference

- Commercial metrics computation: `app/services/performance_pipeline_service.py:233–334`
- PPA scenario analysis: `app/services/performance_pipeline_service.py:337–411`
- P50 target service: `app/services/p50_target_service.py`
- P50 target model: `app/models/p50_target.py:18–53`
- PPA service: `app/services/ppa_service.py`
- PPA model: `app/models/ppa.py:18–55`
- Performance summary model: `app/models/performance_summary.py`
- API: `app/api/v1/endpoints/performance_pipeline.py:438–471`, `app/api/v1/endpoints/p50_targets.py`, `app/api/v1/endpoints/ppas.py`
