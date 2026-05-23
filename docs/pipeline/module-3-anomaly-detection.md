# Module 3 — Anomaly detection & loss quantification

## Purpose

For each hour, decide whether the windfarm under- or over-performed relative to its capability curve, and quantify the energy and revenue cost of underperformance. Produces the **ODI** ("Operational Data Insight") metrics that the platform surfaces as the headline performance KPI.

Two services live here, and they answer different questions — keeping them straight is important:

| Service | File | What it does |
|---|---|---|
| `PerformanceAnomalyService` | `app/services/performance_anomaly_service.py` | **Module 3.** Per-hour underperformance vs the power curve; quantifies lost MWh / EUR; rolls up to ODI. |
| `DataAnomalyService` | `app/services/data_anomaly_service.py` | **Upstream data QA.** Detects raw-data quality problems — capacity-factor > 120 %, data gaps, generation > unit capacity. Has its own resolution workflow. **Not part of Module 3.** Does NOT contribute to ODI. |

The rest of this doc covers `PerformanceAnomalyService`. The data-QA service is referenced in the [Caller graph](#caller-graph--ui) section because both surface in the admin-ui anomalies page.

## Concepts

### MAD-based thresholds and why they're "robust"
MAD = median(|x - median(x)|). It measures spread without being inflated by extreme values the way standard deviation is. With a 2.5× MAD underperformance threshold, the threshold doesn't move when one bad week shows up — so one bad week can't accidentally validate other bad weeks.

### Why the thresholds are asymmetric (2.5× under, 1.5× over)
- **Underperformance (`p_pu < q50 - 2.5 × MAD`)** — needs to be a *real* outlier (~1 in 400 odds) before we charge against it. False positives cost analyst time chasing nothing.
- **Overperformance (`p_pu > q90 + 1.5 × MAD` OR `p_pu > 1.02`)** — almost always a sensor / metering glitch (turbines can't really exceed nameplate for sustained hours). Cheaper threshold (~1 in 30) because false positives just flag a glitch.

### What ODI means in this product
ODI replaces a simpler "availability %" metric. Three perspectives, all stored on `performance_summaries`:

| Metric | Formula | Reads as |
|---|---|---|
| `odi_pct_underperf` | underperf_hours / total_hours × 100 | "% of operational time we were below median" |
| `odi_pct_loss_mwh` | lost_mwh / expected_mwh × 100 | "% energy we *should have* produced that we didn't" |
| `odi_pct_loss_eur` | lost_eur / expected_revenue_eur × 100 | "% revenue we *should have* earned that we didn't" |

MEMORY.md note: "Real ODI now comes from `performance_summaries` (Module 3, power-curve-based), replacing availability placeholder in OPS-01."

### Expected MWh — the counterfactual
For each hour, given its wind speed, `q50_bin × rated_mw` is what the turbine *should* have produced at typical performance. Subtract actual from that to get `lost_mwh`. This is a counterfactual — we're not comparing to nameplate (which is rarely achievable) and we're not comparing to last year (which has its own problems); we're comparing to *what this windfarm typically does at this wind speed*.

### Why EUR loss differs from MWh loss
If 10 MWh of loss falls in January peak hours (€80/MWh) vs June off-peak (€30/MWh), MWh loss is identical but EUR loss differs by ~2.5×. Time-weighted price matters because:
- **Merchant farms** sell at spot — a peak-hour outage is much worse than an off-peak one.
- **PPA farms** sell at contract — peak/off-peak distinction collapses (but contract vs spot becomes the new question).

We compute both and surface both.

### IsolationForest as a secondary layer
Optional, opt-in via `PIPELINE_USE_ISOLATION_FOREST` env var. Trains a 1-class anomaly model on `[wind_speed, p_pu]` with contamination=0.03 (expects 3 % of hours to be anomalies). Stored as `flag_isolation_forest` boolean on `performance_anomalies` rows.

Why secondary:
- MAD has a per-bin worldview: "is this hour an outlier *for this wind speed*?"
- IsolationForest has a holistic worldview: "is this `(wind, power)` point odd in feature space?"
- They catch different failure modes. IsolationForest can spot a gradual curve shift the MAD threshold misses (because the threshold shifts with the data). MAD catches single-hour glitches IsolationForest treats as normal-ish.
- IsolationForest is informational only — **it does not drive loss MWh/EUR**. Auditable physics-grounded loss should not depend on a black-box model.

### Run detection (≥ 24 h)
Consecutive underperformance hours get grouped into "runs". `gap > 1 h` ends a run (`app/services/performance_anomaly_service.py:209`). Runs ≥ 24 h are counted in `long_run_count` on `performance_summaries`. A multi-day sustained run is a much stronger signal (blade damage, control malfunction) than scattered hourly dips.

## Implementation walkthrough

**File:** `app/services/performance_anomaly_service.py:1–671`

| Spec section | Method | Lines |
|---|---|---|
| 3a statistical flags | `classify_hours` | `118–192` |
| 3b IsolationForest | `detect_isolation_forest_anomalies` | `361–400` |
| 3c loss quantification | inside `classify_hours` | `172–185` |
| 3d ODI aggregation | `aggregate_summaries` | `216–275` |
| 3e run detection | `assign_run_ids` | `196–212` |
| PPA price lookup | `_get_ppa_price` | `324–358` |

### `classify_hours` — the main pass

1. Wind-bin each hour (1 m/s bins).
2. Merge `q50_pu, q90_pu, mad_pu` from `power_curve_bins` (capability curve for that year).
3. Set `is_underperf = p_pu < q50_bin - 2.5 × mad_bin`.
4. Set `is_overperf = (p_pu > q90_bin + 1.5 × mad_bin) | (p_pu > 1.02)`.
5. Compute `expected_mwh = q50_bin × rated_mw`.
6. Compute `lost_mwh = max(0, expected_mwh - actual_mwh)` for underperf hours (0 elsewhere).
7. Compute `lost_eur = lost_mwh × price` — price = PPA contract price if a contract covers this hour, else hourly spot from `market_price`.

Rows where curve stats are missing (bin had <30 samples) are not flagged.

### `assign_run_ids` — vectorised run grouping

Sort underperf hours by `hour`. Compute time gaps. `gap > 3600 s` increments `run_id`. Non-underperf rows get `NULL` run_id. Later, `aggregate_summaries` counts runs of size ≥ 24 (in hours).

### Persistence

- One row per flagged hour written to `performance_anomalies` (underperf and overperf both — normal hours are NOT stored).
- Monthly and yearly rollups upserted into `performance_summaries`.

## DB models

### `performance_anomalies` (`app/models/performance_anomaly.py:22–70`)

One row per flagged hour:

| Column | Type | Notes |
|---|---|---|
| `id` | BigInt PK | |
| `windfarm_id` | Int FK | |
| `hour` | TimestampTZ | UTC start of hour |
| `anomaly_type` | VARCHAR(20) | `'underperformance'` or `'overperformance'` |
| `actual_p_pu` | Numeric(6,5) | measured p_pu |
| `expected_p_pu` | Numeric(6,5) | `q50_bin` from capability curve |
| `wind_speed` | Numeric(5,2) | m/s |
| `wind_bin` | Numeric(4,1) | bin left edge |
| `lost_mwh` | Numeric(10,3) | 0 for overperf |
| `lost_eur` | Numeric(12,2) | 0 for overperf |
| `market_price` | Numeric(12,4) | spot or PPA price applied |
| `run_id` | Int | consecutive run group (NULL for overperf and isolated hours) |
| `flag_isolation_forest` | Bool | NULL if IF wasn't run |
| `created_at` | TimestampTZ | |

**Unique:** `(windfarm_id, hour)`.
**Indexes:** `(windfarm_id, anomaly_type)`, `(windfarm_id, hour)`, `(windfarm_id, run_id)`.

### `performance_summaries` — Module 3 columns (`app/models/performance_summary.py:21–82`)

One row per `(windfarm, period_type, year, month)`:

| Column | Notes |
|---|---|
| `period_type` | `'month'` or `'year'` |
| `year`, `month` | month NULL for yearly rows |
| `total_hours` | hours in the period (e.g. 744 in Jan) |
| `underperf_hours` | count flagged underperf |
| `overperf_hours` | count flagged overperf |
| `odi_pct_underperf` | underperf_hours / total_hours × 100 |
| `lost_mwh` | sum of `lost_mwh` |
| `expected_mwh` | sum of `expected_p_pu × rated_mw` |
| `odi_pct_loss_mwh` | lost_mwh / expected_mwh × 100 |
| `lost_eur` | sum of `lost_eur` |
| `expected_revenue_eur` | sum of `expected_mwh × price` |
| `odi_pct_loss_eur` | lost_eur / expected_revenue_eur × 100 |
| `long_run_count` | # of underperf runs ≥ 24 h |
| `max_run_hours` | duration of longest run |

The same table also holds Module 4 wind-norm columns and Module 6 commercial columns — it's the unified rollup table for the pipeline.

**Unique:** `(windfarm_id, period_type, year, month)` with `COALESCE(month, 0)` to make NULL months play nicely with the constraint.

## Caller graph & UI

### Orchestrator
- `PerformancePipelineService.run_pipeline` calls `PerformanceAnomalyService.detect_anomalies_from_df` per year inside a SAVEPOINT (`app/services/performance_pipeline_service.py:133–151`).

### Cron
- Daily 03:00 UTC via `app/cron/pipeline_daily.py`.

### HTTP read APIs
- `GET /api/v1/performance-pipeline/odi/{windfarm_id}` (`app/api/v1/endpoints/performance_pipeline.py:159–212`) — returns ODI metrics from `performance_summaries`, optionally enriched with bidzone peer averages and "vs zone" diffs.
- `POST /api/v1/data-anomalies/detect` (`app/api/v1/endpoints/data_anomalies.py:28`) — runs `DataAnomalyService.detect_anomalies` (the upstream QA service, not Module 3).

### Frontend
- **Admin UI** — `energyexe-admin-ui/src/components/anomalies/`:
  - Tabs for "Detect Anomalies" (data QA workflow) and "Manual Re-aggregation".
  - Resolution workflow on `data_anomalies` rows (pending → investigating → resolved / ignored / false_positive).
- **Client UI** — surfaces ODI tiles, monthly ODI breakdown, and run-detection callouts on windfarm pages.

## Gaps vs spec

| Spec ask | Status | Notes |
|---|---|---|
| Statistical flags (2.5× MAD under, 1.5× MAD over, 1.02 ceiling) | ✓ | `classify_hours` |
| IsolationForest secondary layer (`contamination=0.03`) | ✓ | `detect_isolation_forest_anomalies`, opt-in via env var |
| Loss MWh / EUR per hour | ✓ | `classify_hours:172–185` |
| ODI rollups (monthly + yearly) | ✓ | `aggregate_summaries` |
| Long-run detection (≥ 24 h) | ✓ | `assign_run_ids` + `aggregate_summaries` |
| PPA price as `lost_eur` multiplier when contract covers the hour | ✓ | `_get_ppa_price` |
| **Use `overall_clean` curve as loss reference for structurally constrained hours** | ✗ | Requires Module 1b. Without it, constrained periods are evaluated against their own contaminated yearly curve → losses can be under-reported by an order of magnitude. |
| **`constraint_loss_summary.csv` output** | ✗ | Tied to Module 1b. |
| `flag_structural_constraint` column on `performance_anomalies` | ✗ | Tied to Module 1b. |
| CSV outputs (`anomaly_*.csv`) | ✗ | We store rollups in Postgres and surface via API. |

## File reference

- Service: `app/services/performance_anomaly_service.py:1–671`
- Data-QA service (separate): `app/services/data_anomaly_service.py:1–865`
- Models: `app/models/performance_anomaly.py:22–70`, `app/models/performance_summary.py:21–82`, `app/models/data_anomaly.py`
- Schemas: `app/schemas/data_anomaly.py`
- Tests: `tests/test_performance_anomaly.py`
- Orchestrator call site: `app/services/performance_pipeline_service.py:133–151`
- Read API: `app/api/v1/endpoints/performance_pipeline.py:159–212`
- Frontend: `energyexe-admin-ui/src/components/anomalies/`, `energyexe-admin-ui/src/lib/anomaly-api.ts`, `energyexe-admin-ui/src/types/anomaly.ts`
