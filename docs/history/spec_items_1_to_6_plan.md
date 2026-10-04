# EnergyExe — Spec Items 1-6 Implementation Plan

**Source spec:** `Prioritisation 2026 03 30.pdf` (March 30, 2026)
**Plan date:** 2026-04-17
**Author context:** Backend pipeline already shipped 2026-04-16 (PR #31). ERA5 NaN root-cause fix deployed today. This plan covers what is left to fully realise spec items 1-6, plus the cross-cutting prerequisites items 4-6 depend on.

---

## 0. Current state (as of 2026-04-17)

| # | Spec item | Module | Status | Output coverage | What's missing |
|---|-----------|--------|--------|-----------------|----------------|
| 1 | Client FE comments (OS) | (frontend) | ⏸ Blocked | n/a | `Client FE feedback.docx` not in repo / Downloads — need user to share |
| 2 | Power Curves P50/P10 (ASR) | Module 2 | ✅ Built | 229/1,624 (14.1%) | Per-windfarm `rated_mw` override; ERA5-unblocked re-run; price-zone comparison; client-UI surfacing |
| 3 | Generation concentration (OS) | — | ❌ **Missing entirely** | 0% | Service, table, endpoint, AI prompt — all from scratch |
| 4 | Degradation (ASR) | Module 5 | ✅ Built (math) | 221/1,624 (13.6%) | Compare vs price-zone average; AI agent description; coverage uplift |
| 5 | Disruption Detection (ASR) | Module 3 | ✅ Built (math) | 227/1,624 (14.0%) | Compare vs price-zone average; IsolationForest (optional); AI description; coverage uplift |
| 6 | Wind Normalisation (ASR) | Module 4 | ✅ Built (math) | 229/1,624 (14.1%) | Monthly time-series chart for client UI; AI description; coverage uplift |

### DB inventory

| Table | Rows | Distinct windfarms |
|-------|------:|-------------------:|
| `power_curve_bins` | 67,141 | 229 |
| `performance_anomalies` | ~1,401,153 | 227 |
| `performance_summaries` | 24,609 (22,576 month + 2,033 year) | 229 |
| `degradation_results` | 441 (220 q50 + 221 q90) | 221 |
| `opportunities` | 544 (active per schema below) | 343 |
| `ppas` | 185 | 130 (8.0%) |
| `p50_targets` | 85 | 83 (5.1%) |

### Existing infrastructure we can build on

| File | Purpose |
|------|---------|
| `app/services/performance_pipeline_service.py` | Module 1-6 orchestrator |
| `app/services/power_curve_service.py` | Module 2 |
| `app/services/performance_anomaly_service.py` | Module 3 |
| `app/services/degradation_service.py` | Module 5 |
| `app/services/peer_analysis_service.py` | **Already detects peers** by bidzone/country/owner/turbine; lacks peer-aggregate computation |
| `app/services/comparison_service.py` | Generic windfarm comparison (likely time-series) |
| `app/services/llm_commentary_service.py` | **Claude + OpenAI** integration; section-aware; caches to `report_commentary` |
| `app/services/prompt_builder_service.py` | Prompt templates by section |
| `app/services/opportunity_detection_service.py` | OPS-01..03, MKT-03 detectors |
| `app/api/v1/endpoints/performance_pipeline.py` | Pipeline API surface |
| `app/api/v1/endpoints/report_commentary.py` | LLM commentary surface |

---

## 1. Cross-cutting prerequisites (must land before items 4-6 are "done")

### PRE-A. ERA5 NaN cleanup (unblocks 1,168 windfarms)

**Status:** Code fix deployed 2026-04-17 (`weather_import.py` bbox + NaN guard, migration `a1b2c3d4e5f6`). Operational cleanup pending.

**Steps:**
1. Snapshot: `SELECT COUNT(*) FROM weather_data WHERE wind_speed_100m::text = 'NaN'` (~98M expected).
2. Chunked DELETE by year (avoids lock timeout):
   ```sql
   DELETE FROM weather_data
   WHERE EXTRACT(YEAR FROM hour) = :year AND wind_speed_100m::text = 'NaN';
   ```
3. Bulk re-import via `WeatherImportJob` flow, year-by-year, ≤4 concurrent days (CDS API throttle).
4. Re-run pipeline for the 1,168 windfarms: `poetry run python scripts/backfill_pipeline.py --windfarm-ids ...`
5. Re-run opportunity detection: `POST /api/v1/opportunities/detect`.

**Owner:** Ops (data team). **Effort:** Days (CDS API throttled). **Blocker for:** items 2-6 coverage uplift.

### PRE-B. Peer-aggregate computation (items 4, 5, 6 require)

The spec says items 4, 5, 6 must "compare vs price zone averages." `peer_analysis_service.py` already detects peer groups, but no service computes peer-aggregate metrics.

**New service:** `app/services/peer_aggregate_service.py`

**Methods:**
- `compute_bidzone_aggregate(bidzone_id, year, metric: enum) -> {avg, p10, p50, p90, n}` for each ASR metric:
  - `odi_pct_underperf`, `odi_pct_loss_mwh`, `odi_pct_loss_eur`
  - `degradation_slope_pct_per_year`
  - `wind_norm_index_q50`, `wind_norm_index_q90`
- `compute_country_aggregate(country_id, year, metric) -> ...`
- Cache results in new table `peer_group_aggregates` keyed by `(group_type, group_id, year, metric)` to avoid recomputation per request.

**New table:** `peer_group_aggregates`
```sql
CREATE TABLE peer_group_aggregates (
  id SERIAL PRIMARY KEY,
  group_type VARCHAR(20) NOT NULL,         -- 'bidzone' | 'country' | 'owner' | 'turbine_model'
  group_id INTEGER NOT NULL,
  metric_key VARCHAR(60) NOT NULL,         -- 'odi_pct_underperf' | 'degradation_slope_pct' | ...
  period_type VARCHAR(10) NOT NULL,        -- 'year' | 'month'
  period_year INTEGER NOT NULL,
  period_month INTEGER,
  windfarm_count INTEGER NOT NULL,
  avg_value NUMERIC(12, 4),
  p10_value NUMERIC(12, 4),
  p50_value NUMERIC(12, 4),
  p90_value NUMERIC(12, 4),
  computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT uq_peer_aggregate UNIQUE (group_type, group_id, metric_key, period_type, period_year, period_month)
);
CREATE INDEX ix_peer_aggregate_lookup ON peer_group_aggregates (group_type, group_id, metric_key, period_year);
```

**Refresh strategy:** Daily cron after pipeline run, or on-demand with TTL.

**Effort:** ~3 days (service + migration + tests + cron hook).

### PRE-C. P50 targets bulk import (blocks Module 6 commercial)

**Status:** Only 83/1,624 windfarms have a P50 target (5.1%). Module 6's `Contract_Revenue_vs_P50Target_EUR` is unusable for 95% of farms.

**Action:**
1. Identify upstream P50 source — likely owner-provided forecasts, or computed from rated capacity × historical capacity factor.
2. If no source: write fallback `compute_default_p50()` using last-3-years actual generation, with a `is_estimated=true` flag.
3. Bulk import script: `scripts/seeds/p50/import_p50_targets.py`.
4. Coverage target: ≥80% of pipeline-eligible windfarms.

**Effort:** ~2 days for fallback computation + import; longer if owner data must be sourced.

### PRE-D. Pipeline scheduler (cron)

**Status:** Pipeline is manually triggered. No scheduled execution.

**Action:** Add APScheduler job `pipeline-daily` in `app/cron/`.
- Schedule: 03:00 UTC daily (after weather + generation imports finish ~02:30).
- Runs `performance_pipeline_service.run_pipeline_batch()` for all windfarms with new data since last run.
- Failure alerting via existing `alert_service.py`.

**Effort:** ~1 day.

---

## 2. Item-by-item plan

### ITEM 1 — Client FE comments (OS) ⏸ BLOCKED

**Blocker:** Source document `Client FE feedback.docx` not located. Spec page 1 references it but it's not in `/Users/mdfaisal/Downloads/` or the repo.

**Action required from user:** Share the doc (Slack, Drive, GitHub issue link).

Once received, this spec item likely produces:
- A list of frontend changes for `energyexe-admin-ui` and/or the client UI
- Probably touches: navigation, dashboards, charts, terminology
- Plan to be added to this doc once received.

---

### ITEM 2 — Power Curves P50 and P10 ✅ MATH DONE — enhancements pending

**Spec:** "Constructs empirical power curves from cleaned data, removes overperforming outliers, and produces clean P50 and P10 capability curves used as performance references in all downstream modules."

**Implemented (Module 2):**
- ✅ Per-bin q50, q90, MAD, n
- ✅ Three curve types: `raw`, `capability`, `overall_clean`
- ✅ `min_samples_per_bin=30`, `overperf_mad_k=1.5`, `ceiling_pu=1.02`
- ✅ Yearly + overall curves
- ✅ Stored in `power_curve_bins` (67,141 rows / 229 windfarms)

**Gaps:**

#### 2.1 Per-windfarm `rated_mw` override
- **Why:** Currently `rated_mw` is read from `windfarms.rated_capacity_mw` but not validated against contract data.
- **Action:** Add `power_curve_config` column or use `windfarms.rated_capacity_mw` consistently — verify all 229 processed windfarms have correct rated capacity.
- **Effort:** 1 day audit + fix.

#### 2.2 Coverage uplift (depends on PRE-A)
- After ERA5 cleanup: re-run Module 2 for the 1,168 unblocked windfarms.
- Target: ~85% (1,397/1,624) coverage.

#### 2.3 Price-zone-average comparison (depends on PRE-B)
- Add to `power_curve_service.get_curve_summary()`: include zone-average q50/q90 alongside windfarm's curve.
- Surface in `/performance-pipeline/power-curves/{windfarm_id}` response.

#### 2.4 Client-UI surfacing
- Frontend repo `energyexe-client-ui`: build chart component for P50 vs P10 vs zone average.
- Out of scope for this PR, tracked separately.

**Files to touch:**
- `app/services/power_curve_service.py` — add zone-comparison fields
- `app/api/v1/endpoints/performance_pipeline.py` — extend response schema
- `app/schemas/performance_pipeline.py` — add `PeerComparison` schema

**Total effort:** ~2 days (excluding ERA5 cleanup).

---

### ITEM 3 — Generation Concentration (OS) ❌ NEW MODULE

**Spec:**
> Distribution of power generation by price. Does the wind farm generate in high (or low) price periods? Compare vs price zone averages. AI agent descriptions and prompts; no client facing.

This is a **brand-new module** with no current implementation.

#### 3.1 Concept

For each windfarm-year, partition all generation hours into price deciles (D1 = lowest 10% of hourly prices, D10 = highest). Compute the **share of total MWh** generated in each decile. A turbine that generates more in high-price hours is commercially well-positioned; one that mostly generates in low-price hours has poor capture.

Key metrics:
- `top_decile_share_pct` = MWh in D10 / total MWh × 100 (target: ≥10% means uncorrelated; <10% means inverse correlation)
- `bottom_decile_share_pct` = MWh in D1 / total MWh × 100
- `weighted_avg_capture_price` = SUM(mwh × hourly_price) / SUM(mwh)
- `time_weighted_avg_price` = SUM(hourly_price) / hours = simple average
- `capture_ratio` = `weighted_avg_capture_price` / `time_weighted_avg_price` (1.0 = neutral; >1 = positive correlation)
- Compare vs zone aggregate (PRE-B).

#### 3.2 Database

**New table:** `generation_concentration_summaries`
```sql
CREATE TABLE generation_concentration_summaries (
  id SERIAL PRIMARY KEY,
  windfarm_id INTEGER NOT NULL REFERENCES windfarms(id) ON DELETE CASCADE,
  period_type VARCHAR(10) NOT NULL,           -- 'year' | 'month'
  period_year INTEGER NOT NULL,
  period_month INTEGER,                       -- NULL for yearly
  total_mwh NUMERIC(14, 3),
  total_hours INTEGER,
  weighted_avg_capture_price_eur NUMERIC(12, 4),
  time_weighted_avg_price_eur NUMERIC(12, 4),
  capture_ratio NUMERIC(8, 4),                -- weighted/time
  top_decile_share_pct NUMERIC(6, 3),         -- D10
  top_quartile_share_pct NUMERIC(6, 3),       -- Q4
  bottom_decile_share_pct NUMERIC(6, 3),      -- D1
  bottom_quartile_share_pct NUMERIC(6, 3),    -- Q1
  decile_shares JSONB,                        -- full {d1: %, d2: %, ..., d10: %}
  vs_zone_capture_ratio_diff NUMERIC(8, 4),   -- this WF capture_ratio − zone avg
  vs_zone_top_decile_diff NUMERIC(6, 3),
  pipeline_run_id INTEGER,
  computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT uq_generation_concentration UNIQUE (windfarm_id, period_type, period_year, period_month)
);
CREATE INDEX ix_genconc_windfarm ON generation_concentration_summaries (windfarm_id, period_year);
```

**Migration:** `alembic revision -m "generation_concentration_summaries"`

#### 3.3 Service

**New file:** `app/services/generation_concentration_service.py`

Skeleton:
```python
class GenerationConcentrationService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def compute_for_windfarm(
        self, windfarm_id: int, year: int, force_refresh: bool = False
    ) -> GenerationConcentrationSummary:
        # 1. Load hourly (generation, price) joined on hour for windfarm+year
        # 2. Drop nulls (no price or no generation)
        # 3. Rank hours by price → assign decile (1-10) and quartile (1-4)
        # 4. Group by decile → share_pct = sum(mwh) / total_mwh × 100
        # 5. Capture ratio = SUM(mwh*price) / SUM(mwh) ÷ AVG(price)
        # 6. Look up zone aggregate (PeerAggregateService) → diff fields
        # 7. UPSERT summary
        ...

    async def compute_for_windfarm_monthly(...) -> List[...]:
        ...

    async def compute_for_batch(self, windfarm_ids: List[int], years: List[int]):
        ...
```

#### 3.4 Pipeline integration

Add to `performance_pipeline_service.run_pipeline_batch()`:
```python
# After Module 6 (commercial), before opportunity detection:
await GenerationConcentrationService(self.db).compute_for_windfarm(wf_id, year)
```

#### 3.5 API endpoint

**Add to:** `app/api/v1/endpoints/performance_pipeline.py`
```python
@router.get("/generation-concentration/{windfarm_id}")
async def get_generation_concentration(
    windfarm_id: int,
    year: Optional[int] = None,
    period: str = "year",
    db: AsyncSession = Depends(get_db),
):
    service = GenerationConcentrationService(db)
    return await service.get_summary(windfarm_id, year, period)
```

#### 3.6 AI agent prompt

**Add to:** `app/services/prompt_builder_service.py`

New section type: `generation_concentration`. Prompt template:
```
You are an energy markets analyst. Given the following generation-concentration metrics for {windfarm_name}, write 1-2 paragraphs explaining the commercial implications.

Metrics:
- Capture ratio: {capture_ratio} (1.0 = neutral; >1 = generates in higher-price hours)
- Top decile share: {top_decile_share_pct}% (10% = neutral)
- Bottom decile share: {bottom_decile_share_pct}%
- Zone-average capture ratio: {zone_capture_ratio}
- Difference vs zone: {vs_zone_capture_ratio_diff}

Year: {year}.

Focus on:
- Whether this windfarm benefits from price correlation
- How it compares to peers in the same bidzone
- Any commercial actions to consider (PPA renegotiation, hedging)
```

**No client-facing UI** per spec ("AI agent descriptions and prompts; no client facing").

#### 3.7 Tests

- `tests/test_generation_concentration.py`:
  - Synthetic 8,760-hour dataset with known capture ratio
  - Test deciles sum to 100%
  - Test edge cases: all generation in top decile (perfect correlation), all in bottom (worst case)
  - Test missing price hours are dropped, not counted

**Total effort:** ~5 days (service + table + migration + endpoint + prompt + tests).

---

### ITEM 4 — Degradation ✅ MATH DONE — enhancements pending

**Spec:** "Estimates whether there is a statistically significant long-run trend in operational performance — i.e. whether the turbine is degrading (or recovering) over time, after accounting for wind variability and seasonal effects. Requires power curve for each wind farm. Compare vs price zone averages. AI agent descriptions and prompts; no client facing."

**Implemented (Module 5):**
- ✅ Operational subset filter (wind 4-14 m/s, q50_bin ≥ 0.10)
- ✅ Residual computation: `actual p_pu − reference_bin p_pu`
- ✅ Seasonal decomposition (statsmodels, period 8760 hrs)
- ✅ OLS regression, slope_pct_per_year + 95% CI
- ✅ Runs twice (q50 + q90 reference)
- ✅ 221 windfarms / 441 results

**Gaps:**

#### 4.1 Compare vs price-zone average (depends on PRE-B)
- Extend `degradation_results` with derived (computed at API time):
  - `zone_avg_slope_pct_per_year`
  - `vs_zone_diff_pct`
- Read via `PeerAggregateService.compute_bidzone_aggregate(bidzone_id, year, "degradation_slope_pct_per_year")`.

#### 4.2 AI agent description
- **Verify in `prompt_builder_service.py`:** does it have a `degradation` section type yet?
- If yes: enhance with peer comparison data.
- If no: add new section template (similar to 3.6).

#### 4.3 Coverage uplift (depends on PRE-A)
- After ERA5 cleanup → re-run Module 5 for unblocked farms.

**Files to touch:**
- `app/services/degradation_service.py` — `get_with_peer_comparison()` method
- `app/services/prompt_builder_service.py` — verify/add `degradation` section
- `app/api/v1/endpoints/performance_pipeline.py` — extend response

**Total effort:** ~2 days (excluding PRE-A and PRE-B).

---

### ITEM 5 — Disruption Detection & Loss Quantification ✅ MATH DONE — enhancements pending

**Spec:** "Identifies hours of operational underperformance and overperformance, and quantifies the associated energy and revenue losses. Requires power curve for each wind farm. Compare vs price zone averages. AI agent descriptions and prompts; no client facing."

**Implemented (Module 3):**
- ✅ Underperf classification (`p_pu < q50_bin − 2.5×MAD`)
- ✅ Overperf classification (`p_pu > q90_bin + 1.5×MAD` OR `p_pu > 1.02`)
- ✅ Run detection (gap > 1 hr; long_run_hours = 24)
- ✅ Loss quantification (MWh + EUR using market_price OR PPA)
- ✅ ODI metrics: `odi_pct_underperf`, `odi_pct_loss_mwh`, `odi_pct_loss_eur`
- ✅ Stored: `performance_anomalies` (~1.4M rows), `performance_summaries`

**Gaps:**

#### 5.1 Compare vs price-zone average (depends on PRE-B)
- Extend `performance_anomaly_service`:
  - Per-windfarm yearly ODI fetched alongside zone-aggregate ODI from `peer_group_aggregates`.
- API: enrich `/performance-pipeline/anomalies/{windfarm_id}` with `vs_zone_diff_*` fields.

#### 5.2 IsolationForest (optional per spec Module 3b)
- Add column `flag_isolation_forest` to `performance_anomalies` (already nullable BOOL? — verify; if not, migration).
- In `performance_anomaly_service`:
  ```python
  if HAS_SKLEARN and config.use_isolation_forest:
      clf = IsolationForest(contamination=0.03, random_state=42, n_jobs=-1)
      preds = clf.fit_predict(df[['v', 'p_pu']].to_numpy())
      df['flag_isolation_forest'] = preds == -1
  ```
- Combine into `flag_any_anomaly` (do NOT use for loss calc per spec).

#### 5.3 AI agent description
- Verify/add `disruption_detection` section in `prompt_builder_service.py`.
- Include: total_underperf_hours, lost_mwh, lost_eur, longest_run, ODI vs zone.

#### 5.4 PPA price integration into loss calc
- **Currently:** loss_eur = lost_mwh × market_price always.
- **Spec:** "If a fixed PPA price is configured, that is used instead of the hourly spot price."
- Action: in `performance_anomaly_service`, lookup `ppas.ppa_price_eur_mwh` for the windfarm; if present and contract is active for hour, use PPA price. Else market_price.

#### 5.5 Coverage uplift (depends on PRE-A)
- After ERA5 cleanup: re-run Module 3 for the 1,168 unblocked windfarms.

**Files to touch:**
- `app/services/performance_anomaly_service.py` — IsolationForest + PPA lookup
- `app/models/performance_anomaly.py` — add `flag_isolation_forest` column (if missing)
- Alembic migration
- `app/services/prompt_builder_service.py`
- `app/api/v1/endpoints/performance_pipeline.py`

**Total effort:** ~3 days (excluding PRE-A and PRE-B).

---

### ITEM 6 — Wind Normalisation ✅ MATH DONE — enhancements pending

**Spec:** "Removes the effect of inter-year wind resource variability from the performance signal, producing an index that reflects operational performance independent of how windy each year was. Requires power curve for each wind farm. **Client facing delivery: month time series charts**; AI agent descriptions and prompts."

Note item 6 IS client-facing (unlike 3, 4, 5).

**Implemented (Module 4):**
- ✅ Hourly ratio method (actual_mw / expected_mw from overall_clean curve)
- ✅ Monthly + yearly indices vs historical mean
- ✅ Runs twice (q50 + q90 reference)
- ✅ Stored in `performance_summaries.norm_ratio_*` and `norm_index_*`

**Gaps:**

#### 6.1 Monthly time-series chart for client UI
- Frontend (`energyexe-client-ui`) needs chart component fed by:
  - `GET /api/v1/performance-pipeline/wind-normalisation/{windfarm_id}?period=month&years=2020,2021,2022,2023,2024`
- Verify endpoint returns shape suitable for time-series rendering. Likely needs new endpoint or extension.
- Highlight above/below historical mean with color coding (per spec).

#### 6.2 AI agent description
- Verify/add `wind_normalisation` section in `prompt_builder_service.py`.

#### 6.3 Coverage uplift (depends on PRE-A)
- After ERA5 cleanup: re-run Module 4 for unblocked farms.

**Files to touch:**
- `app/api/v1/endpoints/performance_pipeline.py` — add monthly time-series endpoint if missing
- `app/services/prompt_builder_service.py`
- Frontend `energyexe-client-ui`: new `WindNormalisationChart.tsx` component (separate ticket)

**Total effort:** ~2 days backend + 2 days frontend.

---

## 3. Sequencing & dependencies

```
PRE-A (ERA5 cleanup) ──┐
                       ├─→ Items 2-6 coverage uplift
PRE-B (peer aggregates) ──┐
                          ├─→ Items 4, 5, 6 vs-zone comparison
                          └─→ Item 3 vs-zone comparison

PRE-C (P50 import) ──→ Module 6 commercial (item 7, not in 1-6)
PRE-D (scheduler) ──→ Daily refresh of all modules

Item 1 ⏸ blocked on user-supplied doc

Item 3 (Generation Concentration) — fully independent net-new module
```

**Recommended order:**
1. **Week 1:** PRE-A operational cleanup (ops in parallel) + PRE-B service+table+migration (backend)
2. **Week 2:** Item 3 (new module) + Item 2/4/5/6 zone-comparison enrichment
3. **Week 3:** Item 5 IsolationForest + PPA loss integration; Item 6 monthly endpoint
4. **Week 4:** AI prompts for items 3-6; PRE-D scheduler; full re-run for ERA5-unblocked

---

## 4. Test plan

### Unit tests (per item)
- Item 3 — `tests/test_generation_concentration.py` — synthetic price/generation series, verify deciles + capture ratio
- PRE-B — `tests/test_peer_aggregate.py` — fixture with 5 windfarms in same bidzone, verify aggregates
- Item 5.2 — `tests/test_isolation_forest.py` — synthetic anomalous points, verify detection
- Item 5.4 — `tests/test_ppa_loss_integration.py` — windfarm with PPA contract, verify loss uses PPA not market
- Item 6.1 — `tests/test_wind_norm_endpoint.py` — verify monthly time-series response shape

### Integration tests
- `tests/test_pipeline_full_run.py` — end-to-end on 1 synthetic windfarm: all 6 modules + concentration + opportunity detection
- `tests/test_peer_aggregate_freshness.py` — verify cron updates aggregates after pipeline run

### Manual smoke tests
- Block Island (windfarm 7361) — full pipeline run after ERA5 cleanup; verify:
  - Power curve produced (≥30 bins)
  - ODI computed
  - Generation concentration computed (capture ratio sensible)
  - Wind normalisation index between 80-120 for recent years
  - Degradation slope within ±2%/year
  - At least one opportunity detected if anomalous

---

## 5. Open questions for user

1. **Item 1 doc:** where is `Client FE feedback.docx`? (Slack? Drive? GitHub?)
2. **PRE-C P50 source:** are P50 targets coming from owner forecasts, or do you want a backend-computed default (e.g., last-3-years actual)?
3. **PPA price use (item 5.4):** when a windfarm has both PPA + market price, should loss EUR use PPA always, or only for hours within the PPA contract date range?
4. **Item 3 client facing?** spec says "no client facing" — confirm the AI commentary surfaces only in admin UI, not client UI.
5. **Pipeline scheduler (PRE-D):** acceptable run window — 03:00 UTC, or aligned with a specific data-import job?

---

## 6. Files to be created or modified

### New files
- `app/models/generation_concentration_summary.py`
- `app/models/peer_group_aggregate.py`
- `app/services/generation_concentration_service.py`
- `app/services/peer_aggregate_service.py`
- `app/cron/pipeline_daily.py` (PRE-D)
- `alembic/versions/<hash>_generation_concentration.py`
- `alembic/versions/<hash>_peer_group_aggregates.py`
- `alembic/versions/<hash>_performance_anomaly_isolation_forest.py` (item 5.2)
- `tests/test_generation_concentration.py`
- `tests/test_peer_aggregate.py`
- `tests/test_isolation_forest.py`
- `tests/test_ppa_loss_integration.py`
- `tests/test_wind_norm_endpoint.py`
- `tests/test_pipeline_full_run.py`
- `scripts/seeds/p50/import_p50_targets.py` (PRE-C)

### Modified files
- `app/services/performance_pipeline_service.py` — orchestrator hooks for concentration + peer aggregates
- `app/services/power_curve_service.py` — peer comparison
- `app/services/degradation_service.py` — peer comparison
- `app/services/performance_anomaly_service.py` — IsolationForest + PPA loss integration + peer comparison
- `app/services/prompt_builder_service.py` — sections for concentration, degradation, disruption, wind_norm
- `app/api/v1/endpoints/performance_pipeline.py` — new endpoint for concentration + monthly wind-norm time series
- `app/schemas/performance_pipeline.py` — peer comparison + concentration schemas
- `app/models/performance_anomaly.py` — `flag_isolation_forest` column

---

## 7. Effort summary

| Workstream | Effort (engineer-days) | Dependencies |
|------------|-----------------------:|--------------|
| PRE-A ERA5 cleanup | 5 (ops, parallel) | — |
| PRE-B peer aggregates | 3 | — |
| PRE-C P50 import | 2 | data sourcing |
| PRE-D scheduler | 1 | — |
| Item 1 FE comments | TBD | doc from user |
| Item 2 enhancements | 2 | PRE-A, PRE-B |
| Item 3 generation concentration | 5 | PRE-B |
| Item 4 enhancements | 2 | PRE-A, PRE-B |
| Item 5 enhancements | 3 | PRE-A, PRE-B |
| Item 6 enhancements | 2 backend + 2 FE | PRE-A |
| **Total** | **~25 backend-days + 2 FE + ops re-import time** | |

---

## 8. Out of scope for this plan

- Item 7 (P50 Target & Commercial Analysis) — already implemented; only blocked by P50 data coverage (PRE-C)
- Item 10 ('Averages' data tables) — partially covered by PRE-B; full implementation depends on which "averages" the FE actually consumes
- Bigger architectural changes (multi-tenant, real-time SCADA ingestion, etc.)
- Frontend implementation beyond items 6.1 (delegated to separate frontend tickets)
