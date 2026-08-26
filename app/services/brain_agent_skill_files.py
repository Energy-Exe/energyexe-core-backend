"""Skill file templates — written to the agent sandbox for lazy-loading via `cat`."""

from app.services.opportunity_schemas.schema_names import SCHEMA_NAMES, format_schema_catalogue

# Generated from SCHEMA_NAMES / SCHEMA_ONE_LINERS (single source of truth) so the
# agent always sees the full, current schema set by NAME rather than a hand-kept
# divergent list. Interpolated into SKILL_DOMAIN below.
_SCHEMA_CATALOGUE = format_schema_catalogue()

# Slash-joined list of every SchemaCode value (OPS_01/.../DQ_01), generated from
# the same single source so the opportunities-table reference in SKILL_SCHEMA can
# never go stale (it previously hard-listed only the original 6 schemas).
_SCHEMA_CODES = "/".join(code.value for code in SCHEMA_NAMES)

SKILL_SCHEMA = """# Database Schema Reference

## Core Tables

**windfarms**: id, name, code, nameplate_capacity_mw (Float), location_type (onshore/offshore), foundation_type (fixed/floating), status (operational/decommissioned/under_installation/expanded), country_id, state_id, region_id, bidzone_id, lat, lng, commercial_operational_date, ramp_up_end_date, is_deleted (bool — soft-deleted windfarms, hidden from the client platform)

**generation_data**: hour (timestamptz), windfarm_id, generation_unit_id, generation_mwh (Numeric 12,3), metered_mwh (Numeric 12,3), curtailed_mwh (Numeric 12,3), capacity_mw, capacity_factor (0-1 decimal), consumption_mwh, is_ramp_up (bool), source (ENTSOE/ELEXON/EIA/NVE), quality_flag, completeness
- Unique: (hour, generation_unit_id, source)
- Generation is per generation_unit — GROUP BY windfarm_id for windfarm totals

**price_data**: hour (timestamptz), windfarm_id, bidzone_id, day_ahead_price (Numeric 12,4), intraday_price, currency (EUR/GBP/NOK/DKK), source
- Unique: (hour, windfarm_id, source)

**weather_data**: hour (timestamptz), windfarm_id, wind_speed_100m, wind_direction_deg, temperature_2m_k, temperature_2m_c, source (ERA5)
- Unique: (hour, windfarm_id, source)

**financial_data**: financial_entity_id, period_start, period_end, period_length_months (Numeric — NOT always 12), is_synthetic (bool), currency, revenue, total_revenue, total_operating_expenses, ebitda, depreciation, ebit, net_income, reported_generation_gwh, comment
- Linked via: windfarm_financial_entities(windfarm_id, financial_entity_id)
- Periods are NOT always 12-month calendar years: many entities report Oct–Sep fiscal years and some rows are 3/6/9/15/18-month transition periods. Always read period_start/period_end/period_length_months, label by real dates, and annualise (or flag the mismatch) before period-over-period comparison. Never call a non-12-month row "incomplete".

## Supporting Tables

**turbine_models**: model, supplier, original_supplier, rated_power_kw, cut_in_wind_speed_ms, cut_out_wind_speed_ms, rated_wind_speed_ms, blade_length_m, rotor_diameter_m
**turbine_units**: windfarm_id, turbine_model_id, lat, lng, hub_height_m, status, start_date, end_date
**windfarm_owners**: windfarm_id, owner_id, ownership_percentage
**owners**: id, code, name, type (energy/institutional_investor/community_investors/municipality/private_individual/supply_chain_oem/other/unknown)
**ppas**: windfarm_id, ppa_buyer, ppa_size_mw, ppa_duration_years, ppa_start_date, ppa_end_date, ppa_notes, contract_type (fixed_price/indexed/hybrid/merchant), ppa_status (active/expired/renegotiating), ppa_price_eur_mwh, has_availability_penalties (bool)
**opportunities**: windfarm_id, schema_code (all 19: __OPPORTUNITY_SCHEMA_CODES__), severity (CONFIRMED/INDICATIVE/WATCH/SUPPRESSED), branch (A/B/C), status (ACTIVE/ACKNOWLEDGED/RESOLVED/SUPERSEDED/INACTIVE), data_slots (JSONB), missing_slots (JSONB list), suppression_reason, triggered_by_id, detection_period_start, detection_period_end, detection_run_id, created_at
- `detection_period_start/end` = the window the finding was evaluated over: a rolling ~24 months (720 d) whose END is clipped per windfarm to its last day with metered generation (EPR-126) — for lagging feeds (NVE stops at 31 Dec 2025) the end sits months before the run. `created_at` is the run timestamp; the requested end is `import_job_executions.import_end_date` via `detection_run_id`. `data_slots->>'period'` repeats the effective window.
- LIVE findings =`status='ACTIVE' AND severity != 'SUPPRESSED'`. A SUPPRESSED severity means the finding was gated off by a DQ-01 data gap — do NOT surface it as actionable. A schema with status INACTIVE is data-blocked (e.g. MKT_05/MKT_07) and produces no real findings. Resolve schema_code → human name via the catalogue in skill_domain.md.
**power_curve_bins**: windfarm_id, year (NULL=overall_clean), curve_type (raw/capability/overall_clean), wind_bin (Numeric 2.0-25.0 in 1.0 steps), q50_pu (median P50), q90_pu (90th pct P10), mean_pu, mad_pu, sample_count. Unique: (windfarm_id, year, curve_type, wind_bin)
**performance_anomalies**: windfarm_id, hour, anomaly_type (underperformance/overperformance), actual_p_pu, expected_p_pu, wind_speed, wind_bin, lost_mwh, lost_eur, market_price, run_id. Unique: (windfarm_id, hour)
**performance_summaries**: windfarm_id, period_type (month/year), year, month. ODI: odi_pct_underperf, lost_mwh, expected_mwh, odi_pct_loss_mwh, lost_eur, odi_pct_loss_eur, long_run_count, max_run_hours. Norm: norm_ratio_p50, norm_index_p50, norm_ratio_p10, norm_index_p10. Commercial: constraint_proxy_mwh, lost_value_eur
**degradation_results**: windfarm_id, reference_curve (q50=P50/q90=P10), slope_pu_per_year, slope_pct_per_year, intercept, r_squared, p_value, ci_lower_95, ci_upper_95, baseline_cap_pu, data_points
**p50_targets**: windfarm_id, p50_target_start_date, p50_target_end_date, p50_target_volume_gwh (the SOURCED P50 annual generation target, in GWh), source (provenance URL), comment
  - This is the table for **P50 attainment / P50 target / P50 gap**: `actual GWh ÷ p50_target_volume_gwh`, window = (COD year + 1) → end of previous calendar year.
  - NOT the same as `norm_index_p50` (a wind-normalised performance index in performance_summaries). Never use norm_index_p50 to answer P50-target questions.
**structural_constraint_flags**: windfarm_id, period_start, period_end, duration_hours, flag_trigger, mean_q50_ratio, mean_q90_ratio, review_status (confirmed/pending/dismissed), analyst_notes (free-text CONFIRMED cause of the outage/export-constraint, e.g. "Export cable failure"), reviewed_by, reviewed_at
  - When asked WHY a windfarm lost output / had an outage / was constrained, read analyst_notes for a row covering the period (prefer review_status='confirmed') and use it as the authoritative cause. Only infer if no note exists — do not speculate over a confirmed note.
**generation_concentration_summaries**: windfarm_id, period_type (year/month), year, month, total_mwh, total_hours, weighted_avg_capture_price_eur, time_weighted_avg_price_eur, capture_ratio, top_decile_share_pct, top_quartile_share_pct, bottom_decile_share_pct, bottom_quartile_share_pct, decile_shares (JSONB: {"d1":..,"d2":..,...,"d10":..} — % of generation in each price decile, D1=lowest-price hours, D10=highest), vs_zone_capture_ratio_diff, vs_zone_top_decile_diff, pipeline_run_id, computed_at
  - Unique: (windfarm_id, period_type, year, month)
**peer_group_aggregates**: group_type ('bidzone'/'country'/'owner'/'turbine_model'), group_id, metric_key (see list below), period_type (year/month), year, month, windfarm_count, avg_value, p10_value, p50_value, p90_value, computed_at
  - Unique: (group_type, group_id, metric_key, period_type, year, month)
  - **metric_key values:** 'odi_pct_underperf', 'odi_pct_loss_mwh', 'odi_pct_loss_eur', 'wind_norm_index_p50', 'wind_norm_index_p10', 'degradation_slope_pct_per_year_q50', 'degradation_slope_pct_per_year_q90', 'concentration_capture_ratio', 'concentration_top_decile_share_pct', 'concentration_bottom_decile_share_pct'
  - For a windfarm's bidzone peers: `group_type='bidzone' AND group_id = windfarms.bidzone_id`. Fall back to `group_type='country'` if bidzone row missing.
**data_anomalies**: windfarm_id, anomaly_type, severity, status, period_start, period_end, description
**alert_rules**: user_id, windfarm_id, metric, condition, threshold_value, severity, is_enabled
**countries**: id, code (ISO alpha-3: NOR, GBR, USA, DNK), name
**regions**: id, name
**bidzones**: id, code, name, bidzone_type
**generation_units**: id, name, source, fuel_type, capacity_mw, windfarm_id

## Key Join Patterns

- Country: `windfarms w JOIN countries c ON w.country_id = c.id WHERE c.name = 'Norway'`
- Owners: `windfarms w LEFT JOIN windfarm_owners wo ON wo.windfarm_id = w.id LEFT JOIN owners o ON o.id = wo.owner_id`
- Turbines: `windfarms w JOIN turbine_units tu ON tu.windfarm_id = w.id JOIN turbine_models tm ON tm.id = tu.turbine_model_id`
- Financial: `windfarms w JOIN windfarm_financial_entities wfe ON wfe.windfarm_id = w.id JOIN financial_data fd ON fd.financial_entity_id = wfe.financial_entity_id`
- Gen+Price: `generation_data g JOIN price_data p ON g.windfarm_id = p.windfarm_id AND g.hour = p.hour`
"""

SKILL_QUERIES = """# Common SQL Query Patterns

## Windfarm Lookup
```sql
SELECT w.id, w.name, w.nameplate_capacity_mw, w.location_type, w.status
FROM windfarms w JOIN countries c ON w.country_id = c.id
WHERE c.name = 'Norway' ORDER BY w.name
```

## Capacity Factors (monthly)
DO NOT use `AVG(capacity_factor)` — Postgres silently excludes NULL rows
from AVG, so windfarms with downtime hours where `capacity_factor` is NULL
but `generation_mwh = 0` produce inflated CFs. Always compute CF from raw
sums against nameplate × hours instead. For multi-unit windfarms, sum
generation AND capacity per hour first, then aggregate — averaging per-unit
CFs across units double-counts hours and gives wrong results.

```sql
-- Single-unit OR multi-unit windfarm — both correct
SELECT DATE_TRUNC('month', g.hour) as month,
       ROUND(
         SUM(g.generation_mwh)::numeric
         / NULLIF(w.nameplate_capacity_mw * COUNT(DISTINCT g.hour), 0)
         * 100, 1) as cf_pct,
       ROUND(SUM(g.generation_mwh)::numeric, 0) as gen_mwh
FROM generation_data g JOIN windfarms w ON g.windfarm_id = w.id
WHERE g.windfarm_id = 7182
  AND g.hour >= '2025-01-01' AND g.hour < '2026-01-01'
  AND g.is_ramp_up = false
GROUP BY 1, w.nameplate_capacity_mw ORDER BY 1
```

## Data Availability
```sql
SELECT MIN(hour) as first_date, MAX(hour) as last_date, COUNT(*) as records
FROM generation_data WHERE windfarm_id = 7182
```

## Windfarm with Owners
```sql
SELECT w.name, c.name as country, w.nameplate_capacity_mw, w.location_type,
       o.name as owner_name, wo.ownership_percentage
FROM windfarms w JOIN countries c ON w.country_id = c.id
LEFT JOIN windfarm_owners wo ON wo.windfarm_id = w.id
LEFT JOIN owners o ON o.id = wo.owner_id
WHERE w.name ILIKE '%Tellenes%'
```

## Price Data with Negative Hours
```sql
SELECT DATE_TRUNC('month', hour) as month,
       ROUND(AVG(day_ahead_price)::numeric, 2) as avg_price,
       COUNT(CASE WHEN day_ahead_price < 0 THEN 1 END) as neg_hours,
       currency
FROM price_data WHERE windfarm_id = 7182 AND hour >= '2025-01-01'
GROUP BY 1, currency ORDER BY 1
```

## Weather Data (monthly wind speed)
```sql
SELECT DATE_TRUNC('month', hour) as month,
       ROUND(AVG(wind_speed_100m)::numeric, 1) as avg_wind_ms,
       ROUND(AVG(temperature_2m_c)::numeric, 1) as avg_temp_c
FROM weather_data WHERE windfarm_id = 7201
AND hour >= '2023-01-01' AND hour < '2026-01-01'
GROUP BY 1 ORDER BY 1
```

## Opportunity Queries

```sql
-- Active opportunities for a windfarm
SELECT o.schema_code, o.severity, o.branch, o.data_slots, o.missing_slots
FROM opportunities o WHERE o.windfarm_id = :id AND o.status = 'ACTIVE'
ORDER BY CASE o.severity WHEN 'CONFIRMED' THEN 1 WHEN 'INDICATIVE' THEN 2 ELSE 3 END

-- Opportunity summary across all windfarms
SELECT o.schema_code, o.severity, COUNT(*), w.name
FROM opportunities o JOIN windfarms w ON o.windfarm_id = w.id
WHERE o.status = 'ACTIVE' GROUP BY o.schema_code, o.severity, w.name

-- Capture rate gap from opportunity data
SELECT w.name, o.data_slots->>'gap_pp' as gap_pp, o.data_slots->>'cannibalisation_index' as ci
FROM opportunities o JOIN windfarms w ON o.windfarm_id = w.id
WHERE o.schema_code = 'MKT_01' AND o.status = 'ACTIVE'
```

## Performance Pipeline Queries

### Generation concentration (capture ratio, deciles) by year
```sql
SELECT year, capture_ratio, weighted_avg_capture_price_eur,
       time_weighted_avg_price_eur, top_decile_share_pct,
       bottom_decile_share_pct, decile_shares,
       vs_zone_capture_ratio_diff
FROM generation_concentration_summaries
WHERE windfarm_id = 7361 AND period_type = 'year'
ORDER BY year
```

### ODI underperformance vs bidzone peers (monthly)
```sql
SELECT ps.year, ps.month,
       ps.odi_pct_underperf AS windfarm_odi,
       pa.avg_value AS zone_avg_odi,
       pa.windfarm_count AS peer_n,
       ps.odi_pct_underperf - pa.avg_value AS diff_pp
FROM performance_summaries ps
JOIN windfarms w ON w.id = ps.windfarm_id
LEFT JOIN peer_group_aggregates pa
  ON pa.group_type = 'bidzone'
  AND pa.group_id = w.bidzone_id
  AND pa.metric_key = 'odi_pct_underperf'
  AND pa.period_type = ps.period_type
  AND pa.year = ps.year
  AND pa.month IS NOT DISTINCT FROM ps.month
WHERE ps.windfarm_id = 7361 AND ps.period_type = 'month'
ORDER BY ps.year, ps.month
```

### Monthly wind-normalised performance index (P50 and P10 references)
```sql
SELECT year, month, norm_index_p50, norm_index_p10,
       norm_ratio_p50, norm_ratio_p10
FROM performance_summaries
WHERE windfarm_id = 7361 AND period_type = 'month'
ORDER BY year, month
```

### Power curve comparison (raw vs capability for one year)
```sql
SELECT wind_bin, curve_type, q50_pu, q90_pu, sample_count
FROM power_curve_bins
WHERE windfarm_id = 7361 AND year = 2024
  AND curve_type IN ('raw', 'capability')
ORDER BY wind_bin, curve_type
```
For an all-years reference curve use `year IS NULL AND curve_type = 'overall_clean'`.

### Degradation trend with confidence interval
```sql
SELECT reference_curve, slope_pu_per_year, slope_pct_per_year,
       ci_lower_95, ci_upper_95, r_squared, p_value,
       baseline_cap_pu, data_points
FROM degradation_results WHERE windfarm_id = 7361
```
See the Performance Pipeline domain section for `baseline_cap_pu` caveat before quoting `slope_pct_per_year`.

## SQL Tips
- ROUND requires numeric cast: `ROUND(col::numeric, 2)`
- No trailing semicolons
- Country code column is `code` (alpha-3: NOR, GBR), NOT `iso_code`
- Use `c.name = 'Norway'` not code for readability
- Exclude ramp-up: `WHERE is_ramp_up = false` or `CASE WHEN`
- Data may lag 1-3 months — check availability first
- `peer_group_aggregates.month IS NOT DISTINCT FROM ps.month` joins NULL-to-NULL (yearly rows) correctly
"""

SKILL_DOMAIN = """# Energy Domain Knowledge

**Capacity Factor (CF)**: SUM(generation_mwh) / (nameplate_capacity × hours_in_period). Stored 0-1; display as %.

⚠️ CRITICAL — DO NOT compute CF via `AVG(capacity_factor)`:
- Postgres `AVG()` silently drops NULL rows. Downtime hours typically have
  NULL `capacity_factor` but `generation_mwh = 0`, so the average excludes
  them and produces inflated CFs (e.g., Hamnefjell: AVG → 48.88%, correct
  nameplate-based CF → 44.25% over 7,023 NULL-CF hours).
- For multi-unit windfarms (windfarms with multiple `generation_units`),
  averaging per-unit CFs is BOTH a mathematical error (different
  denominators) AND double-counts hours (`COUNT(hour)` = 17,520 for a
  2-unit year instead of 8,760).
- Correct pattern: `SUM(generation_mwh) / (nameplate × COUNT(DISTINCT hour))`,
  GROUP BY 1, nameplate_capacity_mw. Always pull nameplate from `windfarms`,
  not from the per-row `capacity_mw` which can vary across units.

**Financial reporting periods are NOT always 12 months.** The
`financial_data` table can contain transition-year rows of 6 or 9 months
when an entity changes its fiscal year end. Always read `period_start`
and `period_end` and qualify your answer (e.g. "9-month period ending
2024-09-30") rather than implying a full year. When comparing periods
of different durations, annualise OR call out the mismatch — do NOT
declare a record "incomplete" just because it spans less than 12 months.
Typical: 25-35% onshore, 35-50% offshore. Exclude is_ramp_up=true from averages.

**Curtailment**: Deliberate output reduction. generation_mwh = metered_mwh + curtailed_mwh.
ONLY available from ELEXON (UK). If data source is NVE/ENTSOE/EIA, curtailment is NOT available — say so, don't report as zero.

**Capture Rate**: (SUM(price × gen) / SUM(gen)) / avg_market_price × 100%.
>100% = generating when prices high. <100% = generating when prices low.
Both sides MUST cover the same hours: take avg_market_price over the period the
generation data actually covers, never over price hours past the farm's last
generation reading (Norwegian NVE generation lags the price feed by months —
averaging 2026 prices under 2025 generation halves the rate). Norwegian assets
DO have day-ahead prices (NO1–NO5 via ENTSOE, EUR); capture metrics are computed for them.

**Negative Prices**: renewables > demand → negative wholesale prices. Track: COUNT(CASE WHEN price < 0).
Exposure >2-3% is significant. Typical: 0-3%.

**Bidzone**: Geographic market area with uniform prices. Codes: '10YGB----------A' (GB), '10YNO-2--------T' (NO2).
Each windfarm belongs to one bidzone.

**PPA**: Long-term power purchase agreement. Key: buyer, capacity, duration, price terms.

**Ramp-Up**: Initial commissioning phase. Flagged is_ramp_up=true. Exclude from performance averages.

**Performance** = multi-dimensional: CF + capture rate + curtailment (UK only) + anomalies + revenue/MWh.
Don't default to CF alone.

**Reported vs Metered**: `generation_mwh` (hourly metered) may differ from `reported_generation_gwh` (annual financial) by 2-5%.

## Opportunity Schemas

The platform's opportunity-detection engine evaluates a fixed catalogue of
analytical schemas for each wind farm. **Always refer to a finding by its
human NAME, never by its code** (say "Volatile Disruption Periods", not
"OPS_01"). Each opportunity row carries `schema_code`, `severity`, `branch`,
`status`, `data_slots`, and `missing_slots`; the API also returns a
`schema_name` field with the human name below.

When presenting opportunity findings, calibrate tone by severity:
- **CONFIRMED**: Be direct — name specifics, quantify impact where data allows.
- **INDICATIVE**: Be conditional — "pattern warrants investigation", "estimated at...".
- **WATCH**: Be tentative — "early signal", "recommend monitoring over next 2 quarters".
- **SUPPRESSED**: A finding gated off by the DQ-01 data-gap detector — the
  underlying signal exists but a generation-data gap makes it unreliable. Do
  NOT report SUPPRESSED rows as active findings; mention the data gap instead.

The full schema catalogue (resolve any `schema_code` to the bold NAME):

__SCHEMA_CATALOGUE__

**Branches (root-cause sub-type, where present):** OPS-01 A=event-driven /
B=structural-recurring / C=spot-exposure; OPS-02 A=mechanical-stress /
B=maintenance-timing / C=data-limited; OPS-03 A=incentive-misalignment /
B=geographic-friction / C=contract-unknown; MKT-01 A=profile-mismatch /
B=PPA-structure / C=zone-dynamics; MKT-03 A=zone-structural / B=portfolio-
concentration / C=asset-anomaly.

**Status semantics — read before narrating "active findings":**
- A schema marked **INACTIVE** (currently *PPA Underpricing* and *Forecast
  Deviation*) is blocked on missing data and emits NO per-windfarm rows — never
  imply such a finding exists.
- Rows with `status` ∈ {`ACKNOWLEDGED`, `RESOLVED`, `SUPERSEDED`} or
  `severity = SUPPRESSED` are NOT active findings — exclude them from
  active-findings narratives (query `status = 'ACTIVE'` and
  `severity <> 'SUPPRESSED'`).

**Detection window — read before quoting any period, rate or gap:**
- Each nightly run evaluates a rolling ~24-month window (720 days) per windfarm.
  The window END is clipped to the windfarm's last day with metered generation
  (EPR-126): an NVE farm whose generation stops at 31 Dec 2025 is evaluated
  through 31 Dec 2025 even when the run is in Aug 2026, so price-only months are
  never averaged into a generation-weighted metric. `detection_period_start/end`
  and `data_slots->>'period'` carry that effective window; `created_at` is the
  run date; the requested (un-clipped) end is `import_job_executions.import_end_date`
  via `detection_run_id`. The window START never moves.
- Capture-rate slots (`capture_rate`, `zone_avg_capture`, `gap_pp` on Low Capture
  Rate — Contracting; the cannibalisation index) divide a generation-weighted
  capture price by the zone time-average price over the same clipped window.
  Never recompute the denominator over price hours past the farm's last
  generation reading — that halves NO3 capture rates.
- Not every schema spans the rolling window: P50 Generation Attainment assesses
  complete calendar years (`attainment_year`); High Cannibalisation reports per
  calendar year; Turbine Degradation uses its regression's own span; Volatile
  Disruption Periods' trailing-run logic reads history before the window;
  Fleet-Age / End-of-Life Risk and PPA Expiry Horizon are point-in-time as of the
  RUN date (`as_of_year`, `as_of_date`), not the clipped end; Structural Export
  Constraint carries its own event window. Quote each schema's own period slot,
  not the header window, for those.

When `missing_slots` is populated, acknowledge the data gaps explicitly. Never present uncertain findings as definitive.

## Performance Pipeline

The performance pipeline builds empirical power curves and detects operational issues:

**Power Curves**: Built from wind speed + generation data. P50 (q50_pu) = median output at each wind speed. P10 (q90_pu) = upper capability (90th percentile). Stored in `power_curve_bins` with curve_type 'overall_clean' (all years) or 'capability' (per year).

**ODI (Operational Disruption Index)**: Measures underperformance vs the power curve. `odi_pct_underperf` = % of hours where actual output is statistically below expected (p_pu < q50 - 2.5*MAD). `odi_pct_loss_mwh` = lost energy as % of expected. `odi_pct_loss_eur` = lost revenue as % of expected (see EUR caveat below for the exact denominator).

**Wind Normalisation**: `norm_index_p50` measures operational performance independent of wind. 100 = historical average. >100 = better than average. <100 = worse. Removes the effect of how windy each period was. `norm_index_p10` is the same ratio computed against the P10 upper-capability curve — naturally lower numbers (output rarely reaches P10), useful for spotting ceiling drift.

**Degradation**: `slope_pct_per_year` in `degradation_results` shows the long-run performance trend. Negative = degrading (e.g. -0.5%/yr). Check `p_value` < 0.05 and `r_squared` for statistical significance before reporting. See the degradation caveat below for an important subtlety about the % denominator.

**Lost MWh/EUR**: In `performance_anomalies`, `lost_mwh = max(0, expected - actual)` per underperforming hour. `lost_eur = lost_mwh * market_price` (or PPA price if the windfarm has an active PPA with `ppa_price_eur_mwh` set). Summed in `performance_summaries` by month/year.

### Known metric caveats — read before answering

**ODI EUR % denominator.** `odi_pct_loss_eur` is computed as `SUM(lost_eur) / (SUM(expected_mwh) × AVG(market_price))` per period — the denominator uses the **period-average** market price, not an hourly price-weighted sum. When underperformance concentrates in high-price hours the reported EUR % **understates** true revenue impact; when it concentrates in low-price hours it overstates. When a user asks about EUR loss %, surface this caveat. For an hourly-weighted number, sum `lost_eur` from `performance_anomalies` directly and divide by `SUM((actual_mwh + lost_mwh) × market_price)` computed at the hourly grain.

**Degradation baseline & seasonality.** Two things to caveat:
1. `baseline_cap_pu` in `degradation_results` is currently a placeholder of **0.35** for every windfarm (not the per-windfarm first-year operational capability the spec called for). That means `slope_pct_per_year ≡ slope_pu_per_year / 0.35 × 100`. Treat `slope_pct_per_year` as **indicative**; when precision matters, quote `slope_pu_per_year` in p.u./year directly.
2. The OLS trend fit is applied to monthly-mean residuals — there is **no explicit seasonal decomposition** (spec called for `statsmodels.seasonal_decompose(period=8760)`; not yet wired in). Strong seasonal patterns (summer maintenance windows, winter icing) can bias the slope. Always quote `r_squared` and `p_value` next to the slope; if `p_value > 0.05` the trend is not statistically distinguishable from zero — say so.

## Generation Concentration

Measures how a windfarm's generation is distributed across hourly market prices. Stored in `generation_concentration_summaries` at month and year grain (populated for windfarms with ELEXON/ENTSOE/NVE price coverage).

- **`capture_ratio`** = `weighted_avg_capture_price_eur / time_weighted_avg_price_eur`. 1.0 means the asset captures exactly the zone's time-average price. <0.9 = generating when prices are low (classic wind cannibalisation). >1.0 = generating when prices are high (rare for unhedged onshore wind; possible for hedged or battery-augmented assets).
- **`weighted_avg_capture_price_eur`** — generation-weighted average of hourly price (numerator of capture ratio).
- **`time_weighted_avg_price_eur`** — simple hourly average over the same hours (denominator; treat as the zone reference).
- **`top_decile_share_pct` / `bottom_decile_share_pct`** — % of generation in the 10% of hours with highest / lowest prices. A healthy asset shows top-decile share above 10% and bottom-decile share below 10%.
- **`decile_shares`** (JSONB `{"d1":8.5,...,"d10":12.3}`) — full D1-D10 breakdown; D1 = lowest-price 10% of hours, D10 = highest. If `d1 + d2 > 30%`, the asset concentrates in the bottom quintile of pricing — typical of saturated onshore wind zones.
- **`vs_zone_capture_ratio_diff`** / **`vs_zone_top_decile_diff`** — pre-computed deltas against the windfarm's bidzone peer average. Use these for quick peer commentary.

Use `peer_group_aggregates` (metric keys `concentration_capture_ratio`, `concentration_top_decile_share_pct`, `concentration_bottom_decile_share_pct`) for full peer distributions (avg/p10/p50/p90) rather than re-aggregating raw data.

## Peer Group Aggregates

`peer_group_aggregates` stores pre-computed zone / country / owner / turbine-model averages of key performance metrics. Refreshed by the daily pipeline cron after each module run. **Prefer joining this table to re-aggregating from raw data** — it's consistent with the published module reports and avoids multi-windfarm scans.

- `group_type` ∈ {`bidzone`, `country`, `owner`, `turbine_model`}; `group_id` points at the corresponding table's primary key.
- `metric_key` values listed in `SKILL_SCHEMA` — covers ODI, wind-norm indices, degradation slopes, and concentration metrics.
- Columns: `avg_value`, `p10_value`, `p50_value`, `p90_value`, `windfarm_count`. Quote `p50_value` if the user asks for "typical peer"; quote `avg_value` with `windfarm_count` for "average".
- Join pattern: `ON pa.group_type = 'bidzone' AND pa.group_id = w.bidzone_id AND pa.metric_key = :metric AND pa.period_type = ps.period_type AND pa.year = ps.year AND pa.month IS NOT DISTINCT FROM ps.month`. Fall back to `group_type = 'country'` if the bidzone row is missing.
"""

# Inject the generated schema catalogue (all schemas, by NAME) into the domain
# skill file. Done via replace (not an f-string) because SKILL_DOMAIN contains
# literal `{...}` JSON examples that would break str.format.
SKILL_DOMAIN = SKILL_DOMAIN.replace("__SCHEMA_CATALOGUE__", _SCHEMA_CATALOGUE)

# Inject the full schema_code list (by CODE) into the opportunities-table row of
# SKILL_SCHEMA, same single-source generation so it can't drift to a stale subset.
SKILL_SCHEMA = SKILL_SCHEMA.replace("__OPPORTUNITY_SCHEMA_CODES__", _SCHEMA_CODES)

# EnergyExe chart theme — seeded into the agent sandbox as eexe_style.py so
# agent-generated matplotlib/Plotly charts come out on-brand by default (#161,
# supersedes the prompt-only palette from #50). The tokens mirror the May-2026
# rebrand in energyexe-client-ui/src/styles.css (dark theme: the agent page
# renders charts on navy card surfaces) plus the in-app Recharts accent set.
# Keep in sync with the FE tokens if the design system changes again.
CHART_STYLE_PY = '''"""EnergyExe platform chart theme — applies automatically on import.

Usage in any chart script:

    import eexe_style                      # rcParams applied on import
    from eexe_style import COLORS          # series palette, index by series

    # Plotly:
    fig.update_layout(**eexe_style.PLOTLY_LAYOUT)
"""

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
from cycler import cycler

# --- EnergyExe design tokens (dark theme, May-2026 rebrand) ---
BG = "#0F1B2D"        # card surface the chart sits on
FG = "#FFFFFF"        # titles
TEXT = "#CBD5E1"      # labels / annotations
MUTED = "#94A3B8"     # ticks, secondary text
GRID = "#28395A"      # gridlines / spines / borders

# Series palette, in order: brand electric blue first, then the accent set
# used by the platform's own charts.
COLORS = [
    "#4D96FF",  # brand electric blue (dark-theme primary)
    "#22D3EE",  # cyan
    "#10B981",  # emerald
    "#F59E0B",  # amber
    "#A855F7",  # violet
    "#14B8A6",  # teal
    "#EC4899",  # pink
    "#EF4444",  # red
]

plt.rcParams.update({
    "figure.facecolor": BG,
    "axes.facecolor": BG,
    "savefig.facecolor": BG,
    "savefig.dpi": 150,
    "savefig.bbox": "tight",
    "figure.figsize": (10, 5.5),
    "text.color": TEXT,
    "axes.labelcolor": MUTED,
    "axes.titlecolor": FG,
    "axes.titleweight": "bold",
    "xtick.color": MUTED,
    "ytick.color": MUTED,
    "axes.edgecolor": GRID,
    "axes.linewidth": 0.8,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.grid": True,
    "grid.color": GRID,
    "grid.alpha": 0.7,
    "grid.linestyle": "--",
    "grid.linewidth": 0.6,
    "axes.prop_cycle": cycler(color=COLORS),
    "lines.linewidth": 2,
    "lines.markersize": 4,
    "legend.frameon": False,
    "legend.labelcolor": TEXT,
    "font.family": "sans-serif",
    "font.sans-serif": ["Geist", "Inter", "DejaVu Sans", "sans-serif"],
})

# For Plotly figures: fig.update_layout(**PLOTLY_LAYOUT) then color by COLORS.
PLOTLY_LAYOUT = {
    "paper_bgcolor": BG,
    "plot_bgcolor": BG,
    "font": {"color": TEXT, "family": "Geist, Inter, sans-serif"},
    "title": {"font": {"color": FG}},
    "colorway": COLORS,
    "xaxis": {"gridcolor": GRID, "zerolinecolor": GRID, "tickfont": {"color": MUTED}},
    "yaxis": {"gridcolor": GRID, "zerolinecolor": GRID, "tickfont": {"color": MUTED}},
    "legend": {"font": {"color": TEXT}},
}
'''

SKILL_SOURCES = """# Data Source Capabilities

| Source | Countries | Generation | Prices | Curtailment | Financial | Market Exposure |
| --- | --- | --- | --- | --- | --- | --- |
| ELEXON | UK | Yes | Yes (GBP) | Yes | Yes | Yes |
| NVE | Norway | Yes | Yes (via ENTSOE, EUR) | No | Yes | Yes |
| ENTSOE | Europe (excl. UK) | Yes | Yes (EUR) | No | Partial | Partial |
| EIA | US | Yes | No | No | No | No |
| Energistyrelsen | Denmark | Yes | No | No | No | No |

**Taipower** (Taiwan) and **EEX** (Germany): Do NOT use. Not validated for client use.

If a calculation needs data not available for a source, state the limitation. Don't attempt it.

## Currency

| Source | Currency |
| --- | --- |
| ENTSOE | EUR/MWh |
| ELEXON | GBP/MWh |
| NVE | NOK/MWh |
| Financial data | Varies (EUR, GBP, NOK, DKK) — check `currency` field |

Always state currency. Never implicitly convert between currencies.

## Database Completeness
Our database is a curated subset, not the complete global inventory.
Always say "in our database" when reporting counts.
DB data is authoritative. WebSearch data must be labeled "According to [source]".
"""


# SCADA gold-layer knowledgebase (schema `scada`). Written to the sandbox ONLY
# when the scada schema exists in the connected database (staging today; prod
# after the scada prod cut) and only for admin sessions — see
# brain_agent_service._get_or_create_session. Source of truth for this text:
# energyexe-scada-pipeline scada_pipeline/gold/schema.py + PLAN.md rules G1-G11.
SKILL_SCADA = """# SCADA Turbine Data (schema `scada`)

10-minute turbine SCADA data for 3 research wind farms, processed by the
EnergyExe SCADA pipeline into pre-aggregated "gold" tables. Lives in Postgres
schema `scada` — it is NOT on the default search_path, so ALWAYS
schema-qualify: `scada.dim_farm`, never `dim_farm`.

The tables documented below are the ONLY tables in schema scada. The raw
10-minute interval data (individual wind/power/temperature readings) is NOT
in Postgres — it lives in the pipeline's Parquet lake, which you CAN query
via `python3 silver.py "SELECT ..."` (DuckDB SQL; read skill_scada_silver.md
FIRST). Never invent Postgres tables like `scada.fact_10min`. Routing rule:
daily/monthly KPIs and anything a gold table answers → db.py against schema
scada (authoritative); sub-hourly, per-signal, or raw alarm-event questions
→ silver.py.

## Farms & identity

- `farm` (slug, the join key everywhere): `hill_of_towie` (21 turbines,
  Scotland, data 2016 → 2026-04), `kelmarsh` (6, England, 2016 → 2024),
  `penmanshiel` (14, Scotland, 2016 → 2024). Static research datasets — do
  not expect current data.
- `turbine` = short code (`T01`…); `(farm, turbine)` is the natural key.
- Platform link: `dim_farm.windfarm_id` → `public.windfarms.id`. Only
  Hill of Towie is linked (windfarm_id 7309). Kelmarsh and Penmanshiel are
  NOT platform windfarms — for them, everything lives in schema scada only.

## Dimensions

**scada.dim_farm** (PK farm): name, tz (IANA, all Europe/London), country,
source_format (siemens_wps|greenbyte), windfarm_id (nullable → public.windfarms.id),
bidzone (EIC), n_turbines, rated_kw_total
**scada.dim_turbine** (PK farm,turbine): title, oem, model, rated_kw,
hub_height_m, rotor_diameter_m, lat, lon, cod (commercial operation date)
**scada.dim_turbine_config** (PK farm,turbine,config): SCD2 config epochs
(`baseline`/`aeroup` retrofit), valid_from/valid_to (NULL = open epoch)
**scada.dim_event_category** (PK source_format,category): maps event categories →
is_available (bool), loss_bucket (forced/scheduled/external/requested/
environment/derating/none), precedence (lower wins overlaps)
**scada.dim_signal_capability** (PK farm,signal): status (reported|all_null|absent),
null_pct, first_year, last_year — CHECK THIS before trusting a signal exists
for a farm.
**scada.dim_alarm_code** (PK source_format,source_code): Hill of Towie alarm-code
registry — message (OEM text, NULL for ~most codes), is_stopping, bucket
(same vocabulary as dim_event_category loss_bucket), is_available,
status (`proposed`|`confirmed`), confidence (high|medium|low), events_total,
total_hours (Σ bracketed hours, the Pareto basis), notes (evidence for the
bucket). Covers the top-80 codes by bracketed hours (91.7% of alarm-hours)
plus the 12 OEM-documented codes; codes outside it are unclassified tail.

## Daily turbine facts — PK (farm, turbine, date_utc), index (farm, date_utc)

**scada.completeness_daily**: expected_intervals, rows_present, rows_valid_core,
rows_qc_clean, completeness_pct, pre_cod
**scada.energy_daily**: energy_kwh, energy_method (meter|power_integral|mixed|none),
energy_basis (net|export), meter_net_kwh, meter_export_kwh, meter_import_kwh,
integral_kwh, intervals_meter/integral/gap, pre_cod. Gaps are counted, never
scaled — energy_kwh is what was measured, not an estimate.
**scada.energy_monthly_utc** (PK farm,turbine,month_utc; index farm,month_utc):
per-turbine monthly energy — energy_kwh, intervals_meter/integral/present.
Since the UTC frame flip this is exactly the monthly rollup of energy_daily
(same clock); prefer it for direct per-turbine monthly energy queries.
**scada.availability_daily**: method (timer_based|event_based), expected_h,
available_h, unavailable_h, unaccounted_h, generating_h, availability_pct,
IEC unavailability split: unavail_forced_h / unavail_scheduled_h /
unavail_external_h / unavail_requested_h / unavail_unclassified_h, pre_cod
**scada.losses_daily**: method, potential_kwh (epoch power curve × measured wind),
actual_kwh, loss_total_kwh (potential − actual, negatives kept = over-performance),
loss_downtime_kwh, loss_curtailment_kwh, loss_performance_kwh,
intervals_attributed/no_curve/gap, pre_cod

## Farm roll-up — USE THIS for farm-level questions

**scada.farm_kpis_daily** (PK farm,date_utc; index date_utc): n_turbines,
n_reporting, completeness_pct, energy_kwh, method mixes (n_meter/n_integral/
n_mixed/n_none, n_timer/n_event/n_signal), available_h, unavailable_h,
generating_h, availability_pct, potential_kwh, loss_total/downtime/
curtailment/performance_kwh, capacity_kw, capacity_factor.
Pre-COD turbine-days are EXCLUDED (commercial fleet only) and pct columns are
correct ratio-of-sums — never recompute farm pct by averaging turbine rows.

## Power curves & yearly performance

**scada.power_curve_bins** (PK farm,turbine,config,ws_bin): ws_bin = 0.5 m/s bin
lower edge; n, power_mean_kw, power_p50_kw, power_std_kw, ws_mean_ms
**scada.power_curve_bins_yearly**: same + year (degradation raw material)
**scada.turbine_performance_yearly** (PK farm,turbine,config,year): aep_ref_mwh,
aep_epoch_mwh, performance_index (1.0 = epoch average), bins_used,
intervals_used, ws_coverage_pct (< ~90 ⇒ index unreliable — say so)

## Alarm & event data (all 3 farms)

**scada.alarm_events** (PK farm,turbine,source_code,time_on; indexes
(farm,source_code) and (farm,turbine,time_on)): event-grain log, ~8M rows.
station_id, time_off (NULL = instantaneous status event), duration_h (NULL
when unbracketed — ALWAYS filter `duration_h IS NOT NULL` for duration
analysis), severity_class, message, iec_category + service_category
(greenbyte farms only). Hill of Towie rows are OEM alarm codes;
Kelmarsh/Penmanshiel rows are Greenbyte status events (join
dim_event_category on iec_category for their semantics). time_on/time_off
are UTC.
**scada.alarm_code_daily** (PK farm,turbine,date_utc,source_code; index
farm,date_utc): the rollup to PREFER for per-code Paretos and trends.
events_started, bracketed_events (onset UTC day), alarm_hours = hours the
code was ACTIVE that UTC day (same-code overlaps union-merged, clipped at
UTC midnights, so ≤ 24 h per row).

**scada.losses_hourly** (PK farm,hour_utc; index farm,date_utc): farm-hour
loss frame for price joins. hour_utc is tz-aware UTC; energy_kwh,
potential_kwh, actual_kwh, loss buckets, n_turbines, intervals_*
**scada.revenue_impact_daily** (PK farm,date_utc): £ by loss cause ×
day-ahead price. windfarm_id, currency (GBP), price_source, hours_priced/
unpriced, price_mean_gbp_mwh, energy_mwh, revenue_gross_gbp,
revenue_downtime_gbp, revenue_curtailment_gbp (negative on negative-price
hours), revenue_performance_gbp (negative = over-performance),
revenue_loss_total_gbp
**scada.settlement_recon_daily** (PK farm,date_utc): SCADA (turbine
terminals) vs settlement boundary meter. scada_energy_mwh,
settlement_metered_mwh, energy_delta_mwh (scada − settlement; small stable
positive ≈ 2% = quantified site loss, this is expected not an error),
energy_delta_pct, scada_curtailment_mwh, settlement_curtailed_mwh,
curtailment_delta_mwh, consumption_mwh, hours_scada/settlement/both

## Domain semantics

- **Availability** is IEC 61400-26 time-based SYSTEM availability — every stop
  counts against it regardless of cause. Two lanes: Hill of Towie
  (siemens_wps) is timer_based — OEM timers, unavailability lands in
  unavail_unclassified_h; Kelmarsh/Penmanshiel (greenbyte) are event_based —
  IEC categories from dim_event_category give the forced/scheduled/external/
  requested split.
- **Losses**: potential = the turbine's own epoch power curve applied to
  measured wind; loss = potential − actual, attributed to
  downtime/curtailment/performance. Negative losses are real
  (over-performance) — keep them, don't clip.
- **Curtailment attribution needs a power-setpoint signal, which only
  Hill of Towie reports.** Kelmarsh/Penmanshiel (greenbyte) have no setpoint,
  so their loss_curtailment_kwh is ~0 BY CONSTRUCTION — a signal gap, not
  evidence of no curtailment; any curtailment they had lands in the
  performance/downtime buckets. Never compare curtailment across farms.
- **Config epochs**: Hill of Towie turbines had an AeroUp retrofit —
  compare power curves per `config` (baseline vs aeroup), never blend epochs.
- **An "over-performance day" means `loss_total_kwh < 0`** (fleet net total),
  NOT `loss_performance_kwh < 0`. When asked about over-performance, filter
  and report loss_total_kwh unless the user explicitly asks about the
  performance bucket alone.
- **Cross-farm profitability/revenue comparison is IMPOSSIBLE.** Only
  Hill of Towie has revenue and settlement data. Never rank the three farms
  by profit/revenue, and never estimate Kelmarsh/Penmanshiel revenue from
  average prices as if it were comparable — say the comparison cannot be
  made and offer energy/loss comparisons instead, clearly labeled.
- **Settlement delta convention**: report the full-year delta as
  SUM(energy_delta_mwh) / SUM(scada_energy_mwh) × 100 (SCADA in the
  denominator) — state the denominator if you use a different one.
- **The value-lane tables are NOT calendar-complete.** For any "how many
  days / hours of data" or coverage question about revenue_impact_daily or
  settlement_recon_daily, COUNT the actual rows and SUM the hour columns —
  never assume 365 days or 8,760 hours, and never answer coverage questions
  about these farms from the platform's generation/price tables.
- **Alarm hours are NOT downtime hours.** Different codes overlap in time
  (informational codes fire while the turbine PRODUCES — e.g. code 50950,
  131,698 alarm-hours, is active during normal operation), so summing
  alarm_hours across codes double-counts and mixes production time in.
  `availability_daily` is the ONLY source of truth for downtime; alarms are
  diagnostic enrichment (which code was active when).
- **Alarm buckets are PROPOSALS.** dim_alarm_code.status is `proposed` until
  an analyst confirms it (confidence high/medium/low, evidence in notes).
  Any bucket-based aggregate MUST carry that caveat. The buckets were derived
  empirically from signal signatures — not from OEM documentation.
- **Most Hill of Towie codes are undocumented** (message IS NULL — only 12
  codes have OEM text). NEVER invent what a code means; report the number,
  its empirical stats, and the proposed bucket + confidence if present.
- **~93% of Hill of Towie events are instantaneous** (time_off NULL,
  duration_h NULL) — status transitions, not outages. Any duration or
  hours analysis must filter `duration_h IS NOT NULL` (or use
  alarm_code_daily, which already does).

## Units & conventions

- `date_utc` = UTC calendar day. The WHOLE schema is UTC-keyed (frame
  decision 2026-07-29): every day has exactly 144 ten-min intervals, no DST
  special cases. Pct over multiple days is still ratio-of-sums
  (SUM(numerator)/SUM(denominator)), NEVER AVG of daily pct.
- UTC days mean totals match UTC-keyed sources (raw SCADA, OEM reports, the
  platform's aggregates) exactly, every month. Against a GB-LOCAL-keyed
  source (Elexon settlement statements, invoices, site local-day reports)
  daily/monthly figures legitimately differ at BST period edges — same
  month-edge-hour mechanism, now mirrored: zero difference in GMT months
  (Dec–Feb). If asked why a daily £ figure mismatches a settlement statement
  line, that is the frame difference, not a data error — say so.
- Energy is **kWh** in turbine/daily/hourly tables, **MWh** in the money
  tables (revenue_impact_daily, settlement_recon_daily). Divide kWh by 1000
  before comparing.
- Money is **GBP** (check `currency`); prices £/MWh. State currency.
- `pre_cod` marks pre-commissioning days — excluded from farm KPIs; exclude
  it in turbine-level analysis too unless explicitly asked.
- Every row carries provenance: pipeline_version, computed_at.

Query patterns: `cat skill_scada_queries.md`.
"""

SKILL_SCADA_QUERIES = """# SCADA Query Patterns (schema `scada`)

## Efficiency rules

1. Every number you state must come from a query result. Never derive a
   figure by doing arithmetic in prose — run another query (or a computed
   column) instead; prose arithmetic is where errors creep in. Row counts
   must be exact `count(*)`, not pg_class/reltuples estimates.
2. Use the pre-aggregated tables: `farm_kpis_daily` for farm-level,
   `losses_hourly` for hourly, `revenue_impact_daily` for money. Never
   re-aggregate 142k turbine-day rows when a roll-up already exists.
3. Farm-level percentages come from `farm_kpis_daily` (ratio-of-sums,
   pre-COD handled). Multi-day pct = SUM/SUM, never AVG(pct).
4. Always filter on the indexed keys: `farm` + `date_utc` (or `hour_utc`).
5. Always schema-qualify (`scada.`); cross-schema joins to `public.*` work
   in the same query.

## Monthly farm KPIs

```sql
SELECT date_trunc('month', date_utc) AS month,
       round(SUM(energy_kwh)::numeric / 1000, 1)            AS energy_mwh,
       round((SUM(available_h) / NULLIF(SUM(available_h) + SUM(unavailable_h), 0))::numeric * 100, 2) AS availability_pct,
       round((SUM(energy_kwh) / NULLIF(SUM(capacity_kw) * 24, 0))::numeric * 100, 2) AS capacity_factor_pct,
       round(SUM(loss_total_kwh)::numeric / 1000, 1)        AS loss_mwh
FROM scada.farm_kpis_daily
WHERE farm = 'hill_of_towie' AND date_utc >= '2024-01-01' AND date_utc < '2025-01-01'
GROUP BY 1 ORDER BY 1
```

## Reconciling against external reports (which clock?)

All scada tables are UTC-keyed, so totals match UTC-keyed sources (raw
SCADA, OEM reports, platform aggregates) exactly — monthly energy is just
the Monthly-farm-KPIs pattern, or directly:

```sql
SELECT month_utc, round(SUM(energy_kwh)::numeric / 1000, 1) AS energy_mwh
FROM scada.energy_monthly_utc
WHERE farm = 'hill_of_towie' AND month_utc >= '2024-01-01' AND month_utc < '2025-01-01'
GROUP BY 1 ORDER BY 1
```

If the user compares against a GB-LOCAL-keyed source (Elexon settlement
statements, invoices) and sees small BST-months-only deltas that vanish
Dec–Feb, that is the clock-frame difference, not missing data — explain it
and state which frame you used.

## Loss Pareto by bucket (which loss type dominates)

```sql
SELECT round(SUM(loss_downtime_kwh)::numeric / 1000, 1)    AS downtime_mwh,
       round(SUM(loss_curtailment_kwh)::numeric / 1000, 1) AS curtailment_mwh,
       round(SUM(loss_performance_kwh)::numeric / 1000, 1) AS performance_mwh
FROM scada.farm_kpis_daily
WHERE farm = 'kelmarsh' AND date_utc BETWEEN '2023-01-01' AND '2023-12-31'
```

## Worst loss days / worst turbines

```sql
SELECT date_utc, round((loss_total_kwh/1000)::numeric,1) AS loss_mwh,
       round((loss_downtime_kwh/1000)::numeric,1) AS downtime_mwh
FROM scada.farm_kpis_daily WHERE farm = 'penmanshiel'
ORDER BY loss_total_kwh DESC NULLS LAST LIMIT 10
```
```sql
-- turbine ranking: turbine grain needed, so losses_daily is correct here
SELECT turbine, round(SUM(loss_total_kwh)::numeric/1000, 1) AS loss_mwh
FROM scada.losses_daily
WHERE farm = 'hill_of_towie' AND date_utc >= '2024-01-01' AND NOT pre_cod
GROUP BY turbine ORDER BY 2 DESC LIMIT 10
```

## Revenue impact by cause (Hill of Towie only)

```sql
SELECT date_trunc('month', date_utc) AS month,
       round(SUM(revenue_gross_gbp)::numeric)      AS gross_gbp,
       round(SUM(revenue_downtime_gbp)::numeric)   AS downtime_gbp,
       round(SUM(revenue_curtailment_gbp)::numeric) AS curtailment_gbp,
       round(SUM(revenue_performance_gbp)::numeric) AS performance_gbp
FROM scada.revenue_impact_daily
WHERE farm = 'hill_of_towie' AND date_utc >= '2025-01-01'
GROUP BY 1 ORDER BY 1
```

## Availability trend with IEC split (event-based farms)

```sql
SELECT date_trunc('month', date_utc) AS month,
       round((SUM(available_h)/NULLIF(SUM(expected_h),0))::numeric*100, 2) AS avail_pct,
       round(SUM(unavail_forced_h)::numeric, 1)    AS forced_h,
       round(SUM(unavail_scheduled_h)::numeric, 1) AS scheduled_h,
       round(SUM(unavail_external_h)::numeric, 1)  AS external_h
FROM scada.availability_daily
WHERE farm = 'kelmarsh' AND NOT pre_cod AND date_utc >= '2023-01-01'
GROUP BY 1 ORDER BY 1
```

## Power curve: baseline vs AeroUp retrofit

```sql
SELECT ws_bin, config, power_mean_kw, n
FROM scada.power_curve_bins
WHERE farm = 'hill_of_towie' AND turbine = 'T01'
ORDER BY ws_bin, config
```

## Cross-schema join to the platform (windfarm names, prices)

```sql
SELECT w.name, k.date_utc, round((k.energy_kwh/1000)::numeric, 1) AS mwh,
       round(p.day_ahead_price::numeric, 2) AS da_price
FROM scada.farm_kpis_daily k
JOIN scada.dim_farm f USING (farm)
JOIN public.windfarms w ON w.id = f.windfarm_id
JOIN public.price_data p
  ON p.windfarm_id = f.windfarm_id
 AND p.hour = k.date_utc::timestamp AT TIME ZONE 'UTC' + interval '12 hours'
WHERE k.farm = 'hill_of_towie'
ORDER BY k.date_utc DESC LIMIT 5
```
(Only hill_of_towie has windfarm_id; Kelmarsh/Penmanshiel rows drop out of
this join by design.)

## SCADA vs settlement reconciliation

```sql
SELECT date_trunc('month', date_utc) AS month,
       round(SUM(scada_energy_mwh)::numeric, 1)        AS scada_mwh,
       round(SUM(settlement_metered_mwh)::numeric, 1)  AS settled_mwh,
       round((SUM(energy_delta_mwh)/NULLIF(SUM(scada_energy_mwh),0))::numeric*100, 2) AS delta_pct
FROM scada.settlement_recon_daily
WHERE farm = 'hill_of_towie'
GROUP BY 1 ORDER BY 1 DESC LIMIT 12
```

## Alarm-code Pareto (use alarm_code_daily, join the dim for context)

```sql
-- top codes by active hours in a period; ALWAYS surface bucket status
SELECT a.source_code, d.message, d.bucket, d.status, d.confidence,
       round(SUM(a.alarm_hours)::numeric, 1) AS active_h,
       SUM(a.events_started)                 AS events
FROM scada.alarm_code_daily a
LEFT JOIN scada.dim_alarm_code d
       ON d.source_format = 'siemens_wps' AND d.source_code = a.source_code
WHERE a.farm = 'hill_of_towie'
  AND a.date_utc >= '2024-01-01' AND a.date_utc < '2025-01-01'
GROUP BY 1,2,3,4,5 ORDER BY active_h DESC LIMIT 15
```
Caveat every bucket-based number with "buckets are proposed, not confirmed"
while status = 'proposed'. Codes with NULL dim rows are the unclassified tail.

## Alarm activity vs losses on a bad day (diagnostic join)

```sql
SELECT a.turbine, a.source_code, d.message, d.bucket,
       round(a.alarm_hours::numeric, 1) AS active_h,
       round((l.loss_total_kwh/1000)::numeric, 1) AS turbine_loss_mwh
FROM scada.alarm_code_daily a
JOIN scada.losses_daily l USING (farm, turbine, date_utc)
LEFT JOIN scada.dim_alarm_code d
       ON d.source_format = 'siemens_wps' AND d.source_code = a.source_code
WHERE a.farm = 'hill_of_towie' AND a.date_utc = '2024-01-31'
  AND a.alarm_hours > 1
ORDER BY l.loss_total_kwh DESC, a.alarm_hours DESC LIMIT 20
```
Correlation, not attribution: `availability_daily` stays the downtime truth.

## Longest individual outage events (event grain)

```sql
SELECT turbine, source_code, time_on, time_off,
       round(duration_h::numeric, 1) AS hours
FROM scada.alarm_events
WHERE farm = 'penmanshiel' AND duration_h IS NOT NULL
ORDER BY duration_h DESC LIMIT 10
```
Greenbyte farms: filter/interpret via iec_category (join
dim_event_category); Hill of Towie: via source_code (join dim_alarm_code).
NEVER SUM raw duration_h across codes as "downtime" — codes overlap.
"""

SKILL_SCADA_SILVER = """# SCADA Silver Lake — raw 10-minute data via silver.py (DuckDB)

`python3 silver.py "SELECT ..."` queries the pipeline's silver Parquet lake
(S3) with DuckDB SQL. This is the RAW layer under the gold tables: 10-minute
turbine measurements (~19.8M rows), raw alarm/status events (~8M rows), and
the registry dims. Read-only; results cap at 20 displayed rows + a summary.

Routing: gold (db.py, schema scada) stays AUTHORITATIVE for daily/monthly
KPIs — energy, availability, losses, revenue are pre-computed there with QC
and meter-vs-integral selection you would otherwise have to re-derive. Use
silver ONLY for what gold can't answer: sub-hourly behaviour, per-signal
analysis (temperatures, pitch, rpm, setpoints), power curves from raw points,
alarm event sequences. NEVER recompute a gold KPI from silver and present it
as the platform number.

## Views

**measurements** — one row per (farm, turbine, 10-min interval), 46 columns:
- `ts_start_utc` TIMESTAMP: interval START, UTC (naive — no tz suffix; the
  whole lake is UTC, same frame as gold's date_utc).
- `farm`, `turbine`, `station_id` (BIGINT, hill_of_towie only, NULL elsewhere).
  Key on (farm, turbine) — turbine codes like 'T01' COLLIDE across farms.
- `year` (BIGINT): hive partition key. ALWAYS filter `farm` and, when you
  can, `year` — they prune which files are read; a full unfiltered scan
  reads the whole lake and is slow. `month` is NOT a column — derive from
  ts_start_utc.
- 39 signal columns, all DOUBLE. Main ones (unit): power_kw (kW),
  power_min/max/std_kw, energy_kwh + energy_export/import_kwh (kWh per
  10 min, hill_of_towie only), wind_speed_ms (+min/max/std, m/s),
  wind_dir_deg, nacelle_pos_deg, yaw_pos_deg (deg), gen_rpm, rotor_rpm,
  pitch_a/b/c_deg, power_setpoint_kw, reactive_power_kvar, grid_freq_hz,
  power_factor, ambient_temp_c, nacelle_temp_c, gear_oil_temp_c,
  gearbox_hs/ims_gen/rot_temp_c, main_bearing(_rear)_temp_c,
  gen_bearing_nde/de_temp_c (degC), time_running/ready/error_s (s of 600).
- `qc` (UINTEGER): QC bitmask. **`qc = 0` means clean (~95% of rows) — for
  analysis default to `WHERE qc = 0`.** Bits: 1 range_power, 2 range_wind,
  4 range_temp, 8 range_other, 16 null_core, 32 stuck_anemometer,
  64 duplicate_source_row, 128 pre_commissioning, 256 off_grid. Test a bit
  with `(qc & 128) > 0`. Exclude pre-commissioning rows from any KPI.

CAPABILITY CAVEAT — not every farm reports every signal. kelmarsh and
penmanshiel (greenbyte) have 14 all-null columns incl. energy_kwh,
time_running_s, power_setpoint_kw, yaw_pos_deg, grid_freq_hz, gearbox_*;
some signals start late (hill_of_towie wind_dir_deg only from 2022,
penmanshiel pitch/main-bearing from 2018). CHECK `dim_signal_capability`
(farm, signal, status reported|all_null, null_pct, first_year) BEFORE
declaring "no data" or averaging a mostly-null column.

**alarms** — raw event log, one row per event: farm, turbine, station_id,
time_on, time_off, source_code, severity_class (info|stop|warning|comms|NULL),
message, iec_category (greenbyte farms only), service_category, year (hive).
- `time_off` is NULL on ~93% of rows = instantaneous/status event, NOT
  "still open". For durations filter `time_off IS NOT NULL` and compute
  `epoch(time_off - time_on)`.
- Only 12 of ~589 hill_of_towie source_codes are documented
  (dim_alarm_code). NEVER invent a meaning for an undocumented code — report
  the code number and say it is undocumented.
- Alarm durations OVERLAP across codes — never sum them into "downtime";
  scada.availability_daily (gold) is the downtime truth.

**Dims** (small, safe to SELECT fully): dim_farm, dim_turbine (rated_kw,
model, cod, lat/lon), dim_turbine_config (baseline/aeroup epochs — power
curve comparisons must be epoch-scoped), dim_signal (canonical name, unit,
valid range), dim_signal_map (OEM tag lineage), dim_signal_capability,
dim_alarm_code, dim_event_category (IEC category → availability semantics).

## Golden rules

1. ALWAYS filter farm (+ year when possible) — partition pruning.
2. AGGREGATE IN SQL (GROUP BY / avg / percentile_cont / time_bucket via
   date_trunc). Never SELECT * over raw intervals; for plots, aggregate or
   sample down to <= a few thousand points first, THEN chart.
3. `WHERE qc = 0` for clean analysis; state it when you deviate.
4. DuckDB dialect: date_trunc('hour', ts_start_utc), epoch(interval),
   percentile_cont(0.5) WITHIN GROUP (ORDER BY x). No Postgres-only syntax
   like ::interval tricks; LIMIT is auto-added if you omit it.
5. Gold vs silver numbers may differ slightly by design (gold applies
   meter-QC bands and integral fallback per interval). If asked to
   reconcile, name that mechanism instead of guessing.

Example — hourly power curve points for one turbine, one month:
```sql
SELECT date_trunc('hour', ts_start_utc) AS hour_utc,
       avg(wind_speed_ms) AS ws, avg(power_kw) AS kw
FROM measurements
WHERE farm='hill_of_towie' AND turbine='T07' AND year=2025
  AND ts_start_utc >= '2025-06-01' AND ts_start_utc < '2025-07-01'
  AND qc = 0
GROUP BY 1 ORDER BY 1
```
"""


# Branded PDF report builder seeded into the sandbox as report_pdf.py.
# Lets the agent produce downloadable, document-style PDFs (EPR-68) instead of
# markdown — title, headings, paragraphs, tables, bullets, and embedded charts.
REPORT_PDF_PY = '''"""EnergyExe branded PDF report builder.

Build downloadable, document-style PDF reports / commercial summaries with
selectable text and real tables.

    from report_pdf import Report
    r = Report("Seagreen - Commercial Summary", subtitle="2024 performance")
    r.heading("Generation Performance", level=2)
    r.paragraph("Seagreen generated 4.47 TWh in 2024, a 47.3% capacity factor.")
    r.table([["Year", "Generation (GWh)", "Capacity factor"],
             ["2024", "4,465", "47.3%"], ["2025", "4,428", "46.8%"]])  # row 0 = header
    r.image("seagreen_chart.png", width_in=6.2)   # embed a matplotlib PNG chart
    r.bullets(["PPA with SSE expires 2030", "P50 attainment 89%"])
    r.save("Seagreen_Commercial_Summary.pdf")     # appears as a download button

The API is intentionally forgiving:
  Report(title="", subtitle=None)
  heading(text, level=1|2|3)        # 1=title, 2=section (default), 3=sub-heading
  paragraph(text, style=None)       # style="subtitle" for a muted lead line
  table(rows)                       # row 0 is treated as the header row
  table(headers, rows)              # OR pass header list + body rows separately
  bullets(items); image(path, width_in=6.0); save(filename)
Embed charts by saving them as PNG with matplotlib first, then r.image(path).
"""
from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import (
    Image, ListFlowable, ListItem, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle,
)

BRAND = colors.HexColor("#2563EB")     # electric blue
INK = colors.HexColor("#0F1B2D")       # near-black navy
SLATE = colors.HexColor("#475569")     # body text
LINE = colors.HexColor("#E2E8F0")      # light rule
ZEBRA = colors.HexColor("#F1F5F9")     # alternating row


class Report:
    def __init__(self, title="", subtitle=None):
        self._flow = []
        ss = getSampleStyleSheet()
        self.s_title = ParagraphStyle("t", parent=ss["Title"], textColor=INK, fontSize=22, spaceAfter=4, alignment=TA_LEFT)
        self.s_sub = ParagraphStyle("st", parent=ss["Normal"], textColor=BRAND, fontSize=11, spaceAfter=14)
        self.s_h2 = ParagraphStyle("h2", parent=ss["Heading2"], textColor=BRAND, fontSize=14, spaceBefore=14, spaceAfter=6)
        self.s_h3 = ParagraphStyle("h3", parent=ss["Heading3"], textColor=INK, fontSize=11, spaceBefore=10, spaceAfter=4)
        self.s_body = ParagraphStyle("b", parent=ss["Normal"], textColor=SLATE, fontSize=10, leading=15, spaceAfter=8)
        if title:
            self._flow.append(Paragraph(str(title), self.s_title))
        if subtitle:
            self._flow.append(Paragraph(str(subtitle), self.s_sub))

    def heading(self, text, level=2):
        style = self.s_title if level <= 1 else self.s_h2 if level == 2 else self.s_h3
        self._flow.append(Paragraph(str(text), style))
        return self

    def paragraph(self, text, style=None):
        st = self.s_sub if str(style).lower() in ("subtitle", "sub", "lead") else self.s_body
        self._flow.append(Paragraph(str(text), st))
        return self

    def bullets(self, items):
        li = [ListItem(Paragraph(str(x), self.s_body), leftIndent=10) for x in items]
        self._flow.append(ListFlowable(li, bulletType="bullet", bulletColor=BRAND, start="square"))
        self._flow.append(Spacer(1, 6))
        return self

    def table(self, data, rows=None):
        # Accept either table(rows) [row 0 = header] or table(headers, rows).
        if rows is not None:
            grid = [list(data)] + [list(r) for r in rows]
        else:
            grid = [list(r) for r in data]
        data = [[str(c) for c in r] for r in grid]
        t = Table(data, repeatRows=1, hAlign="LEFT")
        style = [
            ("BACKGROUND", (0, 0), (-1, 0), BRAND),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 9),
            ("TEXTCOLOR", (0, 1), (-1, -1), SLATE),
            ("LINEBELOW", (0, 0), (-1, -1), 0.4, LINE),
            ("TOPPADDING", (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
            ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ]
        for i in range(1, len(data)):
            if i % 2 == 0:
                style.append(("BACKGROUND", (0, i), (-1, i), ZEBRA))
        t.setStyle(TableStyle(style))
        self._flow.append(t)
        self._flow.append(Spacer(1, 10))
        return self

    def image(self, path, width_in=6.0):
        img = Image(path)
        w = width_in * inch
        img.drawHeight = img.drawHeight * (w / img.drawWidth)
        img.drawWidth = w
        img.hAlign = "LEFT"
        self._flow.append(img)
        self._flow.append(Spacer(1, 10))
        return self

    def save(self, filename):
        doc = SimpleDocTemplate(
            filename, pagesize=A4,
            leftMargin=0.8 * inch, rightMargin=0.8 * inch,
            topMargin=0.8 * inch, bottomMargin=0.8 * inch,
            title=filename,
        )
        doc.build(self._flow)
        return filename
'''
