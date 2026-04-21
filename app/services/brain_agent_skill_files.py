"""Skill file templates — written to the agent sandbox for lazy-loading via `cat`."""

SKILL_SCHEMA = """# Database Schema Reference

## Core Tables

**windfarms**: id, name, code, nameplate_capacity_mw (Float), location_type (onshore/offshore), foundation_type (fixed/floating), status (operational/decommissioned/under_installation/expanded), country_id, state_id, region_id, bidzone_id, lat, lng, commercial_operational_date, ramp_up_end_date

**generation_data**: hour (timestamptz), windfarm_id, generation_unit_id, generation_mwh (Numeric 12,3), metered_mwh (Numeric 12,3), curtailed_mwh (Numeric 12,3), capacity_mw, capacity_factor (0-1 decimal), consumption_mwh, is_ramp_up (bool), source (ENTSOE/ELEXON/EIA/NVE), quality_flag, completeness
- Unique: (hour, generation_unit_id, source)
- Generation is per generation_unit — GROUP BY windfarm_id for windfarm totals

**price_data**: hour (timestamptz), windfarm_id, bidzone_id, day_ahead_price (Numeric 12,4), intraday_price, currency (EUR/GBP/NOK/DKK), source
- Unique: (hour, windfarm_id, source)

**weather_data**: hour (timestamptz), windfarm_id, wind_speed_100m, wind_direction_deg, temperature_2m_k, temperature_2m_c, source (ERA5)
- Unique: (hour, windfarm_id, source)

**financial_data**: financial_entity_id, period_start, period_end, currency, revenue, total_revenue, ebitda, depreciation, ebit, net_income, reported_generation_gwh
- Linked via: windfarm_financial_entities(windfarm_id, financial_entity_id)

## Supporting Tables

**turbine_models**: model, supplier, original_supplier, rated_power_kw, cut_in_wind_speed_ms, cut_out_wind_speed_ms, rated_wind_speed_ms, blade_length_m, rotor_diameter_m
**turbine_units**: windfarm_id, turbine_model_id, lat, lng, hub_height_m, status, start_date, end_date
**windfarm_owners**: windfarm_id, owner_id, ownership_percentage
**owners**: id, code, name, type (energy/institutional_investor/community_investors/municipality/private_individual/supply_chain_oem/other/unknown)
**ppas**: windfarm_id, ppa_buyer, ppa_size_mw, ppa_duration_years, ppa_start_date, ppa_end_date, ppa_notes, contract_type (fixed_price/indexed/hybrid/merchant), ppa_status (active/expired/renegotiating), ppa_price_eur_mwh, has_availability_penalties (bool)
**opportunities**: windfarm_id, schema_code (OPS_01/OPS_02/OPS_03/MKT_01/MKT_02/MKT_03), severity (CONFIRMED/INDICATIVE/WATCH), branch (A/B/C), status (ACTIVE/ACKNOWLEDGED/RESOLVED/SUPERSEDED), data_slots (JSONB), missing_slots (JSONB list), triggered_by_id, detection_period_start, detection_period_end
**power_curve_bins**: windfarm_id, year (NULL=overall_clean), curve_type (raw/capability/overall_clean), wind_bin (Numeric 2.0-25.0 in 1.0 steps), q50_pu (median P50), q90_pu (90th pct P10), mean_pu, mad_pu, sample_count. Unique: (windfarm_id, year, curve_type, wind_bin)
**performance_anomalies**: windfarm_id, hour, anomaly_type (underperformance/overperformance), actual_p_pu, expected_p_pu, wind_speed, wind_bin, lost_mwh, lost_eur, market_price, run_id. Unique: (windfarm_id, hour)
**performance_summaries**: windfarm_id, period_type (month/year), year, month. ODI: odi_pct_underperf, lost_mwh, expected_mwh, odi_pct_loss_mwh, lost_eur, odi_pct_loss_eur, long_run_count, max_run_hours. Norm: norm_ratio_p50, norm_index_p50, norm_ratio_p10, norm_index_p10. Commercial: constraint_proxy_mwh, lost_value_eur
**degradation_results**: windfarm_id, reference_curve (q50=P50/q90=P10), slope_pu_per_year, slope_pct_per_year, intercept, r_squared, p_value, ci_lower_95, ci_upper_95, baseline_cap_pu, data_points
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
```sql
SELECT DATE_TRUNC('month', hour) as month,
       ROUND(AVG(CASE WHEN is_ramp_up = false THEN capacity_factor END)::numeric * 100, 1) as cf_pct,
       ROUND(SUM(generation_mwh)::numeric, 0) as gen_mwh
FROM generation_data WHERE windfarm_id = 7182
AND hour >= '2025-01-01' AND hour < '2026-01-01'
GROUP BY 1 ORDER BY 1
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

**Capacity Factor (CF)**: generation / (nameplate_capacity × hours). Stored 0-1; display as %.
Typical: 25-35% onshore, 35-50% offshore. Exclude is_ramp_up=true from averages.

**Curtailment**: Deliberate output reduction. generation_mwh = metered_mwh + curtailed_mwh.
ONLY available from ELEXON (UK). If data source is NVE/ENTSOE/EIA, curtailment is NOT available — say so, don't report as zero.

**Capture Rate**: (SUM(price × gen) / SUM(gen)) / avg_market_price × 100%.
>100% = generating when prices high. <100% = generating when prices low.

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

The platform detects 6 opportunity types for wind farms. When presenting opportunity findings, calibrate tone by severity:
- **CONFIRMED**: Be direct — name specifics, quantify impact where data allows.
- **INDICATIVE**: Be conditional — "pattern warrants investigation", "estimated at...".
- **WATCH**: Be tentative — "early signal", "recommend monitoring over next 2 quarters".

**OPS-01 Volatile Disruption Periods**: Low availability months (ODI proxy). Branch A = event-driven (concentrated in 1-2 months), B = structural/recurring across years, C = spot exposure amplifies cost.

**OPS-02 Performance Seasonality**: High-wind season capacity factor worse than low-wind season. Indicates mechanical stress, maintenance timing, or cannibalisation. Branch A = mechanical stress, B = maintenance timing, C = data-limited.

**OPS-03 Misaligned Contracting**: OEM/AM contract doesn't incentivize uptime. Only fires when OPS-01 exists. Branch A = incentive misalignment (no penalties), B = geographic friction (remote teams), C = contract details unknown.

**MKT-01 Low Capture Rates**: Capture rate gap vs zone average. >10pp = CONFIRMED, 5-10pp = INDICATIVE, 2-5pp = WATCH. Branch A = profile mismatch (high cannibalisation index), B = PPA structure (expiry within 24mo), C = zone dynamics.

**MKT-02 Storage Opportunity**: Downstream of MKT-01. Evaluates BESS potential for energy shifting or MFRR revenue. Currently data-limited for most assets.

**MKT-03 High Cannibalisation**: CI = 1/capture_rate. CI >1.20 for 2+ years = CONFIRMED. Prices depressed when asset generates. Branch A = zone structural (penetration rising), B = portfolio concentration, C = asset-level anomaly.

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
