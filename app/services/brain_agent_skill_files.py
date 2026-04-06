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
**ppas**: windfarm_id, ppa_buyer, ppa_size_mw, ppa_duration_years, ppa_start_date, ppa_end_date, ppa_notes
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

## SQL Tips
- ROUND requires numeric cast: `ROUND(col::numeric, 2)`
- No trailing semicolons
- Country code column is `code` (alpha-3: NOR, GBR), NOT `iso_code`
- Use `c.name = 'Norway'` not code for readability
- Exclude ramp-up: `WHERE is_ramp_up = false` or `CASE WHEN`
- Data may lag 1-3 months — check availability first
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
