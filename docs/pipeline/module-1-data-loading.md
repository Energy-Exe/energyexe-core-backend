# Module 1 — Data loading & cleaning

## Purpose

Pulls hourly generation, wind speed and price for a single windfarm out of Postgres, normalises power to per-unit of nameplate capacity, and applies hard plausibility filters. Produces two DataFrames that every downstream module consumes:

- `df_clean` — every row that passed plausibility checks (wind 0–40 m/s, p_pu in [-0.05, 1.20]).
- `df_curve` — subset of `df_clean` restricted to the curve-fitting range (wind 2–25 m/s).

Module 1 does not write to the database. It runs in memory and the DataFrames are passed into Module 2 and reused by Modules 3, 4, 5.

## Concepts

### Per-unit (p.u.)
We never compare windfarms in raw MW because rated capacities differ (a 50 MW farm producing 25 MW is "running at half" — the same as a 200 MW farm producing 100 MW). `p_pu = generation_mwh / nameplate_capacity_mw` puts every farm on a 0–1 scale (1.0 = full rated output). All downstream maths is in p.u.

### Hard plausibility filters
Real-world energy data has measurement glitches (sensor faults, telemetry hiccups, ramp-up/shutdown noise). Two filters drop the worst:

- **Wind speed 0 ≤ v ≤ 40 m/s.** Below 0 is impossible; above 40 m/s is rare and usually a sensor spike or extreme storm where output is unreliable.
- **Power -0.05 ≤ p_pu ≤ 1.20.** A small negative band accommodates parasitic consumption (turbine drawing tiny amounts to keep itself alive). The upper cap of 1.20 (120 %) catches metering glitches but is generous enough not to drop transient over-rated spikes that physically can happen for a few seconds.

Rows failing either filter are dropped entirely from `df_clean`.

### `df_clean` vs `df_curve`
- `df_clean` is the input to **anomaly detection (Module 3)** and **degradation (Module 5)** — modules that need every operational hour, including very low and very high wind.
- `df_curve` is restricted to 2–25 m/s — the range where a 1 m/s wind bin can be reliably fitted into a power curve. Below 2 m/s the turbine is barely spinning; above 25 m/s many turbines shut down for safety.

### Wind binning (1 m/s)
Continuous wind speed gets bucketed into 1 m/s bins (`[2,3), [3,4), … [24,25)`) so we can compute per-bin statistics. Width is a trade-off: narrower bins = better resolution but fewer samples per bin; wider bins = more samples but smear behaviour across genuinely different operating regimes. 1 m/s is industry standard.

## Implementation walkthrough

All Module 1 logic lives inside `power_curve_service.py`:

| Step | Method | Lines |
|---|---|---|
| Load raw hourly data | `_load_hourly_data` | `app/services/power_curve_service.py:94–181` |
| Hard plausibility filters + curve subset | `apply_hard_filters` | `app/services/power_curve_service.py:185–204` |

### `_load_hourly_data` — three queries + merge

Three separate SQL queries are issued (avoiding a 3-way nested join that misbehaves with the query planner):

1. **Generation** (`app/services/power_curve_service.py:124–138`)
   - Source: `generation_data` table.
   - Aggregation: `SUM(generation_mwh)` across all generation units of the windfarm, per hour.
   - Filter: `HAVING BOOL_OR(is_ramp_up) = false` — excludes any hour where any unit was in ramp-up state (turbine startup, transient).
   - Returns: `hour`, `generation_mwh`, `any_ramp_up`.

2. **Weather** (`app/services/power_curve_service.py:142–153`)
   - Source: `weather_data` table.
   - Aggregation: `AVG(wind_speed_100m)` per hour (usually one row per hour; AVG defends against duplicates).
   - Height: **100 m** (the standard ERA5 reanalysis height; turbine hubs are typically 80–140 m).
   - Returns: `hour`, `wind_speed_100m` → renamed to `wind_speed`.

3. **Price** (`app/services/power_curve_service.py:156–167`)
   - Source: `price_data` table.
   - Aggregation: `AVG(day_ahead_price)` per hour (averages across ENTSOE + ELEXON if a bidzone has both).
   - Currency: EUR/MWh (column default `EUR` on `price_data`).
   - Returns: `hour`, `day_ahead_price` → renamed to `market_price`.

### Merge & p.u. computation (`app/services/power_curve_service.py:169–181`)

- **Inner join** generation + weather on `hour`. Both are required; if wind is missing we can't compare to a curve.
- **Left join** price on `hour`. Optional — used only by Modules 3 (loss EUR) and 6 (commercial). Missing price → loss/revenue columns are NaN downstream.
- Compute `year = hour.year` and `p_pu = generation_mwh / rated_mw`.
- Dedup and sort by `hour`.

Resulting DataFrame has columns: `hour, year, generation_mwh, wind_speed, market_price, p_pu`.

### `apply_hard_filters` — produce `df_clean` and `df_curve`

```python
df_clean = df[
    (df.wind_speed >= 0) & (df.wind_speed <= 40) &
    (df.p_pu >= -0.05) & (df.p_pu <= 1.20) &
    df.wind_speed.notna() & df.p_pu.notna()
]
df_curve = df_clean[(df_clean.wind_speed >= 2) & (df_clean.wind_speed <= 25)]
```

Constants live at the top of the file (`app/services/power_curve_service.py:24–34`):

| Constant | Value | Purpose |
|---|---|---|
| `WIND_MIN_ALLOWED` | 0.0 | hard floor for cleaning |
| `WIND_MAX_ALLOWED` | 40.0 | hard ceiling for cleaning |
| `P_PU_MIN_ALLOWED` | -0.05 | parasitic-load tolerance |
| `P_PU_MAX_ALLOWED` | 1.20 | metering-spike tolerance |
| `WIND_MIN_FOR_CURVE` | 2.0 | lower bound of curve-fit range |
| `WIND_MAX_FOR_CURVE` | 25.0 | upper bound of curve-fit range |
| `BIN_WIDTH` | 1.0 | wind bin width (m/s) |

## Data sources

| Entity | Table | Column(s) read | Notes |
|---|---|---|---|
| Generation | `generation_data` | `hour`, `generation_mwh`, `is_ramp_up` | Summed across all units; ramp-up hours excluded |
| Wind | `weather_data` | `hour`, `wind_speed_100m` | ERA5 Copernicus reanalysis, 100 m height |
| Price | `price_data` | `hour`, `day_ahead_price` | Averaged across sources (ENTSOE + ELEXON if both exist) |
| Rated capacity | `windfarms` | `nameplate_capacity_mw` | Used to compute p_pu |

All timestamps are UTC end-to-end. No local-time / BST conversion is applied at this stage (ELEXON BST handling happens upstream during raw → hourly aggregation — see `MEMORY.md` "ELEXON BST Fix Pattern").

## Aggregation level

**Per-windfarm**, hourly. Generation is summed across all generation units of the windfarm before normalising by the single windfarm-level `nameplate_capacity_mw`. There is no per-unit power curve.

## Caller graph

- Called once at the top of `PerformancePipelineService.run_pipeline` (`app/services/performance_pipeline_service.py:117`).
- The resulting DataFrame is passed via `df_preloaded` arguments into every subsequent module so each one doesn't re-query Postgres.

## Outputs

Module 1 returns two pandas DataFrames in memory — nothing is persisted.

The orchestrator does record per-pipeline-run row counts in its return value, e.g.:

```json
{ "raw_rows": 26280, "clean_rows": 25890, "curve_rows": 23650 }
```

## Gaps vs spec

| Spec ask | Status in code | Notes |
|---|---|---|
| Hard filters at 0/40 wind, -0.05/1.20 p.u. | ✓ implemented | `app/services/power_curve_service.py:185–204` |
| Auto-detect column variants from common names | ✗ not applicable | We pull from typed DB columns, not arbitrary CSV headers — the spec's column-name fallback is irrelevant. |
| `data_quality_report.csv` output | ✗ not produced | Spec output as CSV; we don't generate this. Row counts at each stage are returned in the orchestrator result dict but not persisted. |
| `cleaning_exclusion_summary.csv` output | ✗ not produced | Same — counts exist transiently in the result dict. |
| Per-generation-unit curves | ✗ not implemented | We aggregate to windfarm-level before normalising. Spec is ambiguous; per-unit would require per-unit nameplate and per-unit weather. |
| Timezone awareness | ✓ UTC throughout | DB columns are `DateTime(timezone=True)`; ERA5 is UTC; ELEXON BST correction happens upstream. |
| Ramp-up exclusion | ✓ added (spec doesn't mention) | We filter `is_ramp_up = true` hours — a data quality improvement not in the spec. |

## File reference

- Implementation: `app/services/power_curve_service.py:24–204`
- Called from: `app/services/performance_pipeline_service.py:117` (orchestrator)
- Source models: `app/models/generation_data.py`, `app/models/weather_data.py`, `app/models/price_data.py`, `app/models/windfarm.py`
