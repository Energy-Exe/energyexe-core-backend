# Wind Farm Performance Pipeline — Documentation

Reference docs for the six-module performance pipeline that turns raw hourly generation / weather / price data into the operational KPIs the platform surfaces (ODI, power curves, wind-normalised indices, degradation slopes, commercial summaries).

📋 **[HANDOFF.md](./HANDOFF.md) — current status + what's left to do.** Read this first.

These docs describe **what the system actually does today** — last comprehensive update 2026-05-25 (after the pipeline correctness work landed). The original module spec is `EnergyExe_Pipeline_Module_Documentation - 15 May 2026.docx` (SharePoint → Development → Documentation → Other current docs). Per-module status vs spec is at the bottom of each module doc.

## Module index

| Module | Doc | Service file | Status |
|---|---|---|---|
| 1 — Data loading & cleaning | [module-1-data-loading.md](./module-1-data-loading.md) | inside `power_curve_service.py` | implemented |
| 1b — Structural constraint detection | [module-1b-structural-constraint-detection.md](./module-1b-structural-constraint-detection.md) | `structural_constraint_detection_service.py` | implemented (PR #68 + #72, with B1.5 Q50-ratio extension) |
| 2 — Power curve analysis | [module-2-power-curve.md](./module-2-power-curve.md) | `power_curve_service.py` | implemented |
| 3 — Anomaly detection & loss | [module-3-anomaly-detection.md](./module-3-anomaly-detection.md) | `performance_anomaly_service.py` | implemented |
| 4 — Wind normalisation | [module-4-wind-normalisation.md](./module-4-wind-normalisation.md) | `wind_normalisation_service.py` | implemented (yearly-aggregation fix PR #66) |
| 5 — Degradation | [module-5-degradation.md](./module-5-degradation.md) | `degradation_service.py` | implemented (hourly OLS + per-WF baseline + CI%; PRs #64, #70, #71, #72) |
| 6 — Commercial reporting | [module-6-commercial-reporting.md](./module-6-commercial-reporting.md) | inline in `performance_pipeline_service.py` + `ppa_service.py` + `p50_target_service.py` | implemented (contract revenue + PPA uplift PR #67) |

📋 **[Spec vs implementation — gaps and improvement plan](./spec-vs-implementation.md)** — original line-by-line comparison against the May 2026 reference Python pipeline. **Status section at top tracks which items have shipped.** Body of the doc is preserved as the historical record.

## Architecture at a glance

```
┌──────────────────────────────────────────────────────────────────────┐
│ Source data                                                          │
│   generation_data   (Elexon / ENTSOE / EIA / Taipower / NVE)         │
│   weather_data      (ERA5 Copernicus, 100 m wind)                    │
│   price_data        (ENTSOE / ELEXON spot)                           │
│   windfarms         (nameplate_capacity_mw)                          │
└──────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼  loaded once per windfarm
                  PowerCurveService._load_hourly_data()
                                  │
                          df_clean  /  df_curve     ← Module 1
                                  │
                                  ▼
                  ┌──────────────────────────────┐
                  │ Module 2 — power curves      │  → power_curve_bins
                  │   raw / capability / overall │     (q50, q90, mad, n)
                  └──────────────────────────────┘
                                  │
                                  ▼
            ┌───────────────────────────────────────────────────┐
            │ Module 3 — anomalies & loss                       │  → performance_anomalies
            │   MAD flags · IsolationForest · ODI · runs        │  → performance_summaries (ODI cols)
            └───────────────────────────────────────────────────┘
                                  │
                                  ▼
            ┌───────────────────────────────────────────────────┐
            │ Module 4 — wind normalisation (Q50 & Q90 refs)    │  → performance_summaries
            │   hourly norm_ratio → monthly/yearly index        │     (norm_ratio_p50/p10, norm_index_*)
            └───────────────────────────────────────────────────┘
                                  │
                                  ▼
            ┌───────────────────────────────────────────────────┐
            │ Module 5 — degradation (Q50 & Q90 refs)           │  → degradation_results
            │   residuals → OLS slope %/yr + CI95               │
            └───────────────────────────────────────────────────┘
                                  │
                                  ▼
            ┌───────────────────────────────────────────────────┐
            │ Module 6 — commercial                             │  → performance_summaries
            │   constraint proxy (q90-q50) × rated × price      │     (constraint_proxy_mwh, lost_value_eur)
            │   PPA scenarios (on-demand, not persisted)        │
            └───────────────────────────────────────────────────┘
```

## Orchestration

Everything is driven by **`PerformancePipelineService`** (`app/services/performance_pipeline_service.py`):

- `run_pipeline(windfarm_id, ...)` — executes Modules 1→6 sequentially for one windfarm. Loads the hourly DataFrame **once** and reuses it across modules (avoids redundant SQL). Each module runs inside a SAVEPOINT so one year/reference failing doesn't poison the whole transaction.
- `run_pipeline_batch(...)` — wraps `run_pipeline` in an `ImportJobExecution` and iterates all operational windfarms. One windfarm failing does not abort the rest.

Three triggers:

| Trigger | File | Notes |
|---|---|---|
| Daily cron | `app/cron/pipeline_daily.py:run_pipeline_job` | 03:00 UTC default (after weather + generation imports finish ~02:30). Configurable via `PIPELINE_DAILY_ENABLED`, `PIPELINE_DAILY_HOUR`, `PIPELINE_DAILY_MINUTE`. `max_instances=1`, `coalesce=True`. |
| Manual API | `POST /api/v1/performance-pipeline/run` | Admin UI "Run Pipeline" button. Optional `windfarm_ids` filter. |
| Read API | `GET /api/v1/performance-pipeline/...` | Read-only endpoints for power curves, ODI, wind-norm, degradation, commercial summary. Consumed by admin-ui and client-ui. |

## Output tables

| Table | Grain | Written by | Read by |
|---|---|---|---|
| `power_curve_bins` | (windfarm, year, curve_type, wind_bin) — `year=NULL` for `overall_clean` | Module 2 | Modules 3, 4, 5, 6; admin-ui curve chart; client-ui curve chart |
| `performance_anomalies` | (windfarm, hour) — only flagged hours | Module 3 | admin-ui anomalies panel |
| `performance_summaries` | (windfarm, period_type, year, month) — month=NULL for yearly | Modules 3, 4, 6 | ODI tiles, wind-norm charts, commercial summary |
| `degradation_results` | (windfarm, reference_curve, pipeline_run_id) — two rows per run (q50 + q90) | Module 5 | degradation card + LLM commentary |

## Where each module fits in the spec vs reality

The spec (Word doc) names a Jupyter-notebook-style pipeline that writes CSV files. **Our system is the productionised version**: same maths, but services that read/write Postgres tables rather than CSVs. A few notable differences from the spec:

- **Module 1b is not implemented yet** — the spec's NEW module (post-Niord update) for detecting cable/export failures. Without it, Modules 2/3/5 can be contaminated by a multi-month export outage. See [module-1b doc](./module-1b-structural-constraint-detection.md).
- **Module 5 has no seasonal decomposition** — spec calls for additive decomposition (period 8760 h) before the OLS fit; we skip it. May introduce bias on datasets that don't span whole calendar years cleanly.
- **Module 6 has no CSV export and no multi-year roll-up** — yearly commercial fields exist on `performance_summaries`, PPA scenarios are computed on-demand and not persisted.

Each module doc has a **"Gaps vs spec"** section with details.

## How to read these docs

Each module doc follows the same shape:

1. **Purpose** — what it answers in one paragraph.
2. **Concepts** — plain-language explanation of the statistical / domain ideas (per-unit, MAD, residuals, P50/P10, ODI, etc.). Read this first if you're new to the pipeline.
3. **Implementation walkthrough** — file and line references mapped to the spec steps (2a, 2b, 3a, etc.).
4. **DB model** — columns, grain, unique constraints.
5. **Caller graph** — orchestrator → cron / API → admin-ui / client-ui consumers.
6. **Gaps vs spec** — code-vs-doc deltas worth knowing.

File paths are relative to `energyexe-core-backend/`.
