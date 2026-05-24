# Spec vs implementation — gaps and improvement plan

> **STATUS (2026-05-25): everything in the original improvement plan has shipped.** All headline bugs (A, B, C), Gap D (Module 1b), Gap E (Module 4), the API gaps in Module 5, the Module 6 polish work, and the W1/W2 quick wins are merged to master. See `HANDOFF.md` for the consolidated post-implementation summary and what's left (backfill + release note + analyst-review UI).
>
> The body of this document is preserved verbatim as the historical record of the comparison work. Items now resolved are listed below; check `HANDOFF.md` for which PR shipped each fix.

## Resolved (status 2026-05-25)

| Item from this doc | Resolved by |
|---|---|
| Bug A — Module 5 monthly OLS | PR #64 (hourly OLS + seasonal) |
| Bug B — Module 5 no seasonal decomposition | PR #64 (statsmodels `seasonal_decompose`) |
| Bug C — Module 5 hardcoded `baseline_cap_pu = 0.35` | PR #70 (per-windfarm first-year median) |
| Gap D — Module 1b not implemented | PR #68 (with B1.5 Q50-ratio extension beyond spec) |
| Gap E — Module 4 yearly aggregation basis | PR #66 |
| #5 `ci_95_pct` not exposed | PR #70 |
| #6 `p_value` not in API | PR #70 (via `DegradationResponse`) |
| #9 Module 3/5 loss reference for constrained hours | PR #71 (df_no_over) + PR #72 (active-flag mask) |
| #12 PPA scenario `Revenue_Uplift_vs_Base_EUR` | PR #67 |
| Module 6 `Contract_Revenue_EUR` + vs-P50 | PR #67 |
| Module 1b downstream wiring through Modules 2/3/5 | PR #72 (active = pending_review OR confirmed) |
| W1 dead code in wind_normalisation_service | PR #66 |
| W2 NaN-price warning | PR #69 |
| W3 + W4 docstring updates | PR #64 |

**Still open from this doc:**
- #11 PPA scenario caching — deferred; dashboards aren't hitting it hard yet
- #13 Hourly `lost_mwh_proxy` persistence — deferred; revisit if dashboards need it
- E1 `_load_hourly_data` cross-service import cleanup — tech debt; see `HANDOFF.md`
- E2 `pipeline_run_audit` row counts — tech debt; see `HANDOFF.md`
- E3 `pipeline_run_id` on every output table — partial
- E4 Document IsolationForest opt-in — partial (env var documented in module-3 doc)

---

## Original analysis (preserved for historical context)

Detailed comparison between the May 2026 reference pipeline (`energyexe_pipeline_full.py`, 1,216 lines, Jupyter-style monolith with CSV outputs) and our productionised service-based implementation in `app/services/`.

The reference code was generated for one windfarm running off a CSV. Our system runs nightly across ~360 windfarms, reads/writes Postgres, exposes APIs to admin/client UIs, and enriches with bidzone peer comparison. **Most architectural deltas are deliberate productionisation wins**. The gaps that matter are mathematical / statistical correctness bugs, not architecture.

This doc is structured as:

1. **Headline findings** — what's actually wrong and what to fix first.
2. **Severity-ranked issue list** — every delta, ranked.
3. **Module-by-module deltas** — full detail per module.
4. **Improvement plan** — concrete work items grouped into 4 milestones.

---

## 1. Headline findings

Three statistical bugs are producing numbers that are visibly published in the UI today but are demonstrably wrong relative to the spec:

### 🔴 Bug A — Module 5 (Degradation) fits OLS on monthly aggregates, not hourly residuals

- **Spec** (`energyexe_pipeline_full.py:1019–1031`): builds an hourly residual series → seasonal decompose (additive, period 8760 h) → fit OLS on **per-hour** deseasonalised residuals. `n` ≈ tens of thousands.
- **Ours** (`app/services/degradation_service.py:185–199`, `:208–219`): groupby `(year, month)`, mean of residuals → fit OLS on **monthly means**. `n` ≈ 24–60.

**Consequences:**
- Discards ~99 % of the data points. CI95 width is determined by `n`; ours is artificially narrow because we lost the variance of the underlying hours but kept the same number of degrees of freedom *as if* the means were independent measurements.
- Slope estimate is still unbiased on average but no longer matches the spec, so spec-vs-ours comparison won't reconcile.
- A windfarm with a single bad month moves our slope by 100× more than it would in the spec.

### 🔴 Bug B — Module 5 (Degradation) has no seasonal decomposition

- **Spec** (`energyexe_pipeline_full.py:323–334` and `:1019–1021`): `remove_seasonal_component` calls `statsmodels.tsa.seasonal_decompose(model='additive', period=8760)` and subtracts the seasonal component before fitting.
- **Ours**: no statsmodels call; `degradation_service.py:8` only mentions it in a docstring. Residuals are fit raw.

**Consequences:**
- If the dataset starts/ends in unequal seasons (e.g. Aug 2020 → May 2023), the OLS slope absorbs the season-difference and reports degradation that isn't there. Bias can flip sign for short datasets.
- Once we move to hourly OLS (Bug A), this matters even more — the seasonal cycle has thousands of hours of leverage on the fit.

### 🔴 Bug C — Module 5 (Degradation) `slope_pct_per_year` uses a hardcoded baseline of 0.35 p.u.

- **Spec** (`energyexe_pipeline_full.py:1050–1053`): baseline = median of `cap_bin` (q90 or q50 capability) for first year of data.
- **Ours** (`app/services/degradation_service.py:237`): `baseline_cap_pu = 0.35` — a hardcoded constant with a comment admitting it's a placeholder.

**Consequences:**
- The headline number we render in the UI — `slope_pct_per_year` — is `slope_pu / 0.35 × 100` for every windfarm regardless of its actual capacity factor. A low-CF windfarm (baseline ≈ 0.20) sees its % slope under-reported by ~75 %; a high-CF offshore farm (baseline ≈ 0.50) sees it over-reported by ~40 %.
- This silently makes every windfarm's published degradation % wrong by an amount that varies by site. CI95 stored in p.u./yr is correct; the %/yr derived number is the broken one.

### ⚠️ Gap D — Module 1b not implemented at all

Without structural-constraint detection, Modules 2, 3, and 5 silently get poisoned by cable failures and export outages. We documented this in `module-1b-structural-constraint-detection.md`. **This is greenfield work**, not a bug fix — but it's the highest-impact greenfield item because it gates accurate Module 3 and 5 results on farms with infrastructure events.

### ⚠️ Gap E — Module 4 uses a different historical_mean basis for yearly index

- **Spec** (`energyexe_pipeline_full.py:910–917`): yearly historical_mean is the mean of **yearly** avg_norm_ratios; monthly historical_mean is the mean of monthly avg_norm_ratios. Two different baselines.
- **Ours** (`app/services/wind_normalisation_service.py:176–190`): uses the monthly historical_mean for both the monthly index AND the yearly index.

**Consequences:**
- Numerically, monthly_mean ≠ yearly_mean when month-row-counts vary (e.g. a windfarm commissioned mid-year, or months with missing data). The yearly index can differ by a few percent vs the spec.
- Also: spec computes yearly avg_norm_ratio as `groupby('year').mean()` on the monthly table (mean of monthly means). Ours uses `groupby('year').mean()` on the hourly table (mean across all hours). With month-size imbalance, the two diverge.

---

## 2. All deltas ranked by severity

| # | Severity | Module | Delta | Recommendation |
|---|---|---|---|---|
| 1 | 🔴 high | 5 | OLS on monthly aggregates, not hourly | Switch to hourly residuals |
| 2 | 🔴 high | 5 | No seasonal decomposition | Add statsmodels `seasonal_decompose` |
| 3 | 🔴 high | 5 | Hardcoded `baseline_cap_pu = 0.35` | Compute from first-year capability median |
| 4 | 🔴 high | 1b | Module 1b not implemented | Build it — high blast radius on 2/3/5 |
| 5 | 🟠 med | 5 | CI95 stored as p.u./yr, not also as %/yr | Add `ci_lower_95_pct`, `ci_upper_95_pct` (or derive on read) |
| 6 | 🟠 med | 5 | `p_value` computed but not exposed via API | Add to response schema |
| 7 | 🟠 med | 4 | Yearly historical_mean basis differs from spec | Align with spec — separate yearly baseline |
| 8 | 🟠 med | 4 | Yearly avg computed from hours, not monthly means | Match spec — `mean(mean per month)` |
| 9 | 🟠 med | 3 | Loss reference doesn't swap to `overall_clean` for constrained hours | Wire after Module 1b lands |
| 10 | 🟠 med | 3 | No `flag_any_anomaly` (OR of stat + IF) stored | Derive on read; or add column |
| 11 | 🟡 low | 6 | PPA scenario results not persisted (recomputed on each request) | Optional: cache in `ppa_scenario_results` table |
| 12 | 🟡 low | 6 | No `Revenue_Uplift_vs_Base_EUR` when base PPA price is in scenarios | Add to scenario response |
| 13 | 🟡 low | 6 | Constraint proxy stored as annual aggregate only — no hourly | Acceptable; revisit if dashboards need hourly |
| 14 | 🟡 low | 6 | No CSV export for commercial summary | Optional — UI consumes JSON directly |
| 15 | 🟡 low | 1 | No `data_quality_report.csv` / `cleaning_exclusion_summary.csv` | We log counts; could persist if useful |
| 16 | 🟡 low | 2 | Curve plotting (PNG) absent | Charts render client-side; not needed |
| 17 | 🟢 info | All | We process per-windfarm-batch; spec is single-windfarm-CSV | Productionisation win |
| 18 | 🟢 info | 3 | Spec also flags `flag_any_anomaly` via IsolationForest; ours stores `flag_isolation_forest` separately and excludes from loss | We're correct per spec intent — IF is informational only |
| 19 | 🟢 info | 1 | Spec has `auto_detect_columns`; ours pulls from typed DB columns | Not applicable |
| 20 | 🟢 info | 6 | Spec uses `cfg.p50_target_mwh_per_year = 150_000` constant; ours uses per-windfarm `p50_targets` table | We are better here |
| 21 | 🟢 info | All | Bidzone peer aggregate enrichment not in spec; we add it via `PeerAggregateService` | We are better here |

---

## 3. Module-by-module deltas

### Module 1 — Data loading & cleaning

| Aspect | Spec | Ours | Verdict |
|---|---|---|---|
| Hard filters (0/40 wind, -0.05/1.20 p_pu) | ✓ | ✓ | Match |
| `df_clean` / `df_curve` split | ✓ | ✓ | Match |
| `df_clean["flag_outside_curve_wind"]` boolean column | ✓ | ✗ | Cosmetic — spec keeps the row with a flag; we drop it. Equivalent for downstream. |
| `month_period` column derived | ✓ | ✗ | We derive `month` from `hour.dt.month` instead. Equivalent. |
| `data_quality_report.csv` | ✓ | ✗ | We log counts; not persisted. |
| `cleaning_exclusion_summary.csv` | ✓ | ✗ | Same — counts in return dict, not persisted. |
| `auto_detect_columns` from CSV variants | ✓ | n/a | We read from typed DB columns. |
| `ramp_up` exclusion | not mentioned | ✓ added | Data quality improvement. |
| Source | CSV | Postgres (3 queries → merge) | Productionisation. |

**Verdict:** functionally equivalent. No bugs.

### Module 1b — Structural constraint detection

| Aspect | Spec | Ours |
|---|---|---|
| Leave-one-year-out reference Q90 per wind bin | ✓ `_detect_constraint_periods` (`:425–494`) | ✗ |
| Wind-banded ratio thresholds (0.70 for 7-10 m/s, 0.80 for 10-25 m/s) | ✓ `cfg.constraint_detection_bands` | ✗ |
| Run grouping + minimum duration (336 h) | ✓ | ✗ |
| `structural_constraint_flags.csv` with `review_status='pending_review'` | ✓ | ✗ |
| `flag_structural_constraint` propagated through Modules 2/3/5 | ✓ | ✗ |
| Analyst review workflow | spec leaves to backend | ✗ |
| Warning logs for each candidate run | ✓ | ✗ |

**Verdict:** **entirely missing**. The agent in `module-1b-structural-constraint-detection.md` sketches the schema and integration point.

### Module 2 — Power curve analysis

| Aspect | Spec | Ours | Verdict |
|---|---|---|---|
| `_build_bin_agg`: q50, q90, MAD, n; filter `n >= 30` | ✓ | ✓ `compute_bin_stats` | Match |
| Yearly raw curves stored | ✓ | ✓ | Match |
| Overperformance flag: `p_pu > q90_bin + 1.5×MAD` OR `> 1.02` | ✓ | ✓ `flag_overperformance` | Match |
| Capability curve = yearly stats from `df_no_over` | ✓ | ✓ | Match |
| `overall_clean` from all-years `df_no_over` | ✓ | ✓ (year=NULL row) | Match |
| **Capability and overall_clean built from `df_curve_clean` (excludes constraints)** | ✓ | ✗ | Tied to Module 1b. Without 1b, `overall_clean` can be diluted by constraint hours. |
| Propagate `flag_structural_constraint` into `df_no_over` (`:587–592`) | ✓ | ✗ | Tied to Module 1b. |
| CSV outputs (`yearly_power_curves*.csv`) | ✓ | ✗ | We persist to Postgres. |
| Idempotent (delete before insert) | n/a (CSV) | ✓ | Productionisation. |

**Verdict:** mathematically identical. Constraint-aware behaviour blocked on Module 1b.

### Module 3 — Anomaly detection & loss

| Aspect | Spec | Ours | Verdict |
|---|---|---|---|
| Underperf flag: `p_pu < q50_bin - 2.5×MAD` | ✓ | ✓ | Match |
| Overperf flag: `p_pu > q90_bin + 1.5×MAD` OR `> 1.02` | ✓ | ✓ | Match |
| IsolationForest with `contamination=0.03` | ✓ | ✓ (opt-in) | Match |
| `flag_any_anomaly` = `under | over | IF` stored as a column | ✓ | ✗ | Cosmetic — we have the components, can derive. |
| `expected_mwh = q50_bin × rated_mw` from yearly capability | ✓ | ✓ | Match |
| **For `flag_structural_constraint=True` hours: swap reference to `overall_clean.q50`** | ✓ (`:716–725`) | ✗ | Tied to Module 1b. **High impact** — without it, constrained hours are scored against contaminated yearly curves. |
| Loss in EUR = `lost_mwh × price` (PPA if applicable, else spot) | ✓ (`:270–280`) | ✓ (`_get_ppa_price`) | Match |
| Spec: fill missing market_price with mean of non-NaN; raise if all-NaN and no PPA | ✓ | ✗ — left-joined NaN passes through silently | We silently drop NaN-priced hours from EUR sums. Spec is more vocal. |
| Run detection (gap > 1 h breaks run) | ✓ | ✓ | Match |
| `long_run_hours` filter (≥ 24 h) on a separate output | ✓ separate `anomaly_long_runs.csv` | ✓ tracked as `long_run_count`, `max_run_hours` on `performance_summaries` | Match in spirit |
| `constraint_loss_summary` per-run breakdown | ✓ (`:782–821`) | ✗ | Tied to Module 1b. |
| ODI metrics (3 flavours) | ✓ | ✓ on `performance_summaries` | Match |

**Quirk to flag.** Spec computes `lost_mwh_underperf = (expected - actual).clip(lower=0)` for *every* hour, then separately tracks `lost_mwh_underperf_only` where `flag_underperf_anomaly=True`. So it persists "potential loss" for non-anomaly hours too (rows where actual was slightly below expected but didn't cross the 2.5×MAD threshold). Ours only writes a `performance_anomalies` row when flagged, so we never see the "sub-threshold underperformance" tail. This is intentional in our schema and *probably* fine — but worth knowing if a stakeholder asks "why doesn't ODI sum to a known-good number".

**Verdict:** core logic matches. Two real gaps tied to Module 1b. One philosophical difference (sub-threshold tail not persisted) — defensible.

### Module 4 — Wind normalisation

| Aspect | Spec | Ours | Verdict |
|---|---|---|---|
| `norm_ratio = actual_mw / expected_mw` from `overall_clean` curve | ✓ | ✓ | Match |
| Wind floor 4 m/s | ✓ | ✓ `NORM_WIND_MIN_MPS` | Match |
| Q50 and Q90 both run | ✓ | ✓ | Match |
| Monthly avg from hourly, then `monthly historical_mean = mean(monthly avg)` | ✓ | ✓ | Match |
| Monthly index `= avg / monthly_historical_mean × 100` | ✓ | ✓ | Match |
| **Yearly avg = `mean of monthly avg per year`** (mean of means) | ✓ (`:911–913`) | ✗ — we average hours per year | Slight mismatch when months have unequal hours |
| **Yearly historical_mean = mean of yearly avg ratios** (a *different* baseline) | ✓ (`:915`) | ✗ — we reuse the monthly historical_mean | Yearly index drift |
| Hourly ratios CSV per reference | ✓ | ✗ persisted; available on-demand via API | Productionisation choice. |

**Concrete numerical impact:** for windfarms where every month has the same hours (no gaps), the two methods coincide. For farms with partial-month coverage (commissioned mid-year, restored after outage, etc.), our yearly index can drift 1–3 %. Small but real.

**Verdict:** monthly indices match. Yearly indices drift slightly when month-row-counts are uneven.

### Module 5 — Degradation

This is where the biggest divergence sits. Three bugs and a couple of API gaps.

| Aspect | Spec | Ours | Verdict |
|---|---|---|---|
| Operational range 4–14 m/s | ✓ | ✓ | Match |
| Drop bins where `q50 < 0.10` | ✓ | ✓ | Match |
| Residual = `p_pu - cap_bin` (per-year capability) | ✓ | ✓ | Match |
| **Seasonal decomposition (statsmodels, additive, period 8760)** | ✓ (`:1020`) | ✗ | **Bug B** |
| **OLS fit on hourly residuals (deseasonalised)** | ✓ (`:1023–1031`) | ✗ — fits monthly aggregates | **Bug A** |
| `year_fraction = year + (dayofyear-1)/365.25` per hour | ✓ | ✗ — `year + (month - 0.5)/12` per month | Consequence of Bug A |
| OLS slope, intercept, R², stderr, t-distribution CI95 | ✓ | ✓ on monthly data | Math is right, granularity wrong |
| **Baseline = median of cap_bin in first-year hours** | ✓ (`:1050–1053`) | ✗ — hardcoded `0.35` | **Bug C** |
| `slope_pct = slope / baseline × 100` | ✓ | ✓ but with wrong baseline | Consequence of Bug C |
| `ci95_pct` returned alongside `ci95` (p.u.) | ✓ (`:1061–1062`) | ✗ — only stored in p.u./yr; %/yr CI not exposed | API gap |
| **Exclude `flag_structural_constraint` hours from OLS fit** | ✓ (`:1004–1008`) | ✗ | Tied to Module 1b |
| `n_constraint_hours_excluded` reported | ✓ | ✗ | Tied to Module 1b |
| `p_value` available | ✓ | computed but not in API response | Schema gap |

**Numerical impact estimate.** For a typical windfarm with `baseline ≈ 0.40` and our hardcoded `0.35`:
- Our `slope_pct_per_year` is `(0.40 / 0.35 − 1) ≈ +14 %` too negative-magnitude (over-reports degradation).
- For windfarms with lower CF (`baseline ≈ 0.25`), `0.35 / 0.25 − 1 ≈ +40 %` under-reports degradation by 30–40 %.
- For high-CF offshore (`baseline ≈ 0.55`), over-reports by ~35–55 %.

The CI95 in p.u./yr is correct (a robust output) but the headline %/yr is systematically off by a windfarm-specific amount.

### Module 6 — Commercial reporting

| Aspect | Spec | Ours | Verdict |
|---|---|---|---|
| Constraint proxy `lost_mwh_proxy = (q90 - q50) × rated_mw` per hour | ✓ (`:1138–1147`) | ✓ (within `_compute_commercial_metrics`) | Match |
| Annual roll-up: `Actual_MWh`, `Contract_Revenue_EUR`, `Contract_Revenue_vs_P50Target_EUR`, `LostEnergyProxy_MWh`, `LostValue_EUR` | ✓ | ⚠️ partial — `constraint_proxy_mwh` and `lost_value_eur` persisted; revenue/gap **only via on-demand scenario endpoint** | We don't store Contract_Revenue_EUR or vs-P50 gap. Recomputed each request. |
| PPA scenarios with `Value_of_1pct_EUR_per_year` | ✓ | ✓ on-demand | Match |
| `Revenue_Uplift_vs_Base_EUR` when base PPA price is in scenarios | ✓ (`:1184–1191`) | ✗ | Gap |
| P50 target as fixed `cfg.p50_target_mwh_per_year = 150_000` | ✓ | ✓ better — per-windfarm via `p50_targets` table | We are better |
| CSV outputs | ✓ | ✗ | Productionisation choice. |
| Persist hourly `lost_mwh_proxy` timeseries | ✓ via `ts` DataFrame (but only CSV anyway) | ✗ | Acceptable; revisit if dashboards need it |

**Verdict:** core math matches; persistence is shallower (annual aggregates only). The Revenue_Uplift column is a missing nice-to-have. P50-target handling is genuinely better than spec.

---

## 4. Improvement plan

Grouped into milestones. Each milestone is independently shippable.

### Milestone A — Fix Module 5 (degradation) bugs ★ ship first

Effort: ~3–5 days. Highest correctness payoff. No new tables.

1. **A1. Switch OLS to hourly residuals.** Change `compute_residuals` to return per-hour residuals (not monthly aggregates). Change `fit_degradation_trend` to take hourly. `year_fraction` becomes `year + (dayofyear - 1) / 365.25`.
2. **A2. Add seasonal decomposition.** Add `statsmodels` to `pyproject.toml`. Port the spec's `remove_seasonal_component` helper. Gate on `len(series) >= 2 × 8760`; pass-through with warning otherwise.
3. **A3. Compute baseline_cap_pu from data.** In `fit_degradation_trend`, compute baseline as the median of the reference column (`q50_pu` or `q90_pu`) for the **first year** of data in the operational range. Store on `degradation_results.baseline_cap_pu`.
4. **A4. Expose `p_value` and `ci_*_pct` in the API response.** Add fields to `DegradationResponse` schema; convert CI95 from p.u./yr → %/yr using the stored baseline.

**Risk:** existing `degradation_results` rows will have a different (correct) baseline → published %/yr values will shift. Communicate to stakeholders before rollout. A one-time re-run of the daily pipeline is sufficient.

**Tests:** add tests in `tests/test_degradation.py` covering (i) golden hourly+seasonal vs monthly-no-seasonal on a synthetic dataset; (ii) baseline_cap computation matches spec on a fixture; (iii) `ci95_pct` is consistent with `ci95_pu / baseline × 100`.

### Milestone B — Build Module 1b (structural constraint detection)

Effort: ~5–8 days. Highest blast radius — unlocks correct Module 3 / Module 5 for any farm with infrastructure events.

5. **B1. New service `app/services/structural_constraint_detection_service.py`** with `detect_constraints(df_clean, df_curve, windfarm_id)` that implements:
   - Leave-one-year-out Q90 reference per wind bin.
   - Observed Q90 per (wind_bin, calendar month).
   - Band-based ratio thresholds (default `[{7-10: 0.70}, {10-25: 0.80}]`).
   - Run grouping with `min_hours = 336`.
6. **B2. New model + migration `structural_constraint_flags`** — schema as proposed in `module-1b-structural-constraint-detection.md`: period_start/end, duration_hours, wind_bins_affected, mean_q90_ratio, review_status (`pending_review` | `confirmed` | `dismissed`), reviewed_by, reviewed_at, analyst_notes.
7. **B3. Orchestrator integration.** In `performance_pipeline_service.run_pipeline`, between Module 1 (`df_clean/df_curve` produced) and Module 2 (`pcs.build_power_curves`), call the detector and produce `df_curve_clean = df_curve[~flag_structural_constraint]`. Pass `df_curve_clean` (and the constraint runs) into Module 2.
8. **B4. Wire constraint flag through Modules 2, 3, 5.**
   - Module 2: build `capability` and `overall_clean` from `df_curve_clean`. (Already idempotent — just feed cleaner input.)
   - Module 3: when `flag_structural_constraint=True`, use `overall_clean.q50` instead of `yearly_capability.q50` as the loss reference. Add `flag_structural_constraint` column on `performance_anomalies`.
   - Module 5: drop constrained hours from the OLS fit. Add `n_constraint_hours_excluded` to `degradation_results`.
9. **B5. Review API.** New endpoints `GET /api/v1/structural-constraints?status=pending_review` and `POST /api/v1/structural-constraints/{id}/review` for analyst confirm/dismiss. Admin UI gets a review queue page (separate ticket).
10. **B6. Notification.** Post-cron hook in `pipeline_daily.py` that queries new `pending_review` rows since last run and emails via `app/services/alert_service.py` or similar.

**Risk:** the auto-detector will generate false positives (prolonged maintenance, noise curtailment). Mitigation: every flag lands as `pending_review`; only `confirmed` flags affect downstream modules. Until an analyst confirms, Module 2's curves use all data (current behaviour). We're additive, not destructive.

**Tests:** synthetic dataset with a known 7-month cable failure → detector flags it; without B1–B4 the OLS slope is steeper than the true degradation; with them, it matches truth within noise.

### Milestone C — Fix Module 4 (wind normalisation) yearly aggregation

Effort: ~1 day. Numerical alignment with spec.

11. **C1. Align yearly aggregation.** In `compute_indices`, build yearly avg as `monthly_index.groupby('year').avg_norm_ratio.mean()` (mean of monthly means) rather than averaging hours. Use a separate `yearly_historical_mean` based on yearly avg_norm_ratios. Update `norm_index_p50` / `_p10` writeback accordingly.

**Risk:** published yearly indices will shift 1–3 % for windfarms with uneven monthly coverage. Minor — usually within UI tolerance — but worth a release note.

**Tests:** add a fixture with one fully-covered year (8760 h) and one half-covered year (4380 h) → assert yearly_index for half-covered year uses 6-month average, not 12-month-weighted average.

### Milestone D — Module 6 polish + persistence

Effort: ~2 days. Lower priority unless dashboards demand it.

12. **D1. Persist `Contract_Revenue_EUR` and `Contract_Revenue_vs_P50Target_EUR` per yearly summary row.** Add columns on `performance_summaries` (or compute on read from `actual_mwh`, `lost_value_eur`, `p50_targets`).
13. **D2. Add `Revenue_Uplift_vs_Base_EUR` to PPA scenario response** when the windfarm's contracted PPA price is in the scenario list.
14. **D3. Optional: cache PPA scenario results.** Only if dashboards start hitting the on-demand endpoint hard. New table `ppa_scenario_results (windfarm_id, year, price_scenario, ...)` if needed.

### Cross-cutting: cleanup and observability

15. **E1. Stop using `_load_hourly_data` as a private cross-service import.** `DegradationService`, `WindNormalisationService`, and `PerformanceAnomalyService` all reach into `PowerCurveService._load_hourly_data`. Promote to a public helper or to a dedicated `HourlyDataLoader` (would also make Module 1's responsibilities explicit).
16. **E2. Persist row counts at each cleaning stage.** Spec's `data_quality_report.csv` and `cleaning_exclusion_summary.csv` are useful — add a small `pipeline_run_audit` row per `(windfarm, pipeline_run_id)` with `raw_rows, clean_rows, curve_rows, overperf_removed, constraint_excluded, etc.` Cheap, helps debug pipeline drift.
17. **E3. Surface `pipeline_run_id` on every output table.** Module 5 already does; Modules 3, 4, 6 don't consistently. Enables joining "this Module 3 row was produced by run X which had these audit counts".
18. **E4. Document the IsolationForest opt-in.** It's gated by `PIPELINE_USE_ISOLATION_FOREST` env var; default off. Either commit to "always on, informational only" or remove the dead branch.

---

## 5. Quick wins (under a day each, ship immediately)

- **W1.** Fix the unused `val = getattr(...)` line at `app/services/wind_normalisation_service.py:211` (variable is assigned then overwritten on `:212` — dead code).
- **W2.** Add `p_value` to `DegradationResponse` schema — it's already computed and stored, just not surfaced.
- **W3.** Add a warning log when market_price has > 5 % NaN rows (per spec at `:275–279`). Currently they're silently dropped from EUR loss sums.
- **W4.** Replace the docstring at `degradation_service.py:8` ("optionally remove seasonal component") so it's accurate until Milestone A lands — currently it claims a feature we don't have.

---

## 6. What we should NOT change

These are deliberate productionisation choices, not gaps:

- **No CSV outputs.** Postgres + API > Jupyter CSV for multi-tenant production.
- **Per-windfarm batch.** Spec is single-windfarm; ours iterates 360+ windfarms in `run_pipeline_batch`.
- **Bidzone peer enrichment.** Not in spec; high product value.
- **Per-windfarm P50 targets via `p50_targets` table.** Spec uses a single constant; ours is correctly time-bounded per windfarm.
- **No matplotlib chart generation.** Charts are React + Recharts in the UI; spec's `Agg` backend was for the Jupyter setup.
- **APScheduler cron + savepoint-per-year.** Productionisation; not in spec.

---

## 7. Reading order if you're new to this

1. `README.md` — pipeline architecture and orchestration.
2. The per-module docs — concept-first explanations of what each module does.
3. **This doc** — what's different from the reference code and what to fix.
4. The reference Python code `energyexe_pipeline_full.py` if you want the canonical maths in one place.
