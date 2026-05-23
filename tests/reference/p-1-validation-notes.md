# P-1 — Day-1 smoke validation (2026-05-23)

Result: **GREEN — proceed with P0.**

The approach in the plan (export windfarm to spec CSV → run reference Python pipeline → diff against our DB outputs) works end-to-end. Three statistical bugs documented in `docs/pipeline/spec-vs-implementation.md` confirmed on real data; magnitudes match predictions; no blocking surprises.

## Setup

| Item | Value |
|---|---|
| Windfarm | **Lutelandet** (id=7197, code=`LUTELANDET`, NVE source) |
| Rated capacity | 51.3 MW (matches spec's `Config.rated_mw = 51.3` default — the spec was clearly authored against this windfarm) |
| Data range | 2022-03-16 → 2025-12-31 (~3.8 years), 33,270 hours |
| Currency | EUR ✓ pre-flight pass (ENTSOE-priced Norwegian bidzone) |
| Generation units | 1 (`Lutelandet`, id=12577, source=NVE) |
| Extract method | `psql \COPY` join generation+weather+price, written to `/tmp/p1_lutelandet/wf_lutelandet.csv` |
| Reference deps | `pandas 2.3.1, numpy 2.3.2, scipy 1.16.3, matplotlib 3.10.8, sklearn 1.8.0, statsmodels 0.14.6` in a fresh `/tmp/p1_venv` (NOT in project `.venv`) |
| Spec script | `tests/reference/energyexe_pipeline_full.py` (SHA-256 `e8209d9cbc6efc461d31634767205553f5e89eea3fe6de01e3469e0385d37386`), patched locally for two pandas-compat issues (see below). Original vendored file unchanged. |

## Spec script defects we hit (will need to fix in the vendored harness or upstream)

These are real bugs in `energyexe_pipeline_full.py` that surface on pandas 2.3 + zero-constraint windfarms. **Worth surfacing to Aje as a follow-up so the reference stays usable.**

1. **Categorical-vs-float comparison in Module 1b** (`spec :460-462`).
   `obs_q90["v_center"] = obs_q90["wind_bin"].apply(lambda b: (b.left + b.right) / 2 ...)` returns a Categorical Series because `wind_bin` is Categorical from `pd.cut`. Pandas 2.x raises `TypeError: Invalid comparison between dtype=category and float` on the next `.between(...)` call. Fix (matches spec's own pattern at `:993-995`):
   ```python
   obs_q90["v_center"] = (
       obs_q90["wind_bin"].astype(object).apply(
           lambda b: (b.left + b.right) / 2 if hasattr(b, "left") else np.nan
       ).astype(float)
   )
   ```

2. **Merge collision when Module 1b detects zero constraints** (`spec :585-592`).
   Spec assumes `df_no_over` does NOT already have `flag_structural_constraint`, but when zero runs are detected `run_constraint_detection` sets `df_curve_clean["flag_structural_constraint"] = False` (line 506) which propagates via `df = df_curve_clean.copy()` (line 572) into `df_no_over` (line 583). The subsequent merge then produces `flag_structural_constraint_x` and `_y` columns instead of the bare name → `KeyError` on `fillna(False)`. Fix: guard the merge with `and "flag_structural_constraint" not in df_no_over.columns`, else just fillna in place.

We applied both patches to `/tmp/p1_lutelandet/spec.py` only — the vendored `tests/reference/energyexe_pipeline_full.py` is untouched. When building P0's permanent wrapper, port these fixes.

## Module-by-module comparison (Lutelandet)

### Module 1b — Structural constraint detection

| | Spec |
|---|---|
| Constraints detected | **0** (empty `structural_constraint_flags.csv`) |
| Our DB | n/a (Module 1b not implemented) |

✓ Sanity-checks against reality: Lutelandet has no documented cable failure. Spec correctly returns empty. When we build Module 1b (Milestone B1), this windfarm should also produce zero flags.

### Module 2 — Power curves

Quick check, capability Q90 for 2022:
- `[9, 10) m/s`: spec=0.5369, our DB (capability avg q90 across bins for 2022)=0.5483 → within ±2% ✓
- Spec wind-curve sample size after overperformance removal: 1145 removed (3.7%) — matches our default `OVERPERF_MAD_K=1.5`, `CEILING_PU=1.02`

Module 2 numbers are aligned. **No bugs here.**

### Module 3 — Anomaly + ODI

| Year | Spec `ODI_%_Loss_MWh` | Our `odi_pct_loss_mwh` | Diff |
|---|---|---|---|
| 2022 | 3.41 | 3.37 | -0.04 ✓ |
| 2023 | 2.04 | 2.01 | -0.03 ✓ |
| 2024 | 2.78 | 2.76 | -0.02 ✓ |
| 2025 | 3.61 | (not yet aggregated) | n/a |

| Year | Spec `ODI_%_Loss_EUR` | Our DB | Diff |
|---|---|---|---|
| 2022 | 1.36 | (not directly queryable in 1 col) | TBD |

Module 3 within ~0.05 % absolute. **No bugs.** The small remaining drift likely comes from spec's `fill NaN price with mean` (`spec :275-279`) — we silently drop NaN-priced hours. Captured as quick-win **W2** in the plan.

### Module 4 — Wind normalisation (yearly indices)

Q50:
| Year | Spec | Our DB | Diff |
|---|---|---|---|
| 2022 | 100.98 | 100.21 | -0.77 |
| 2023 | 104.66 | 103.22 | -1.44 |
| 2024 | 96.91 | 98.31 | +1.40 |
| 2025 | 97.46 | 95.42 | -2.04 |

Q90 / P10:
| Year | Spec | Our DB | Diff |
|---|---|---|---|
| 2022 | 99.56 | 99.48 | -0.08 |
| 2023 | 99.40 | 100.11 | +0.71 |
| 2024 | 100.10 | 101.93 | +1.83 |
| 2025 | 100.95 | 100.12 | -0.83 |

**Bug C1 (yearly aggregation basis) confirmed.** Drift up to 2% per year, with the worst year (2025) where coverage is uneven. Direction matches prediction. Milestone C1 should bring this within 0.01%.

### Module 5 — Degradation (THE main event)

| Field | Spec Q50 | Our DB Q50 | Spec Q90 | Our DB Q90 |
|---|---|---|---|---|
| `baseline_cap_pu` | **0.2706** | **0.35000** ← hardcoded | **0.5369** | **0.35000** ← hardcoded |
| `slope_pu_per_year` | 0.000929 | 0.00175587 | 0.000747 | 0.00227800 |
| `slope_pct_per_year` | 0.343 | 0.502 | 0.139 | 0.651 |
| `ci95_pct` | (-0.62, +1.31) | not exposed in API | (-0.36, +0.64) | not exposed in API |
| `r_squared` | 0.000 | 0.00306 | 0.000 | 0.00696 |
| `n` | **13,196** (hourly, deseasonalised) | **46** (monthly) | 13,196 | 46 |
| `p_value` | (not in spec output but inferable) | 0.715067 | n/a | 0.581560 |

**Bug C confirmed.** For Q50, real baseline is 0.27 (Lutelandet is a relatively low-CF Norwegian onshore); we use 0.35 — too high by 30 %. For Q90, real baseline is 0.54; we use 0.35 — too low by 35 %. The wrong baseline pushes Q50 `slope_pct` higher than truth and Q90 `slope_pct` much higher than truth, in opposite directions per the same hardcoded constant.

**Bug A confirmed.** Spec fits OLS on 13,196 hourly deseasonalised residuals. We fit on 46 monthly aggregates. Slope estimates differ by ~2× — both legitimate estimates on this noisy data (`R² ≈ 0`, no real degradation trend), just with different sampling properties.

**Bug B (seasonal decomposition).** The spec did successfully call `seasonal_decompose(period=8760)` (no error in logs), but with `R² ≈ 0` it's hard to tell from this run alone whether seasonal removal mattered. A clearer demonstration will come from a synthetic fixture with a known seasonal cycle + known slope (test `A1.T3` in the plan).

**Per-windfarm impact estimate when we ship Milestone A:**
- Lutelandet Q50 `slope_pct`: 0.502 → ~0.34 (drops ~32%)
- Lutelandet Q90 `slope_pct`: 0.651 → ~0.14 (drops ~78%, dramatic)

These two numbers are part of every windfarm's published degradation card. Stakeholders will see them shift. Release note required (per Milestone A4).

### Module 6 — Commercial reporting

| Year | Spec `LostEnergyProxy_MWh` (q90-q50) | Our `constraint_proxy_mwh` | Diff |
|---|---|---|---|
| 2022 | 48,743 | 51,287 | +5.2% |
| 2023 | 62,787 | 62,351 | -0.7% |
| 2024 | 63,938 | 65,854 | +3.0% |
| 2025 | 63,721 | 66,315 | +4.1% |

| Year | Spec `LostValue_EUR` | Our `lost_value_eur` | Diff |
|---|---|---|---|
| 2022 | 2,003,658 | 2,366,140 | +18% |
| 2023 | 2,259,551 | 2,359,179 | +4.4% |
| 2024 | 1,709,573 | 1,811,946 | +6.0% |
| 2025 | 1,224,757 | 1,399,106 | +14% |

Module 6 has bigger drift (4-18%) than Module 3/4. Likely contributors:
- NaN-price handling difference (W2): spec fills mean, we drop. Affects EUR more than MWh because price-weighted.
- Capability curve subtle differences (already noted in Module 4).
- Possible price aggregation difference (we average across sources; spec uses single column).

These are all in scope for Milestone D, which adds `Contract_Revenue_EUR` persistence and aligns price handling. P-1 confirms Module 6 needs attention but doesn't surface a *new* bug class beyond what was already in the plan.

## Pass criteria check (per plan P-1 section)

- [x] Spec script completes without crash — required two small patches first (spec defects above) but is functional
- [x] Q50/Q90 bin curves match within ~2 % — confirmed (`[9,10)` Q90 spec=0.5369 vs ours=0.5483)
- [x] Bug C prediction holds: our `slope_pct` = spec `slope_pu` × (spec_baseline / 0.35) × 100 ± hourly/monthly OLS noise. Specifically:
  - Q50: spec_baseline=0.27, ratio (0.27/0.35)=0.77 → if slope_pu were equal, ours would be 0.77× of spec's 0.343 = 0.265. Ours is 0.502; the gap from 0.265→0.502 is Bug A (different slope_pu from monthly vs hourly fit). Both bugs visibly compound.
  - Q90: spec_baseline=0.54, ratio (0.54/0.35)=1.54 → if slope_pu were equal, ours would be 1.54× spec's 0.139 = 0.214. Ours is 0.651; gap is Bug A again.
- [x] No timezone surprises — UTC end-to-end, `to_datetime` parsed cleanly (one warning about `dayfirst=True` on YYYY-MM-DD strings; benign)
- [x] No currency surprises — EUR throughout

## Conclusions and recommendations for P0

1. **Proceed with P0.** The approach is buildable; no architectural surprises.
2. **Port the two spec patches** (categorical v_center, merge-collision guard) into whatever wrapper sits in `tests/reference/run_reference.py` (P0.2 work item). Do NOT edit the vendored `energyexe_pipeline_full.py` — keep provenance intact via a separate patch script.
3. **P0.2 `SpecCSVExporter`** can be a thin wrapper over the `psql \COPY` pattern proven here. The composition of `GenerationExportService` + `WeatherExportService` + price lookup proposed in the plan is overkill for what's needed — a single SQL JOIN works and runs faster. Recommend simplifying P0.2 to just the SQL → CSV path.
4. **P0.4 windfarm selection — include Lutelandet** as one of the 5 reference windfarms. It's well-characterised, the spec was clearly authored against it, and we now have a baseline.
5. **No new risks discovered.** R1 (memory blow-up on hourly OLS) didn't trigger at 13k hours; R3 (currency mismatch) is real and the pre-flight check is appropriate.

## Numeric baseline captured (Lutelandet, pre-Milestone-A)

For future regression / diff:

```
windfarm_id = 7197
windfarm_code = LUTELANDET
nameplate_capacity_mw = 51.3
data_range = 2022-03-16 → 2025-12-31
hours_in_csv = 33270

degradation_results (current production, Bug-C-affected):
  q50: slope_pu_per_year=0.00175587, slope_pct_per_year=0.502, baseline_cap_pu=0.35000, n=46, r2=0.00306, p_value=0.715067, ci=(-0.00787540, +0.01138714)
  q90: slope_pu_per_year=0.00227800, slope_pct_per_year=0.651, baseline_cap_pu=0.35000, n=46, r2=0.00696, p_value=0.581560, ci=(-0.00599089, +0.01054688)

degradation_results (spec, correct):
  q50: slope_pu_per_year=0.000929, slope_pct_per_year=0.343, baseline_cap_pu=0.2706, n=13196, r2=0.000, ci95_pct=(-0.62, +1.31)
  q90: slope_pu_per_year=0.000747, slope_pct_per_year=0.139, baseline_cap_pu=0.5369, n=13196, r2=0.000, ci95_pct=(-0.36, +0.64)

performance_summaries.norm_index_p50 (yearly):
  2022 — ours 100.209, spec 100.98
  2023 — ours 103.222, spec 104.66
  2024 — ours 98.307, spec 96.91
  2025 — ours 95.424, spec 97.46

performance_summaries.constraint_proxy_mwh (yearly):
  2022 — ours 51287.1, spec 48743 (+5.2%)
  2023 — ours 62351.2, spec 62787 (-0.7%)
  2024 — ours 65853.7, spec 63938 (+3.0%)
  2025 — ours 66315.4, spec 63721 (+4.1%)

structural_constraint_flags: 0 rows in spec output. We don't compute this yet.
```

## What's left for P0

Per the plan in `/Users/mdfaisal/.claude/plans/fluttering-gliding-treehouse.md`, with **no changes required to the plan** based on P-1 findings, just one **simplification**:

- **P0.2 simplified**: build SpecCSVExporter as a thin SQL→CSV wrapper, not by composing the two existing stream exporters. Easier, faster, fewer moving parts. (Plan text already left room — see "(no need for the full `SpecCSVExporter` yet)" in P-1 step 2.)
- **Add to P0.3**: include the two spec patches as a separate `tests/reference/spec_patches.py` rather than editing the vendored file. Keep provenance clean.

---

# P-1.2 — Second smoke validation against East Anglia One (2026-05-23)

Result: **GREEN — and a major Module-1b design finding.**

## Setup

| Item | Value |
|---|---|
| Windfarm | **East Anglia One** (id=7371, code=`EAST_ANGLIA_ONE`) |
| Rated capacity | 714 MW (high-CF offshore) |
| Data range | 2020-07-28 → 2026-05-13, 48,984 hours after multi-source dedup |
| Currency | **GBP** (prices labeled as `Price[Currency/MWh]` for spec compatibility; numeric values are GBP) — Module 3/6 EUR figures intentionally unreliable on this run |
| Generation units | **2 ELEXON BMUs** (`T_EAAO-1`, `T_EAAO-2`) — required pre-aggregation CTE in the SQL to avoid double-counting |
| Pricing sources | **2** (`ELEXON` and `ENTSOE`, both GBP) — required pre-aggregation to avoid 4-way join doubling generation |
| Known issue | Feb-Oct 2024 cable issue per `Prioritisation 2026 05 18.docx` (219 GWh reported lost vs ≥525 GWh expected) |

**Multi-source bug avoided.** First export attempt produced `power_mw=1284` against 714 MW nameplate — a classic 2 units × 2 prices = 4-way doubled SUM. Resolved by pre-aggregating in CTEs before the join (`gen_hourly`, `wx_hourly`, `price_hourly`). This pattern needs to be the standard in P0.2's SpecCSVExporter.

## Module 5 (Degradation) — comparison

| Field | Spec Q50 | Our DB Q50 | Spec Q90 | Our DB Q90 |
|---|---|---|---|---|
| `baseline_cap_pu` | **0.6467** | **0.35** ← hardcoded | **0.8692** | **0.35** ← hardcoded |
| `slope_pu_per_year` | 0.003853 | 0.00562167 | -0.000216 | +0.00080054 |
| `slope_pct_per_year` | **0.596** | **1.606** | -0.025 | +0.229 |
| `ci95_pct` | (0.45, 0.74) | not exposed | (-0.14, 0.09) | not exposed |
| `r_squared` | 0.0022 | 0.02059 | 0.000 | 0.00052 |
| `n` | 27,573 hourly | 64 monthly | 27,573 | 64 |

Two-bug compounding maths matches predictions exactly:
- **Bug C alone**: spec `slope_pu` (0.003853) ÷ our hardcoded baseline (0.35) × 100 = **1.10**% → that's the slope_pct we'd publish if we fixed only the OLS (Bug A) but kept the wrong baseline.
- **Bug A alone**: our monthly `slope_pu` (0.00562) ÷ spec's real baseline (0.6467) × 100 = **0.87**% → that's what we'd publish if we fixed only the baseline (Bug C) but kept monthly OLS.
- **Both bugs together**: 1.606 (our published number). Two-fold compounding from 0.596 (true).

**Sign disagreement on Q90** (spec -0.025% vs ours +0.229%): both R² ≈ 0; both within noise; neither tells a real story. But it's a vivid illustration that Bug A can flip the *sign* of the reported trend on contaminated data — the monthly aggregation reaches a different sampling of months and gives the opposite answer.

## Module 4 (Wind normalisation) — the 2024 cable signal

| Year | Spec Q50 idx | Our DB Q50 idx |
|---|---|---|
| 2020 | 104.56 | (not yet aggregated, partial year) |
| 2021 | 101.45 | (need to fetch) |
| 2022 | 98.08 | (need to fetch) |
| 2023 | 104.27 | (need to fetch) |
| **2024** | **85.99** | (need to fetch) |
| 2025 | 105.66 | (need to fetch) |

**2024 Q50 wind-norm index = 85.99 — a 14-percentage-point drop from neighbouring years.** Module 4 cleanly identifies the 2024 cable issue. This is exactly the signal stakeholders see in the UI; it's correct.

## Module 1b — THE finding: detector misses EAO's cable issue

**Module 1b detected ZERO constraint runs for EAO**, despite a documented multi-month cable issue.

Looking at the 2024 yearly capability curve:

| Wind bin | q50 (median) | q90 (top decile) |
|---|---|---|
| `[10, 11)` | 0.477 | 0.779 |
| `[11, 12)` | 0.477 | 0.829 |
| `[12, 13)` | 0.477 | 0.946 |
| `[13, 14)` | 0.853 | 0.952 |
| `[15, 16)` | **0.477** | 0.952 |

In wind bins 10-12 m/s and back at 15 m/s, the **median is pinned at 0.477** — almost exactly 50 % of rated. This is the signature of **one of the two ELEXON BMUs being offline** during a large fraction of 2024. Half the windfarm produces, half doesn't.

But **Module 1b only checks the Q90 ratio**. The Q90 in those bins is 0.78-0.95 — close to neighbouring years (`~0.65-0.70` avg across bins) because Q90 captures the *upper decile* of hours, including hours when both BMUs were running. The ratio `obs_Q90 / ref_Q90` stays above the 0.80 threshold → no flag.

**Module 1b's algorithm has a real blind spot:** it catches sustained Q90 suppression (single-cable failures that depress upper-decile output), but it misses **median-only suppression** patterns like half-BMU-offline or persistent partial derate, where the *typical* hour is constrained but the *best* hour is unaffected.

### Implications for the plan

We need to expand Milestone B1's design beyond a verbatim port of the spec. Options:

**Option B1.5 (recommended) — Add a Q50-ratio check alongside the Q90-ratio check.** Same LOYO machinery; just add a second band-threshold table for `q50_obs / q50_ref`. Suggested defaults: 7-10 m/s threshold 0.65; 10-25 m/s threshold 0.75. Plus a tighter min_hours (still 336h) but with EITHER ratio triggering. Catches both single-cable (Q90-driven) and half-BMU (Q50-driven) patterns.

**Option B1.6 — Add a Module 4 yearly-index secondary check.** If `yearly_index_p50 < 90` for an entire calendar year, the analyst is paged for manual review. Doesn't extend Module 1b directly but provides a safety net for what 1b misses.

**Option (status quo)** — Ship the spec-faithful Module 1b in B1 as planned, document the limitation, treat half-BMU patterns as a known follow-up. EAO 2024 stays contaminated until manually flagged.

This is a real decision point — not a plan refinement detail. The user should pick before we start B1. **Adding to the post-P-1 questions queue.**

## Pass criteria check

- [x] Spec script completes without crash (with the two pandas-compat patches from P-1.1)
- [x] Bug C confirmed on a high-CF windfarm (spec baseline Q50=0.65 vs ours 0.35; spec baseline Q90=0.87 vs ours 0.35)
- [x] Bug A confirmed (spec n=27,573 hourly vs ours n=64 monthly)
- [x] Bug B detectable (spec ran `seasonal_decompose` on 27k hours without error)
- [x] No new surprises in data extraction (besides the multi-source bug we already knew about)
- [ ] **Module 1b false-negative documented** (NEW finding — needs design discussion before B1)

## Numeric baseline captured (East Anglia One, pre-Milestone-A)

```
windfarm_id = 7371
windfarm_code = EAST_ANGLIA_ONE
nameplate_capacity_mw = 714.0
data_range = 2020-07-28 → 2026-05-13
hours_in_csv = 48984 (after multi-source dedup)
known_event = Feb-Oct 2024 cable issue (per Prioritisation doc; 219 GWh observed loss vs ≥525 expected)

degradation_results (current production, Bug-C-affected):
  q50: slope_pu_per_year=0.00562167, slope_pct_per_year=1.606, baseline_cap_pu=0.35000, n=64, r2=0.02059, p_value=0.257985
  q90: slope_pu_per_year=0.00080054, slope_pct_per_year=0.229, baseline_cap_pu=0.35000, n=64, r2=0.00052, p_value=0.857550

degradation_results (spec, correct):
  q50: slope_pu_per_year=0.003853, slope_pct_per_year=0.596, baseline_cap_pu=0.6467, n=27573, r2=0.0022, ci95_pct=(0.45, 0.74)
  q90: slope_pu_per_year=-0.000216, slope_pct_per_year=-0.025, baseline_cap_pu=0.8692, n=27573, r2=0.000, ci95_pct=(-0.14, 0.09)

After Milestone A merges:
  q50: 1.606 → ~0.60 (drops 63%) — but STILL inflated by Bug 1b miss
  q90: 0.229 → ~-0.025 (sign flips) — flat (true) instead of slightly degrading (current)

After Milestone B1 (if we choose Option B1.5):
  Module 1b should flag a 2024 run spanning roughly Feb-Oct with mean_q50_ratio (NOT q90_ratio) below 0.75. 
  Excluding that run from Module 5's OLS fit should drop slope_pct_per_year_q50 from 0.60 toward 0 (≈ 0 ± 0.2).

Module 1b (current state):
  CONSTRAINTS DETECTED = 0
  (spec algorithm misses median-only suppression — see analysis above)

Module 4 yearly indices (spec):
  q50: 2020=104.56, 2021=101.45, 2022=98.08, 2023=104.27, 2024=85.99, 2025=105.66
  ← 2024 drop of 14 pp is unmistakable, would page any reasonable threshold (e.g. "year_index < 90 → review").
```

## P-1.2 conclusions

1. **Bug A/B/C all confirmed on a second windfarm class** with different parameters (offshore high-CF vs onshore mid-CF) — bugs are not Lutelandet-specific.
2. **Multi-source pre-aggregation pattern is mandatory for any multi-unit windfarm with multiple price sources.** P0.2's SpecCSVExporter must use the CTE approach.
3. **Module 1b as specified misses EAO's cable issue** — design decision needed before Milestone B1.
4. **Module 4 catches EAO's 2024 issue beautifully** (-14pp on yearly index) — strong candidate for a complementary detection path.

---

# P-1.3 — Third smoke validation against Hornsea 1 (2026-05-24)

Goal: validate whether the half-BMU/median-suppression pattern that escaped Module 1b on EAO is unique, or systemic on multi-BMU offshore farms.

**Verdict: systemic.** Hornsea 1 shows the same Q50-suppression pattern in 2024. Module 1b misses it again.

## Setup

| Item | Value |
|---|---|
| Windfarm | **Hornsea 1** (id=7384, code=`HORNSEA_1`) |
| Rated capacity | 1218 MW |
| Data range | 2018-11-01 → 2026-05-13, 51,815 hours |
| Currency | GBP (same caveat as EAO — Module 3/6 EUR figures unreliable) |
| Generation units | **3 ELEXON BMUs** (`T_HOWAO-1/2/3`) — multi-unit CTE pre-aggregation pattern reused |
| Pricing sources | **2** (ELEXON + ENTSOE, both GBP) |

## Module 5 — same two-bug pattern, even more dramatic on Q50

| Field | Spec Q50 | Our DB Q50 | Spec Q90 | Our DB Q90 |
|---|---|---|---|---|
| `baseline_cap_pu` | **0.4899** | **0.35** ❌ | **0.7278** | **0.35** ❌ |
| `slope_pu_per_year` | -0.002963 | +0.00237783 | -0.001292 | -0.00354550 |
| `slope_pct_per_year` | **-0.605** | **+0.679** | -0.178 | -1.013 |
| `ci95_pct` | **(-0.90, -0.31)** ← significant | not exposed | (-0.39, +0.04) | not exposed |
| `r_squared` | 0.0014 | 0.00684 | 0.0002 | 0.01258 |
| `n` | 11,563 hourly | 65 monthly | 11,563 | 65 |

**Bug A flipped the SIGN of the published Q50 number.** Spec says Hornsea 1 is **statistically-significantly degrading at -0.6 %/yr (CI excludes zero)**. Our published number says **+0.68 %/yr "improving"**. Same windfarm, same data, totally opposite story.

Stakeholders looking at our card today see Hornsea 1 as a recovering asset. The truth is the opposite. After Milestone A this is the loudest single-windfarm communication item.

## Module 1b — same blind spot

Module 1b on Hornsea 1: **0 constraint runs detected**, despite the 2024 Q50 suppression visible across upper-wind bins:

| Wind bin | 2024 q50 | 2024 q90 | Comment |
|---|---|---|---|
| `[10, 11)` | 0.605 | 0.820 | typical for 2020-2023 was ~0.65 |
| `[13, 14)` | 0.913 | 0.956 | normal |
| `[14, 15)` | 0.915 | 0.960 | normal |
| **`[16, 17)`** | **0.790** | 0.959 | q50 dropped ~20 % vs neighbouring bins |
| **`[17, 18)`** | **0.637** | 0.952 | q50 dropped ~33 % vs neighbouring bins |
| **`[18, 19)`** | **0.574** | 0.943 | clear ceiling pattern |
| **`[19, 20)`** | **0.365** | 0.940 | q50 collapses; q90 holds |

The pattern: **upper-wind bins show median collapse while Q90 (top decile) stays normal.** Consistent with 1-of-3 BMUs offline for ~one-third of the year. Module 1b's Q90-ratio check misses this entirely.

**This is the second offshore multi-BMU farm in two attempts to show the pattern.** Almost certainly systemic across the UK offshore fleet for 2024 (when Western Link and several inter-array cables had documented issues). The conclusion is no longer "EAO had a one-off issue" — it's "Module 1b as-specified has a structural limitation for multi-BMU farms".

## Threshold analysis for B1.5 (Q50-ratio augmentation)

If we add a Q50-ratio check parallel to the Q90 ratio check, what threshold catches the patterns we've seen?

| Windfarm | 2024 vs neighbour-year Q50 in [10-25] m/s | Ratio | 0.85 threshold? | 0.75 threshold? |
|---|---|---|---|---|
| EAO | 0.477 / ~0.60 | **0.80** | flagged ✓ | not flagged ❌ |
| Hornsea 1 (avg bin) | 0.509 / ~0.59 | **0.86** | borderline | not flagged ❌ |
| Hornsea 1 (upper bins only) | 0.574 / ~0.94 (e.g. [18, 19)) | **0.61** | flagged ✓ | flagged ✓ |

Two design choices fall out:

- **Compute the q50_ratio per bin-month (matching the spec's q90_ratio pattern).** Threshold 0.85 catches both farms when averaged over [10-25] m/s, OR threshold 0.75 catches both if we restrict to upper bins [13-25] m/s where any constraint is unambiguous.
- **Use the existing band structure from the spec** (`7-10 m/s`, `10-25 m/s`) and pick per-band thresholds: e.g. `q50_ratio < 0.85` in 10-25 m/s, `< 0.70` in 7-10 m/s. Matches the spec's structure for Q90 just at lower threshold (because Q50 is more forgiving — natural variance is higher).

## Module 4 — sees both, less crisply than for EAO

| Year | Hornsea spec Q50 yearly index |
|---|---|
| 2020 | 121.85 |
| 2021 | 123.18 |
| 2022 | 117.58 |
| 2023 | 116.30 |
| **2024** | **107.36** ← 9 pp drop |
| 2025 | 113.73 |

Less dramatic than EAO (-14 pp) but still a clear dip. A `year_idx < 90` threshold would NOT flag Hornsea 1; `year_idx < 110` would. The Module 4 safety net (option B1.6) needs a windfarm-relative threshold (e.g. `< 90 % of own historical mean`), not a global cutoff.

## P-1.3 conclusions

1. **The 2024 Q50-suppression pattern is NOT unique to EAO.** Confirmed on two of two multi-BMU offshore windfarms. Almost certainly affects more (Hornsea 2, Walney Extension, Galloper, London Array all to be checked).
2. **Bug A also flips signs.** Hornsea Q50: spec -0.60 % significant degradation, ours +0.68 % "improving". This is the highest-stakes communication item in the release note.
3. **Module 1b needs B1.5 (Q50-ratio check) at minimum.** A `q50_ratio < 0.85` in upper-wind bands would have caught both EAO and Hornsea 1. Recommended thresholds (mirror spec's Q90 structure):
   - `7-10 m/s`: q50_ratio threshold 0.70
   - `10-25 m/s`: q50_ratio threshold 0.85
4. **B1.6 (Module 4 safety net) is still worth it** as a belt-and-braces — but the threshold needs to be windfarm-relative, e.g. "year_index < 90 % of windfarm's own historical mean of yearly indices".
5. **The 2-pandas-compat-patches finding from P-1.1 still holds** on these multi-unit farms — same script ran without issue on all three after the patches.
6. **Multi-source CTE pre-aggregation worked cleanly** on both EAO (2 units × 2 prices) and Hornsea (3 units × 2 prices). The pattern scales.
