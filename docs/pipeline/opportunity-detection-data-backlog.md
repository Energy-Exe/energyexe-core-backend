# Opportunity Detection — Data Backlog & Accuracy Caveats

> Tracking doc for [Opp-Det #116] (M8 #30). Captures the three opportunity-detection
> items that are deferred on **data availability** or **accuracy**, so they are not
> forgotten once the upstream data lands. **No code** is required by #116 itself — this
> doc is the acceptance artifact; each checklist becomes the activation gate.
>
> Source spec: `energyexe_opportunity_schemas_15 May 2026.docx` (SharePoint → Development site).
> Plan: `~/.claude/plans/wobbly-crafting-mitten.md`.

## Summary

| Item | Schema | State | Blocker | Activation owner |
|---|---|---|---|---|
| (a) PPA underpricing | **MKT-05** | INACTIVE (no rows) | `ppas.ppa_price_eur_mwh` NULL fleet-wide | re-activate per #106 |
| (b) Forecast deviation | **MKT-07** | INACTIVE (no rows) | no hourly forecast-vs-actual data ingested | re-activate per #106 |
| (c) Turbine degradation | **OPS-04** | ACTIVE, capped at INDICATIVE | `degradation_results.baseline_cap_pu` is a 0.35 placeholder | lift cap per #99 |

Related implementation issues: **#99** (OPS-04 turbine degradation — plan item #12) and
**#106** (MKT-05 & MKT-07 INACTIVE handling — plan item #19).

---

## (a) MKT-05 — PPA underpricing

**Current state.** `mkt05_ppa_underpricing.detect()` is a documented no-op; the registry marks
`SCHEMA_STATUS[MKT_05] = "INACTIVE"` so `run_for_windfarm` skips it and no per-windfarm rows are
produced (see #106).

**Activation criterion.** Activate once a PPA strike price (`ppas.ppa_price_eur_mwh`, or equivalent
column) is populated for a meaningful share of the portfolio. Underpricing fires on the gap between
the prevailing spot/day-ahead price and the contracted PPA price:

- `spot − PPA  > €20/MWh` → CONFIRMED
- `spot − PPA  > €15/MWh` → INDICATIVE
- `spot − PPA  > €10/MWh` → WATCH

(Exact thresholds per the spec; placeholders are recorded in the module docstring.)

**Activation steps.**
1. Populate `ppa_price_eur_mwh` (data import / backfill).
2. Replace the no-op `detect()` with a real detector: add a `load_ppa_strike_price()` /
   spot-price accessor to `DetectionContext`, a pure `classify_ppa_underpricing_severity(spot_minus_ppa)`,
   and a pure `compute_spot_minus_ppa(...)`.
3. Flip `SCHEMA_STATUS[MKT_05]` from `"INACTIVE"` to `"ACTIVE"` in `registry.py`.

**Test plan (when activated).**
- `test_classify_ppa_underpricing_severity_boundaries` → €20.01→CONFIRMED, €20→INDICATIVE,
  €15→WATCH, €10→None (verify `>`/`>=` against the spec).
- `test_detect_none_when_no_strike_price` → snapshot-safety: still returns None when price absent.
- `test_registry_marks_mkt05_active` once flipped.
- Add a deliberate `EXPECTED_SNAPSHOT` delta only if a characterization scenario gains PPA price data.

---

## (b) MKT-07 — Forecast deviation

**Current state.** `mkt07_forecast_deviation.detect()` is a documented no-op;
`SCHEMA_STATUS[MKT_07] = "INACTIVE"` (see #106).

**Activation criterion.** Activate once hourly **forecast-vs-actual** generation data is ingested.
Fires on forecast error (MAPE — mean absolute percentage error):

- `MAPE > 25%` → CONFIRMED
- `MAPE > 15%` → INDICATIVE
- `MAPE >  8%` → WATCH

**Activation steps.**
1. Ingest a forecast series (source TBD) alongside `generation_data` so per-hour
   forecast/actual pairs exist.
2. Add a `load_forecast_actual_pairs()` accessor + pure `compute_mape(pairs)` +
   `classify_forecast_deviation_severity(mape_pct)`; replace the no-op `detect()`.
3. Flip `SCHEMA_STATUS[MKT_07]` to `"ACTIVE"`.

**Test plan (when activated).**
- `test_compute_mape_formula` → a known pair-set → exact MAPE.
- `test_classify_forecast_deviation_severity_boundaries` → 25.01→CONFIRMED, 15.01→INDICATIVE,
  8.01→WATCH, 8.0→None.
- `test_detect_none_when_no_forecast_data` → snapshot-safety.
- `test_registry_marks_mkt07_active` once flipped.

---

## (c) OPS-04 — Turbine degradation accuracy cap

**Current state.** OPS-04 (`ops04_turbine_degradation.py`) is **ACTIVE** but
`classify_degradation_severity` **caps a would-be-CONFIRMED finding at INDICATIVE**, because the
degradation regression baseline (`degradation_results.baseline_cap_pu`) is a hardcoded **0.35
placeholder** rather than a real per-windfarm first-year capability. Every OPS-04 finding carries
`data_slots["baseline_caveat"] = True`, and the would-be-CONFIRMED rows are flagged via
`data_slots["confirmed_eligible"]` so they can be promoted later (see #99).

**Activation criterion.** Lift the INDICATIVE cap once `baseline_cap_pu` is computed as a genuine
per-windfarm first-year (or commissioning-period) capability factor from the pipeline, not a
constant.

**Activation steps.**
1. Replace the 0.35 placeholder in the degradation pipeline with a real per-windfarm
   `baseline_cap_pu`.
2. In `ops04_turbine_degradation.classify_degradation_severity`, remove the INDICATIVE cap so a
   steep, significant slope can reach CONFIRMED; drop `data_slots["baseline_caveat"]`.
3. Backfill/re-run the degradation pipeline so `degradation_results` carries real baselines.

**Test plan (when activated).**
- `test_severity_confirmed_no_longer_capped` → (slope=-4.0, p=0.04) → **CONFIRMED** (was INDICATIVE).
- Existing OPS-04 boundary/exclusion tests (artifact guard, floating, <3yr) stay green.
- Promote any rows previously flagged `confirmed_eligible`.
- Update the `EXPECTED_SNAPSHOT` only if a characterization scenario injects degradation data.

---

## Acceptance gate for #116

- [x] Activation criteria documented for each of MKT-05, MKT-07, OPS-04.
- [x] Test plan documented for each.
- [x] Cross-linked to #99 (OPS-04) and #106 (MKT-05/07).

When the upstream data lands, the per-item checklists above become the activation tasks.
