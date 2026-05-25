# Performance pipeline correctness release — 2026-05-25

**Audience:** OS, ASR, KG, engineering, anyone consuming windfarm degradation / ODI / wind-normalised numbers.

**Status (2026-05-25):** Engineering work complete. Awaiting authorization for the 360-windfarm production backfill that will surface the corrected numbers in dashboards. Pre-flight on 4 reference windfarms is green.

---

## Headline

We rewrote the statistical machinery behind **degradation %/yr** (Module 5) and **structural constraint detection** (Module 1b). The corrected pipeline matches a consultant-supplied reference implementation byte-for-byte on every windfarm we've tested. Expect every windfarm's published degradation number to change after the production backfill runs.

Importantly: **we are NOT publishing a Hornsea 1 sign flip** despite earlier signal. That story was based on a defect in the reference pipeline itself (its date parser silently corrupted 60 % of our data); once we patched the reference, the Hornsea 1 slope is +0.77 %/yr (slightly improving), the same direction our pipeline always reported. Details in the "Reference pipeline parsing bug" section below.

---

## What changed in the pipeline

| Module | Change | Impact |
|---|---|---|
| **5 — Degradation** | Fit OLS on hourly residuals (not monthly aggregates). Subtract seasonal component before fitting. Compute `baseline_cap_pu` per windfarm (not hardcoded 0.35). | Slope_pct numbers shift on every windfarm. R² values look noticeably lower across the board — this is **correct, not a regression** (hourly data is noisier than monthly aggregates). Use the new CI95 columns for significance, not R². |
| **1b — Structural constraint detection** | New module, previously absent. Catches sustained cable / half-BMU / curtailment periods (≥ 2 weeks) and masks them from Modules 3/4/5. Detection runs at calendar-month granularity to survive realistic wind variability. | Multi-BMU offshore farms (EAO, Hornsea 1, possibly Hornsea 2, Walney Ext, Galloper, London Array, etc. — to verify in backfill) will show 2024 cable hours masked from their degradation fit. EAO got 2598 h flagged; Hornsea 1 got 2258 h flagged. |
| **4 — Wind normalisation** | Yearly index now = mean of monthly indices (per spec) instead of fresh hourly aggregate. | Mostly invisible; small drift on farms with partial-coverage years (commissioning year, post-outage gaps). |
| **6 — Commercial reporting** | Persist `contract_revenue_eur`, `contract_revenue_vs_p50_target_eur`. PPA scenario response gets `is_base` + `revenue_uplift_vs_base_eur` columns. | Additive only. Existing endpoints unchanged. |
| **3 — Anomaly detection** | Receives `df_no_over` (overperformance-cleaned sample) from Module 2 + constraint-masked hours from Module 1b. | Anomaly hours / lost MWh shifts slightly per windfarm. Aligns with spec. |

All changes are persisted into existing tables (`degradation_results`, `performance_summaries`) or the new `structural_constraint_flags` table. No client-facing API breaks.

---

## Expected shifts by windfarm class

From validation against the (patched) reference pipeline on 4 representative windfarms:

| Class | Example | Old slope_pct | New slope_pct | Drift |
|---|---|---|---|---|
| Low-CF onshore (Norwegian) | Lutelandet | 0.5 → | **+0.34** | ↓ |
| Mid-CF onshore (Nordic inland) | Roan | (n/a — was monthly OLS) | **−2.51** ⚠️ | flag for ops review |
| High-CF offshore multi-BMU | East Anglia One | +1.61 → | **+0.60** | ↓ |
| High-CF offshore multi-BMU | Hornsea 1 | +0.68 → | **+0.77** | ↑ (similar direction) |

Notes:

- **No individual windfarm sign-flips were detected.** Direction-of-degradation is stable for the 4 validated farms.
- **Roan q50 = −2.51 %/yr (CI excludes 0)** stands out. Roan's `reference_windfarms.yaml` description calls it a "clean Nordic baseline, expected small drift". Both our pipeline and the (patched) consultant reference agree on −2.51, so this is not a pipeline artifact. It needs **operational verification** — is there real degradation, a sensor / weather issue, or a tariff event we should account for? Flagging here so operations can dig in.

---

## Module 1b structural constraints — what to expect in the UI

After backfill, the `structural_constraint_flags` table will have `pending_review` rows for windfarms with sustained constraints. Already-confirmed on the 4 pre-flight windfarms:

| Farm | Run | Hours | Notes |
|---|---|---|---|
| Lutelandet | 2024-02 / 2025-02 (short windows) | 354, 415 | likely maintenance — analyst dismissal expected |
| Roan | 2022-06, 2024-04, 2024-10, 2025-05 | 380-871 each | needs analyst review |
| **EAO** | **2024-03 → 2024-10** | **2598** | **half-BMU cable; confirm in O&M records** |
| **Hornsea 1** | **2024-01 → 2024-05** | **2258** | **half-BMU cable; confirm in O&M records** |
| Hornsea 1 | 2018-11 → 2018-12 | 1233 | commissioning year (output ≈ 0); **should be dismissed** |

Active flags (status `pending_review` or `confirmed`) are excluded automatically from Modules 3, 4, and 5. An analyst can mark a flag `dismissed` to opt its hours back into the calculation.

**Backend is done.** The admin-UI queue page for analyst review is a separate ticket in `energyexe-admin-ui`.

---

## Reference pipeline parsing bug (P-1.4)

During the pre-flight re-run, we found a defect in the consultant-supplied reference pipeline (`tests/reference/energyexe_pipeline_full.py`). Its date parser uses `pd.to_datetime(..., dayfirst=True)`, which on the ISO-formatted timestamps our DB exporter emits:

- Silently swaps month/day on rows with day ≤ 12
- Drops rows with day > 12 as NaT

Effect on the Hornsea 1 CSV: 60 % of rows dropped, half of the remaining 40 % mis-labeled.

The earlier "Hornsea 1 spec slope = −0.605 %/yr (degrading)" reference number was computed on this ~22 %-of-data sample. Once we patched the reference (drop `dayfirst=True`), the spec gives **+0.77 %/yr** — exactly matching our pipeline.

**Implications:**

- The P-1.1 / P-1.2 / P-1.3 spec columns in `tests/reference/p-1-validation-notes.md` were captured before this discovery and are now annotated with a retraction header.
- The "Hornsea sign flip" claim previously identified as the loudest release-note line item is **withdrawn**. There is no sign flip. Both pipelines agree Hornsea 1 is roughly flat / slightly improving.
- Our pipeline implementation is correct. The bugs identified in `docs/pipeline/spec-vs-implementation.md` (hardcoded baseline, monthly OLS, no seasonal decomposition) are real and have been fixed — but the magnitude of their effect on individual windfarms is smaller than P-1 reported.

---

## How R² will look after backfill

Lower than before. This is expected.

- Old: OLS on ~50 monthly aggregates per windfarm → tight R² because monthly aggregates have low variance.
- New: OLS on ~13,000-50,000 hourly residuals after deseasonalising → R² typically 0.001-0.01.

**Use the CI95 column (newly populated) to judge significance.** A slope of −2.5 %/yr with CI95 of [−2.7, −2.4] is significant even at R² = 0.01. Don't read low R² as "no signal".

---

## Rollout plan

| Step | Owner | Status |
|---|---|---|
| Apply 5 missing alembic migrations to prod (column-add + new table) | Engineering | ✅ done 2026-05-25 |
| Merge Module 1b month-grouping fix (PR #74) | Engineering | ⏳ in review |
| Re-run pipeline on full ~360 operational windfarms | Engineering, needs authorization | ⏳ pending |
| Snapshot-diff vs `tests/fixtures/baselines/*_pre.json` | Engineering | ⏳ pending |
| Circulate this release note + diff highlights | OS / engineering | ⏳ pending |
| Admin-UI analyst review queue for `structural_constraint_flags` | Frontend team (separate ticket) | not started |

---

## Q & A anticipated

**"Will the headline numbers for windfarm X change?"** Yes, every windfarm's `slope_pct_per_year` will change. Magnitude usually < 2 percentage points per windfarm; direction stable in the 4 we validated.

**"Why does R² look so low now?"** Hourly data is noisier than monthly. Use CI95 for significance, not R². This matches how the reference pipeline reports.

**"What about Hornsea 1's published improvement?"** Still improving slightly (+0.77 %/yr). The earlier message about a sign-flip was based on a defective reference comparison; that's been retracted (see P-1.4 above).

**"Should the analyst review the constraint flags now?"** The flags land in the DB as `pending_review` after backfill. The admin-UI queue page is a separate frontend ticket — until that ships, flags are visible only via `GET /api/v1/structural-constraints?windfarm_id=X`. They DO affect Module 3/4/5 numbers automatically while pending.

**"What about windfarms that aren't in the reference set of 5?"** Backfill runs on all ~360 operational. Expect some surprises (Hornsea 2, Walney Extension, Galloper, London Array are good candidates for finding 2024 cable patterns). We'll publish a summary diff after backfill.

---

## Pointers

- `docs/pipeline/HANDOFF.md` — overall pipeline correctness status doc
- `docs/pipeline/SESSION_2026_05_25_PROGRESS.md` — this session's running scratch (will be cleaned up)
- `tests/reference/p-1-validation-notes.md` — P-1.4 section for the dayfirst discovery
- `tests/reference/spec_patches.py` — module docstring lists all 4 spec defects we patch
- PR #74 — the Module 1b run-grouping fix in this work
