# Session progress — 2026-05-25 pre-flight + Module 1b fix + spec bug discovery

Single-file scratchpad for this session's work. Will be cleaned up into proper docs once 5-step sequence completes.

## Findings (locked)

1. **Production DB was missing 5 alembic migrations.** Discovered during pre-flight v1; columns `ci_lower_95_pct`, `contract_revenue_eur`, `n_constraint_hours_excluded` and table `structural_constraint_flags` weren't present. Daily cron had been silently failing Module 5/6 persistence for weeks. ✅ Fixed by stamping `e4a1c83d9b21` + `e2f3a4b5c6d7` (out-of-band tables) and applying the column migrations. Prod head now `f3a4b5c6d7e8`.

2. **Module 1b spec-faithful run-grouping shatters on real data.** Per-hour `(constrained != shift()).cumsum()` is broken by interleaved sub-7 m/s wind hours: EAO 2024 fragments into 320 sub-runs (median 3h, max 133h) when the cable issue is genuinely sustained for 7 months. ✅ Fixed via month-level grouping with 25% flagged-share threshold. Now detects EAO 2024 (2598h run, q50=0.477) and Hornsea 1 2024 (2258h run, q50=0.473) cleanly. 14/14 unit tests pass.

3. **Spec script's `dayfirst=True` silently corrupts ISO timestamps.** On `YYYY-MM-DD HH:MM:SS` input:
   - Day ≤ 12: month/day swapped (e.g. 2024-11-01 → 2024-01-11)
   - Day > 12: dropped as NaT
   Net: spec processes ~40% of the data, half of it mis-labeled.

4. **The "Hornsea sign-flip" release-note headline was bogus.** Validation notes' spec value of `-0.605%/yr (degrading)` was computed on dayfirst-corrupted data. With the patch, spec gives `+0.77%/yr` — matching our v2 pipeline to 3 decimal places.

5. **All 4 reference windfarms match spec-patched output.** Our Module 5 is correct:

   | WF | Spec (patched) q50 slope | Our v2 q50 slope | Match? |
   |---|---|---|---|
   | LUTELANDET | +0.34 %/yr (n=13196) | +0.341 %/yr (n=13197) | ✅ |
   | ROAN | −2.51 %/yr (n=35255) | −2.509 %/yr (n=35257) | ✅ |
   | EAO | +0.60 %/yr (n=27573) | +0.595 %/yr (n=27574) | ✅ |
   | HORNSEA 1 | +0.77 %/yr (n=28283) | +0.770 %/yr (n=28288) | ✅ |

## 5-step sequence (user-approved 2026-05-25)

| # | Step | Status |
|---|---|---|
| 1 | Patch `tests/reference/spec_patches.py` for dayfirst + module-4 cat-vs-int | ✅ done |
| 2 | Append P-1.4 to `tests/reference/p-1-validation-notes.md` | ✅ done |
| 3 | Re-validate Lutelandet/EAO/Roan against spec-patched | ✅ done (all match) |
| 4 | Commit Module 1b run-grouping fix + spec_patches addition | ⏳ pending |
| 5 | Draft release note with corrected narrative | ⏳ pending |

## Prod backfill scope (queued)

Will run after step 5 lands.

- Target: all ~360 operational windfarms via `scripts/backfill_pipeline.py --pipeline-only`
- Expected: every windfarm's Module 5 number changes; Module 1b detects multi-month constraints on the multi-BMU offshore farms (EAO, Hornsea 1, Hornsea 2, London Array, Galloper, Walney Extension if affected by 2024 cable issues)
- Existing pre-flight v3 already wrote Module 5/6/1b for windfarms 7197/7210/7371/7384 — those are correct

## Open questions

- **Roan −2.51 %/yr q50 (CI excludes 0)** — large drift for what `reference_windfarms.yaml` calls "clean Norwegian inland baseline, expected: small drift". Worth investigating: real degradation, or another data issue? Not blocking; flag in release note as "this is what the corrected pipeline says, please verify operationally".
- **Hornsea 2018-11/12 commissioning false-positive (2 flag rows, q50≈0)** — detector caught it but it's not a real constraint. Out of scope for this session; will be filtered when analyst review UI ships.

## Files touched this session

| Path | Change |
|---|---|
| `app/services/structural_constraint_detection_service.py` | Rewrote `group_into_runs` (month-level) + new constants |
| `tests/test_structural_constraint_detection.py` | Added `TestRealisticWindRunGrouping` (3 new tests) |
| `docs/pipeline/module-1b-structural-constraint-detection.md` | Algorithm note + new constants |
| `tests/reference/spec_patches.py` | Added patches 3 (dayfirst) and 4 (module-4 cat) |
| `tests/reference/p-1-validation-notes.md` | (pending) P-1.4 section + retraction of dayfirst-tainted numbers |
| **Prod RDS** | Applied 5 missing migrations (alembic stamp + upgrade head) |
| `docs/pipeline/SESSION_2026_05_25_PROGRESS.md` | (this file) |
