# Session progress — 2026-05-25/26 pre-flight + Module 1b fix + spec bug discovery + 360-WF backfill

**Status as of 2026-05-26 12:40 local:** Most engineering work complete. Backfill ~42% done across multiple runs (v1→v5). Pool cascade bug forces periodic restarts. **About to compact + restart computer; pick up at v6 from `backfill_remaining_wfs.txt`.**

---

## 🚀 Resume after restart — TL;DR

You are restarting the machine. v5 backfill (PID will be killed by the restart) was running. When you come back, do this:

```bash
cd /Users/mdfaisal/Documents/energyexe/energyexe-core-backend

# 1. Sanity check master is up to date
git checkout master && git pull

# 2. Inspect what's left (143 WFs in the file at restart time, may be fewer if v5 made progress)
wc -w docs/pipeline/backfill_remaining_wfs.txt

# 3. (Optional) Re-derive the remaining list from current DB state to capture any progress v5 made:
poetry run python -c "
import asyncio
from sqlalchemy import text
from app.core.database import get_session_factory

async def m():
    factory = get_session_factory()
    async with factory() as db:
        # Hourly-source windfarms eligible for pipeline
        r = await db.execute(text('''
            SELECT DISTINCT w.id FROM windfarms w
            JOIN generation_units gu ON gu.windfarm_id = w.id
            WHERE w.status = 'operational' AND w.nameplate_capacity_mw > 0
              AND gu.source IN ('ELEXON', 'ENTSOE', 'NVE', 'Taipower')
              AND EXISTS (SELECT 1 FROM weather_data wd WHERE wd.windfarm_id = w.id LIMIT 1)
              AND EXISTS (SELECT 1 FROM generation_data g WHERE g.windfarm_id = w.id AND g.is_ramp_up = false LIMIT 1)
            ORDER BY w.id
        '''))
        all_eligible = [row[0] for row in r.fetchall()]
        # WFs that have a fresh degradation_results row (post-2026-05-25 = after migrations applied)
        r2 = await db.execute(text('''
            SELECT DISTINCT windfarm_id FROM degradation_results
            WHERE created_at > '2026-05-25 12:00'  -- after migrations applied
        '''))
        done = {row[0] for row in r2.fetchall()}
        remaining = [w for w in all_eligible if w not in done]
        print(f'Eligible: {len(all_eligible)}, Done: {len(done)}, Remaining: {len(remaining)}')
        with open('docs/pipeline/backfill_remaining_wfs.txt', 'w') as f:
            f.write(' '.join(str(w) for w in remaining))

asyncio.run(m())
"

# 4. Launch v6 backfill (caffeinate keeps Mac awake; tee captures log)
caffeinate -i poetry run python scripts/backfill_pipeline.py --pipeline-only \
    --windfarm-ids $(cat docs/pipeline/backfill_remaining_wfs.txt) \
    2>&1 | tee /tmp/backfill_hourly_v6.log
```

Expect it to run ~5 min per windfarm. The pool cascade may strike again after 70-100 WFs; if so, kill + re-derive remaining + relaunch as v7.

---

## What's done (engineering)

**5 PRs landed today:**

| Run/PR | What | Status |
|---|---|---|
| Prior session | PRs #62-#73 (pipeline correctness milestones) | merged |
| PR #74 | Module 1b month-level grouping + spec_patches.py patches 3+4 + P-1.4 docs + release-notes-2026-05-25.md | merged (commit `e367d2a`) |
| Migrations | Applied 5 missing alembic migrations to prod RDS (alembic head now `f3a4b5c6d7e8`) | applied |
| Spec validation | Re-validated 4 reference WFs against patched spec — all match within 1% | done |
| Release note | `docs/pipeline/release-notes-2026-05-25.md` drafted | done |

## What's done (data, in prod)

**~103 unique windfarms successfully reprocessed** with the corrected pipeline. List: `docs/pipeline/backfill_completed_wfs.txt`

Successful runs:
- Pre-flight v3 (4 WFs: Lutelandet, Roan, EAO, Hornsea 1)
- v3 main backfill (83 WFs — killed at pool cascade)
- v4 backfill (18 WFs — killed at pool cascade)
- v5 in progress at machine restart (will die during restart)

## What's left

**143 hourly-source windfarms** still need processing. List: `docs/pipeline/backfill_remaining_wfs.txt`

Includes:
- **7188** — persistent failure, fails immediately with empty exception (data issue, not pool). Needs separate investigation.
- **7234, 7235** — return "no yearly curves produced" (insufficient data — these will SKIP cleanly when re-attempted; can ignore).
- ~140 other WFs not yet attempted.

**NOT in scope:** 1,308 EIA/ENERGISTYRELSEN monthly-granularity windfarms. Daily cron handles them naturally; the hourly pipeline doesn't materially benefit them.

---

## Pool cascade issue — what happened and why

**Symptom:** Every few hours / every 70-100 successful WFs, the backfill hits a cascade where ALL subsequent windfarms fail with:
```
asyncpg.exceptions.ConnectionDoesNotExistError: connection was closed in the middle of operation
Can't reconnect until invalid savepoint transaction is rolled back
```

**Root cause:** RDS closes an idle pool connection mid-transaction. The mid-transaction state (SAVEPOINT) becomes invalid. SQLAlchemy can't recover — all subsequent operations on that connection (and ones inheriting the bad state) fail with the same "needs rollback" error.

**Why pool_pre_ping doesn't help:** `pool_pre_ping=True` is already enabled (and `pool_recycle=300`). Both check connections at session START. The failure happens DURING a session, after pre-ping has already passed.

**Mitigation (current):** Kill + restart the process. Fresh asyncpg pool, problem resets. Loses any in-flight WF's work, but the script is idempotent per WF.

**Better mitigation (deferred work):**
- Wrap each WF in a try/except that catches the cascade pattern and disposes the engine
- Run the backfill in smaller subprocess chunks (e.g. 30 WFs per python invocation) — process restart kills the bad pool

Either fix is ~10-20 lines in `scripts/backfill_pipeline.py`. Worth doing before any future backfill of similar scale.

---

## Backfill runs — timeline

| # | When | Scope | Outcome |
|---|---|---|---|
| v1 | 2026-05-25 ~14:00 | All 1,554 eligible (no source filter) | Killed at 3 WFs after discovering EIA+ENERGISTYRELSEN bloat. Re-scoped. |
| v2 | 2026-05-25 ~17:30 | 246 hourly-source WFs | Stalled silently after 1 WF (Mac sleep). Caffeinate added for v3. |
| v3 | 2026-05-25 23:33 → 2026-05-26 07:47 | 246 hourly WFs | Pool cascade at WF 7262. 83 OK + 2 SKIP + 9 ERR. Killed. |
| v4 | 2026-05-26 09:44 → 11:49 (hung till 12:25 kill) | 161 WFs (retries + remaining) | Pool cascade at WF 7279. 18 OK + 0 SKIP + 1 ERR. Killed. |
| v5 | 2026-05-26 12:27 → ~now | 143 WFs (retries + remaining) | Running at compact-time. Will die at machine restart. |
| v6 | (post-restart) | ~140 WFs (recompute from DB state) | TBD |

---

## Findings (locked from this session)

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

6. **Pool cascade is a known recurring issue.** See section above. Mitigation: restart, optionally chunk into 30-WF subprocess invocations.

7. **Roan q50 = -2.51 %/yr is real.** Spec-patched and our pipeline both agree. Operational verification needed — was the windfarm degrading or is there a data issue?

---

## Open questions / follow-ups (NOT done this session)

| Item | Priority | Notes |
|---|---|---|
| **Investigate 7188's persistent failure** | M | Fails immediately with empty exception. Likely data issue, not pool. Worth digging in to see why. |
| **Backfill the remaining ~140 WFs** | H | v6 onwards, after restart |
| **Fix pool cascade in backfill script** | M | 10-20 LOC change; chunk into subprocess invocations OR catch ConnectionDoesNotExistError + engine.dispose() |
| **Roan degradation verification** | L | Both pipelines agree; needs operational check |
| **Circulate release note** | H | `docs/pipeline/release-notes-2026-05-25.md` — circulate to OS / ASR / KG after backfill complete |
| **Admin-UI analyst review queue** | L | Separate ticket in `energyexe-admin-ui` repo |
| **EIA/ENERGISTYRELSEN monthly windfarms** | L | Daily cron handles them naturally; don't need manual backfill |

---

## Key files (what to read after restart)

| File | Purpose |
|---|---|
| `docs/pipeline/SESSION_2026_05_25_PROGRESS.md` | **(this file)** session tracker |
| `docs/pipeline/HANDOFF.md` | Overall pipeline correctness status doc |
| `docs/pipeline/release-notes-2026-05-25.md` | Stakeholder-facing release note (drop "Hornsea sign-flip" headline) |
| `docs/pipeline/backfill_completed_wfs.txt` | **103 WF IDs already successfully processed** |
| `docs/pipeline/backfill_remaining_wfs.txt` | **143 WF IDs still to process (v6 input)** |
| `tests/reference/p-1-validation-notes.md` | P-1.4 section has the dayfirst discovery; retraction header above older P-1.x sections |
| `tests/reference/spec_patches.py` | Module docstring lists all 4 spec defects we patch |

---

## Files modified this session

| Path | Change | Committed? |
|---|---|---|
| `app/services/structural_constraint_detection_service.py` | Rewrote `group_into_runs` (month-level) + new constants | ✅ PR #74 |
| `tests/test_structural_constraint_detection.py` | Added `TestRealisticWindRunGrouping` (3 new tests) | ✅ PR #74 |
| `docs/pipeline/module-1b-structural-constraint-detection.md` | Algorithm note + new constants | ✅ PR #74 |
| `tests/reference/spec_patches.py` | Added patches 3 (dayfirst) and 4 (module-4 cat) | ✅ PR #74 |
| `tests/reference/p-1-validation-notes.md` | P-1.4 section + retraction header | ✅ PR #74 |
| `docs/pipeline/release-notes-2026-05-25.md` | NEW — release narrative | ✅ PR #74 |
| `docs/pipeline/SESSION_2026_05_25_PROGRESS.md` | NEW — this scratch | ✅ PR #74 + edits today |
| `docs/pipeline/backfill_completed_wfs.txt` | NEW — for v6 resume | ⚠️ uncommitted |
| `docs/pipeline/backfill_remaining_wfs.txt` | NEW — for v6 resume | ⚠️ uncommitted |
| **Prod RDS** | Applied 5 missing migrations (`alembic stamp` + `upgrade head`) | n/a (DDL) |
