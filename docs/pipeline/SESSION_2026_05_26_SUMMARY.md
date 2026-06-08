# Session 2026-05-26 — Pipeline Correctness Backfill Complete

Continuation of [SESSION_2026_05_25_PROGRESS.md](./SESSION_2026_05_25_PROGRESS.md).

## Final state

- **235/246 hourly-source windfarms (95.5%)** have post-migration analytical results in `degradation_results` (created today)
- **3 PRs merged** + **5 alembic migrations** applied to prod RDS (head: `f3a4b5c6d7e8`)
- **1 known persistent failure**: 7404 (Ormonde, 150 MW UK offshore) — root cause identified, not yet fixed

## Backfill runs (chronological)

| Run | Mode | Started | Duration | Successful WFs | Notes |
|---|---|---|---|---|---|
| v3 | sequential (caffeinate'd) | 2026-05-26 07:00 | ~2h | ~70 | Hit pool cascade at WF 7262-7268; killed |
| v4 | sequential, with retries | 2026-05-26 09:00 | ~1h | partial | Pool cascade at 7279; hung |
| v5 | sequential | 2026-05-26 11:30 | 1h 24m | 15 | Killed by user's machine restart at 12:40 |
| **v6** | **parallel-4** | **2026-05-26 14:09** | **2h 46m wall-clock** | **79** | Chunks 1/2/4 finished; chunk 3 killed early (3-consec err cascade) |
| **v7** | **parallel-2** | **2026-05-26 18:44** | **2h 15m wall-clock** | **49** | Both chunks ran to completion at ~89% success rate |
| **v8** | **sequential targeted** | **2026-05-26 22:35** | **16 min** | **2/3** | 7342 ✓, 7428 ✓, 7404 ✗ (peer-aggregate rollback) |

Cumulative tally over v6+v7+v8: **130 OK / 29 ERR / 8 SKIP across 167 attempts.**

## Key learnings about RDS pool cascade

The pattern that wrecked v6 chunk 3 and 7404 is:
- RDS drops idle connections after some `idle_session_timeout` window
- asyncpg pool gives back a closed connection on next `acquire()`
- SQLAlchemy treats the failed query as a savepoint corruption → **invalid SAVEPOINT cascade**: every subsequent operation in the same session fails with `Can't reconnect until invalid savepoint transaction is rolled back`
- In `scripts/backfill_pipeline.py`, the cascade can affect every subsequent WF in the same chunk

**Mitigations that worked:**
- Reducing parallelism from 4 → 2 cut error rate from ~30% to ~6%
- Each chunk gets its own asyncpg pool, so cascade in one chunk doesn't poison the others
- Killing and restarting a stuck chunk creates a fresh pool

**Not yet fixed:**
- `pool_pre_ping=True` (validates connections before checkout) — would catch dropped connections at the borrow boundary
- `pool_recycle=600` (force connection refresh every 10 min) — short enough to stay under RDS idle timeout
- Connection keepalive at TCP level — also handles network-layer drops

## 7404 (Ormonde) — root cause

**Why it persistently fails (4 attempts in v6/v7/v8 — same error each time):**

7404 has **14 years of data** (2013-2026) → modules 1-6 take **~7 minutes** of CPU-heavy work. By the time the orchestrator starts peer aggregate refresh, the RDS connection has been idle long enough to be dropped. The first peer aggregate query (`SELECT windfarms.id FROM windfarms WHERE bidzone_id = $1`) hits a dead connection.

**The real bug — bigger than just 7404:**

Modules 1-6 produce correct results (logs confirm `module_5_complete slope_pct=0.43` etc.) but **the writes don't persist** when the post-modules peer aggregate refresh fails. The modules and peer aggregate refresh share one transaction; when peer agg refresh fails, the whole thing rolls back.

Verified via direct DB check: 7404's `degradation_results.updated_at = 2026-05-24 06:23` — pre-backfill, unchanged after v8.

**Proper fix:** decouple peer aggregate refresh from the module 1-6 transaction. Commit module results first; treat peer aggregate refresh as best-effort, can be retried separately.

**Workaround:** none in code right now. The daily cron will pick 7404 up tomorrow and likely fail again the same way.

**Filed as follow-up.** Until fixed, 7404's dashboard will show its 2026-05-24 results (slope_pct=0.21 and 0.71 for q50/q90 respectively) — still valid analytics, just missing the Module 1b month-grouping benefit.

## The 10 other "remaining" WFs (Categories B + C from investigation)

### Category B: New windfarms with insufficient history (7 WFs)
These ran today successfully (have `performance_summaries` from 2026-05-26) but Module 5 correctly returned NULL slope because they have only 1-2 years of post-ramp-up data:
- 7241 Yunlin (640 MW, Taiwan, cod 2025-08-21)
- 8749 Benbrack (67 MW, UK, cod 2025-11-01)
- 8779 Crystal Rig 4 (49 MW, UK, cod 2026-03-09)
- 8780 Camster II (36 MW, UK, cod 2026-04-23)
- 8782 Hagshaw Hill Repowering (79 MW, UK, cod 2025-11-12)
- 8783 Douglas West Extension (66 MW, UK, cod 2026-03-11)
- 8784 Kilgallioch Extension (51 MW, UK, cod None)

**Not bugs.** Pipeline correctly recognizes insufficient data for degradation slope.

### Category C: Ramp-up flagger overreach (3 WFs)
These had **SKIPs** (no yearly curves) because most of their hours are flagged `is_ramp_up=true`:
- 7234 Changfang & Xidao 1 (100 MW, Taiwan): **only 28 non-rampup rows out of 20k** — 99.9% rampup
- 7235 Changfang & Xidao 2 (500 MW, Taiwan): 1,252 non-rampup out of 13.9k
- 7388 Iles d'Yeu (496 MW, France, ENTSOE): 1,584 non-rampup out of 14.9k

**Data-side issue.** The ramp-up flagger needs investigation — too many operational hours are being flagged as ramp-up for these new offshore WFs. Separate problem from the pipeline correctness work.

## Outstanding items

### From earlier in this initiative (still pending)
- [ ] Snapshot-diff post-backfill vs `tests/fixtures/baselines/*_pre.json`
- [ ] Circulate `docs/pipeline/release-notes-2026-05-25.md` to OS / ASR / KG
- [ ] Admin-UI analyst review queue for `structural_constraint_flags`

### New from today
- [ ] **GitHub issue**: 7404 transaction-architecture bug. Decouple peer aggregate refresh from module 1-6 commits.
- [ ] **GitHub issue**: ramp-up flagger overreach on new offshore WFs (7234, 7235, 7388 specifically; check Changfang/Xidao 1's 99.9% flag rate)
- [ ] **Config tweak**: add `pool_pre_ping=True` + `pool_recycle=600` to asyncpg engine
- [ ] **Backfill script**: catch `ConnectionDoesNotExistError` and call `engine.dispose()` to reset the pool between WFs
- [ ] Investigate why daily cron has been failing the structural constraint persistence — likely now fixed by the e2f3a4b5c6d7 migration application, but verify cron next firing

## How to verify

```bash
cd /Users/mdfaisal/Documents/energyexe/energyexe-core-backend
poetry run python -c "
import asyncio
from sqlalchemy import text
from app.core.database import get_session_factory

async def m():
    factory = get_session_factory()
    async with factory() as db:
        r = await db.execute(text('''
            SELECT COUNT(DISTINCT windfarm_id) FROM degradation_results
            WHERE created_at > NOW() - INTERVAL '36 hours'
        '''))
        print(f'WFs with post-migration deg_results: {r.scalar()}')
asyncio.run(m())
"
# Expected: 235
```

## Files written / referenced

- `docs/pipeline/backfill_remaining_wfs.txt` — final remaining 11 WF IDs (10 expected non-fixes + 7404)
- `docs/pipeline/backfill_completed_wfs.txt` — early-run snapshot (not updated since 12:40)
- `/tmp/backfill_chunk_{1..4}.log` — v6 parallel-4 chunk logs
- `/tmp/backfill_v7_chunk_{1,2}.log` — v7 parallel-2 chunk logs
- `/tmp/backfill_v8.log` — v8 sequential retry log (contains the 7404 root-cause trace at lines 280-300)

