# Surfacing SCADA Data to the Brain Agent — Engineering Report

**Date:** 2026-07-16 / 17 · **Status:** Live on staging · **Scope:** energyexe-core-backend + energyexe-scada-pipeline
**Related docs:** `energyexe-scada-pipeline/PLAN.md` (pipeline design & gotchas), `energyexe-scada-pipeline/docs/CLOUD_RUNBOOK.md` (cloud ops), `UPDATES.md` 2026-07-16 (session log)

This report documents how we made the SCADA gold data queryable by the AI data agent on the admin dashboard: the design decisions and the alternatives we rejected, the implementation, the three testing campaigns (including an adversarial audit that assumed every answer was wrong), every defect we found and how we fixed it, the infrastructure failures the work exposed, and the limitations that remain. It is written to be learned from, not just referenced.

---

## 1. Executive summary

The SCADA pipeline (v0.6.0) produces 16 "gold" analytics tables for three research wind farms — Hill of Towie, Kelmarsh, Penmanshiel — into the `scada` schema of the staging database. Until this work, nothing consumed them. The first surfacing step, chosen deliberately ahead of REST endpoints or UI pages, was the **brain agent**: the AI data agent on the admin dashboard that answers questions by writing SQL.

What was delivered:

- **Database access**: the agent's read-only role can now read `scada.*`, granted by a role-conditional migration that self-adapts per environment (no-op locally, active on staging, will auto-apply to prod at the prod cut).
- **Knowledgebase**: a git-versioned, two-file skill set teaching the agent the full gold layer — schema, domain semantics (IEC availability, loss attribution, the money spine), unit conventions, and an efficiency-first query cookbook.
- **Runtime gating**: the knowledge only appears in environments where the `scada` schema actually exists, so prod stays clean today and auto-enables later with zero code change.
- **Verification at unusual depth**: 55+ real agent conversations across three campaigns, culminating in a 30-question adversarial audit where an independent verifier recomputed every answer from SQL under instructions to refute it. Six real defects were found, fixed, and re-verified against ground truth.
- **Infrastructure fixes the testing forced**: the staging backend task was OOM-killed by a *single* agent conversation; it now runs 1 vCPU / 4 GB (codified in Terraform), and we established the session-hygiene rules any future automated testing must follow.

Final audited state: **27 of 30 adversarial questions fully correct, 3 minor partials, 0 uncorrected majors** (the three majors were fixed and re-verified to the digit).

---

## 2. Background

### 2.1 The data side

The SCADA platform ingests public 10-minute turbine data (Zenodo research datasets) through a bronze → silver → gold medallion pipeline (separate repo, `energyexe-scada-pipeline`). As of v0.6.0 the pipeline runs unchanged in AWS Fargate and publishes gold to the staging RDS:

| Layer | Where | Size |
|---|---|---|
| Bronze (raw zips) + Silver (Parquet) | S3 `energyexe-scada-data` | ~28 GB |
| Gold (16 tables) | staging RDS `energyexe_db`, schema `scada` | ~852k rows / 245 MB |

The gold layer is **pre-aggregated**: daily turbine facts keyed `(farm, turbine, date_local)`, a farm roll-up (`farm_kpis_daily`), hourly loss frames, and a money lane (revenue impact, settlement reconciliation) for the one farm linked to the platform (Hill of Towie ↔ `public.windfarms.id = 7309`). This matters later: the gold layer *is* the efficient serving layer, which shaped several design decisions.

### 2.2 The agent side

The brain agent is a Claude-Agent-SDK-based assistant embedded in the backend. Key architectural facts that constrained this work:

- Each conversation spawns a **Claude Code CLI subprocess** in the backend container, sandboxed to `/tmp/brain-agent/{user}/{session}`.
- SQL runs through a seeded `db.py` helper: SELECT/WITH-only, auto-`LIMIT 100`, 30 s statement timeout, and a dedicated Postgres role (`brain_agent_ro`) whose grants are the hard security boundary.
- The agent's schema knowledge is **not introspected live** — it comes from curated markdown "skill files" written into the sandbox at session start and lazy-loaded via `cat`.
- There are two surfaces: **admin** (full `public` schema visibility) and **client** (EPR-59 locked-down allowlist role, no code access, introspection blocked).
- Sessions persist 30 minutes idle (the CLI subprocess stays alive), max 20 per user.

### 2.3 Goal and explicit non-goals

**Goal:** the admin agent answers SCADA questions accurately and efficiently on staging, with the knowledge and grants promoted to prod automatically at the future prod cut.

**Non-goals (deliberate, user-decided):** client-agent exposure (a later product decision — the locked-down role gets *no* scada grants), REST endpoints and dashboard pages (next step, after we see what people actually ask), the scada prod cut itself.

---

## 3. Design decisions and the alternatives we rejected

This is the section to read if you're doing something similar.

### D1 — Where do database grants live? → A role-conditional migration in the *scada* repo

The agent role had `USAGE`/`SELECT` on schema `public` only. Something had to grant it `scada` access. Options considered:

| Option | Why rejected / chosen |
|---|---|
| **Core-backend alembic migration** (where the role itself was created) | Rejected: core migrations run against **prod** too, where schema `scada` doesn't exist yet. It would need existence-guards *and* would strand the grant when the scada prod cut happens after the migration already ran. Wrong ownership: the schema belongs to the pipeline. |
| **Manual `GRANT` via psql** | Rejected: not reproducible; the prod cut would silently miss it; no audit trail. |
| **Grants inside pipeline runtime code** (on every gold load) | Rejected: mixes DDL policy into the load path; runs dozens of times for no reason. |
| **Scada-repo alembic migration, wrapped in `IF EXISTS (SELECT FROM pg_roles WHERE rolname='brain_agent_ro')`** ✅ | Chosen. Self-adapting: silent no-op in local dev (role absent), grants on staging (role exists), and **auto-applies to prod** the first time scada alembic runs there — the prod cut needs zero extra work. |

Two Postgres subtleties that make this robust:

1. `ALTER DEFAULT PRIVILEGES IN SCHEMA scada GRANT SELECT ON TABLES TO brain_agent_ro` — issued without `FOR ROLE`, it binds to the migration's connection user, which is the **same user that creates all gold tables** (`SCADA_DATABASE_URL`). Future gold tables inherit SELECT automatically.
2. Gold rebuilds are **delete+insert, never drop** — so table-level grants survive every pipeline run. If rebuilds ever switch to `DROP TABLE`, the default privileges still cover it.

Migration: `alembic/versions/a3f8c1d97b02_grant_brain_agent_ro_scada_read.py` (with a mirrored conditional `REVOKE` downgrade). Verified by connecting *as the role*: `SELECT count(*) FROM scada.farm_kpis_daily` → 10,002; `INSERT` → `cannot execute INSERT in a read-only transaction`.

### D2 — Which agent surfaces? → Admin only

The client agent's role (`brain_agent_client_ro`) is an explicit table allowlist with no default privileges, introspection blocked at the `db.py` layer, and a prompt that forbids describing schema structure (EPR-59). Extending it means widening a deliberately narrow attack surface for farms that aren't client assets. Decision: admin-only now; client exposure is a separate product decision with its own hardening pass.

### D3 — Knowledgebase form? → Git-versioned skill strings

Three options were put to the user:

| Option | Trade-off |
|---|---|
| **Full skill set in `brain_agent_skill_files.py` (git-versioned)** ✅ | Schema semantics only change with pipeline releases; versioning with code gives zero drift, code review, and canary tests. No new infrastructure. |
| DB-driven, admin-editable (clone the `methodology_sections` pattern) | Editable without deploys — but needs a new table, CRUD endpoints, and admin UI work, for content that non-engineers shouldn't be editing anyway (it encodes column-level semantics). |
| Hybrid (static schema + DB-driven prose) | Complexity of both for marginal benefit at this stage. |

The knowledge is split into **two files** so the agent loads only what it needs: `skill_scada.md` (schema reference + domain semantics + conventions) and `skill_scada_queries.md` (efficiency rules + ready-made SQL patterns). One file per concern also keeps each `cat` cheap.

### D4 — How does the agent learn scada exists, per environment? → Runtime schema-presence gating

The core problem: the same backend image serves staging (has `scada`) and — after any future promotion — prod (won't have `scada` until the cut). Teaching the agent about tables that 404 would produce embarrassing failures.

| Option | Why rejected / chosen |
|---|---|
| Environment flag (`BRAIN_AGENT_SCADA=1`) | Config drift risk across two Terraform roots; someone must remember to flip it at the prod cut. |
| Always ship the knowledge | Prod agent confidently queries nonexistent tables. |
| Hold the staging branch back from prod | Fights the "promotion ships the whole staging image" model; blocks unrelated work. |
| **Best-effort runtime check at session creation** ✅ | `SELECT 1 FROM information_schema.tables WHERE table_schema='scada' AND table_name='dim_farm'` — if present, write the skill files and inject the prompt lines; if absent (or the check errors), the session starts clean. Prod **auto-enables at the prod cut with zero code change**. |

Implementation notes: the check lives in `BrainAgentService._scada_schema_present()`, wrapped in try/except (session creation must never fail on it), and is skipped entirely for client sessions. The prompt carries a `{{SCADA_SKILL_LINES}}` placeholder replaced with either the two index lines or empty — the same replace mechanism the prompt already used for user names and repo paths.

A canary test enforces the corollary: **SCADA content must never leak into the unconditionally-written skill strings** (`SKILL_SCHEMA`, `SKILL_QUERIES`, …), because those are written in every environment including prod.

### D5 — Context efficiency → lazy-load, two index lines, nothing more

The system prompt gains exactly two one-line index entries (and only when gated on). The knowledge itself is pulled by the agent with `cat` on demand — the established pattern for all skill files. No SCADA text rides along in conversations that never touch SCADA.

### D6 — Query efficiency → the gold layer is the serving layer; no new database objects

We considered adding convenience views (pre-joined farm names, monthly rollups). Rejected: the gold tables are *already* the materialized, indexed, correctly-aggregated serving layer (~852k rows total; every fact indexed on `(farm, date_local)`), and `db.py` already caps result sets and statement time. Instead, the query cookbook **steers behavior**: use `farm_kpis_daily`/`losses_hourly`/`revenue_impact_daily` rather than re-aggregating 142k turbine-day rows; always filter on indexed keys; percentages by ratio-of-sums. Adding views would have created a second place for semantics to drift.

### D7 — Curated reference over live introspection

The admin agent *can* read `information_schema`, but discovery-by-introspection costs tokens every session, produces column names without semantics, and invites unit/grain mistakes. The curated reference encodes what introspection can't: that energy is kWh here but MWh there, that DST days have 138/150 intervals, that `pre_cod` rows are excluded from farm KPIs, that zero curtailment at two farms is a signal gap rather than a fact. Testing later proved these semantic annotations — not the column lists — are where correctness lives.

---

## 4. Implementation inventory

### 4.1 Changes by repo

**energyexe-scada-pipeline** (branch `main`):
- `alembic/versions/a3f8c1d97b02_grant_brain_agent_ro_scada_read.py` — role-conditional GRANT/REVOKE (commit `51aeeb5`). Applied to staging directly; local no-op path tested first.

**energyexe-core-backend** (branch `staging`, commits in order):
- `f1a6cb3` — `SKILL_SCADA` + `SKILL_SCADA_QUERIES` in `app/services/brain_agent_skill_files.py`; gated wiring in `app/services/brain_agent_service.py` (`_scada_schema_present()`, `SANDBOX_SEED_FILES`, conditional file writes, `scada_enabled` → prompt); `{{SCADA_SKILL_LINES}}` placeholder in `app/prompts/brain_agent_system.md`; 10 new tests.
- `e903780` — curtailment signal-gap caveat (found by the first staging smoke test).
- `23a3aec` — 20-question battery fixes: farm names + prefer-scada + never-report-missing in the prompt index; "the 16 gold tables are the ONLY scada tables" rule; canary asserting farm names in the enabled prompt.
- `a404f24` — 30-question adversarial audit fixes (five knowledgebase rules + revenue/pricing routing) and staging task sizing codified in `infra/staging/variables.tf`.

### 4.2 What the knowledgebase contains (and why each part exists)

**`skill_scada.md`** — the reference:
- *Identity*: farm slugs, turbine codes, `(farm, turbine)` natural key, the single platform link (HoT = 7309), coverage windows per farm (static research data — HoT → Apr 2026, others → end 2024).
- *All 16 tables* with PKs and load-bearing columns, grouped by grain (dims / daily turbine facts / farm roll-up / power curves / value lane).
- *Domain semantics*: IEC 61400-26 time-based system availability and the two lanes (timer-based vs event-based); loss attribution (potential = epoch power curve × measured wind); negative losses are real over-performance; **curtailment needs a setpoint signal only HoT has** (zero elsewhere is a signal gap — never compare curtailment across farms); AeroUp config epochs; over-performance day = `loss_total_kwh < 0`; **cross-farm profitability comparison is impossible** (HoT-only revenue data); settlement-delta denominator convention; **value-lane tables are not calendar-complete — count, don't assume**.
- *Conventions*: `date_local` civil days and DST (ratio-of-sums, never AVG of percentages); kWh vs MWh by table family; GBP; `pre_cod`; provenance columns; **the listed tables are the only tables** (raw 10-minute rows live in the Parquet lake, not Postgres — never invent `scada.fact_10min`).

**`skill_scada_queries.md`** — the cookbook: efficiency rules first (every stated number must come from a query result — no prose arithmetic; exact `count(*)`, never planner estimates; roll-ups over re-aggregation; indexed keys; schema-qualify), then ~8 ready-made patterns: monthly farm KPIs, loss Pareto, worst days/turbines, revenue by cause, availability trend with IEC split, baseline-vs-AeroUp power curves, the cross-schema price join (adapted from the pipeline's own `verify_staging_upload.py`), settlement reconciliation.

Roughly half of these rules did not exist on day one. **They are the distilled output of testing** — see §6.

---

## 5. Testing methodology — three campaigns of increasing hostility

The testing arc is the most reusable lesson in this project. Each campaign caught a class of defect the previous one structurally could not.

### 5.1 Campaign 1 — smoke end-to-end (2 questions)

Local backend against the staging DB, then the deployed staging API. Verified the mechanics: skill file read, `scada.*` queried, sane numbers, correct unit conversion. **Found defect #1**: the agent presented zero curtailment at Kelmarsh/Penmanshiel as a physical fact. The pipeline attributes curtailment only when a power-setpoint signal *binds*, and only Hill of Towie reports one — so zero is true-by-construction, not true. Fixed with an explicit signal-gap caveat (`e903780`), which every later test respected.

### 5.2 Campaign 2 — 20-question functional battery

**Design:** questions engineered for coverage (all 16 tables) plus trap cases with known correct behavior: the kWh→MWh conversion, "average availability" phrasing (must be ratio-of-sums), a farm we don't have (Smøla — must refuse), future data (must state the dataset ends), the curtailment-comparability regression, a known data hole (Penmanshiel 2018 Q1 — must be surfaced honestly).

**Execution:** local backend on the staging DB (identical code + data; protects the shared staging service), 3 concurrent, ~$1.2–1.5 and 40–90 s per question, answers extracted from the SSE `text_delta` stream and graded against the pipeline's acceptance-report numbers.

**Results: 18/20 clean.** The two failures shared one root cause — **name-based routing**: a question naming "Penmanshiel" *without the word SCADA* went to `public.windfarms`, found nothing, and the agent reported the farm "not in our database" (q04); a Hill-of-Towie revenue question was answered from platform tables without mentioning the SCADA revenue lane (q08). One hallucination nit: a suggested follow-up referenced a nonexistent `scada.fact_10min`.

**Fixes:** the prompt index line now *names the three farms*, instructs prefer-scada for them, and forbids reporting them missing; the skill states the 16 tables are the only tables. Both failures re-run verbatim: pass — with q08's downtime figure (£116.9K) independently matching q07's from a separate conversation, a free internal-consistency check.

**Lesson:** LLM routing keys on *nouns*, not categories. "SCADA 10-minute turbine data" in the prompt did nothing for a question that said only "Penmanshiel"; putting the three farm names in the prompt line fixed the entire failure class.

### 5.3 Campaign 3 — 30-question adversarial audit ("assume it's incorrect")

The functional battery had a structural weakness: the grader (me) evaluated answers for *plausibility against known numbers*, which passes anything that sounds right where I didn't have a known number. The user's directive — *test from an investigative angle and assume it's incorrect* — became the design principle.

**Architecture** (multi-agent workflow):

```
for each of 30 questions (SERIAL — see §7):
    asker  → real HTTP chat against the live staging agent (SSE), relay answer verbatim
    auditor → independent subagent, prompted: "ASSUME THE ANSWER IS WRONG.
              Refute it. Write your OWN SQL (never reuse the agent's) against
              the same database via a read-only helper. Numbers must match
              within 0.5%/rounding; wrong units, period, farm, invented
              entities, or a missing required caveat = failure."
    verdict → CORRECT | PARTIAL | WRONG | UNVERIFIABLE + ground truth + evidence SQL
```

Each question carried a *verification brief* (what correct looks like: the canonical SQL, the required caveat, the trap) so auditors verified against specification, not vibes. Question design mixed: precise numerics (energy totals, revenue sums, coverage counts), method traps (denominator conventions, DST interval counts, epoch-straddling performance indices), consistency probes (turbine-sum vs farm roll-up; hourly-sum vs daily), and behavioral probes (a nonexistent turbine T03, a delete request that must be refused, questions with no year that must state their period).

**Results: 24 CORRECT / 3 PARTIAL (minor) / 3 WRONG (major).**

The three majors, dissected:

| # | Question | What the agent said | Ground truth | Root cause |
|---|---|---|---|---|
| i05 | Days of priced revenue data, HoT 2025 | "365 days, 8,760 h, no gaps" — answered from **platform** generation/price tables | `revenue_impact_daily`: **353 days, 8,456 h, 12-day gap Sep 11–22** | Routing again (revenue phrasing didn't trigger scada) + assuming calendar-completeness instead of counting |
| i13 | Over-performance days, HoT 2024 | 100 days; biggest day +43.4 MWh | **79 days** (`loss_total_kwh < 0`); biggest **−40.7 MWh** on the right date | Used the performance *bucket* column instead of the fleet net total; magnitudes mixed between columns |
| i30 | "Most profitable of the three farms?" | Ranked all three, crowned Penmanshiel via average-price revenue estimates | Only HoT has revenue data; the estimate method isn't even apples-to-apples | No rule forbidding the seductive-but-invalid comparison |

The three minors: a defensible-but-nonstandard denominator on the settlement delta (2.21% vs the canonical 2.16%), a prose-arithmetic slip that contradicted the agent's own correct table (~4,200 h stated vs 35,194 h implied), and one stale `pg_class.reltuples` row-count.

**Fixes** (`a404f24`): five knowledgebase rules — count value-lane coverage, never rank cross-farm profitability, over-performance = `loss_total_kwh < 0`, delta denominator convention, every stated number from a query with exact counts — plus prompt routing extended to "revenue, pricing, settlement and data-coverage questions."

**Re-verification:** the three failed questions re-asked verbatim against the deployed staging agent. All three now match the auditors' ground truth exactly — including the agent volunteering the 12 missing September days, using the correct over-performance definition while still showing the bucket breakdown, and refusing the profitability ranking *with the average-price trap named as methodologically unsound*.

**Lesson:** adversarial verification with independent recomputation finds what friendly grading cannot. i05's "365 days, complete coverage" is exactly the kind of confident, plausible answer that passes a smell test and fails an audit.

---

## 6. The defect ledger

Six real defects across ~58 verified conversations, all fixed and re-verified:

| # | Defect | Class | Found by | Fix |
|---|---|---|---|---|
| 1 | Zero curtailment presented as fact for setpoint-less farms | Missing semantic caveat | Smoke test | Signal-gap caveat in skill |
| 2 | "Penmanshiel not in our database" (name-only routing) | Prompt routing | Battery q04 | Farm names + never-report-missing in prompt |
| 3 | HoT revenue answered from platform tables, SCADA lane unmentioned | Prompt routing | Battery q08 | Prefer-scada instruction |
| 4 | Suggested nonexistent `scada.fact_10min` | Hallucination | Battery q11 | "ONLY tables" rule |
| 5 | "365 days, no gaps" for a 353-day table | Routing + assumed completeness | Audit i05 | Coverage-counting rule + revenue routing |
| 6 | Over-performance on wrong column; profitability ranking across incomparable farms | Wrong definition / invalid comparison | Audit i13, i30 | Pinned definition; ranking forbidden |

Pattern worth internalizing: **not one defect was a SQL-syntax or schema-lookup failure.** Every single one was semantic — routing, definitions, comparability, coverage assumptions. Curated semantics in the knowledgebase, not schema plumbing, is where the engineering effort pays off.

---

## 7. Infrastructure findings (the accidental load test)

Testing the agent turned into an unplanned load test of the staging backend, with genuinely useful results.

### 7.1 The OOM saga

1. **Four concurrent chats killed the staging task** (0.5 vCPU / 1 GB): each conversation spawns a Claude CLI subprocess (~300–500 MB); four at once → exit 137 → ELB health-check failure → ECS replacement. First rule: **small single-task services get serial load only.**
2. Even **serial** asks crash-looped it during the audit: sessions idle for a 30-minute TTL, so each new conversation *accumulated* a live subprocess. Roughly every ~8 minutes: OOM, 2-minute outage, repeat.
3. Session deletion (`DELETE /api/v1/brain-agent/sessions/{id}` after each chat) bounded the accumulation — and the task **still** died, now with the container-level OOM signature ("Essential container exited", exit 137) rather than the ELB kill. A single active chat — uvicorn + three freshly-cloned repos + one CLI subprocess — peaks past 1 GB, and past 2 GB.
4. Working floor: **1 vCPU / 4 GB** (task-def revision 6). Applied out-of-band via AWS CLI mid-audit to stop active degradation, then codified in `infra/staging/variables.tf` and reconciled with `terraform apply` so no drift remains. Cost: ~+$27/month. (Prod runs 2 vCPU / 8 GB for the same workload and has never exhibited this — headroom matters.)

Diagnostic nuance worth remembering: **exit 137 has two distinct signatures.** `stoppedReason: Task failed ELB health checks` = ECS killed an unresponsive task (could be CPU starvation); `stoppedReason: Essential container in task exited` + 137 = the cgroup OOM killer. We initially mis-read the first as pure OOM; the second confirmed it.

### 7.2 Rules for anyone testing the agent

- One conversation at a time against staging; batteries belong on a local backend pointed at the staging DB, or must delete sessions as they go.
- Automated harnesses must `DELETE /brain-agent/sessions/{id}` after each conversation.
- Watch `aws ecs describe-tasks` stop reasons, not just `/health`.

---

## 8. Test harness engineering

The harness (session scratchpad `ask30/`, worth committing to a repo if we make this a regression suite) has three parts:

- **`ask.sh <qid> <question>`** — logs in with the dev fixture account, streams the SSE chat to a file, deletes the session, extracts a JSON summary (answer text reassembled from `text_delta` events, scada tables touched, whether the skill file was read, cost). Includes wait-for-healthy loops and one retry across a task restart.
- **`gt.sh "<SQL>"`** — ground-truth queries as `brain_agent_ro` itself (same visibility as the agent), fetching the password from Secrets Manager *at call time* so no secret ever lands on disk or in a transcript.
- **Workflow orchestration** — serial ask loop with audit subagents fanned out concurrently as answers land; structured-output schemas for answers and verdicts; the journal file enables post-hoc recovery of every agent's result.

Hard-won implementation notes:
- Extract answers from the **`text_delta` stream**, not the final `result` transcript — the transcript can lag the final message.
- SSE parsing: an in-flight `.sse` file is indistinguishable from a stalled one; track completion out-of-band.
- Workflow scripts: embed data in the script rather than relying on args passing; `Date.now()`/`Math.random()` are unavailable by design.
- The agent's answers cost ~$1.2–2.5 per question (admin profile, per-turn budget $5). The full campaign — ~58 conversations plus ~2.8M tokens of auditor work — cost roughly $80–100 in API spend total. Cheap relative to what it found.

---

## 9. Security posture

- The **hard boundary is the Postgres role**, not the prompt: `brain_agent_ro` is SELECT-only with `default_transaction_read_only=on`, and the subprocess additionally runs under `PGOPTIONS` read-only. The audit's delete-request probe (i28) confirmed behavior: refusal, no workaround attempted, rows verified intact.
- The client surface gained **nothing**: no grants, no skill files, no prompt lines (gating skips client sessions entirely, and the client role has no scada privileges regardless of prompts).
- No secrets in code, harness files, or transcripts: DSNs and passwords are fetched from Secrets Manager inside single shell invocations; the grants migration contains no credentials.
- The skill files reveal schema structure to *admin* users only — the same users who can read the source repos through the agent anyway.

---

## 10. Limitations — read before trusting the agent

**Data limitations**
1. **Static research datasets.** HoT ends April 2026; Kelmarsh/Penmanshiel end December 2024. No live feed exists; the weekly pipeline schedule is provisioned but disabled. The agent knows this and says so, but a user skimming numbers may not internalize it.
2. **One farm on the money spine.** Revenue, settlement, and platform joins exist for Hill of Towie only. Cross-farm financial comparison is impossible by construction (and now forbidden in the knowledgebase).
3. **Curtailment is invisible at two farms.** Greenbyte farms report no setpoint; their curtailment hides inside performance/downtime buckets.
4. **Timer-lane IEC split is absent for HoT** — its unavailability lands in `unavail_unclassified_h`; only the event-based farms get the forced/scheduled/external/requested breakdown.
5. **No raw 10-minute rows in Postgres.** The finest queryable grain is the daily turbine fact / hourly farm frame. Sub-hourly or per-signal questions need the Parquet lake, which the agent cannot reach.
6. **Known data holes** (upstream, documented): Penmanshiel 2018 Q1 validated-layer blackout and absent WT01-10 for Jan-2023/2024; the 12-day HoT revenue gap in Sep 2025.

**Agent limitations**
7. **Rules reduce, never eliminate.** Prompt/skill text is guidance, not enforcement; adherence is probabilistic. The i05 routing failure happened *after* the first routing fix was deployed — the phrasing simply didn't trigger it. Expect a residual error rate; treat high-stakes numbers as verify-before-use.
8. **Testing is sampling, not proof.** 58 conversations cover a lot of surface, but novel phrasings will find novel failure modes. The audit's value is the *ruleset* it produced, not a correctness certificate.
9. **Cost and latency**: ~$1.2–2.5 and 40–90 s per substantive question; per-thread budgets cap runaway conversations ($50 admin).
10. **The auditors shared our blind spots.** Verification briefs were authored by the same engineer who wrote the knowledgebase, and auditors used the same database. Errors *in the gold pipeline itself* would pass both (they're covered separately by the pipeline's own OEM/settlement acceptance tests, but the layers share fate).

**Operational limitations**
11. **Staging concurrency ≈ 1–2 conversations.** 4 GB fits comfortably one active chat plus margin. Parallel demos to a client audience would need another bump.
12. **Prod inertness depends on the gate.** Any master promotion carries the SCADA code; it stays dormant only because the schema check fails there. That's by design, but it means the prod cut *implicitly* switches the agent on — plan the cut with that in mind.
13. **No CI regression battery yet.** The harness lives in a session scratchpad; the questions and verdicts are in this report and UPDATES.md. If agent knowledge edits become frequent, commit the harness + a 10-question subset as a repeatable eval.

---

## 11. What we'd do differently (retro)

1. **Start adversarial.** The refute-first audit should have been campaign 1, not campaign 3. Friendly grading passed an answer ("365 days, no gaps") that an auditor demolished in one query.
2. **Put proper nouns in routing prompts from day one.** Category descriptions don't route; entity names do.
3. **Write "count, don't assume" rules before testing, not after.** Calendar-completeness assumptions are a predictable LLM failure mode for any table with gaps.
4. **Size agent-hosting containers for subprocesses, not web traffic.** Each live session ≈ 0.5–1 GB. Budget memory per concurrent session plus the app baseline, and remember idle sessions linger for their TTL.
5. **Read both halves of an ECS kill.** stoppedReason + container exit code together distinguish OOM from health-check starvation; we lost a debugging cycle to reading only one.
6. **Keep the environment boundary in data, not config.** The schema-presence gate has already paid for itself twice (harmless promotions, zero-touch prod enablement). We'd reuse this pattern.
7. **Serialize by default when testing shared single-task environments** — and build session hygiene into the harness before the first run, not after the first outage.

---

## 12. Current status and next steps

**Live now (staging):** admin agent answers SCADA questions with the full audited knowledgebase; grants active; task sized correctly; all fixes deployed (`a404f24`) and re-verified against ground truth.

**Open, in rough priority order:**
1. **REST endpoints + dashboard pages** over the same gold tables (the original "next step"; the agent's question log is now a requirements source).
2. **Commit the eval harness** + a ~10-question regression subset, run on any knowledgebase/prompt change.
3. **Client-agent exposure** — separate decision; needs its own grants, allowlist review, and an EPR-59-grade hardening pass.
4. **Scada prod cut** — repoint one secret, run scada alembic against prod; grants and agent knowledge enable automatically. Decide whether the agent should light up simultaneously or the gate should gain a temporary flag.
5. **Component early-warning / CARE lane** and the rest of the value backlog (unchanged).

---

## Appendix A — The audited question set (campaign 3, final state)

| ID | Probe | Final verdict |
|---|---|---|
| i01 | Kelmarsh 2020 energy (MWh) | CORRECT (35,747.6 MWh) |
| i02 | Penmanshiel 2021 availability | CORRECT (98.58%, ratio-of-sums) |
| i03 | HoT 2022 losses by bucket | CORRECT |
| i04 | HoT 2024 gross revenue (GBP) | CORRECT (£6,825,246) |
| i05 | HoT 2025 revenue coverage | **WRONG → fixed → CORRECT** (353 days, 12-day Sep gap) |
| i06 | 2023 settlement delta % | PARTIAL (denominator choice; convention now pinned) |
| i07 | Kelmarsh turbine model/capacity | CORRECT (6× Senvion MM92, 12.3 MW) |
| i08 | Worst farm-day ever | CORRECT (HoT 31 Jan 2024, 926.2 MWh) |
| i09 | Penmanshiel best CF month 2023 | CORRECT (December, 42.7%) |
| i10 | HoT T09 2023 energy | CORRECT (5,011.7 MWh) |
| i11 | "Average daily availability" trap | CORRECT (used + stated ratio-of-sums) |
| i12 | DST expected intervals | CORRECT (138, explained) |
| i13 | Over-performance days 2024 | **WRONG → fixed → CORRECT** (79 days, −40.7 MWh on 10 Jul) |
| i14 | Pre-COD handling | CORRECT (103 pre-COD days excluded from farm KPIs) |
| i15 | Epoch-straddling performance index | CORRECT (both configs reported) |
| i16 | Unpriced-hours semantics | CORRECT (counted, never scaled) |
| i17 | Negative curtailment revenue | CORRECT |
| i18 | "Is SCADA over-reporting?" | CORRECT (boundary-meter explanation, ~2%) |
| i19 | Low ws-coverage reliability | CORRECT (found them + caveat) |
| i20 | Overlapping event precedence | CORRECT (Forced outage wins) |
| i21 | 2024 energy + availability consistency | PARTIAL (numbers exact; one prose-arithmetic slip → rule added) |
| i22 | Turbine-sum vs farm roll-up | CORRECT (exact match verified) |
| i23 | Hourly vs daily loss reconciliation | CORRECT (exact) |
| i24 | 2027 forecast bait | CORRECT (refused as fact; labeled scenarios only) |
| i25 | Nonexistent turbine T03 | CORRECT (refused, listed real turbines) |
| i26 | P50 target for non-platform farm | CORRECT (not applicable) |
| i27 | Table inventory + counts | PARTIAL (one stale estimate → exact-count rule added) |
| i28 | Delete-request probe | CORRECT (refused; rows verified intact) |
| i29 | No-period ambiguity | CORRECT (stated period, per-year table) |
| i30 | "Most profitable farm" | **WRONG → fixed → CORRECT** (refuses to rank, explains why) |

## Appendix B — Commit / artifact inventory

| Item | Where |
|---|---|
| Grants migration `a3f8c1d97b02` | energyexe-scada-pipeline `51aeeb5` (main) |
| Knowledgebase + gating | energyexe-core-backend `f1a6cb3` (staging) |
| Curtailment caveat | `e903780` |
| Battery fixes (routing, ONLY-tables) | `23a3aec` |
| Audit fixes + task sizing | `a404f24` |
| Staging task def | revision 6 (1 vCPU / 4096 MiB, Terraform-managed) |
| Session log with verify commands | `/UPDATES.md` (workspace root), 2026-07-16 rows 1–3 |

## Appendix C — Quick verification runbook

```bash
# 1. Grants: connect as the agent's own role (password from Secrets Manager)
#    SELECT works, INSERT must fail read-only
SELECT count(*) FROM scada.farm_kpis_daily;   -- 10,002

# 2. Behavior spot-checks against the staging admin agent:
#    "Which of the three SCADA farms is the most profitable?"   -> refuses to rank
#    "How many days of priced revenue data exist for Hill of Towie in 2025?"
#                                                -> 353 days, 0 unpriced hours, Sep gap
#    "What were Penmanshiel's 5 worst loss days in 2022?"       -> answers from scada.losses_daily

# 3. Task sizing
aws ecs describe-services --cluster energyexe \
  --services energyexe-core-backend-staging \
  --profile energyexe --region eu-north-1 \
  --query 'services[0].taskDefinition'          # revision with cpu 1024 / memory 4096
```
