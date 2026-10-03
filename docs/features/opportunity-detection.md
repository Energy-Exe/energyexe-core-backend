# Opportunity detection (Perform)

Fleet-wide detection of commercial / operational / financial / data-quality "opportunities" on
the public-data windfarms (not SCADA — that register is described in [`scada-api.md`](scada-api.md)).
Findings land in the `opportunities` table and surface in both portals, in the brain agent and in
the reports platform.

## Where the code is

| Module | Role |
|---|---|
| [`app/services/opportunity_schemas/registry.py`](../../app/services/opportunity_schemas/registry.py) | `SCHEMA_REGISTRY` — the **ordered** map `SchemaCode → detect(ctx)`; iteration order is detection order. `SCHEMA_DEPENDENCIES` (OPS_03 needs OPS_01, MKT_02 needs MKT_01 — the prerequisite's row id is wired into `triggered_by_id`). `run_for_windfarm()` turns `DetectorResult`s into `Opportunity` ORM rows and is the **single persist point**. |
| `opportunity_schemas/<code>_<name>.py` | One detector per schema, pure-ish `detect(ctx) -> Optional[DetectorResult]`. 19 `SchemaCode` members: OPS_01..08, MKT_01..07, FIN_01..03, DQ_01 (the "18" in the initiative name is shorthand). MKT_05 and MKT_07 are registered but **inactive** (no PPA price / forecast data). |
| `opportunity_schemas/context.py` | `DetectionContext` — per-windfarm memoised async accessors over the upstream queries, so each query runs once per farm regardless of how many detectors need it. Accepts a `prefetched` dict so detector tests run without Postgres. |
| `opportunity_schemas/schema_names.py` | `SCHEMA_NAMES` — the only place codes map to human-facing names; every user surface must show names, not codes. |
| `opportunity_schemas/evidence.py` | formats a finding's `data_slots` for the web report and PDF. |
| [`app/services/opportunity_detection_service.py`](../../app/services/opportunity_detection_service.py) | `OpportunityDetectionService.run_detection_job(windfarm_ids=None, period_months=24, schema_codes=None, job_id=None)` — creates the `import_job_executions` row, iterates operational windfarms, builds a `DetectionContext` per farm and delegates to `run_for_windfarm`; commits **per windfarm**. The module docstring still says "6 schemas" from before the expansion — the registry is authoritative. |
| `app/services/financial_opex_metrics.py` | shared OPEX/MWh definition for FIN-02 / FIN-03 ([`financial-ratios.md`](financial-ratios.md)). |
| `app/models/opportunity.py` | `Opportunity`, `OpportunityStatus`, `SchemaCode`. Unique partial index `(windfarm_id, schema_code) WHERE status = 'ACTIVE'` — one active finding per schema per farm; a new run supersedes the old row. |

Windows: the fleet window is `now − period_months × 30 d → now`; each finding stores its own
detection period, and reports clip it per farm to the report period (EPR-126 / EPR-117).

## How it runs

1. **Nightly** — phase 3 of the pipeline ECS task, after the performance batch and the
   monthly-view refresh ([`docs/operations/scheduled-jobs.md`](../operations/scheduled-jobs.md)).
   A detection failure does not mask a successful batch (exit code 2).
2. **Manual** — `POST /api/v1/opportunities/detect`: with `windfarm_ids` it runs **synchronously**
   and returns the per-run summary (single-asset debugging); without, it creates the job row,
   schedules a FastAPI `BackgroundTasks` run over the whole fleet and returns the `job_id` to
   poll. `schema_codes` restricts either branch. **Never call the fleet-wide branch on prod
   through the API during the day** — it holds the single worker for minutes.
3. **Backfills** — `scripts/jobs/run_detection_shard.py --total-shards N --shard-index i`
   (round-robin shards, a fresh session per windfarm so one dropped RDS connection costs one
   farm, `--only-missing-since TS` to top up a run that died). `scripts/jobs/run_detection_jobs.py
   opportunity-detection` is the unsharded backstop.

## Reading and acting on findings

`/api/v1/opportunities` (`get_current_active_user`): `GET /` (filters, portfolio scoping, client
visibility via `is_client_request`), `GET /{id}`, `PATCH /{id}` (status changes —
`OpportunityStatus`: `ACTIVE` → `ACKNOWLEDGED` / `RESOLVED`; `SUPERSEDED` is set by the next run, `INACTIVE` marks data-blocked schemas), `POST /detect`. The admin-ui has an "Opportunities" section;
the client portal shows them per farm and in the Opportunity report.

Related: structural-constraint flags from pipeline Module 1b have their own review API
(`/api/v1/structural-constraints`, confirmed-only gating — [`docs/pipeline/module-1b-structural-constraint-detection.md`](../pipeline/module-1b-structural-constraint-detection.md));
data-availability caveats per schema are tracked in
[`docs/pipeline/opportunity-detection-data-backlog.md`](../pipeline/opportunity-detection-data-backlog.md).

## Tests

`tests/opportunity_schemas/` (per-detector, DB-free via `prefetched`), `tests/test_opportunity_detection.py`,
`tests/test_opportunity_detection_integration.py`, `tests/test_detection_failure_handling.py`,
`tests/test_opportunities_endpoint.py`, `tests/test_opportunity_model_enums.py`.
