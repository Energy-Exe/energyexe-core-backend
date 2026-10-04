# SCADA API

The backend's read side of the SCADA platform: the portal dashboard charts, the Revenue-at-Risk
register, the shared PPA register, the private offtake export used by the offline preparer, and
the isolated measured-data preview. **The pipeline that produces the data lives in the sibling
repo** — its canonical guide is
[`energyexe-scada-pipeline/docs/opportunities/SCADA_OPPORTUNITY_SCHEMA.md`](../../../energyexe-scada-pipeline/docs/opportunities/SCADA_OPPORTUNITY_SCHEMA.md)
(that link, like the others into the pipeline repo below, assumes the four repos are checked
out side by side under one workspace directory). Decisions are numbered `D-0xx` there.

## Data contract

All pipeline-owned tables live in Postgres schema **`scada`** (gold roll-ups, the opportunity
register) and are **not** on the `search_path` — every query is schema-qualified raw SQL on the
ordinary request session; no separate engine. The SQL source of truth for the charts is the
pipeline's `docs/ui/queries.sql`. Every `/scada` endpoint returns **503** while the `scada`
schema (or the findings tables) is absent on an environment and **404** on an unknown farm slug.
The agent (brain agent) sees only these gold roll-ups — raw 10-minute data was removed from its
reach on 2026-10-02 ([`brain-agent/scada-integration.md`](brain-agent/scada-integration.md)).

## Endpoints

| Prefix / module | Auth | What |
|---|---|---|
| `/api/v1/scada` — `scada.py` → `ScadaService` | any authenticated active user (`get_current_active_user`); the frontend's `canAccessScada` gate decides who sees the portal. No farm-level ACL yet by product decision (D-014) | `GET /farms`, `/heartbeat`, and one endpoint per dashboard chart: `energy-waterfall`, `revenue-waterfall`, `availability`, `completeness`, `degradation`, `downtime-fingerprint`, `alarm-pareto`, `cumulative-losses`, `settlement-recon`, `scada-vs-boav`, `aeroup`, `annual-cost`, `method-mix`, `league`, `wind-index`, `midwind-fade`, `portfolio`, `replay-days`, `replay`, `self-consumption`, `turbines`. `GET /ingestion-summary` (measured-run summary, v1/v2 schema) requires `get_current_internal_user`. |
| `/api/v1/scada/opportunities` — `scada_opportunities.py` → `ScadaOpportunityService`, `ScadaFindingService` | same as above | Revenue-at-Risk register persisted by the pipeline (`scada.opportunity_register` / `opportunity_run` / `dim_opportunity_trigger`): `GET /` (ranked feed), `/summary`, `/triggers`, `/by-year`, `/{id}`; `PUT /actions` writes the human lifecycle state (acknowledge / resolve / notes) into the public table `scada_finding_action`, keyed by the natural key `(farm, trigger, scope, cls)` — never by register row id, which changes on every run. The £ headline sums only additive classes (REALIZED + RECOVERABLE + CURTAILMENT). |
| `/api/v1/scada/ppas` — `scada_ppas.py` → `ScadaPpaService` (EPR-97, EPR-143) | **`get_current_internal_user`** (superuser AND `is_internal`); every route audited with `@audit_action` ([`audit-system.md`](audit-system.md)) | The detailed SCADA offtake-contract register, **shared per wind farm** (one official set of terms per farm, "entered by" provenance in `created_by_id`). `GET ""`, `GET/DELETE /by-code/{ppa_code}`, `GET/PUT/DELETE /{ppa_id}`, `POST ""`. Overlapping *Active* terms on one farm are refused at entry (D-041) inside one transaction with farm-level locks. Completely separate from the Perform `/ppas` table. |
| `scripts/export_scada_offtake.py` → `app/services/scada_offtake_export.py`, `scada_private_files.py` | operator CLI, not an HTTP route | EPR-138 stage 1: exports a farm's PPA terms and platform FX into **private local JSON** for the pipeline's offline preparer. Output roots are validated against a deny-list of sync folders (Dropbox, OneDrive, …) and the files are written with restrictive permissions. Never a valuation API. Historical notes: [`docs/archive/scada-opportunities/`](../archive/scada-opportunities/). |
| `app/scada_preview/` + `scripts/start_sfe_preview.py` | local fixture login only | Isolated local-only FastAPI app serving immutable measured-data runs from a local `energyexe_sfe_preview` database. Independent of `app.main` and `app.core`. Runbook: [`docs/operations/sfe-local-preview.md`](../operations/sfe-local-preview.md). |

`SCADA_INGESTION_ENABLED` (`config.py`) gates the measured-ingestion surface in the main app.

## Related operations docs

- Partner uploads into the inbound S3 drop zone (`infra/partner_inbound.tf`):
  [`partner-upload-sfe.md`](../operations/partner-upload-sfe.md),
  [`partner-upload-varanger-kraft.md`](../operations/partner-upload-varanger-kraft.md).
- Granting `is_internal`: [`EPR-143-internal-users-grant.md`](../operations/EPR-143-internal-users-grant.md).
- Pipeline-side publishing to a database (the "staging" secret that points at prod): the
  pipeline repo's `docs/operations/db-publishing.md`.

## Tests

`tests/test_scada_endpoints.py`, `tests/test_scada_preview_schemas.py` (byte-compares
`schemas_v2.py` with the pipeline's `summary_contract_v2.py` when the sibling checkout exists),
`preview_tests/` (outside `testpaths`, run by hand).
