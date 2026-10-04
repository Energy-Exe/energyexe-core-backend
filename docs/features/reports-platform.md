# Reports platform (EPR-81 / 82 / 83 / 88)

Client- and admin-facing generated reports (web + PDF) with deterministic data sections and
AI-written narrative sections. Code lives in [`app/services/reports/`](../../app/services/reports/)
and [`app/api/v1/endpoints/reports.py`](../../app/api/v1/endpoints/reports.py); prompts in
[`app/prompts/reports/`](../../app/prompts/reports/). The frontend renderer is the client-ui
`report-kit` (section `kind` keys are shared — keep them in lock-step).

## Pieces

| Module | Role |
|---|---|
| `registry.py` | **Declarative report-type specs** (`ReportTypeSpec` → sections with `key`, `kind`, a data builder and an optional `NarrativeSpec`). Two types today: `opportunity_report` (executive summary, key metrics, generation chart, findings, wind-norm chart, capture-rate chart, action plan) and `digest_report`. Adding a report type = one spec here + prompts + any new data builders; no orchestrator or endpoint changes. |
| `context.py` | `ReportContext` — the read-only inputs handed to every data builder (`db`, `report_id`, `scope_type` `windfarm` \| `portfolio`, `period_start/end`, the `Windfarm` or `Portfolio`, `params`). Builders must not commit; the orchestrator owns transactions. |
| `data_builders/` | `common.py`, `opportunity.py`, `digest.py` — deterministic slices (one async function per section). |
| `orchestrator.py` | **Two-pass runner** as a fire-and-forget asyncio task (the `weather_imports` pattern): wave 1 data sections concurrently (each in its own session), wave 2 Pass-1 narratives, wave 3 the Pass-2 executive summary. Narratives respect `REPORTS_MAX_COST_USD`; once exceeded the rest are SKIPPED and the report lands `PARTIAL`. Also `sweep_stuck_reports()` (run at startup) and `pdf_is_stale()`. |
| `narrative_service.py` | Single-shot Anthropic Messages calls (not the brain-agent loop). Structured output enforced by forcing a tool call whose `input_schema` is the section's schema, then Pydantic validation. Models from `REPORTS_LLM_MODEL_SECTION` / `REPORTS_LLM_MODEL_SUMMARY`. Prompts are `app/prompts/reports/{report_type}/{prompt_key}.txt` with `$terminology_rules` substituted. |
| `fact_check.py` | Numeric fact-check for the executive summary: every number it cites must appear (within a small relative tolerance) in the sections it cites, or the summary fails — a fabricated figure can never render first. |
| `terminology.py` | House vocabulary (EPR-117): P50 is only ever the **"Generation target"**; `PROMPT_RULES` goes into every prompt, `find_violations` lints the model output (one corrective retry, then accept + log). A test runs the same lint over the deterministic labels. |
| `service.py` | `ReportService` — CRUD, scoping, retain-on-export versioning: a report whose PDF was downloaded (or that is locked) is frozen; changes create a new version. |
| `pdf/` | `builder.py` (`PdfBuilder`, reportlab), `charts.py`, `renderers/{opportunity,digest}.py`. The PDF is stored in S3 (`report.pdf_s3_key`, `app/services/s3_service.py`) and re-rendered when stale. |
| `app/services/opportunity_schemas/evidence.py` | Per-schema evidence formatting shared by the web report and the PDF so both render identical strings. |

Settings (`app/core/config.py`): `REPORTS_LLM_MODEL_SECTION`, `REPORTS_LLM_MODEL_SUMMARY`,
`REPORTS_MAX_COST_USD` (hard budget per generation run), `ANTHROPIC_API_KEY`.

## API (`/api/v1/reports`; collection routes need the trailing slash — `redirect_slashes=False`)

| Route | Notes |
|---|---|
| `GET /reports/types` | report-type metadata from the registry |
| `POST /reports/` → 202 | create + start generation (fire-and-forget) |
| `GET /reports/`, `GET /reports/{id}`, `GET /reports/{id}/status` | list / full / lightweight polling |
| `GET /reports/{id}/versions` | retained versions |
| `POST /reports/{id}/generate`, `/regenerate`, `/sections/{section_key}/generate` → 202 | whole report / new version / one section |
| `GET /reports/{id}/pdf` | downloads (and freezes) the PDF |
| `DELETE /reports/{id}` | removes the report chain |

## Operator tooling

- `scripts/rerun_report_narratives.py` — re-run narrative sections on existing live reports after
  a prompt or vocabulary fix (`--find-term bankable --dry-run`, `--report-id`, `--sections`).
  Frozen reports are never touched. Run inside the backend environment (one-off ECS task with the
  prod task definition, or locally against a DB tunnel) where `DATABASE_URL` and
  `ANTHROPIC_API_KEY` are set.
- The older per-windfarm commentary (`/report-commentary/*`, `app/services/llm_commentary_service.py`,
  `LLM_PROVIDER` / `LLM_MODEL`) predates this platform and is independent of it.

## Gotchas learned on the way

- An `AsyncSession` must never be shared across concurrently running sections — one session per
  section.
- The findings a report shows are **period-scoped** (EPR-117): detection windows are clipped per
  farm to the report period.
- Full calendar years only for FIN-01 attainment in the digest.
- Report deletion removes the whole version chain, not just one row.
