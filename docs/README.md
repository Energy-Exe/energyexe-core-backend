# energyexe-core-backend — documentation index

Start here. Top-level orientation is in [`../README.md`](../README.md) (how to run it, ports,
deploy flow) and [`../ARCHITECTURE.md`](../ARCHITECTURE.md) (layering, directory tree, build).
Infrastructure runbooks live next to the Terraform: [`../infra/README.md`](../infra/README.md)
(prod) and [`../infra/STAGING.md`](../infra/STAGING.md). The workspace-level index for all four
repos is `../../docs/README.md`.

## Conventions

- **One index per repo** (this file) with CURRENT and HISTORICAL sections. Every historical doc
  carries a dated banner at the top and a link to its replacement.
- Dated artefacts go under `docs/history/` (or `docs/archive/`), never at the repo root.
- **Session logs are not committed.** The durable lesson goes to
  [`history/lessons.md`](history/lessons.md); the user-facing change goes to the workspace
  `UPDATES.md`.
- When a doc moves, grep `app/`, `scripts/`, `tests/`, `infra/`, `.github/` and `*.md` for the
  old path and fix references in the same commit.
- `README.md` files that sit beside code (`scripts/seeds/**`, `scripts/fixes/`,
  `tests/reference/`) stay there; they are listed below so nothing is orphaned.
- claude-mem `**/CLAUDE.md` stubs are gitignored.

## CURRENT

### Operations
| Doc | What |
|---|---|
| [operations/scheduled-jobs.md](operations/scheduled-jobs.md) | Every EventBridge-scheduled job: Lambda-triggered imports, the 01:30 UTC weather task, the 03:00 UTC pipeline + detection task, and the operator-run scripts that are deliberately not scheduled |
| [operations/observability.md](operations/observability.md) | GlitchTip via the Sentry SDK, `GIT_SHA` release tagging, cron monitors, request ids, startup sweepers, CloudWatch alarms |
| [operations/partner-upload-sfe.md](operations/partner-upload-sfe.md) | Instructions sent to SFE for uploading SCADA files to the inbound S3 drop zone |
| [operations/partner-upload-varanger-kraft.md](operations/partner-upload-varanger-kraft.md) | Same, for Varanger Kraft (Raggovidda) |
| [operations/sfe-local-preview.md](operations/sfe-local-preview.md) | The isolated local-only preview API for measured SCADA runs (`app/scada_preview/`) |
| [operations/EPR-143-internal-users-grant.md](operations/EPR-143-internal-users-grant.md) | Granting `users.is_internal` per environment by hand |
| [../.github/workflows/README.md](../.github/workflows/README.md) | The deploy workflows and the manual import trigger; why GitHub `schedule:` was retired |

### Features
| Doc | What |
|---|---|
| [features/auth-and-users.md](features/auth-and-users.md) | JWT, user columns and roles, the `get_current_*` dependencies, internal-staff gate, brain-agent access policy, invitations, consents, Resend email |
| [features/reports-platform.md](features/reports-platform.md) | Reports platform (EPR-81/82/88): registry, two-pass orchestrator, narratives + fact-check, PDF, `rerun_report_narratives.py` |
| [features/opportunity-detection.md](features/opportunity-detection.md) | The 19-schema detector registry, how detection runs (nightly / manual / sharded backfill), the `/opportunities` API |
| [features/scada-api.md](features/scada-api.md) | `/scada` dashboard endpoints, Revenue-at-Risk register, SCADA PPA register, private offtake export, preview — with links into the pipeline repo's canonical guide |
| [features/prices-fx-market-data.md](features/prices-fx-market-data.md) | Price ingestion (ENTSOE / Elexon MID), windfarm-level hourly prices, capture-rate analytics, ECB FX conversion |
| [features/financial-ratios.md](features/financial-ratios.md) | Financial ratios from filed accounts + metered generation; shared OPEX/MWh module; peer summary |
| [features/alerts-and-anomalies.md](features/alerts-and-anomalies.md) | User alert rules / notifications (no evaluation engine yet) and data-anomaly detection + re-aggregation |
| [features/audit-system.md](features/audit-system.md) | What is audited, how rows are persisted, client IP behind the ALB, the `@audit_action` decorator |
| [features/brain-agent/requirements.md](features/brain-agent/requirements.md) | Brain agent requirements and SSE/transcript architecture (April 2026, with a what-changed banner) |
| [features/brain-agent/readonly-role.md](features/brain-agent/readonly-role.md) | The two read-only Postgres roles the agent connects with, and how to rotate them |
| [features/brain-agent/scada-integration.md](features/brain-agent/scada-integration.md) | The July 2026 engineering story of giving the agent SCADA knowledge (raw access since removed — see banner) |

### Performance pipeline
| Doc | What |
|---|---|
| [pipeline/README.md](pipeline/README.md) | Module index, architecture, orchestration, output tables |
| [pipeline/module-1-data-loading.md](pipeline/module-1-data-loading.md) | Module 1 — data loading & cleaning |
| [pipeline/module-1b-structural-constraint-detection.md](pipeline/module-1b-structural-constraint-detection.md) | Module 1b — structural constraint detection (confirmed-only gating) |
| [pipeline/module-2-power-curve.md](pipeline/module-2-power-curve.md) | Module 2 — power curves |
| [pipeline/module-3-anomaly-detection.md](pipeline/module-3-anomaly-detection.md) | Module 3 — performance anomalies & loss |
| [pipeline/module-4-wind-normalisation.md](pipeline/module-4-wind-normalisation.md) | Module 4 — wind normalisation |
| [pipeline/module-5-degradation.md](pipeline/module-5-degradation.md) | Module 5 — degradation |
| [pipeline/module-6-commercial-reporting.md](pipeline/module-6-commercial-reporting.md) | Module 6 — commercial reporting |
| [pipeline/opportunity-detection-data-backlog.md](pipeline/opportunity-detection-data-backlog.md) | Opportunity-detection items deferred on data availability / accuracy |

### Beside the code
| Doc | What |
|---|---|
| [../scripts/jobs/README.md](../scripts/jobs/README.md) | Pointer to `operations/scheduled-jobs.md` |
| [../scripts/seeds/raw_generation_data/README.md](../scripts/seeds/raw_generation_data/README.md) | Raw generation import overview (all sources) |
| [../scripts/seeds/raw_generation_data/elexon/README.md](../scripts/seeds/raw_generation_data/elexon/README.md), [entsoe](../scripts/seeds/raw_generation_data/entsoe/README.md), [eia](../scripts/seeds/raw_generation_data/eia/README.md) (+ [EIA_DATA_ANALYSIS.md](../scripts/seeds/raw_generation_data/eia/EIA_DATA_ANALYSIS.md)), [taipower](../scripts/seeds/raw_generation_data/taipower/README.md), [nve](../scripts/seeds/raw_generation_data/nve/README.md), [energistyrelsen](../scripts/seeds/raw_generation_data/energistyrelsen/README.md) | Per-source import scripts and quirks |
| [../scripts/seeds/raw_generation_data/import_optimization.md](../scripts/seeds/raw_generation_data/import_optimization.md) | How the bulk imports were made fast (narrative) |
| [../scripts/seeds/aggregate_generation_data/README.md](../scripts/seeds/aggregate_generation_data/README.md) | Raw → hourly/monthly aggregation pipeline |
| [../scripts/seeds/weather_data/README.md](../scripts/seeds/weather_data/README.md), [WEATHER_DATA_COMPLETE_GUIDE.md](../scripts/seeds/weather_data/WEATHER_DATA_COMPLETE_GUIDE.md) | ERA5 weather import scripts and tables (the daily job is in `operations/scheduled-jobs.md`) |
| [../scripts/seeds/turbine_units/README.md](../scripts/seeds/turbine_units/README.md), [../scripts/seeds/windfarm_and_generation_unit/README.md](../scripts/seeds/windfarm_and_generation_unit/README.md) | Master-data seeding from spreadsheets |
| [../scripts/fixes/INACTIVE_UNITS_REMEDIATION_LOG.md](../scripts/fixes/INACTIVE_UNITS_REMEDIATION_LOG.md) | Inactive generation units remediation log (working file beside its scripts) |
| [../tests/reference/VERSION.md](../tests/reference/VERSION.md), [p-1-validation-notes.md](../tests/reference/p-1-validation-notes.md) | The vendored May-2026 reference pipeline and its validation notes |

## HISTORICAL (dated banners; kept for the record)

| Doc | Frozen at | Replacement / status |
|---|---|---|
| [history/lessons.md](history/lessons.md) | rolling | Durable lessons from retired session logs |
| [history/HANDOFF-2026-05-25.md](history/HANDOFF-2026-05-25.md) | 2026-05-25 | Pipeline correctness handoff → `pipeline/README.md` |
| [history/spec-vs-implementation.md](history/spec-vs-implementation.md) | 2026-05-25 | Spec comparison; everything shipped |
| [history/rerun-on-confirm-design.md](history/rerun-on-confirm-design.md) | 2026-05-29 | Design note, not implemented |
| [history/ELEXON_BST_FIX_LOG.md](history/ELEXON_BST_FIX_LOG.md) | 2026-02-06 | Elexon BST / data-gap fixes applied |
| [history/ENERGISTYRELSEN_DOUBLE_COUNTING_FIX.md](history/ENERGISTYRELSEN_DOUBLE_COUNTING_FIX.md) | 2025-10-30 | Energistyrelsen double-counting resolution |
| [history/EPR-48-HANDOFF.md](history/EPR-48-HANDOFF.md) | 2026-06-23 | Portfolio section handoff (shipped since) |
| [history/dead-code-audit-2026-07.md](history/dead-code-audit-2026-07.md) | 2026-07-25 | Endpoint / UI dead-code audit |
| [history/session_summary_2026_04_17_18.md](history/session_summary_2026_04_17_18.md) | 2026-04-18 | Spec items 1–6 session summary |
| [history/spec_items_1_to_6_plan.md](history/spec_items_1_to_6_plan.md) | 2026-04-17 | Spec items 1–6 implementation plan |
| [history/release_2026-04-16.html](history/release_2026-04-16.html), [history/remaining_work_plan.html](history/remaining_work_plan.html) | 2026-04-16 | Release note and remaining-work plan (HTML) |
| [history/SYSTEM_DOCUMENTATION-2025-11.md](history/SYSTEM_DOCUMENTATION-2025-11.md) | 2025-11 | Pre-AWS whole-platform documentation |
| [archive/scada-opportunities/scada-epr138-postgres-verification.md](archive/scada-opportunities/scada-epr138-postgres-verification.md), [scada-private-offtake-export.md](archive/scada-opportunities/scada-private-offtake-export.md) | 2026-09-15 | EPR-138 evidence, superseded by the pipeline repo's canonical SCADA guide (linked from the files) |

## Follow-ups (recorded, not done)

- **CI runs no tests, flake8 or mypy** — `.github/workflows/` only deploys. `poetry run pytest`
  is a local responsibility until a test workflow is added.
- `preview_tests/` sits outside `testpaths = ["tests"]` and is never collected by a plain
  `pytest`; run `.venv/bin/python -m pytest preview_tests --noconftest --no-cov -q` by hand.
- Many `tests/test_*_api.py` modules are integration tests that expect a live server on
  `127.0.0.1:8001` and fail with connection errors otherwise; they should be marked
  `integration` so the default `-m 'not integration'` deselects them.
- `pyproject.toml` / `poetry.lock` and `requirements.txt` are maintained by hand in parallel;
  nothing checks they agree.
- The alerts feature has rules/notifications CRUD but no evaluation engine
  ([features/alerts-and-anomalies.md](features/alerts-and-anomalies.md)).
