# Dead-Code Audit — Backend Endpoints + Admin UI + Client Portal

**Date:** 2026-07-25
**Scope:** energyexe-core-backend (~325 registered routes), energyexe-admin-ui, energyexe-client-ui
**Status:** REPORT ONLY — nothing in this document has been deleted. One live bug was fixed separately (see [Fix applied](#fix-applied)).

## Method

- Enumerated every registered route from `app/api/v1/router.py` + `app/main.py` (all 47 endpoint files are registered — there are **no orphaned router files**).
- Enumerated every API path consumed by both frontends (all `src/lib/*-api.ts` clients + inline `fetch` calls, SSE streams, dynamic URLs).
- Checked non-frontend consumers: GitHub Actions (`scheduled-imports.yml`), `app/cron/`, `scripts/`, brain agent, infra/ALB health checks, and the energyexe-scada-pipeline repo. Findings: the **only** HTTP consumers outside the two UIs are the 5 GitHub-cron curls to `POST /api/v1/import-jobs/trigger/{job_name}` and the ALB health check on root `GET /health`. The cron scheduler, brain agent, and all scripts call services/DB directly, not HTTP.
- For each frontend, identified exported API functions/hooks with zero usages, routes with no inbound navigation, and components with zero imports.

## ⚠️ Caveats before deleting anything

1. **Mid-build features:** several unused components are scaffolding for in-progress work. Nothing here should be bulk-deleted without a per-item look.
2. **EPR-77 hidden sections are NOT dead code** — the client-portal hide (June 2026, PR #193) was deliberately reversible. Everything serving those sections is listed in the [Keep appendix](#appendix--deliberately-hidden-epr-77--keep), including backend endpoints referenced only by currently-dead hooks.
3. "Referenced only by dead hooks" means the endpoint would 404-safe-delete *today*, but the frontend function exists and could be wired up tomorrow. These are weaker candidates than "zero references anywhere."
4. Ops endpoints (manual triggers, Swagger auth) have no code callers by design — kept, listed in [Keep](#b-keep--endpoints-with-no-code-callers-but-a-real-purpose).

---

## Executive summary

| Area | Finding | Count |
|---|---|---|
| Backend | Endpoints with zero callers anywhere | ~60 |
| Backend | Endpoints kept despite no code callers (ops/infra/Swagger/hidden features) | ~25 |
| Both UIs | Frontend code calling **nonexistent** backend endpoints | 12 paths |
| Admin UI | Dead exported API functions | ~66 |
| Admin UI | Orphaned routes / zero-import components | 1 route + ~8 files |
| Client UI | Dead exported hooks/functions | ~55 |
| Client UI | Unreachable routes (EPR-8/9 leftovers) / zero-import components | 7 routes + ~40 files |
| Fixed | Admin export button called nonexistent endpoint | fixed 2026-07-25 |

---

# Part 1 — Backend

## A. Endpoints with ZERO callers found (removal candidates)

No caller in admin-ui, client-ui, GitHub workflows, cron, scripts, infra, docs-executable, or the scada-pipeline repo. Paths relative to `/api/v1`. *(dead-hook)* = a frontend function for it exists but is itself unused.

### Whole-file / near-whole-file candidates

- **exchange_rates.py — the entire router**: `GET /exchange-rates`. The ECB rates import runs via `POST /import-jobs/trigger/ecb-rates-daily`; nothing reads rates over HTTP.
- **raw_data_fetch.py — 6 of 10 routes**: `POST /raw-data/{entsoe,elexon,eia,taipower,nve,energistyrelsen}/fetch`. Superseded by the used unified `POST /raw-data/fetch` and by `POST /external-sources/*/fetch` (both used by admin). The 3 `POST /raw-data/*/upload` SSE routes **are used** — keep those.

### price_data.py (13 of 26 routes unused)

- `POST /prices/fetch`, `GET /prices/raw`, `POST /prices/process`, `GET /prices/processed`
- `POST /prices/elexon/fetch`, `POST /prices/elexon/process`, `POST /prices/elexon/sync` — ⚠️ before deleting, confirm the `elexon-prices-daily` job invokes the service in-process (job registry) rather than these HTTP routes
- `POST /prices/analytics/capture-rate`, `POST /prices/analytics/capture-rate/compare`, `POST /prices/analytics/revenue`, `POST /prices/analytics/price-profile` (the GET-by-id/bidzone variants ARE used)
- `GET /prices/availability` *(dead-hook)*, `POST /prices/fetch-day` *(dead-hook)*

### performance_pipeline.py (6 routes unused, 3 more dead-hook-only)

- `GET /performance-pipeline/power-curves/{id}`, `GET .../normalisation/{id}`, `GET .../summary/{id}`, `POST .../ppa-scenarios/{id}`, `POST .../generation-concentration/{id}/compute`, `GET .../peer-aggregates/{group_type}/{group_id}/{metric_key}`
- Dead-hook-only in client: `GET .../generation-concentration/{id}`, `GET .../degradation/{id}`, `GET .../odi/{id}` *(hooks `useGenerationConcentration`/`useDegradation`/`useODI` unused)*
- `POST /performance-pipeline/run` is KEPT (ops trigger — see B)

### admin.py

- `POST /admin/users/{id}/deactivate`, `POST /admin/users/{id}/reactivate`
- `GET /admin/users/{id}/features`, `PUT /admin/users/{id}/features`

### audit_logs.py

- `GET /audit-logs/resource/{type}/{id}` *(dead-hook)*, `GET /audit-logs/user/{id}/history` *(dead-hook)*, `GET /audit-logs/my/history` *(dead-hook)*

### weather_data.py

- `GET /weather-data/fetch-jobs/{job_id}` (zero refs)
- `GET /weather-data/missing-dates` *(dead-hook)*, `GET .../windfarms/{id}/timeseries` *(dead-hook)*, `GET .../windfarms/{id}/temperature-impact` *(dead hooks in BOTH UIs)*, `GET .../windfarms/{id}/weather-summary` *(dead-hook)*

### windfarm_reports.py

- `POST /windfarms/{id}/capacity-factor-distribution` (zero refs)
- `GET /windfarms/{id}/peer-groups` (zero refs)
- `GET /api/v1/health` — duplicate of root `/health`; ALB targets root only

### generation.py

- `POST /generation/raw/store` (zero refs)
- `POST /generation/override` *(dead-hook)*, `GET /generation/hourly` *(dead hooks in BOTH UIs)*

### alerts.py

- `PATCH /alerts/triggers/{id}/status` — client uses the dedicated `/acknowledge` and `/resolve` routes instead. (Rest of alerts = hidden feature, kept.)

### brain_agent.py

- `GET /brain-agent/sessions` (list — zero refs; docs mention tests using DELETE, not GET)
- `GET /brain-agent/sessions/{id}/files/{filename}` — the backend emits `/brain-agent/files/{user_id}/{thread_id}/{filename}` URLs instead (`brain_agent_service.py:966`); that sibling route IS used dynamically

### users.py

- `PUT /users/me` (zero refs — even the hidden client Settings page doesn't call it)

### windfarms.py

- `GET /windfarms/{id}/with-generation-units` (zero refs; both UIs use `/windfarms/{id}/generation-units`)
- `GET /windfarms/code/{code}` *(dead-hook)*
- Oddity: `GET /windfarms` is registered twice (`""` and `"/"`) to the same function — harmless with `redirect_slashes=False`, but one registration could go

### Reference-data `search` + `code/{code}` routes (~22, all dead-hook-only)

Admin generated `useSearch*` / `use*ByCode` hooks for every reference resource; none are used anywhere. Affected routes:

- `GET /{res}/search` and `GET /{res}/code/{code}` for: **bidzones, cables, control-areas, countries, market-balance-areas, owners, projects, regions, states, substations, turbine-units**
- Also: `GET /states/country/{country_id}`, `GET /turbine-models/search`, `GET /turbine-models/model/{model}`, `GET /turbine-models/{id}` (bare get — detail pages use `with-turbine-units`)
- substations extras: `GET /substations/{id}/owners`, `PUT /substations/{id}/owners/{owner_id}`, `DELETE /substations/{id}/owners/{owner_id}` (admin sets owners via the bulk `POST /substations/{id}/owners` and `with-owners` routes)

### Singles (dead-hook-only unless noted)

- `GET /agent-question-templates/{id}`
- `GET /financial-data/{id}`
- `GET /generation-units/count`
- `GET /import-jobs/{id}`
- `GET /methodology-sections/{id}` (zero refs — admin edits via list + PUT)
- `GET /platform-updates/{id}` (zero refs)
- `GET /windfarms/{id}/p50-targets/active` (zero refs)
- `GET /weather-imports/{id}`
- `GET /ppas`, `GET /ppas/{id}`, `POST /ppas/import` (admin manages PPAs via `by-windfarm` within windfarm detail)

## B. KEEP — endpoints with no code callers but a real purpose

| Endpoint | Why keep |
|---|---|
| `POST /import-jobs/trigger/{job_name}` | 5 GitHub-cron callers (`scheduled-imports.yml`): entsoe-daily, taipower-hourly, elexon-daily, eia-monthly, ecb-rates-daily. Intentionally unauthenticated. |
| Root `GET /health` | ALB target-group health checks, prod + staging (`infra/alb.tf`, `infra/staging/alb.tf`) |
| `POST /performance-pipeline/run`, `POST /opportunities/detect`, `POST /data-anomalies/detect` | Manual ops triggers (detect routes also used by admin UI) |
| `POST /auth/token` | OAuth2 form flow — powers the Swagger UI "Authorize" button |
| `GET /brain-agent/files/{user_id}/{thread_id}/{filename}` | Consumed via dynamic URLs the agent returns in messages |
| `GET /turbine-units/stats` | Used by client `/turbines` page — which is itself an unreachable route (see Part 4); they live or die together |
| Bare `GET /windfarms/{id}` | Only dead references today, but core REST — recommend keep |
| All hidden-section endpoints (EPR-77) | See appendix — includes all of alerts.py, report_commentary.py, client anomalies subset, `/windfarms/{id}/peer-comparison`, `/windfarms/{id}/rankings` |

---

# Part 2 — Frontend↔backend mismatches (bugs / stale code)

1. **FIXED — admin export button was broken.** `useExportData` (`src/lib/generation-api.ts`) called `GET /generation/export` — an endpoint that does not exist — with a hardcoded fallback URL to the retired Coolify domain. Live callers: the Raw Data Browser and Import/Export pages. See [Fix applied](#fix-applied).
2. **Admin sidebar `/settings` nav entry has NO route file** — visible broken link in the sidebar (`admin-layout.tsx` `settingsItems`). Needs either a settings route or removal of the entry. *(Not fixed — flagged for a separate decision.)*
3. **`POST /auth/refresh` doesn't exist in the backend** but is defined in both UIs' `auth-api.ts` (`refreshToken`). Verified never called — dead code, not a runtime bug. If token refresh is ever wanted, the endpoint must be built first.
4. **Admin `comparison-api.ts` is ~90% stale.** Only `getWindfarmStatistics` is used (via `hooks/use-windfarm-statistics.ts`). The other functions call endpoints that don't exist: `/comparison/performance`, `/capacity-factors`, `/rankings`, `/geographic`, `/patterns`, `/sources`, `/countries`. The live comparison page bypasses this file (uses `apiClient` directly for `/windfarms/names` + `/comparison/compare`).
5. **Client `dashboard-api.ts` dead hooks call nonexistent endpoints**: `/generation/rankings`, `/generation/data-freshness`, `/generation/aggregated` (hooks `useTopPerformers`, `useLowPerformers`, `useDataFreshness`, `useGenerationAggregated` — all unused).
6. **Stale doc/comment references**: `scripts/fixes/api_check_contamination.py` docstring cites `/api/v1/comparison/data` and `/api/v1/generation-data/export` (neither exists); `docs/pipeline/HANDOFF.md:52` and `docs/pipeline/spec-vs-implementation.md:287` cite `POST /structural-constraints/{id}/review` (actual route is `PATCH /structural-constraints/{flag_id}`); commented-out router imports in `router.py:187-190` refer to endpoint modules that no longer exist.

---

# Part 3 — Admin UI dead code

## Routes

- **`src/routes/_protected/windfarms/analytics.tsx`** (`/windfarms/analytics`) — orphaned; nothing links or navigates to it. Its component `WindfarmAnalytics` is rendered inline on the dashboard instead.
- By-design, not dead: `/reset-password` (email link entry); `data-sources/{eia,elexon,…}.tsx` (reached via dynamic link from `data-sources/index.tsx`).

## Zero-import components

- `src/components/windfarms/report/WindfarmReportTab.tsx` — old "full report" entry point (detail page uses `WindfarmReportPrintView`; the route uses `SimplifiedReportView`)
  - Transitively dead with it: `report/sections/ExecutiveSummarySection.tsx`, `report/sections/PeerComparisonSection.tsx`, `report/sections/RankingsSection.tsx`
- `src/components/windfarms/simplified-report/charts/PowerCurveHistogramChart.tsx`
- Dead re-export barrels (components are imported directly instead): `windfarms/prices/index.ts`, `simplified-report/charts/index.ts`, `simplified-report/sections/index.ts`, `simplified-report/static/index.ts`
- Everything else under `report/charts/` + `report/{CommentaryDisplay,EnhancedCoverPage,TableOfContents}.tsx` is **still used** via `WindfarmReportPrintView`.

## Dead exported API functions (~66, by file)

- **agent-question-templates-api**: `useAgentQuestionTemplate`
- **audit-logs-api**: `useResourceAuditHistory`, `useUserAuditHistory`, `useMyAuditHistory`
- **auth-api**: `authApi` object export (hooks are used)
- **bidzones / cables / control-areas / countries / market-balance-areas / owners / projects / regions / substations / turbine-units-api**: each has dead `useSearch<Res>` + `use<Res>ByCode`
- **states-api**: `useSearchStates`, `useStatesByCountry`, `useStateByCode`
- **turbine-models-api**: `useSearchTurbineModels`, `useTurbineModel`, `useTurbineModelByModel`
- **turbine-units-api** (extra): `useTurbineUnit`
- **substations-api** (extra): `useCreateSubstation` (superseded by `useCreateSubstationWithOwners`)
- **comparison-api**: everything except `getWindfarmStatistics` (see Part 2 #4)
- **financial-data-api**: `useFinancialData`
- **generation-api**: `useGenerationHourlyData`, `useManualOverride` (`useExportData` removed by the fix)
- **generation-units-api**: `generationUnitsApi` object export, `useGenerationUnitsCount`
- **import-jobs-api**: `useImportJob`
- **opportunities-api**: `SCHEMA_DOMAIN_ORDER`, `SLOT_META` consts
- **ppas-api**: `usePPAs`, `usePPA`, `useImportPPAs`
- **prices-api**: `getPriceStatistics`, `getPriceCoverage`, `getCaptureRate`, `getRevenueMetrics`, `getGenerationPriceCorrelation`, `getAvailableBidzones`, `getBidzoneCaptureRates`, `compareCaptureRates`, `getPriceAvailability`, `useFetchPricesDay` (the `use*` wrappers of the first six ARE used)
- **users-api**: `usersApi` object export, `useUsers` (superseded by `useAllUsers`)
- **weather-data-api**: `useWeatherMissingDates`, `useWeatherTimeseries`, `useTemperatureImpact`, `useWeatherSummary`
- **weather-imports-api**: `useWeatherImportJob`
- **windfarms-api**: `useWindfarm`, `useWindfarmByCode`, `getWindfarmsByCountry`, `getWindfarmsByBidzone` (hook wrappers of the last two ARE used)

## Misc

- No commented-out/hidden nav in the admin sidebar (unlike client). Only dead state stubs at `admin-layout.tsx:301-304`.
- `comparison/generation-curtailment-chart.test.tsx` tests a chart that IS used — fine.
- Duplicate-name files that are all live (awareness only): `windfarm-form-modal` vs `improved-windfarm-form-modal`; `weather/` vs `report/charts/` chart trios.

---

# Part 4 — Client UI dead code

## Unreachable routes (EPR-8/9 consolidation leftovers — "delisted, not deleted")

No inbound `Link`/`navigate`/sidebar entry; reachable only by typed URL:

- `/help` (`help-page.tsx` links OUT to other pages, but nothing links IN)
- `/turbines` (sole consumer of backend `GET /turbine-units/stats`)
- `/wind-farms/$windfarmId/equipment` (not in `windfarm-tabs.tsx`)
- `/analytics/revenue`, `/analytics/weather`, `/analytics/generation` (consolidated into Performance per EPR-8/9)
- `/comparison` top-level (live one is `/analytics/performance/comparison`)

By-design, not dead: `/wind-farms/$windfarmId/p50` (redirect stub for old bookmarks), `/reset-password`, `/invitation/$token` (email entry), `/demo`, `/demo/windfarm` (standalone showcase layer).

## Zero-import components

**Old-dashboard cluster** (the live `/dashboard` imports section components directly; this whole composition is dead):
`components/dashboard/{dashboard-page,dashboard-header,quick-access-panel,kpi-cards,generation-chart,map-preview,recent-alerts,recent-anomalies,quick-stats,index.ts}` — note `dashboard/generation-chart.tsx` name-collides with the live `generation/generation-chart.tsx`.

**Entire `components/showcase/` tree** (~20 files: `ShowcaseSection`, `data/`, `feedback/`, `forms/`, `navigation/` + barrels) — the demo route uses `WindfarmDetailsPage`/`DemoLayout`, not showcase.

**Portfolio extras** (exported by `portfolio/index.ts` but never imported; possibly staged for future work): `favorite-button.tsx`, `favorites-list.tsx`, `add-to-portfolio-menu.tsx`, `portfolio-hub-insights.tsx`.

**Singles** (possibly mid-build scaffolding): `windfarms/quick-actions.tsx`, `performance/wind-normalisation-chart.tsx`, `generation/generation-kpis.tsx`.

**ui/ primitives**: `ui/breadcrumb.tsx`, `ui/sidebar.tsx`.

## Dead exported hooks/functions (~55, by file)

- **alerts-api**: `useAlertRule`, `useUpdateAlertRule`, `useUnreadCount`, `useAlertsOverview` (+ their internal request fns)
- **anomalies-api**: `useAnomaly`, `useUpdateAnomaly`, `useDeleteAnomaly`
- **dashboard-api**: `useGenerationSummary`, `useTopPerformers`, `useLowPerformers`, `useDataFreshness`
- **generation-api**: `useGenerationHourly`, `useGenerationAggregated`, `useGenerationStats`, `useGenerationAvailability`, `useQualityStats`
- **market-api**: `usePriceCoverage`, `usePriceProfile`, `useGenerationPriceCorrelation`, `getCaptureRateLabel`
- **performance-anomalies-api**: `usePerformanceAnomalies` (+ `listPerformanceAnomalies`)
- **performance-pipeline-api**: `useGenerationConcentration`, `useDegradation`, `useODI`
- **portfolio-api**: `usePortfolioItems`, `useCheckMultipleFavorites` (+ request fns)
- **reports-api**: `usePeerComparison`, `useRankings`, `useCommentaries`, `useDeleteCommentary`, `useUsageStats`, `getCommentary` (fully orphaned) (+ request fns)
- **turbine-units-api**: `useTurbineUnit`
- **weather-api**: `useDiurnalPattern`, `useWindSpeedDurationCurve`, `useWeatherGenerationCorrelation`, `usePowerCurve`, `useCapacityFactorByWind`, `useEnergyRose`, `useTemperatureImpact`, `useWeatherHeatmap`, `getDirectionName`
- **windfarms-api**: `useWindfarmsByCountry`, `useWindfarmsInBidzone` (+ request fns)

Note: many of these belong to hidden feature areas, but they are dead **independently of the hide** — even the retained hidden page components use a different subset of hooks.

---

# Appendix — Deliberately hidden (EPR-77) — KEEP

Implemented via `hidden: true` flags + commented imports (PR #193, issue #179 for the anomalies tab). Routes and pages retained and reachable by direct URL. **None of this is dead code.**

- Hidden sidebar entries (`app-layout.tsx`): Opportunities `/opportunities`, Alerts `/alerts`, Anomalies `/anomalies`, Reports `/reports`, Export `/export`, Settings `/settings`
- Hidden windfarm tabs (`windfarm-tabs.tsx`): Benchmarking, Report; Anomalies tab commented out (issue #179)
- Hidden dashboard section: `NewFlagsSection` import commented in `_protected.dashboard.tsx` (EPR-77)
- Re-enable: flip `hidden` to `false` on the item / uncomment the import + JSX
- Backend endpoints kept because of this: **all of alerts.py**, **all of report_commentary.py**, the client anomalies subset, `POST /windfarms/{id}/peer-comparison`, `GET /windfarms/{id}/rankings`

---

# Fix applied

**2026-07-25 — admin Generation Data export button** (the only change made alongside this report):

- `useExportData` in `energyexe-admin-ui/src/lib/generation-api.ts` called nonexistent `GET /generation/export` with a hardcoded fallback to the retired Coolify domain → every export from the Raw Data Browser and Import/Export pages failed. The function has been removed.
- Direct re-wiring to the real endpoint (`GET /export/generation/csv`) was tested and rejected: that endpoint caps exports at 500 windfarms, and the quick-buttons' "whole source / all sources" semantics can never satisfy it (all=1,627 windfarms; EIA alone=1,163). Verified live against the local backend.
- Instead, both pages' export buttons now open the dedicated Export tool (`/generation-data/export`), which already drives the real endpoint with full windfarm/geography/source/date/granularity filtering. The Import/Export page's export card (with its never-functional JSON/Excel options and decorative date-range selector) was reduced to a description + "Open Export Tool" button; the Raw Data Browser's dead "Export JSON" button was removed.

# Suggested next steps (when you want to act on this)

1. **Quick wins, lowest risk:** delete the admin `useSearch*`/`use*ByCode` hook family + their ~22 backend routes; delete `exchange_rates.py`; delete the 6 per-source `raw-data/*/fetch` routes; remove duplicate `GET /api/v1/health`.
2. **Medium:** prune `price_data.py` (13 routes) after confirming the elexon-prices job path; prune the unused `performance_pipeline.py` GETs; delete the admin comparison-api stale functions and client dashboard-api dead hooks (they call nonexistent endpoints anyway).
3. **Decide per-item (mid-build risk):** client old-dashboard cluster, showcase tree, portfolio extras, unreachable EPR-8/9 routes.
4. **Separate fixes:** admin `/settings` sidebar link (add route or remove entry); decide whether `/auth/refresh` should exist (build endpoint) or the `refreshToken` fns should go.
