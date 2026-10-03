> **Historical handoff (2026-06-23).** The status line below ("all UNMERGED") is frozen at writing time; EPR-48 was subsequently merged and promoted to production (see the workspace `UPDATES.md`). Kept as implementation evidence only. Moved here from the workspace root on 2026-10-04.

# EPR-48 "Portfolio section" — Implementation Handoff

**Date:** 2026-06-23 · **Status:** all 14 child tickets implemented across 3 phased PRs, **all UNMERGED**. Builds pass; runtime verification of Phase C is deferred until the backend (Phase A) deploys.

Jira epic: https://energyexe.atlassian.net/browse/EPR-48 (14 child tickets). Plan file: `/Users/mdfaisal/.claude/plans/plan-to-fix-all-snuggly-flask.md`.

---

## PRs (merge in this order)

| # | Repo | Branch | Base | PR | Notes |
|---|------|--------|------|----|-------|
| **A** | energyexe-core-backend | `epr-48-phase-a-portfolio-backend` | `master` | [#131](https://github.com/Energy-Exe/energyexe-core-backend/pull/131) | **Merging auto-deploys to AWS Fargate + Railway.** Merge & verify FIRST. |
| **B** | energyexe-client-ui | `epr-48-phase-b-portfolio-cosmetic` | `main` | [#190](https://github.com/faisal-energyexe/energyexe-client-ui/pull/190) | Cosmetic only; degrades gracefully without A. |
| **C** | energyexe-client-ui | `epr-48-phase-c-portfolio-reworks` | **`epr-48-phase-b-portfolio-cosmetic`** (stacked) | [#191](https://github.com/faisal-energyexe/energyexe-client-ui/pull/191) | **Retarget base → `main` after #190 merges.** Consumes Phase A fields. |

`gh` write access: use the **`Mohammad-Faisal`** account (it's the active one; pushes to both `Energy-Exe/*` and `faisal-energyexe/*` worked).

### Remaining work for the human
1. Review/merge **#131** → backend deploys. Verify on AWS (see Verification below).
2. Merge **#190**.
3. **Retarget #191 base from the Phase-B branch to `main`**, then merge.
4. **Runtime-verify Phase C** once #131 is live (C4/C5/C6 read new backend fields — not yet verified live).

---

## Two real backend bugs (root causes) — both fixed in Phase A / #131

### EPR-64 — Performance Trend CF "÷1000"
`app/api/v1/endpoints/generation.py` `get_portfolio_performance` (~line 1042) trend query did `SUM(wf.nameplate_capacity_mw)` over the `generation_data ⋈ windfarms` **hourly** join grouped by month → each farm's nameplate counted once **per hourly row** (~720×/month for hourly farms; ~N-rows× for monthly EIA farms), inflating the CF denominator and shrinking CF by that factor. **Fix:** CTE pre-aggregating generation per `(month, windfarm)` then joining nameplate once per farm. **Prod-verified read-only:** 1485 MW farm 14.0% → **42.6%**; on hourly farms the old value was ~0.05%.

### EPR-55 — "previous year" timezone off-by-one
Calendar boundary built as `new Date(y,0,1)` (LOCAL midnight) then `.toISOString()` shifts back by the local offset; east of UTC (dev = GMT+6) the start becomes `2024-12-31T18:00Z`, leaking a spurious "Dec 2024" monthly bucket and slightly inflating totals (Guleslettene 735.6 vs 735.5 MWh). **Fix:** build boundaries with `Date.UTC(...)` (front-end) / the wind-farm view sends date-only strings via `getDefaultYearRange`.

---

## Phase A — backend (PR #131, 3 files)

All changes additive or correctness; `py_compile` clean. **Did NOT run `black`** — repo isn't black-clean, reformat would add noise; edits match surrounding style.

- `app/api/v1/endpoints/generation.py`
  - **A1/EPR-64:** rewrote `trend_query` with `monthly_farm` CTE (pre-agg per month+windfarm, join nameplate once); simplified the Python CF calc to `total_mwh / (total_capacity * period_hours) * 100`.
- `app/api/v1/endpoints/price_data.py`
  - **A2/EPR-70:** `get_portfolio_capture_rates` `statistics.avg_capture_rate` now **generation-weighted** (`Σrevenue/Σgeneration ÷ market_avg`), not a flat mean. (The `/revenue` endpoint's `avg_capture_rate` at ~line 894 was already weighted — that's what the Overview card reads.)
  - **A3/EPR-62:** added `bidzone_name` to capture-rates query (scope CTE `b.name`, SELECT, GROUP BY, response dict).
  - **A4/EPR-62:** `get_portfolio_revenue` now returns `by_farm_period` (per-(farm,month) revenue, top-20 farms) for the stacked chart.
- `app/api/v1/endpoints/weather_data.py` `get_portfolio_weather_summary`
  - **A5/EPR-66:** added `wf.name` to `bucket_query` + `by_farm_month` array (per-farm monthly avg wind & temp).
  - **A6/EPR-66:** correlation query `LEFT JOIN bidzones bz` + `bz.name AS bidzone_name`.
  - **A7/EPR-66:** correlation query adds `STDDEV(wind)`, `PERCENTILE_CONT(0.5) WITHIN GROUP (...)` P50, prevailing direction = `(mode() WITHIN GROUP (ORDER BY round(wind_direction_deg/22.5)::int % 16)) * 22.5`, `AVG(temperature_2m_c)`. (Column is `wind_direction_deg`, NOT `wind_direction_100m`.)

DB validation used asyncpg directly against prod RDS (`DATABASE_URL` in backend `.env`, strip `+asyncpg`); **asyncpg needs `datetime` objects for timestamptz params**, not strings.

---

## Phase B — client-ui cosmetic (PR #190, 11 files)

| Ticket | File | Change |
|---|---|---|
| EPR-67 | `portfolio-tabs.tsx` + 3 route files | removed Insights/Reports/Settings tabs + deleted routes |
| EPR-71 | `portfolio-tabs.tsx` | tab icons → Info / BarChart3 / Cloud (match wind-farm) |
| EPR-62 | `portfolio-tabs.tsx` | Revenue tab → "Market Exposure" (path stays `/revenue`) |
| EPR-66 | `portfolio-tabs.tsx` | Weather moved after Generation |
| EPR-50 | `portfolio-manager.tsx` | removed Favorites tab (render list directly) |
| EPR-52 | `portfolio-hub-overview.tsx` | Revenue KPI → "Implied revenue"; removed Open anomalies / opportunities tiles (grid 6→4) |
| EPR-70 | `portfolio-hub-overview.tsx` | cards → "Weighted avg. capacity factor / capture rate" |
| EPR-56 | `portfolio-hub-overview.tsx` | removed CF-distribution + seasonal-wind charts |
| EPR-55 | `portfolio-hub-overview.tsx` | "Revenue Trend" → "Implied Revenue Trend"; x-axis monthly cadence (no day stamps for "previous year") |
| EPR-60 | `portfolio-generation-page.tsx` | removed "Needs Attention" table; "Total Capacity" → "Total Active Capacity" |
| EPR-62 | `portfolio-revenue-page.tsx` | rename trend chart + Revenue column to "Implied"; bidzone name; removed % delta pills (+ dropped prior-window fetch) |
| EPR-66 | `portfolio-weather-page.tsx` | removed delta pills + the two Best/Lowest correlation tables |
| — | `portfolio-analytics-api.ts` | added `bidzone_name` + `by_farm_period` types |

---

## Phase C — client-ui reworks (PR #191, stacked; 20 files, 4 new)

**New shared modules:**
- `src/lib/portfolio-time-range.ts` — single `TimePeriod` / `TIME_PERIODS` / `getDateRange(period, anchor?)`. Presets match wind farms: `7d, 30d, 90d, ytd, prevcy, 1y, all` (`all` → `Date.UTC(2010,0,1)`). UTC-built `prevcy`/`ytd` boundaries.
- `src/lib/use-table-sort.ts` — `useTableSort(rows, initialKey, initialDir)` → `{sorted, sortKey, sortDir, toggleSort}`.
- `src/components/analytics/sortable-head.tsx` — `<SortableHead label sortKey activeKey dir onSort align tooltip?>`.
- `src/components/analytics/kpi-card.tsx` — `KPICard` + `DeltaPill` **extracted from the deleted** `portfolio-performance-page.tsx` (peer pages imported them).

| Ticket | What |
|---|---|
| EPR-55 (C1) | UTC boundaries in overview `range` memo + shared `getDateRange` |
| EPR-49 (C2) | `viewMode` prop → `PortfolioList` switches card grid vs list |
| EPR-57 (C3) | overview + generation + revenue + weather + peer pages all use `portfolio-time-range.ts`; each analytics page keeps its own `getAggregation` (gen: daily/weekly/monthly; rev: day/week/month) extended for the new union |
| EPR-53 (C4) | overview top-5/bottom-5 → one sortable table (Rank/Farm/Country/Capacity/Generation/CF%/Capture Rate/Data Quality) from `usePortfolioPerformance.performance_ranking` + capture-rate join; deleted `PerformerTable` |
| EPR-62/60/64 (C5) | revenue "Top Farms" bar → stacked monthly **Farm Contribution** (from `by_farm_period`); Capture Rate Ranking sortable; **deleted Performance tab** (route + `portfolio-performance-page.tsx`, removed from `portfolio-tabs.tsx` + `analytics/index.ts`); moved Performance Trend chart into `portfolio-generation-page.tsx` (added `usePortfolioPerformance`) |
| EPR-66 (C6) | weather page migrated to shared filter; two charts → per-windfarm month-to-month wind (left) + temp (right) LineCharts from `by_farm_month` (capped to 8 farms by data coverage); All Farm Correlations table: `overflow-x-auto` + frozen first col (`sticky left-0 bg-card z-10`), sortable, correlation tooltip, "Correlation"→"Power/wind correlation", bidzone name replaces windfarm code under name, + columns Std Dev / P50 / Prevailing Dir (compass) / Mean Temp |
| — | `portfolio-analytics-api.ts` | added `by_farm_month` + `PortfolioWeatherFarmMonth` type; correlation type gained `bidzone_name, wind_std, wind_p50, prevailing_direction_deg, avg_temperature` |
| repointed | `peer-overview-tab.tsx`, `peer-group-page.tsx`, `peer-market-revenue-tab.tsx`, `peer-generation-tab.tsx` | imports → `kpi-card.tsx` / `portfolio-time-range.ts` |

---

## Decisions made (confirmed with user)
- **EPR-70:** make capture rate truly generation-weighted (done in A2), not just relabel.
- **EPR-66 correlations table:** "key subset" of the wind-farm weather stats (mean wind, std dev, P50, prevailing dir, mean temp) — NOT the full ~9 metrics.
- **Delivery:** phased PRs (A→B→C).
- **EPR-57 judgment call (flag for user):** all tabs now share the wind-farm preset set — this **dropped the old `6m` and `3y`** options in favour of `7d`/`YTD`/`All time`. If `3y` is wanted back, add one line to `TIME_PERIODS` in `portfolio-time-range.ts` + a case in `getDateRange`.

## Gotchas
- client-ui `tsconfig.json` has `noUnusedLocals`/`noUnusedParameters` and `build = "vite build && tsc"` → every removed symbol needs its import trimmed or the build fails.
- Deleting route files requires `routeTree.gen.ts` regeneration via `vite build` (the router plugin runs in build mode). `git rm` aborts the whole batch if one file has local mods — use `git rm -f`.
- The Overview tab reads its capture-rate KPI from `/revenue` (`usePortfolioRevenue`), the Revenue tab's ranking from `/capture-rates` (`usePortfolioCaptureRates`) — two different endpoints.

## Verification
- **Backend (after #131 deploys):** `GET /api/v1/generation/portfolio/performance` → `performance_trend[].capacity_factor` in 0–60 range; `/prices/analytics/portfolio/capture-rates` farms have `bidzone_name`; `/prices/analytics/portfolio/revenue` has `by_farm_period`; `/weather-data/portfolio/summary` has `by_farm_month` + correlation rows have `bidzone_name`/`wind_std`/`wind_p50`/`prevailing_direction_deg`/`avg_temperature`.
- **Frontend (`/verify` skill, `pnpm dev` port 3006):** every tab's time dropdown shows **All time**; "Previous year" trend starts at Jan (no Dec spike) and matches the wind-farm number (Guleslettene); landing card/list toggle switches layout; Overview = one sortable ranking table; no Performance tab (its trend chart on Generation reads tens-of-%); Weather = per-farm wind/temp lines + scrollable sortable correlations table.

## Static verification pass (2026-06-24, pre-merge)
Done on the open branches before any merge — does NOT replace the live runtime checks above (still blocked on #131 deploying):
- **Frontend Phase C** (`epr-48-phase-c-portfolio-reworks`): `pnpm build` (`vite build` + strict `tsc`) → **clean, exit 0**. `routeTree.gen.ts` regenerated fine after the 4 route deletions; no unused-symbol errors despite `noUnusedLocals`.
- **Backend Phase A** (`epr-48-phase-a-portfolio-backend`): all 3 files `py_compile` OK. Diff self-reviewed — CF CTE preserves the original inner-join semantics; `market_avg` (used by the new generation-weighted capture stat) is defined at `price_data.py:1051` and already drives per-farm rates; `month_names` is in scope before `by_farm_month`; `wind_direction_deg` confirmed as the real column (`models/weather_data.py:81`, Numeric(5,2)).
- **API contract parity** (backend response keys ↔ FE types): exact match on `by_farm_period` (`PortfolioFarmPeriodRevenue`), `by_farm_month` (`PortfolioWeatherFarmMonth`), correlation extras (`bidzone_name`/`wind_std`/`wind_p50`/`prevailing_direction_deg`/`avg_temperature` → `PortfolioWeatherCorrelation`), and capture-rates `bidzone_name`. No silent-blank risk from key mismatches.

## Logged
- `UPDATES.md` (workspace root) has 3 entries (Phase A/B/C) with verify URLs.
- Memory: `project_epr48_portfolio_section.md` + MEMORY.md index line.
