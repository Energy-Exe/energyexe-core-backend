"""One-off: populate methodology_sections with current platform knowledge (#177).

Upserts by section_key — updates the stale seeded sections and adds new ones
for the performance pipeline and curtailment. Run:
    poetry run python scripts/_populate_methodology.py
"""

import asyncio

from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.core.database import get_engine

SECTIONS = [
    {
        "section_key": "data-sources",
        "title": "Data sources",
        "description": "Where the platform pulls generation, weather, prices, and reference data from — and how each source is normalised.",
        "sort_order": 10,
        "content_md": """\
- **Generation** — ENTSOE (European TSO data), Elexon (GB Balancing & Settlement Code data, including B1610 metered volumes), NVE (Norway operator returns), EIA / EIA-930 (US), Taipower (Taiwan), Energistyrelsen (Denmark). Hourly metered MWh after normalisation; multi-unit windfarms are aggregated by unit. US and Danish sources report **monthly** — sub-monthly views are unavailable for those assets.
- **Curtailment** — Elexon BOAV (bid-offer acceptance volumes). Available for **UK assets only**; for other markets curtailment is genuinely unavailable, not zero.
- **Weather** — ERA5 reanalysis (Copernicus CDS). 10/100 m wind speed and direction, temperature, surface pressure. Sampled at the windfarm centroid, hourly.
- **Prices** — ENTSOE day-ahead (EUR), Elexon MID (GBP). Half-hourly settlement aggregated to hourly. Norway has no day-ahead price feed in the platform, so capture-price metrics are suppressed for Norwegian assets.
- **Financials** — annual (occasionally 6/9-month transition-period) entity accounts: revenue, EBITDA, EBIT, net income, reported generation. Linked to windfarms via financial entities; multi-windfarm entities are mapped explicitly.
- **Reference** — country / region / bidzone tables; ownership shares; PPA contracts; manufacturer power curves per turbine model; turbine-unit registry (model, hub height, coordinates, commissioning dates).""",
    },
    {
        "section_key": "data-harmonisation",
        "title": "Data harmonisation",
        "description": "How heterogeneous source data is converted to a single comparable hourly series per windfarm.",
        "sort_order": 20,
        "content_md": """\
Every source feeds into a raw table (`generation_data_raw`) with its native period, timezone, and identifier. The aggregation step converts each raw record to a clean hourly UTC row in `generation_data` with `metered_mwh` (actual settled energy) and `capacity_factor` (MWh / nameplate / hours).

Notable corrections: Elexon raw timestamps before 2020 carry a BST offset bug that the aggregation step corrects via `settlement_date + settlement_period`. French ENTSOE unit responses contain both generation and consumption rows and are split on data direction before aggregation. Multi-unit windfarms are summed by unit, then assigned to the windfarm; capacity-factor uses nameplate-based denominators rather than averaging unit-level CFs (which silently drops hours with no reading).

Hours inside a windfarm's commissioning **ramp-up window** are flagged (`is_ramp_up`) and excluded from performance averages by default — most views offer a toggle to include them.""",
    },
    {
        "section_key": "wind-resource",
        "title": "Wind resource, power curves & normalisation",
        "description": "How the platform builds an expected-power signal from weather + empirical power curves, and what wind-normalisation means in our reports.",
        "sort_order": 30,
        "content_md": """\
For each windfarm the platform builds **empirical power curves** from its own history: hourly ERA5 wind speed is binned (1 m/s bins, cut-in to cut-out) against observed per-unit output. Two reference curves are kept per windfarm:

- **P50 curve** — the *median* output observed at each wind speed: the realistic expectation.
- **P10 (capability) curve** — the 90th-percentile output at each wind speed: what the asset proves it *can* do.

Curves are built per calendar year and as an all-years "clean" curve, with curtailed, ramp-up, and constraint-affected hours excluded so the reference is not contaminated by known disruptions.

**Expected generation** for any hour = wind speed mapped through the P50 curve × operational capacity at that hour. **Wind-normalised indices** compare actual output to this expectation: an index of 100 means the asset performed at its historical norm regardless of how windy the period was; below 100 means operational underperformance that wind conditions cannot explain. This is what enables apples-to-apples comparison across years and across windfarms in different wind regimes.""",
    },
    {
        "section_key": "capacity-factor",
        "title": "Capacity factor & generation metrics",
        "description": "How CF, generation, availability, and P50 targets are defined and where they appear in the UI.",
        "sort_order": 40,
        "content_md": """\
- **Capacity factor (CF)** = `sum(metered_mwh) / (nameplate_mw × distinct hours in period)`. Always nameplate-based — never an average of hourly CFs, which would drop hours with missing readings and inflate the result. For multi-unit windfarms, generation is summed across units before dividing by the windfarm's total nameplate. Typical ranges: 25–35% onshore, 35–50% offshore.
- **Availability** = fraction of expected-operational hours where the unit produced above a low threshold (idle filter).
- **P50 target** — operator-published annual P50 generation, divided by 12 to get the monthly target. The Generation tab shows cumulative actual vs cumulative P50 and a "gap in months" KPI. Gap = actual − P50, so positive means generation is ahead of target and negative means it is behind. Where the operator figure is unavailable, an auto-computed long-run target may be used and is labelled as such.
- **Metered vs reported** — hourly metered totals can differ from annually reported financial-statement generation by 2–5%; the platform treats metered hourly data as the operational source of truth.""",
    },
    {
        "section_key": "commercial",
        "title": "Commercial & revenue metrics",
        "description": "Capture price, capture rate, generation concentration, and how PPA values feed in.",
        "sort_order": 50,
        "content_md": """\
- **Capture price** = `sum(price × generation) / sum(generation)` over the period — the volume-weighted price the windfarm actually captured.
- **Capture rate** = `capture price / time-average period price`. Above 100% means the windfarm generated more when prices were high; below ~90% indicates classic wind cannibalisation (generating into depressed prices).
- **Generation concentration** — the platform also breaks generation into market-price *deciles*: the share of energy produced in the 10% cheapest hours (D1) up to the 10% most expensive (D10). A healthy profile has a top-decile share above 10% and a bottom-decile share below 10%. Each asset's profile is compared to its bidzone peers.
- **Negative-price exposure** — the share of generating hours with a negative day-ahead price. Exposure above 2–3% is significant merchant downside.
- **PPAs** are layered on top: where a windfarm has a long-term PPA, spot-market analyses (capture rate, implied revenue) are flagged as a *theoretical exercise* — they show what the asset would earn if fully spot-exposed. Implied revenue uses day-ahead prices only.
- Currency is normalised to a user-selected display currency using period exchange rates. Norway is price-data-absent — capture metrics are suppressed for Norwegian assets.""",
    },
    {
        "section_key": "performance-pipeline",
        "title": "Performance pipeline (ODI, degradation, lost energy)",
        "description": "The daily analytics pipeline that scores operational performance against each asset's own power curve.",
        "sort_order": 55,
        "content_md": """\
A daily pipeline evaluates every windfarm against its empirical power curves and publishes:

- **ODI (Operational Disruption Index)** — the share of hours where actual output sits statistically below the P50 expectation for the observed wind speed. Reported three ways: % of hours underperforming, lost energy as % of expected MWh, and lost revenue as % of expected revenue.
- **Lost MWh / lost revenue** — for each underperforming hour, `lost = expected − actual`, valued at the hourly market price (or the contracted PPA price where one applies). Summed monthly and annually.
- **Wind-normalised performance index** — actual vs expected output with wind removed (100 = historical norm). Computed against both the P50 curve (realistic) and P10 curve (capability ceiling).
- **Degradation trend** — a long-run regression on monthly performance residuals, expressed as % per year. A slope is only treated as meaningful when statistically significant (p < 0.05); seasonal effects can bias short histories, so significance and fit quality are always reported alongside.
- **Constraint awareness** — confirmed structural export constraints are masked out of power-curve construction and detection so an asset is not penalised twice for a known grid limitation. Capacity phasing (farms that grew in stages) is handled with capacity-aware normalisation.

Peer context for all of these comes from pre-computed aggregates (see Peer comparison).""",
    },
    {
        "section_key": "curtailment",
        "title": "Curtailment",
        "description": "How curtailed energy is measured, where it comes from, and where it appears.",
        "sort_order": 57,
        "content_md": """\
Curtailment is deliberate output reduction instructed by the system operator (grid constraints, balancing actions). The platform ingests **Elexon BOAV** (bid-offer acceptance volumes) daily and stores curtailed MWh per hour alongside metered generation, so for UK assets:

`available energy = metered_mwh + curtailed_mwh`

- The Generation tab shows a curtailment chart (per period, with the share of potential output lost) for windfarms that actually experienced curtailment.
- Curtailment-aware analytics (power curves, ODI) exclude curtailed hours from "underperformance" — being turned down is not a fault.
- **Coverage is UK-only.** Other markets (Norway, continental Europe, US, Taiwan) do not publish per-unit curtailment in a comparable form; for those assets curtailment is reported as *unavailable*, never as zero.""",
    },
    {
        "section_key": "opportunities",
        "title": "Opportunities & anomalies",
        "description": "How the platform flags assets that need attention — the schema catalogue, severities, and data-quality gating.",
        "sort_order": 60,
        "content_md": """\
The opportunity-detection engine evaluates a fixed catalogue of analytical schemas for each windfarm, across four families:

**Operational**
- **Volatile Disruption Periods** — recurring low-availability months; concentrated or structural disruption.
- **Performance Seasonality** — the high-wind season underperforms the low-wind season; mechanical stress or maintenance-timing signal.
- **Misaligned Contracting Strategy** — OEM/asset-management contract doesn't incentivise uptime (only raised where disruption is already flagged).
- **Turbine Degradation** — statistically significant long-run power-curve decline.
- **Grid Curtailment** — curtailed energy as a share of available output (UK only).
- **Persistent Power-Curve Underperformance** — consecutive months with the wind-normalised index below threshold.
- **Fleet-Age / End-of-Life Risk** — turbines in their final operating years; repowering/capex risk.
- **Structural Export Constraint** — confirmed grid constraint suppressing output for a sustained window.

**Market**
- **Low Capture Rate — Contracting** — capture-rate gap vs the bidzone average; hedging/contracting exposure.
- **Low Capture Rate — Storage** — battery-shifting opportunity downstream of a low capture rate.
- **High Cannibalisation** — prices systematically depressed when the asset generates.
- **PPA Expiry Risk** — PPA approaching expiry; re-contracting / merchant-exposure risk.
- **Negative-Price Hours Exposure** — generating into negative wholesale prices.

**Financial**
- **P50 Generation Attainment** — actual generation below the P50 target for consecutive years.
- **Onshore / Offshore OPEX Overrun** — operating cost per MWh above the zone median for the asset class.

**Data quality**
- **Generation Data Gaps** — a gap detector that *gates* the others: findings that depend on generation data inside a gap window are suppressed rather than shown as false alarms.

Each finding carries a **severity** — *Confirmed* (direct, quantified), *Indicative* (pattern warrants investigation), or *Watch* (early signal, monitor) — and, where relevant, a root-cause branch. Schemas awaiting data (PPA pricing, forecast feeds) are inactive and never produce findings.""",
    },
    {
        "section_key": "peers",
        "title": "Peer comparison",
        "description": "How peer groups are built and what the comparison views are actually comparing against.",
        "sort_order": 70,
        "content_md": """\
Peer aggregates are pre-computed daily by the pipeline for four group types: **bidzone**, **country**, **owner**, and **turbine model**. For each group and metric the platform stores the average, P10, median (P50), and P90 across member windfarms, together with the member count — so peer comparisons are consistent with published module reports and cheap to query.

Covered metrics include ODI (hours / energy / revenue variants), wind-normalised indices (P50 and P10 based), degradation slopes, and generation-concentration measures (capture ratio, top/bottom-decile shares).

Where a windfarm's bidzone group is too thin, views fall back to the country group and flag the limited peer set. The comparison page additionally lets you compare any subset of accessible windfarms head-to-head on generation, capacity factor, and (where price data exists) capture metrics.""",
    },
]


async def main() -> None:
    Session = async_sessionmaker(get_engine(), expire_on_commit=False)
    async with Session() as db:
        for s in SECTIONS:
            res = await db.execute(
                text(
                    """
                    INSERT INTO methodology_sections
                        (section_key, title, description, content_md, sort_order)
                    VALUES (:section_key, :title, :description, :content_md, :sort_order)
                    ON CONFLICT (section_key) DO UPDATE SET
                        title = EXCLUDED.title,
                        description = EXCLUDED.description,
                        content_md = EXCLUDED.content_md,
                        sort_order = EXCLUDED.sort_order,
                        updated_at = now()
                    RETURNING id, (xmax = 0) AS inserted
                    """
                ),
                s,
            )
            row = res.first()
            print(f"{'inserted' if row.inserted else 'updated '} #{row.id}  {s['section_key']}")
        await db.commit()

    print("done")


if __name__ == "__main__":
    asyncio.run(main())
