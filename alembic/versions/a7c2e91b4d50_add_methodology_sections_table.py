"""Add methodology_sections table with seeded content (client-ui #177)

Revision ID: a7c2e91b4d50
Revises: d4e5f6a7b8c9
Create Date: 2026-06-04 02:30:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "a7c2e91b4d50"
down_revision = "d4e5f6a7b8c9"
branch_labels = None
depends_on = None


# Seed content converted 1:1 from the previously hardcoded client-ui
# methodology page (src/components/methodology/methodology-page.tsx).
SEED_SECTIONS = [
    {
        "section_key": "data-sources",
        "title": "Data sources",
        "description": "Where the platform pulls generation, weather, prices, and reference data from — and how each source is normalised.",
        "sort_order": 10,
        "content_md": (
            "- **Generation** — ENTSOE (European TSO data), Elexon (GB Balancing & "
            "Settlement Code data), NVE (Norway operator returns), EIA / EIA-930 (US), "
            "Taipower (Taiwan). Hourly metered MWh after normalisation; multi-unit "
            "windfarms are aggregated by unit.\n"
            "- **Weather** — ERA5 reanalysis (Copernicus CDS). 10/100 m wind speed and "
            "direction, temperature, surface pressure. Sampled at the windfarm centroid.\n"
            "- **Prices** — ENTSOE day-ahead (EUR), Elexon MID (GBP). Half-hourly "
            "settlement aggregated to hourly. Norway has no day-ahead price data and so "
            "capture-price metrics are suppressed for Norwegian assets.\n"
            "- **Reference** — country / region / bidzone tables; ownership data; PPA "
            "contracts; manufacturer power curves (per turbine model)."
        ),
    },
    {
        "section_key": "data-harmonisation",
        "title": "Data harmonisation",
        "description": "How heterogeneous source data is converted to a single comparable hourly series per windfarm.",
        "sort_order": 20,
        "content_md": (
            "Every source feeds into a raw table (`generation_data_raw`) with its native "
            "period, timezone, and identifier. The aggregation step converts each raw "
            "record to a clean hourly UTC row in `generation_data` with `metered_mwh` "
            "(actual settled energy) and `capacity_factor` (MWh / nameplate / hours).\n\n"
            "Notable corrections: Elexon raw timestamps before 2020 carry a BST offset "
            "bug that the aggregation step corrects via `settlement_date + "
            "settlement_period`. French ENTSOE unit responses contain both generation "
            "and consumption rows and are split on `data_direction` before aggregation. "
            "Multi-unit windfarms are summed by unit, then assigned to the windfarm; "
            "capacity-factor uses nameplate-based denominators rather than averaging "
            "unit-level CFs (which silently drops NULLs)."
        ),
    },
    {
        "section_key": "wind-resource",
        "title": "Wind resource & normalisation",
        "description": "How the platform builds an expected-power signal from weather + power curves, and what wind-normalisation means in our reports.",
        "sort_order": 30,
        "content_md": (
            "For each operational hour, ERA5 wind speed at hub height is mapped through "
            "the windfarm's representative power curve (turbine model × site air-density "
            "correction) to compute an *expected* hourly MWh per turbine. Expected power "
            "per windfarm is sum-of-turbines for the operational fleet at that hour.\n\n"
            "**Wind-normalised generation** for a period is `actual / expected × "
            "long-run expected` — i.e. the generation you would have observed in a "
            "\"neutral\" wind year. This is what enables apples-to-apples comparison "
            "across years and across windfarms in different wind regimes.\n\n"
            "Hours flagged as ramp-up, curtailed, or missing in either generation or "
            "weather are excluded from the normalisation denominator."
        ),
    },
    {
        "section_key": "capacity-factor",
        "title": "Capacity factor & generation metrics",
        "description": "How CF, generation, and availability metrics are defined and where they appear in the UI.",
        "sort_order": 40,
        "content_md": (
            "- **Capacity factor (CF)** = `sum(metered_mwh) / (nameplate_mw × distinct "
            "hours in period)`. Always nameplate-based — never an average of hourly CFs, "
            "which would drop NULL-CF hours and inflate the result.\n"
            "- **Availability** = fraction of expected-operational hours where the unit "
            "produced above a low threshold (idle filter).\n"
            "- **P50 target** — operator-published annual P50 generation, divided by 12 "
            "to get the monthly target. The P50 tab shows cumulative actual vs "
            "cumulative P50 and a \"gap in months\" KPI (how many months of average "
            "production it would take to close the deficit)."
        ),
    },
    {
        "section_key": "commercial",
        "title": "Commercial & revenue metrics",
        "description": "Capture price, capture rate, and how PPA values feed in.",
        "sort_order": 50,
        "content_md": (
            "- **Capture price** = `sum(price × generation) / sum(generation)` over the "
            "period — the volume-weighted price the windfarm actually captured.\n"
            "- **Capture rate** = `capture price / average period price`. Above 100% "
            "means the windfarm generated more when prices were high.\n"
            "- PPAs are layered on top: revenue per hour is the contracted rate for the "
            "contracted volume and the spot rate for the residual. Currency is "
            "normalised to a user-selected display currency using period-end ECB rates.\n"
            "- Norway is treated as price-data-absent — capture metrics and the "
            "capture-price flag tile on the dashboard are suppressed for Norwegian "
            "assets."
        ),
    },
    {
        "section_key": "opportunities",
        "title": "Opportunities & anomalies",
        "description": "How the platform flags assets that need attention — schemas, severities, and how flags translate to dashboard tiles.",
        "sort_order": 60,
        "content_md": (
            "- **OPS-01 Low ODI / availability** — power-curve-based Operational "
            "Deviation Index drops below the peer floor for ≥ 4 days.\n"
            "- **OPS-02 Seasonal CF gap** — monthly capacity factor lags peer median by "
            "> 1 standard deviation for the same calendar month.\n"
            "- **OPS-03 Price-realisation gap** — capture rate < peer median; only "
            "fires where price data exists.\n"
            "- **MKT-01–03** — market / curtailment / constraint flags. Severity is "
            "CRITICAL / HIGH / MEDIUM / LOW based on how far the metric sits below the "
            "peer distribution."
        ),
    },
    {
        "section_key": "peers",
        "title": "Peer comparison",
        "description": "How peer groups are built and what the comparison page is actually comparing against.",
        "sort_order": 70,
        "content_md": (
            "Peer groups are built per windfarm from same-country + same-location-type "
            "+ nameplate within ±50% of the subject windfarm. Peer aggregates (median, "
            "P10, P90) are recomputed daily by the pipeline and stored so comparison "
            "views are query-time cheap. The comparison page lets you compare any "
            "subset of accessible windfarms head-to-head on generation, capacity "
            "factor, and (where available) capture metrics."
        ),
    },
]


def upgrade() -> None:
    table = op.create_table(
        "methodology_sections",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("section_key", sa.String(length=100), nullable=False),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("description", sa.String(length=500), nullable=True),
        sa.Column("content_md", sa.Text(), nullable=False),
        sa.Column("sort_order", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_methodology_sections_id"), "methodology_sections", ["id"], unique=False
    )
    op.create_index(
        op.f("ix_methodology_sections_section_key"),
        "methodology_sections",
        ["section_key"],
        unique=True,
    )

    op.bulk_insert(table, SEED_SECTIONS)


def downgrade() -> None:
    op.drop_index(op.f("ix_methodology_sections_section_key"), table_name="methodology_sections")
    op.drop_index(op.f("ix_methodology_sections_id"), table_name="methodology_sections")
    op.drop_table("methodology_sections")
