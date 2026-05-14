"""seed more agent question template routes

Revision ID: b8f5a92c4d31
Revises: a1c4e2d9f7b3
Create Date: 2026-05-14 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB


# revision identifiers, used by Alembic.
revision = "b8f5a92c4d31"
down_revision = "a1c4e2d9f7b3"
branch_labels = None
depends_on = None


NEW_ROWS = [
    {
        "route_path": "/_protected/wind-farms/$windfarmId/anomalies",
        "label": "Wind farm — Anomalies",
        "questions": [
            {"template": "List recent anomalies for {windfarmName}", "fallback": None},
            {"template": "Are anomalies for {windfarmName} trending up or down this {dateRange}?", "fallback": None},
        ],
    },
    {
        "route_path": "/_protected/wind-farms/$windfarmId/report",
        "label": "Wind farm — Report",
        "questions": [
            {"template": "Summarise the {dateRange} report for {windfarmName}", "fallback": None},
            {"template": "What are the key takeaways from {windfarmName}'s latest report?", "fallback": None},
        ],
    },
    {
        "route_path": "/_protected/analytics/generation",
        "label": "Analytics — Generation",
        "questions": [
            {"template": "Which windfarms generated the most over the {dateRange}?", "fallback": "Which windfarms generated the most this month?"},
            {"template": "Show capacity-factor trends across my fleet for the {dateRange}", "fallback": "Show capacity-factor trends across my fleet"},
        ],
    },
    {
        "route_path": "/_protected/analytics/performance",
        "label": "Analytics — Performance",
        "questions": [
            {"template": "Which windfarms are underperforming over the {dateRange}?", "fallback": "Which windfarms are underperforming?"},
            {"template": "Rank my fleet by P50 attainment for the {dateRange}", "fallback": "Rank my fleet by P50 attainment"},
        ],
    },
    {
        "route_path": "/_protected/analytics/revenue",
        "label": "Analytics — Revenue",
        "questions": [
            {"template": "Which windfarms drove the most revenue over the {dateRange}?", "fallback": "Which windfarms drove the most revenue?"},
            {"template": "Show capture-rate trends across my fleet for the {dateRange}", "fallback": "Show capture-rate trends across my fleet"},
        ],
    },
    {
        "route_path": "/_protected/analytics/weather",
        "label": "Analytics — Weather",
        "questions": [
            {"template": "Summarise wind conditions across my fleet for the {dateRange}", "fallback": "Summarise wind conditions across my fleet"},
            {"template": "Which sites had the most favourable weather over the {dateRange}?", "fallback": "Which sites had the most favourable weather recently?"},
        ],
    },
    {
        "route_path": "/_protected/comparison",
        "label": "Comparison",
        "questions": [
            {"template": "Compare the selected windfarms on capacity factor", "fallback": None},
            {"template": "Where does each compared site over- or underperform?", "fallback": None},
        ],
    },
    {
        "route_path": "/_protected/map",
        "label": "Map",
        "questions": [
            {"template": "Which regions have the highest concentration of my fleet?", "fallback": None},
            {"template": "Highlight sites with active alerts on the map", "fallback": None},
        ],
    },
    {
        "route_path": "/_protected/opportunities",
        "label": "Opportunities",
        "questions": [
            {"template": "What are the highest-value open opportunities right now?", "fallback": None},
            {"template": "Group opportunities by severity", "fallback": None},
        ],
    },
    {
        "route_path": "/_protected/reports",
        "label": "Reports",
        "questions": [
            {"template": "Summarise the latest fleet report", "fallback": None},
            {"template": "Which sites moved the most in the rankings this month?", "fallback": None},
        ],
    },
    {
        "route_path": "/_protected/turbines",
        "label": "Turbines",
        "questions": [
            {"template": "Which turbine models are most common across my fleet?", "fallback": None},
            {"template": "Any turbine-level downtime patterns worth investigating?", "fallback": None},
        ],
    },
]


def upgrade() -> None:
    seed_table = sa.table(
        "agent_question_templates",
        sa.column("route_path", sa.String()),
        sa.column("label", sa.String()),
        sa.column("questions", JSONB()),
    )
    op.bulk_insert(seed_table, NEW_ROWS)


def downgrade() -> None:
    route_paths = tuple(r["route_path"] for r in NEW_ROWS)
    op.execute(
        sa.text("DELETE FROM agent_question_templates WHERE route_path IN :paths").bindparams(
            sa.bindparam("paths", route_paths, expanding=True)
        )
    )
