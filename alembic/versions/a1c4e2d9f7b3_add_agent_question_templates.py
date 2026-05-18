"""add agent_question_templates table

Revision ID: a1c4e2d9f7b3
Revises: b9d8e3a5c2f1
Create Date: 2026-05-13 00:30:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB


# revision identifiers, used by Alembic.
revision = "a1c4e2d9f7b3"
down_revision = "b9d8e3a5c2f1"
branch_labels = None
depends_on = None


SEED_ROWS = [
    {
        "route_path": "/_protected/dashboard",
        "label": "Dashboard",
        "questions": [
            {"template": "Summarise my portfolio performance this month", "fallback": None},
            {"template": "Which of my windfarms are underperforming?", "fallback": None},
            {"template": "Any anomalies I should know about?", "fallback": None},
        ],
    },
    {
        "route_path": "/_protected/wind-farms/",
        "label": "Wind farms",
        "questions": [
            {"template": "Rank my windfarms by capacity factor over the last 30 days", "fallback": None},
            {"template": "Which UK windfarms had the most curtailment this year?", "fallback": None},
            {"template": "Compare offshore vs onshore performance", "fallback": None},
        ],
    },
    {
        "route_path": "/_protected/wind-farms/$windfarmId/",
        "label": "Wind farm — Overview",
        "questions": [
            {"template": "How is {windfarmName} performing this {dateRange}?", "fallback": None},
            {"template": "What's the equipment status for {windfarmName}?", "fallback": None},
            {"template": "Any open alerts or anomalies for {windfarmName}?", "fallback": None},
        ],
    },
    {
        "route_path": "/_protected/wind-farms/$windfarmId/generation",
        "label": "Wind farm — Generation",
        "questions": [
            {"template": "What was {windfarmName}'s output for the {dateRange}?", "fallback": None},
            {"template": "How does {windfarmName}'s availability compare to its P50 target?", "fallback": None},
            {"template": "Any unusual generation dips for {windfarmName} in the {dateRange}?", "fallback": None},
        ],
    },
    {
        "route_path": "/_protected/wind-farms/$windfarmId/weather",
        "label": "Wind farm — Weather",
        "questions": [
            {"template": "What were {windfarmName}'s wind-speed conditions over the {dateRange}?", "fallback": None},
            {"template": "How does {windfarmName}'s recent weather compare to its long-term average?", "fallback": None},
        ],
    },
    {
        "route_path": "/_protected/wind-farms/$windfarmId/market",
        "label": "Wind farm — Market",
        "questions": [
            {"template": "What capture rate did {windfarmName} achieve over the {dateRange}?", "fallback": None},
            {"template": "Did {windfarmName} have negative-price exposure in the {dateRange}?", "fallback": None},
            {"template": "How do power prices for {windfarmName}'s zone compare year-over-year?", "fallback": None},
        ],
    },
    {
        "route_path": "/_protected/wind-farms/$windfarmId/equipment",
        "label": "Wind farm — Equipment",
        "questions": [
            {"template": "List the turbines installed at {windfarmName}", "fallback": None},
            {"template": "Any equipment-related downtime patterns for {windfarmName}?", "fallback": None},
        ],
    },
    {
        "route_path": "/_protected/wind-farms/$windfarmId/financials",
        "label": "Wind farm — Financials",
        "questions": [
            {"template": "What's {windfarmName}'s revenue trend over the last 3 years?", "fallback": None},
            {"template": "How do {windfarmName}'s OPEX numbers compare to peers?", "fallback": None},
        ],
    },
    {
        "route_path": "/_protected/wind-farms/$windfarmId/benchmarking",
        "label": "Wind farm — Benchmarking",
        "questions": [
            {"template": "How does {windfarmName} compare to its peer group?", "fallback": None},
            {"template": "Which peers consistently outperform {windfarmName}?", "fallback": None},
        ],
    },
    {
        "route_path": "/_protected/portfolios",
        "label": "Portfolios",
        "questions": [
            {"template": "Summarise performance across all my portfolios", "fallback": None},
            {"template": "Which portfolio has the highest capacity factor this year?", "fallback": None},
        ],
    },
    {
        "route_path": "/_protected/portfolios/$portfolioId",
        "label": "Portfolio",
        "questions": [
            {"template": "How is this portfolio performing this month?", "fallback": None},
            {"template": "Which windfarm in this portfolio is underperforming the most?", "fallback": None},
        ],
    },
    {
        "route_path": "/_protected/anomalies",
        "label": "Anomalies",
        "questions": [
            {"template": "What are the most critical anomalies right now?", "fallback": None},
            {"template": "Group recent anomalies by windfarm", "fallback": None},
        ],
    },
    {
        "route_path": "/_protected/alerts",
        "label": "Alerts",
        "questions": [
            {"template": "Summarise unresolved alerts", "fallback": None},
            {"template": "Which alerts need attention this week?", "fallback": None},
        ],
    },
]


def upgrade() -> None:
    op.create_table(
        "agent_question_templates",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("route_path", sa.String(length=255), nullable=False),
        sa.Column("label", sa.String(length=255), nullable=False),
        sa.Column("questions", JSONB(), nullable=False),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("route_path", name="uq_agent_question_templates_route_path"),
    )
    op.create_index(
        op.f("ix_agent_question_templates_id"),
        "agent_question_templates",
        ["id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_agent_question_templates_route_path"),
        "agent_question_templates",
        ["route_path"],
        unique=False,
    )

    seed_table = sa.table(
        "agent_question_templates",
        sa.column("route_path", sa.String()),
        sa.column("label", sa.String()),
        sa.column("questions", JSONB()),
    )
    op.bulk_insert(seed_table, SEED_ROWS)


def downgrade() -> None:
    op.drop_index(
        op.f("ix_agent_question_templates_route_path"),
        table_name="agent_question_templates",
    )
    op.drop_index(
        op.f("ix_agent_question_templates_id"), table_name="agent_question_templates"
    )
    op.drop_table("agent_question_templates")
