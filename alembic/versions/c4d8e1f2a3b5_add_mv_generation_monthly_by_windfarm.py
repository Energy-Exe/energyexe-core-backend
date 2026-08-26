"""Add mv_generation_monthly_by_windfarm — monthly net generation per windfarm/source

Feeds the OPEX-per-MWh denominator (FIN-02/FIN-03 detectors + report metrics,
``app/services/financial_opex_metrics.py``). Reading hourly ``generation_data``
per fiscal filing costs ~9,000 random heap pages per filing-year (a windfarm's
rows are scattered across the table); a 70-farm peer cohort is millions of
reads, which is why the aggregate is precomputed.

Created ``WITH NO DATA`` so this migration is instant at container start
(``alembic upgrade head`` runs before uvicorn). The first population and every
nightly refresh happen in the pipeline task
(``app/services/generation_monthly_view.refresh_generation_monthly_view``) —
or manually via ``scripts/jobs/refresh_generation_monthly.py``. Until the
first refresh, queries against the view raise "has not been populated" and the
consumers degrade to "no OPEX metric" (logged), never to a wrong number.

The unique index is required for ``REFRESH ... CONCURRENTLY`` (readers keep the
previous snapshot during a refresh).

Revision ID: c4d8e1f2a3b5
Revises: b3c7d21f0a94
Create Date: 2026-08-26 21:40:00.000000

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "c4d8e1f2a3b5"
down_revision = "b3c7d21f0a94"
branch_labels = None
depends_on = None

MV_NAME = "mv_generation_monthly_by_windfarm"

# Keep in sync with app/services/generation_monthly_view.MV_DEFINITION_SQL
# (the service only refreshes; the definition is owned here).
MV_SELECT = """
SELECT windfarm_id,
       source,
       (date_trunc('month', hour AT TIME ZONE 'UTC'))::date AS month,
       SUM(generation_mwh - COALESCE(consumption_mwh, 0))   AS net_mwh,
       COUNT(DISTINCT hour)                                  AS hours_with_data
FROM generation_data
WHERE windfarm_id IS NOT NULL
GROUP BY windfarm_id, source, (date_trunc('month', hour AT TIME ZONE 'UTC'))::date
"""


def upgrade() -> None:
    op.execute(f"CREATE MATERIALIZED VIEW {MV_NAME} AS {MV_SELECT} WITH NO DATA")
    op.execute(
        f"CREATE UNIQUE INDEX uq_mv_gen_monthly_wf_source_month "
        f"ON {MV_NAME} (windfarm_id, source, month)"
    )
    op.execute(f"CREATE INDEX ix_mv_gen_monthly_wf_month ON {MV_NAME} (windfarm_id, month)")


def downgrade() -> None:
    op.execute(f"DROP MATERIALIZED VIEW IF EXISTS {MV_NAME}")
