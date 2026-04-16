"""add performance pipeline tables

Revision ID: 788b2ee80007
Revises: b064d48e436b
Create Date: 2026-04-14 16:04:48.720557

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = '788b2ee80007'
down_revision = 'b064d48e436b'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # --- power_curve_bins ---
    op.create_table(
        'power_curve_bins',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('windfarm_id', sa.Integer(), sa.ForeignKey('windfarms.id', ondelete='CASCADE'), nullable=False, index=True),
        sa.Column('year', sa.Integer(), nullable=True),
        sa.Column('curve_type', sa.String(20), nullable=False),
        sa.Column('wind_bin', sa.Numeric(4, 1), nullable=False),
        sa.Column('q50_pu', sa.Numeric(6, 5), nullable=True),
        sa.Column('q90_pu', sa.Numeric(6, 5), nullable=True),
        sa.Column('mean_pu', sa.Numeric(6, 5), nullable=True),
        sa.Column('mad_pu', sa.Numeric(6, 5), nullable=True),
        sa.Column('sample_count', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint('windfarm_id', 'year', 'curve_type', 'wind_bin', name='uq_pcb_wf_year_type_bin'),
    )
    op.create_index('ix_pcb_windfarm_year', 'power_curve_bins', ['windfarm_id', 'year'])

    # --- performance_anomalies ---
    op.create_table(
        'performance_anomalies',
        sa.Column('id', sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column('windfarm_id', sa.Integer(), sa.ForeignKey('windfarms.id', ondelete='CASCADE'), nullable=False, index=True),
        sa.Column('hour', sa.DateTime(timezone=True), nullable=False),
        sa.Column('anomaly_type', sa.String(20), nullable=False),
        sa.Column('actual_p_pu', sa.Numeric(6, 5), nullable=True),
        sa.Column('expected_p_pu', sa.Numeric(6, 5), nullable=True),
        sa.Column('wind_speed', sa.Numeric(5, 2), nullable=True),
        sa.Column('wind_bin', sa.Numeric(4, 1), nullable=True),
        sa.Column('lost_mwh', sa.Numeric(10, 3), nullable=True),
        sa.Column('lost_eur', sa.Numeric(12, 2), nullable=True),
        sa.Column('market_price', sa.Numeric(12, 4), nullable=True),
        sa.Column('run_id', sa.Integer(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint('windfarm_id', 'hour', name='uq_perf_anomaly_wf_hour'),
    )
    op.create_index('ix_perf_anomaly_wf_type', 'performance_anomalies', ['windfarm_id', 'anomaly_type'])
    op.create_index('ix_perf_anomaly_wf_hour', 'performance_anomalies', ['windfarm_id', 'hour'])
    op.create_index('ix_perf_anomaly_run', 'performance_anomalies', ['windfarm_id', 'run_id'])

    # --- performance_summaries ---
    op.create_table(
        'performance_summaries',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('windfarm_id', sa.Integer(), sa.ForeignKey('windfarms.id', ondelete='CASCADE'), nullable=False, index=True),
        sa.Column('period_type', sa.String(10), nullable=False),
        sa.Column('year', sa.Integer(), nullable=False),
        sa.Column('month', sa.Integer(), nullable=True),
        # ODI metrics
        sa.Column('total_hours', sa.Integer(), nullable=True),
        sa.Column('underperf_hours', sa.Integer(), nullable=True),
        sa.Column('overperf_hours', sa.Integer(), nullable=True),
        sa.Column('odi_pct_underperf', sa.Numeric(6, 3), nullable=True),
        sa.Column('lost_mwh', sa.Numeric(12, 3), nullable=True),
        sa.Column('expected_mwh', sa.Numeric(12, 3), nullable=True),
        sa.Column('odi_pct_loss_mwh', sa.Numeric(6, 3), nullable=True),
        sa.Column('lost_eur', sa.Numeric(14, 2), nullable=True),
        sa.Column('expected_revenue_eur', sa.Numeric(14, 2), nullable=True),
        sa.Column('odi_pct_loss_eur', sa.Numeric(6, 3), nullable=True),
        sa.Column('long_run_count', sa.Integer(), nullable=True),
        sa.Column('max_run_hours', sa.Integer(), nullable=True),
        # Wind normalisation
        sa.Column('norm_ratio_p50', sa.Numeric(8, 5), nullable=True),
        sa.Column('norm_index_p50', sa.Numeric(8, 3), nullable=True),
        sa.Column('norm_ratio_p10', sa.Numeric(8, 5), nullable=True),
        sa.Column('norm_index_p10', sa.Numeric(8, 3), nullable=True),
        # Commercial
        sa.Column('constraint_proxy_mwh', sa.Numeric(12, 3), nullable=True),
        sa.Column('lost_value_eur', sa.Numeric(14, 2), nullable=True),
        # Metadata
        sa.Column('pipeline_run_id', sa.Integer(), sa.ForeignKey('import_job_executions.id', ondelete='SET NULL'), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint('windfarm_id', 'period_type', 'year', 'month', name='uq_perf_summary_wf_period'),
    )
    op.create_index('ix_perf_summary_wf_year', 'performance_summaries', ['windfarm_id', 'year'])

    # --- degradation_results ---
    op.create_table(
        'degradation_results',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('windfarm_id', sa.Integer(), sa.ForeignKey('windfarms.id', ondelete='CASCADE'), nullable=False, index=True),
        sa.Column('reference_curve', sa.String(5), nullable=False),
        sa.Column('analysis_start', sa.Date(), nullable=False),
        sa.Column('analysis_end', sa.Date(), nullable=False),
        sa.Column('data_points', sa.Integer(), nullable=False),
        sa.Column('slope_pu_per_year', sa.Numeric(8, 6), nullable=True),
        sa.Column('slope_pct_per_year', sa.Numeric(6, 3), nullable=True),
        sa.Column('intercept', sa.Numeric(8, 6), nullable=True),
        sa.Column('r_squared', sa.Numeric(6, 5), nullable=True),
        sa.Column('p_value', sa.Numeric(8, 6), nullable=True),
        sa.Column('ci_lower_95', sa.Numeric(8, 6), nullable=True),
        sa.Column('ci_upper_95', sa.Numeric(8, 6), nullable=True),
        sa.Column('baseline_cap_pu', sa.Numeric(6, 5), nullable=True),
        sa.Column('pipeline_run_id', sa.Integer(), sa.ForeignKey('import_job_executions.id', ondelete='SET NULL'), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint('windfarm_id', 'reference_curve', 'pipeline_run_id', name='uq_degradation_wf_ref_run'),
    )
    op.create_index('ix_degradation_wf', 'degradation_results', ['windfarm_id'])


def downgrade() -> None:
    op.drop_table('degradation_results')
    op.drop_table('performance_summaries')
    op.drop_table('performance_anomalies')
    op.drop_table('power_curve_bins')
