"""Add CHECK constraint blocking NaN inserts into weather_data.

Background: ERA5 imports historically wrote NaN values into wind_speed_100m,
wind_direction_deg, and temperature_2m_k columns when xarray.interp returned
NaN for out-of-bbox or masked grid cells. PostgreSQL `numeric` accepts NaN as
a valid value, and `NOT NULL` does not block NaN, so 98M NaN rows accumulated
silently. The application-level NaN guard in `weather_import.py` and
`fetch_daily_all_windfarms.py` is the primary fix; this constraint is a
defensive net to make any future regression fail loudly at the DB layer.

Uses NOT VALID so existing NaN rows are not validated (they are deleted by
the operational runbook separately). New inserts are checked from the moment
the constraint is added.

Revision ID: 2026041700_weather_no_nan
Revises: d7f91a2b3c4e
Create Date: 2026-04-17

Note: Renamed from a1b2c3d4e5f6 (which collided with an existing turbine-units
migration) to 2026041700_weather_no_nan to break the alembic cycle.
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "2026041700_weather_no_nan"
down_revision = "d7f91a2b3c4e"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # `x = x` evaluates to false when x is NaN — the standard PG idiom for
    # "is not NaN". Combined with the existing NOT NULL on these columns this
    # rules out both NULL and NaN. Wrapped in IF NOT EXISTS for idempotency.
    op.execute("""
        DO $$ BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint WHERE conname = 'chk_weather_no_nan'
            ) THEN
                ALTER TABLE weather_data
                ADD CONSTRAINT chk_weather_no_nan
                CHECK (
                    wind_speed_100m = wind_speed_100m
                    AND wind_direction_deg = wind_direction_deg
                    AND temperature_2m_k = temperature_2m_k
                ) NOT VALID;
            END IF;
        END $$
    """)


def downgrade() -> None:
    op.execute("ALTER TABLE weather_data DROP CONSTRAINT chk_weather_no_nan")
