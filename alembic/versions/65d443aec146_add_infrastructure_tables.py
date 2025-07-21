"""add infrastructure tables

Revision ID: 65d443aec146
Revises: 3ea9ffb688a9
Create Date: 2025-07-20 22:16:39.608226

"""
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision = "65d443aec146"
down_revision = "3ea9ffb688a9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create windfarms table
    op.create_table(
        "windfarms",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("code", sa.String(length=50), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("country_id", sa.Integer(), nullable=False),
        sa.Column("state_id", sa.Integer(), nullable=False),
        sa.Column("region_id", sa.Integer(), nullable=True),
        sa.Column("bidzone_id", sa.Integer(), nullable=True),
        sa.Column("market_balance_area_id", sa.Integer(), nullable=True),
        sa.Column("control_area_id", sa.Integer(), nullable=True),
        sa.Column("nameplate_capacity_mw", sa.Integer(), nullable=True),
        sa.Column("project_id", sa.Integer(), nullable=True),
        sa.Column("owner_id", sa.Integer(), nullable=True),
        sa.Column("commercial_operational_date", sa.Date(), nullable=True),
        sa.Column("first_power_date", sa.Date(), nullable=True),
        sa.Column("lat", sa.Float(), nullable=True),
        sa.Column("lng", sa.Float(), nullable=True),
        sa.Column("polygon_wkt", sa.Text(), nullable=True),
        sa.Column("foundation_type", sa.String(length=100), nullable=True),
        sa.Column("location_type", sa.String(length=100), nullable=True),
        sa.Column("status", sa.String(length=100), nullable=True),
        sa.Column("notes", sa.String(length=300), nullable=True),
        sa.Column("alternate_name", sa.String(length=255), nullable=True),
        sa.Column("environmental_assessment_status", sa.String(length=100), nullable=True),
        sa.Column("permits_obtained", sa.Boolean(), nullable=False, default=False),
        sa.Column("grid_connection_status", sa.String(length=100), nullable=True),
        sa.Column("total_investment_amount", sa.Numeric(precision=15, scale=2), nullable=True),
        sa.Column("investment_currency", sa.String(length=3), nullable=True),
        sa.Column("address", sa.Text(), nullable=True),
        sa.Column("postal_code", sa.String(length=20), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(
            ["bidzone_id"],
            ["bidzones.id"],
        ),
        sa.ForeignKeyConstraint(
            ["control_area_id"],
            ["control_areas.id"],
        ),
        sa.ForeignKeyConstraint(
            ["country_id"],
            ["countries.id"],
        ),
        sa.ForeignKeyConstraint(
            ["market_balance_area_id"],
            ["market_balance_areas.id"],
        ),
        sa.ForeignKeyConstraint(
            ["owner_id"],
            ["owners.id"],
        ),
        sa.ForeignKeyConstraint(
            ["project_id"],
            ["projects.id"],
        ),
        sa.ForeignKeyConstraint(
            ["region_id"],
            ["regions.id"],
        ),
        sa.ForeignKeyConstraint(
            ["state_id"],
            ["states.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("code"),
    )
    op.create_index(op.f("ix_windfarms_id"), "windfarms", ["id"], unique=False)
    op.create_index(op.f("ix_windfarms_code"), "windfarms", ["code"], unique=False)

    # Create substations table
    op.create_table(
        "substations",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("code", sa.String(length=50), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("owner_id", sa.Integer(), nullable=True),
        sa.Column("substation_type", sa.String(length=100), nullable=True),
        sa.Column("lat", sa.Float(), nullable=False),
        sa.Column("lng", sa.Float(), nullable=False),
        sa.Column("current_type", sa.String(length=2), nullable=True),
        sa.Column("array_cable_voltage_kv", sa.Integer(), nullable=True),
        sa.Column("export_cable_voltage_kv", sa.Integer(), nullable=True),
        sa.Column("transformer_capacity_mva", sa.Integer(), nullable=True),
        sa.Column("commissioning_date", sa.Date(), nullable=True),
        sa.Column("operational_date", sa.Date(), nullable=True),
        sa.Column("notes", sa.String(length=300), nullable=True),
        sa.Column("address", sa.Text(), nullable=True),
        sa.Column("postal_code", sa.String(length=20), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(
            ["owner_id"],
            ["owners.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("code"),
    )
    op.create_index(op.f("ix_substations_id"), "substations", ["id"], unique=False)
    op.create_index(op.f("ix_substations_code"), "substations", ["code"], unique=False)

    # Create turbine_units table
    op.create_table(
        "turbine_units",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("code", sa.String(length=50), nullable=False),
        sa.Column("windfarm_id", sa.Integer(), nullable=False),
        sa.Column("turbine_model_id", sa.Integer(), nullable=False),
        sa.Column("lat", sa.Float(), nullable=False),
        sa.Column("lng", sa.Float(), nullable=False),
        sa.Column("status", sa.String(length=100), nullable=True),
        sa.Column("hub_height_m", sa.Numeric(precision=6, scale=2), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(
            ["turbine_model_id"],
            ["turbine_models.id"],
        ),
        sa.ForeignKeyConstraint(
            ["windfarm_id"],
            ["windfarms.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("code"),
    )
    op.create_index(op.f("ix_turbine_units_id"), "turbine_units", ["id"], unique=False)
    op.create_index(op.f("ix_turbine_units_code"), "turbine_units", ["code"], unique=False)

    # Create cables table
    op.create_table(
        "cables",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("code", sa.String(length=50), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("type", sa.String(length=100), nullable=True),
        sa.Column("owner_id", sa.Integer(), nullable=True),
        sa.Column("from_type", sa.String(length=50), nullable=False),
        sa.Column("from_id", sa.Integer(), nullable=False),
        sa.Column("to_type", sa.String(length=50), nullable=False),
        sa.Column("to_id", sa.Integer(), nullable=False),
        sa.Column("current_type", sa.String(length=10), nullable=True),
        sa.Column("voltage_kv", sa.Integer(), nullable=True),
        sa.Column("length_km", sa.Numeric(precision=8, scale=2), nullable=True),
        sa.Column("landing_point_lat", sa.Float(), nullable=True),
        sa.Column("landing_point_lng", sa.Float(), nullable=True),
        sa.Column("route_wkt", sa.Text(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(
            ["owner_id"],
            ["owners.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("code"),
    )
    op.create_index(op.f("ix_cables_id"), "cables", ["id"], unique=False)
    op.create_index(op.f("ix_cables_code"), "cables", ["code"], unique=False)


def downgrade() -> None:
    # Drop infrastructure tables in reverse order
    op.drop_index(op.f("ix_cables_code"), table_name="cables")
    op.drop_index(op.f("ix_cables_id"), table_name="cables")
    op.drop_table("cables")

    op.drop_index(op.f("ix_turbine_units_code"), table_name="turbine_units")
    op.drop_index(op.f("ix_turbine_units_id"), table_name="turbine_units")
    op.drop_table("turbine_units")

    op.drop_index(op.f("ix_substations_code"), table_name="substations")
    op.drop_index(op.f("ix_substations_id"), table_name="substations")
    op.drop_table("substations")

    op.drop_index(op.f("ix_windfarms_code"), table_name="windfarms")
    op.drop_index(op.f("ix_windfarms_id"), table_name="windfarms")
    op.drop_table("windfarms")
