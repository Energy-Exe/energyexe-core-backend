"""Restore generation_data_raw 4-col unique constraint.

Migration ``d3e4f5g6h7i8`` was supposed to install the
``uq_generation_data_raw_source_type_identifier_period`` constraint but the
production DB ended up without it (the alembic_version row advanced past the
migration even though the DDL did not land — likely because the prior 3-col
constraint was already gone, so ``drop_constraint`` short-circuited and the
new ``create_unique_constraint`` block silently did not run on that DB).

Without this constraint every importer that does ``ON CONFLICT (source,
source_type, identifier, period_start)`` raises ``InvalidColumnReferenceError``
and silently aborts the chunk — which is what produced months of jobs marked
``success`` with ``records_imported=0``.

This migration is idempotent: it skips the index/constraint create if either
already exists, so it is safe to run on environments where the original
migration *did* land.

Revision ID: e9f2c8a4b6d1
Revises: d7f91a2b3c4e
Create Date: 2026-04-28
"""

from alembic import op


revision = "e9f2c8a4b6d1"
down_revision = "2026041703_iforest"
branch_labels = None
depends_on = None


CONSTRAINT_NAME = "uq_generation_data_raw_source_type_identifier_period"


def upgrade() -> None:
    # CONCURRENTLY can't run inside a transaction
    with op.get_context().autocommit_block():
        op.execute(
            f"""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_indexes
                    WHERE tablename = 'generation_data_raw'
                      AND indexname = '{CONSTRAINT_NAME}'
                ) THEN
                    EXECUTE 'CREATE UNIQUE INDEX CONCURRENTLY {CONSTRAINT_NAME} '
                            'ON generation_data_raw (source, source_type, identifier, period_start)';
                END IF;
            END$$;
            """
        )

    op.execute(
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint WHERE conname = '{CONSTRAINT_NAME}'
            ) THEN
                EXECUTE 'ALTER TABLE generation_data_raw '
                        'ADD CONSTRAINT {CONSTRAINT_NAME} '
                        'UNIQUE USING INDEX {CONSTRAINT_NAME}';
            END IF;
        END$$;
        """
    )


def downgrade() -> None:
    op.execute(f"ALTER TABLE generation_data_raw DROP CONSTRAINT IF EXISTS {CONSTRAINT_NAME}")
    op.execute(f"DROP INDEX IF EXISTS {CONSTRAINT_NAME}")
