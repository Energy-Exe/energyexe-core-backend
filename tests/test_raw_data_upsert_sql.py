"""Compile-time regression test for the generation_data_raw upsert (EPR-108).

The jsonb_set path argument must render as an inline literal: when it was a
VARCHAR bind parameter, Postgres failed every manual-fetch batch at parse time
with "function jsonb_set(jsonb, character varying, jsonb) does not exist".
"""

from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy.dialects import postgresql

from app.services.raw_data_storage_service import _build_raw_upsert_stmt


def _sample_record():
    now = datetime(2026, 7, 15, tzinfo=timezone.utc)
    return {
        "source": "ENTSOE",
        "source_type": "api",
        "identifier": "19WBALTICPOWERPO",
        "period_start": now,
        "period_end": now,
        "period_type": "PT15M",
        "value_extracted": Decimal("12.4"),
        "unit": "MW",
        "data": {"eic_code": "19WBALTICPOWERPO"},
        "created_at": now,
        "updated_at": now,
    }


def test_jsonb_set_path_renders_inline_text_array():
    sql = str(
        _build_raw_upsert_stmt([_sample_record()]).compile(dialect=postgresql.dialect())
    )

    # The path must be an inline literal, not a bind parameter
    assert "'{previous_value}'" in sql
    assert "%(jsonb_set" not in sql

    # Old value comes from the existing row (table-qualified), new data from excluded
    assert "to_jsonb(generation_data_raw.value_extracted)" in sql
    assert "excluded.data" in sql

    # jsonb_set is STRICT: a NULL old value must fall back to a jsonb null
    assert "coalesce" in sql.lower()
    assert "'null'::jsonb" in sql

    # Conflict target unchanged
    assert "ON CONFLICT (source, source_type, identifier, period_start)" in sql
