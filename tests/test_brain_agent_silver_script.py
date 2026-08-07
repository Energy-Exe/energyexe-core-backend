"""Functional tests for the seeded silver.py helper (DuckDB over Parquet).

Executes the actual SILVER_HELPER_SCRIPT template as a subprocess against a
tiny local Parquet lake laid out exactly like the real one (hive farm=/year=
dirs, month in the FILENAME, registry dims as flat files). No S3, no network.
"""
import json
import subprocess
import sys

import pytest

duckdb = pytest.importorskip("duckdb")

from app.services.brain_agent_silver_script import SILVER_HELPER_SCRIPT  # noqa: E402

DIMS = [
    "dim_farm", "dim_turbine", "dim_turbine_config", "dim_signal",
    "dim_signal_map", "dim_signal_capability", "dim_alarm_code",
    "dim_event_category",
]


@pytest.fixture()
def mini_lake(tmp_path):
    """A tiny silver tree: 3 measurement rows, 1 alarm row, 8 one-row dims."""
    meas_dir = tmp_path / "lake" / "measurements_10m" / "farm=testfarm" / "year=2024"
    alarm_dir = tmp_path / "lake" / "alarms" / "farm=testfarm" / "year=2024"
    reg_dir = tmp_path / "lake" / "registry"
    for d in (meas_dir, alarm_dir, reg_dir):
        d.mkdir(parents=True)

    con = duckdb.connect()
    con.execute(
        f"""
        COPY (
            SELECT * FROM (VALUES
                (TIMESTAMP '2024-01-01 00:00:00', 'T01', 100.0, 5.0, 0),
                (TIMESTAMP '2024-01-01 00:10:00', 'T01', 200.0, 6.0, 0),
                (TIMESTAMP '2024-01-01 00:20:00', 'T01', 300.0, 7.0, 16)
            ) t(ts_start_utc, turbine, power_kw, wind_speed_ms, qc)
        ) TO '{meas_dir / "month=01.parquet"}' (FORMAT PARQUET)
        """
    )
    con.execute(
        f"""
        COPY (
            SELECT TIMESTAMP '2024-01-01 04:00:00' AS time_on,
                   TIMESTAMP '2024-01-01 05:00:00' AS time_off,
                   'T01' AS turbine, '1005' AS source_code
        ) TO '{alarm_dir / "month=01.parquet"}' (FORMAT PARQUET)
        """
    )
    for dim in DIMS:
        con.execute(
            f"COPY (SELECT '{dim}' AS name) TO '{reg_dir / (dim + '.parquet')}' (FORMAT PARQUET)"
        )
    con.close()

    script = tmp_path / "silver.py"
    script.write_text(SILVER_HELPER_SCRIPT)
    return tmp_path / "lake", script


def _run(script, lake, sql):
    return subprocess.run(
        [sys.executable, str(script), sql],
        capture_output=True,
        text=True,
        timeout=60,
        env={"SCADA_SILVER_URI": str(lake), "PATH": "/usr/bin:/bin"},
    )


def test_aggregate_query_over_measurements(mini_lake):
    lake, script = mini_lake
    r = _run(script, lake, "SELECT count(*) AS n, sum(power_kw) AS p FROM measurements WHERE qc = 0")
    assert r.returncode == 0, r.stdout + r.stderr
    assert "2" in r.stdout and "300.0" in r.stdout


def test_hive_partition_columns_usable(mini_lake):
    lake, script = mini_lake
    r = _run(script, lake, "SELECT farm, year, count(*) FROM measurements GROUP BY 1, 2")
    assert r.returncode == 0, r.stdout + r.stderr
    assert "testfarm" in r.stdout and "2024" in r.stdout


def test_dims_and_alarm_duration(mini_lake):
    lake, script = mini_lake
    r = _run(script, lake, "SELECT count(*) FROM dim_signal")
    assert r.returncode == 0
    r = _run(
        script, lake,
        "SELECT epoch(time_off - time_on) / 3600 AS h FROM alarms WHERE time_off IS NOT NULL",
    )
    assert r.returncode == 0 and "1.0" in r.stdout


def test_mutations_and_duckdb_escapes_rejected(mini_lake):
    lake, script = mini_lake
    for bad in (
        "INSERT INTO measurements VALUES (1)",
        "SELECT 1; DROP TABLE measurements",
        "ATTACH 'x.db'",
        "INSTALL httpfs",
        "SET memory_limit='100GB'",
        "COPY measurements TO '/tmp/x.csv'",
        "CREATE TABLE t AS SELECT 1",
    ):
        r = _run(script, lake, bad)
        assert r.returncode == 2, f"not rejected: {bad}\n{r.stdout}"
        assert json.loads(r.stdout.strip())["error"]


def test_unconfigured_lake_is_clean_error(mini_lake, tmp_path):
    _, script = mini_lake
    r = subprocess.run(
        [sys.executable, str(script), "SELECT 1 FROM measurements"],
        capture_output=True,
        text=True,
        timeout=60,
        env={"PATH": "/usr/bin:/bin"},
    )
    assert r.returncode == 2
    assert "not configured" in r.stdout
