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


# ---- native-period siblings (pipeline D-034): optional, lazy, never fatal ----

@pytest.fixture()
def native_lake(mini_lake):
    """mini_lake + a 5-min farm: measurements_5m and status_observations."""
    lake, script = mini_lake
    m5 = lake / "measurements_5m" / "farm=lutelandet" / "year=2025"
    st = lake / "status_observations" / "farm=lutelandet" / "year=2025"
    for d in (m5, st):
        d.mkdir(parents=True)
    con = duckdb.connect()
    con.execute(
        f"""
        COPY (
            SELECT * FROM (VALUES
                (TIMESTAMP '2025-03-01 00:00:00', 'T09', 1200.0, 350.0, 0),
                (TIMESTAMP '2025-03-01 00:05:00', 'T09', 2400.0,  10.0, 0),
                (TIMESTAMP '2025-03-01 00:10:00', 'T09', 3000.0,  90.0, 0),
                (TIMESTAMP '2025-03-01 00:15:00', 'T09', NULL,    90.0, 16)
            ) t(ts_start_utc, turbine, power_kw, wind_dir_deg, qc)
        ) TO '{m5 / "month=03.parquet"}' (FORMAT PARQUET)
        """
    )
    con.execute(
        f"""
        COPY (
            SELECT * FROM (VALUES
                ('T09', 'EFFBEGAARS', TIMESTAMP '2025-03-01 00:00:00', '0'),
                ('T09', 'EFFBEGAARS', TIMESTAMP '2025-03-01 01:00:00', '13'),
                ('T09', 'EFFBEGAARS', TIMESTAMP '2025-03-01 03:30:00', '0')
            ) t(turbine, channel, ts_utc, raw_value)
        ) TO '{st / "month=03.parquet"}' (FORMAT PARQUET)
        """
    )
    con.close()
    return lake, script


def test_missing_optional_view_does_not_break_core_views(mini_lake):
    """Code deploys BEFORE the data lands: every existing query must still work."""
    lake, script = mini_lake
    assert not (lake / "measurements_5m").exists()
    r = _run(script, lake, "SELECT count(*) AS n FROM measurements")
    assert r.returncode == 0 and "3" in r.stdout
    assert _run(script, lake, "SELECT count(*) FROM alarms").returncode == 0
    assert _run(script, lake, "SELECT count(*) FROM dim_farm").returncode == 0


def test_missing_optional_view_gives_a_hint_not_a_stack_trace(mini_lake):
    lake, script = mini_lake
    r = _run(script, lake, "SELECT count(*) FROM measurements_5m")
    assert r.returncode == 2
    err = json.loads(r.stdout.strip())["error"]
    assert "measurements_5m" in err and "No data has landed" in err and "unaffected" in err


def test_5m_view_queryable_and_kwh_is_kw_over_12(native_lake):
    lake, script = native_lake
    r = _run(
        script, lake,
        "SELECT farm, year, count(*) AS n, sum(power_kw) / 12 AS kwh "
        "FROM measurements_5m WHERE farm = 'lutelandet' GROUP BY 1, 2",
    )
    assert r.returncode == 0, r.stdout + r.stderr
    assert "lutelandet" in r.stdout and "550.0" in r.stdout          # (1200+2400+3000)/12


def test_10m_view_stays_10m_only(native_lake):
    lake, script = native_lake
    r = _run(script, lake, "SELECT count(*) AS n, count(DISTINCT farm) AS f FROM measurements")
    assert r.returncode == 0
    assert "3 | 1" in r.stdout                                       # the 5-min farm never leaks in


def test_skill_recipe_10min_bucketing_with_circular_mean(native_lake):
    """The recipe quoted in the skill text: strict both-halves buckets, vector mean
    for bearings (350 & 10 -> 0, not 180)."""
    lake, script = native_lake
    r = _run(
        script, lake,
        "SELECT time_bucket(INTERVAL '10 minutes', ts_start_utc) AS b, "
        "CASE WHEN count(power_kw) = 2 THEN avg(power_kw) END AS power_kw, "
        "round((degrees(atan2(avg(sin(radians(wind_dir_deg))), avg(cos(radians(wind_dir_deg))))) + 360) % 360, 3) AS wd "
        "FROM measurements_5m WHERE farm = 'lutelandet' GROUP BY 1 ORDER BY 1",
    )
    assert r.returncode == 0, r.stdout + r.stderr
    lines = r.stdout.splitlines()
    assert "1800.0" in lines[3] and lines[3].rstrip().endswith(("| 0.0", "| 360.0"))
    assert "NULL" in lines[4] and "90.0" in lines[4]                 # half-present bucket -> no power


def test_status_observations_state_durations(native_lake):
    lake, script = native_lake
    r = _run(
        script, lake,
        "WITH s AS (SELECT raw_value, ts_utc, lead(ts_utc) OVER (PARTITION BY turbine, channel ORDER BY ts_utc) AS nxt "
        "FROM status_observations WHERE farm = 'lutelandet' AND channel = 'EFFBEGAARS') "
        "SELECT raw_value, sum(epoch(nxt - ts_utc)) / 3600 AS hours FROM s WHERE nxt IS NOT NULL GROUP BY 1 ORDER BY 1",
    )
    assert r.returncode == 0, r.stdout + r.stderr
    assert "13 | 2.5" in r.stdout


def test_optional_views_are_lazy(native_lake):
    """A query that doesn't name an optional view must not pay for (or fail on) it."""
    lake, script = native_lake
    import shutil
    shutil.rmtree(lake / "status_observations")
    r = _run(script, lake, "SELECT count(*) FROM measurements_5m")
    assert r.returncode == 0, r.stdout
