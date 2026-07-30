"""SCADA silver-lake helper script template — written to the agent sandbox at
session creation (admin sessions with the scada schema present only).

Queries the pipeline's silver Parquet lake directly (DuckDB over S3 or a local
directory) — the 10-minute measurements, raw alarm events, and registry dims
that are deliberately NOT loaded into Postgres. Read-only by construction:
the IAM grant is GetObject/ListBucket on the silver/ prefix, and the script
enforces SELECT/WITH-only on top.

Efficiency notes (why the script looks the way it does):
- NO union_by_name on read_parquet: silver enforces one identical schema per
  dataset, and union_by_name would read every file footer from S3 at each
  bind (this script is a fresh process per query — nothing caches across
  calls). Binding reads ONE footer; hive pruning on farm=/year= pathnames
  happens before any footer read; month selection prunes via row-group
  zone maps on ts_start_utc (one row group per file with min/max stats).
- memory_limit/threads are capped because the sandbox shares the backend
  task's 4 GB with uvicorn — a runaway query must not OOM the service.
"""

SILVER_HELPER_SCRIPT = '''#!/usr/bin/env python3
"""EnergyExe SCADA silver lake query helper (DuckDB SQL over Parquet). Read-only.

Usage: python3 silver.py "SELECT farm, count(*) FROM measurements WHERE farm='kelmarsh' AND year=2023 GROUP BY 1"

Views: measurements (10-min intervals), alarms (raw events),
dim_farm, dim_turbine, dim_turbine_config, dim_signal, dim_signal_map,
dim_signal_capability, dim_alarm_code, dim_event_category.
DuckDB SQL dialect. Always filter farm (and year when possible) — they prune
partitions. Aggregate in SQL; never SELECT * over raw intervals.
Output is a text table with max 20 displayed rows; larger results get a
statistical summary of ALL rows appended.
"""
import json, os, re, sys, threading

MAX_DISPLAY_ROWS = 20
DEFAULT_LIMIT = 200
QUERY_TIMEOUT_S = 120
MEMORY_LIMIT = "1GB"
THREADS = 2

DIMS = [
    "dim_farm", "dim_turbine", "dim_turbine_config", "dim_signal",
    "dim_signal_map", "dim_signal_capability", "dim_alarm_code",
    "dim_event_category",
]

DANGEROUS_KEYWORDS = [
    "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE",
    "CREATE", "GRANT", "REVOKE", "EXECUTE", "COPY", "VACUUM",
    "ATTACH", "DETACH", "INSTALL", "LOAD", "EXPORT", "IMPORT",
    "PRAGMA", "CALL", "SET",
]


def validate_sql(sql: str) -> str:
    """Validate SQL is read-only. Strip OFFSET. Add LIMIT if missing."""
    sql = sql.strip().rstrip(";").strip()
    if not sql:
        return json.dumps({"error": "Empty SQL query"})

    cleaned = re.sub(r"--[^\\n]*", " ", sql)
    cleaned = re.sub(r"/\\*.*?\\*/", " ", cleaned, flags=re.DOTALL)
    upper = cleaned.upper().strip()

    if not upper.startswith("SELECT") and not upper.startswith("WITH"):
        return json.dumps({"error": "Only SELECT/WITH queries are allowed."})

    for kw in DANGEROUS_KEYWORDS:
        if re.search(rf"\\b{kw}\\b", upper):
            return json.dumps({"error": f"Keyword \\'{kw}\\' not allowed."})

    sql = re.sub(r"\\bOFFSET\\s+\\d+", "", sql, flags=re.IGNORECASE)

    if "LIMIT" not in upper:
        sql += f" LIMIT {DEFAULT_LIMIT}"

    return sql


def open_lake():
    """Connect DuckDB and create the silver views. Returns (conn, error)."""
    import duckdb

    root = os.environ.get("SCADA_SILVER_URI", "").rstrip("/")
    if not root:
        return None, "SCADA silver lake is not configured (SCADA_SILVER_URI unset)."

    conn = duckdb.connect()
    conn.execute(f"SET memory_limit = '{MEMORY_LIMIT}'")
    conn.execute(f"SET threads = {THREADS}")
    conn.execute("SET enable_object_cache = true")
    if root.startswith("s3://"):
        region = os.environ.get("AWS_DEFAULT_REGION", "eu-north-1")
        conn.execute(
            "CREATE OR REPLACE SECRET scada "
            f"(TYPE s3, PROVIDER credential_chain, REGION '{region}')"
        )
    conn.execute(
        "CREATE VIEW measurements AS SELECT * FROM read_parquet("
        f"'{root}/measurements_10m/**/*.parquet', hive_partitioning = 1)"
    )
    conn.execute(
        "CREATE VIEW alarms AS SELECT * FROM read_parquet("
        f"'{root}/alarms/**/*.parquet', hive_partitioning = 1)"
    )
    for dim in DIMS:
        conn.execute(
            f"CREATE VIEW {dim} AS SELECT * FROM read_parquet('{root}/registry/{dim}.parquet')"
        )
    return conn, None


def run_query(sql: str) -> str:
    """Execute SQL against the silver lake and return a text table."""
    result = validate_sql(sql)
    if result.startswith("{"):
        return result
    sql = result

    conn = None
    try:
        conn, err = open_lake()
        if err:
            return json.dumps({"error": err})

        watchdog = threading.Timer(QUERY_TIMEOUT_S, conn.interrupt)
        watchdog.start()
        try:
            cur = conn.execute(sql)
            columns = [d[0] for d in cur.description] if cur.description else []
            rows = cur.fetchall()
        finally:
            watchdog.cancel()
        total_rows = len(rows)

        if total_rows == 0:
            return "No rows returned."

        display_rows = rows[:MAX_DISPLAY_ROWS]
        lines = []
        lines.append(" | ".join(columns))
        lines.append("-" * min(len(lines[0]), 120))
        for row in display_rows:
            vals = [(str(v) if v is not None else "NULL") for v in row]
            lines.append(" | ".join(vals))

        header = f"Total: {total_rows} rows"
        if total_rows > MAX_DISPLAY_ROWS:
            header += f" (showing top {MAX_DISPLAY_ROWS})"
        result_text = header + "\\n" + "\\n".join(lines)

        if total_rows > MAX_DISPLAY_ROWS:
            summary_parts = []
            for i, col in enumerate(columns):
                vals = [row[i] for row in rows if row[i] is not None]
                if not vals:
                    continue
                try:
                    num_vals = [float(v) for v in vals]
                    summary_parts.append(
                        f"{col}: min={min(num_vals):.1f}, max={max(num_vals):.1f}, "
                        f"avg={sum(num_vals)/len(num_vals):.1f}, median={sorted(num_vals)[len(num_vals)//2]:.1f}"
                    )
                except (ValueError, TypeError):
                    unique = len(set(str(v) for v in vals))
                    summary_parts.append(f"{col}: {unique} unique values")

            result_text += "\\n\\nSummary of ALL {0} rows:\\n".format(total_rows)
            result_text += "\\n".join(summary_parts)
            result_text += "\\n\\nNote: This is all the data. Do NOT make additional queries for remaining rows."

        return result_text

    except Exception as e:
        return json.dumps({"error": str(e)})
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(json.dumps({"error": "Usage: python3 silver.py \\"SELECT ...\\""}))
        sys.exit(1)
    out = run_query(sys.argv[1])
    print(out)
    if out.startswith('{"error"'):
        sys.exit(2)
'''
