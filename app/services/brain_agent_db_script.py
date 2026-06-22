"""DB helper script template — written to the agent sandbox at session creation."""

DB_HELPER_SCRIPT = '''#!/usr/bin/env python3
"""EnergyExe Database Query Helper. Read-only, auto-limited, text output.

Usage: python3 db.py "SELECT * FROM windfarms LIMIT 10"

Output is a text table with max 20 displayed rows.
If more rows exist, a statistical summary of ALL rows is appended.
OFFSET is stripped — pagination is not supported.
"""
import json, os, re, sys

MAX_DISPLAY_ROWS = 20
DEFAULT_LIMIT = 100
STATEMENT_TIMEOUT_MS = 30000

DANGEROUS_KEYWORDS = [
    "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE",
    "CREATE", "GRANT", "REVOKE", "EXECUTE", "COPY", "VACUUM",
]


def validate_sql(sql: str) -> str:
    """Validate SQL is read-only. Strip OFFSET (no pagination). Add LIMIT if missing."""
    sql = sql.strip().rstrip(";").strip()
    if not sql:
        return json.dumps({"error": "Empty SQL query"})

    # Strip comments for keyword checking
    cleaned = re.sub(r"--[^\\n]*", " ", sql)
    cleaned = re.sub(r"/\\*.*?\\*/", " ", cleaned, flags=re.DOTALL)
    upper = cleaned.upper().strip()

    if not upper.startswith("SELECT") and not upper.startswith("WITH"):
        return json.dumps({"error": "Only SELECT/WITH queries are allowed."})

    for kw in DANGEROUS_KEYWORDS:
        if re.search(rf"\\b{kw}\\b", upper):
            return json.dumps({"error": f"Mutation keyword \\'{kw}\\' not allowed."})

    # Client surface (EPR-59): block schema introspection so the client agent
    # cannot enumerate or describe the database structure / relationships.
    if os.environ.get("BRAIN_AGENT_BLOCK_INTROSPECTION") == "1" and re.search(
        r"\\b(INFORMATION_SCHEMA|PG_CATALOG|PG_CLASS|PG_ATTRIBUTE|PG_CONSTRAINT|PG_NAMESPACE|PG_TABLES|PG_VIEWS|PG_INDEXES|PG_ROLES|PG_STAT|PG_DESCRIPTION)\\b",
        upper,
    ):
        return json.dumps({"error": "Schema introspection is not available."})

    # Strip OFFSET — pagination is not supported, all data comes in one query
    sql = re.sub(r"\\bOFFSET\\s+\\d+", "", sql, flags=re.IGNORECASE)

    # Auto-add LIMIT if not present
    if "LIMIT" not in upper:
        sql += f" LIMIT {DEFAULT_LIMIT}"

    return sql


def run_query(sql: str) -> str:
    """Execute SQL and return text table result."""
    import psycopg2

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        return json.dumps({"error": "DATABASE_URL not set"})

    # Validate
    result = validate_sql(sql)
    if result.startswith("{"):
        return result  # Error JSON
    sql = result

    conn = None
    try:
        conn = psycopg2.connect(db_url)
        conn.set_session(readonly=True, autocommit=True)
        cur = conn.cursor()
        cur.execute(f"SET statement_timeout = {STATEMENT_TIMEOUT_MS}")
        cur.execute(sql)

        columns = [desc[0] for desc in cur.description] if cur.description else []
        rows = cur.fetchall()
        total_rows = len(rows)

        if total_rows == 0:
            return "No rows returned."

        # Build text table (top MAX_DISPLAY_ROWS only)
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

        # For large results, build a comprehensive summary from ALL rows
        if total_rows > MAX_DISPLAY_ROWS:
            summary_parts = []
            for i, col in enumerate(columns):
                vals = [row[i] for row in rows if row[i] is not None]
                if not vals:
                    continue
                # Check if numeric
                try:
                    num_vals = [float(v) for v in vals]
                    summary_parts.append(
                        f"{col}: min={min(num_vals):.1f}, max={max(num_vals):.1f}, "
                        f"avg={sum(num_vals)/len(num_vals):.1f}, median={sorted(num_vals)[len(num_vals)//2]:.1f}"
                    )
                except (ValueError, TypeError):
                    # Non-numeric: show unique count
                    unique = len(set(str(v) for v in vals))
                    summary_parts.append(f"{col}: {unique} unique values")

            result_text += "\\n\\nSummary of ALL {0} rows:\\n".format(total_rows)
            result_text += "\\n".join(summary_parts)
            result_text += "\\n\\nNote: This is all the data. Do NOT make additional queries for remaining rows."

        return result_text

    except Exception as e:
        return json.dumps({"error": str(e)})
    finally:
        # Always return the connection — the bare `except` above used to leak it
        # on any query error (closing the connection also closes its cursor).
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(json.dumps({"error": "Usage: python3 db.py \\"SELECT ...\\""}))
        sys.exit(1)
    print(run_query(sys.argv[1]))
'''
