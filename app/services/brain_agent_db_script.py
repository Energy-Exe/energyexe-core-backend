"""DB helper script template — written to the agent sandbox at session creation."""

DB_HELPER_SCRIPT = '''#!/usr/bin/env python3
"""EnergyExe Database Query Helper. Read-only, auto-limited, JSON output.

Usage: python3 db.py "SELECT * FROM windfarms LIMIT 10"
"""
import json, os, re, sys

MAX_ROWS = 50
DEFAULT_LIMIT = 100
STATEMENT_TIMEOUT_MS = 30000

DANGEROUS_KEYWORDS = [
    "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE",
    "CREATE", "GRANT", "REVOKE", "EXECUTE", "COPY", "VACUUM",
]


def validate_sql(sql: str) -> str:
    """Validate SQL is read-only and add LIMIT if missing."""
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

    # Auto-add LIMIT if not present
    if "LIMIT" not in upper:
        sql += f" LIMIT {DEFAULT_LIMIT}"

    return sql


def run_query(sql: str) -> str:
    """Execute SQL and return JSON result."""
    import psycopg2

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        return json.dumps({"error": "DATABASE_URL not set"})

    # Validate
    result = validate_sql(sql)
    if result.startswith("{"):
        return result  # Error JSON
    sql = result

    try:
        conn = psycopg2.connect(db_url)
        conn.set_session(readonly=True, autocommit=True)
        cur = conn.cursor()
        cur.execute(f"SET statement_timeout = {STATEMENT_TIMEOUT_MS}")
        cur.execute(sql)

        columns = [desc[0] for desc in cur.description] if cur.description else []
        rows = cur.fetchall()
        total_rows = len(rows)

        # Format as compact text table (not JSON) to minimize context size
        display_rows = rows[:MAX_ROWS]

        # Build text table
        lines = []
        # Header
        lines.append(" | ".join(columns))
        lines.append("-" * min(len(lines[0]), 120))
        # Rows
        for row in display_rows:
            vals = [(str(v) if v is not None else "NULL") for v in row]
            lines.append(" | ".join(vals))

        result_text = f"Rows: {total_rows} returned, {len(display_rows)} shown\\n"
        result_text += "\\n".join(lines)
        if total_rows > MAX_ROWS:
            result_text += f"\\n... ({total_rows - MAX_ROWS} more rows. Use OFFSET/LIMIT to paginate.)"

        cur.close()
        conn.close()
        return result_text

    except Exception as e:
        return json.dumps({"error": str(e)})


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(json.dumps({"error": "Usage: python3 db.py \\"SELECT ...\\""}))
        sys.exit(1)
    print(run_query(sys.argv[1]))
'''
