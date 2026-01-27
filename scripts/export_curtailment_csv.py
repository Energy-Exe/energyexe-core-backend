#!/usr/bin/env python3
"""
Export monthly curtailment data for all windfarms (2020-2025) to CSV.

Usage:
    poetry run python scripts/export_curtailment_csv.py
    poetry run python scripts/export_curtailment_csv.py --output /path/to/file.csv
"""

import csv
import argparse
import os
from datetime import datetime

import psycopg2
from dotenv import load_dotenv

load_dotenv()

QUERY = """
WITH curtailed_windfarms AS (
    SELECT DISTINCT windfarm_id
    FROM generation_data
    WHERE curtailed_mwh > 0
      AND hour >= '2020-01-01'
      AND hour < '2026-01-01'
)
SELECT
    w.name AS windfarm_name,
    c.name AS country,
    w.nameplate_capacity_mw,
    to_char(date_trunc('month', gd.hour), 'YYYY-MM') AS month,
    ROUND(SUM(gd.generation_mwh)::numeric, 3) AS generation_mwh,
    ROUND(COALESCE(SUM(gd.metered_mwh), SUM(gd.generation_mwh))::numeric, 3) AS metered_mwh,
    ROUND(COALESCE(SUM(gd.curtailed_mwh), 0)::numeric, 3) AS curtailed_mwh,
    ROUND(
        CASE
            WHEN SUM(gd.generation_mwh) > 0
            THEN COALESCE(SUM(gd.curtailed_mwh), 0) / SUM(gd.generation_mwh) * 100
            ELSE 0
        END::numeric, 2
    ) AS curtailment_pct,
    COUNT(gd.id) AS data_points
FROM generation_data gd
JOIN windfarms w ON gd.windfarm_id = w.id
JOIN curtailed_windfarms cw ON gd.windfarm_id = cw.windfarm_id
LEFT JOIN countries c ON w.country_id = c.id
WHERE gd.hour >= '2020-01-01'
  AND gd.hour < '2026-01-01'
GROUP BY w.name, c.name, w.nameplate_capacity_mw,
         date_trunc('month', gd.hour)
ORDER BY w.name, month
"""


def get_db_url():
    raw = os.getenv("DATABASE_URL", "")
    # Convert async URL to sync for psycopg2
    return raw.replace("postgresql+asyncpg://", "postgresql://")


def main():
    parser = argparse.ArgumentParser(description="Export curtailment data to CSV")
    parser.add_argument(
        "--output",
        default="curtailment_monthly_2020_2025.csv",
        help="Output CSV path (default: curtailment_monthly_2020_2025.csv)",
    )
    args = parser.parse_args()

    db_url = get_db_url()
    if not db_url:
        print("ERROR: DATABASE_URL not set in .env")
        return

    print(f"Connecting to database...")
    conn = psycopg2.connect(db_url)

    try:
        cur = conn.cursor()
        print("Running query...")
        cur.execute(QUERY)
        rows = cur.fetchall()
        headers = [
            "windfarm_name",
            "country",
            "nameplate_capacity_mw",
            "month",
            "generation_mwh",
            "metered_mwh",
            "curtailed_mwh",
            "curtailment_pct",
            "data_points",
        ]

        with open(args.output, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(rows)

        # Summary stats
        windfarms = set()
        total_curtailed = 0
        for row in rows:
            windfarms.add(row[0])
            total_curtailed += float(row[6] or 0)

        print(f"\nExported {len(rows)} rows to {args.output}")
        print(f"Windfarms with curtailment: {len(windfarms)}")
        print(f"Total curtailed: {total_curtailed:,.1f} MWh ({total_curtailed/1000:,.1f} GWh)")
        print(f"Date range: 2020-01 to 2025-12")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
