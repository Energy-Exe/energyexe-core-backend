"""Refresh ``mv_generation_monthly_by_windfarm`` (monthly net generation per windfarm).

Runs nightly inside the pipeline task; use this CLI for the first population
after the migration, or to refresh on demand after a large generation import.

    poetry run python scripts/jobs/refresh_generation_monthly.py            # auto mode
    poetry run python scripts/jobs/refresh_generation_monthly.py --full  # plain REFRESH

Uses its own engine without the app's per-query command timeout — the refresh
scans all of generation_data. DATABASE_URL comes from the environment / .env.
"""

import argparse
import asyncio
import logging
import sys

sys.path.insert(0, ".")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--full",
        action="store_true",
        help="plain REFRESH instead of CONCURRENTLY (auto-detected for the first population)",
    )
    args = ap.parse_args()

    # structlog in standalone scripts emits nothing at info without this.
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    from app.services.generation_monthly_view import refresh_generation_monthly_view

    summary = asyncio.run(
        refresh_generation_monthly_view(concurrently=False if args.full else None)
    )
    print(summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())
