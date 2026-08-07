"""One-off: collapse EPR-98 duplicate turns baked into saved agent threads.

The duplicate-send storm (fixed 2026-08-07) wrote the same user turn into live
SDK sessions repeatedly; threads persisted before the guard landed carry those
adjacent identical message pairs. This applies the same rule as the service's
_drop_consecutive_duplicates (adjacent same-type, same-content, no toolCalls)
to agent_threads.messages and fixes message_count.

Run:
    poetry run python scripts/cleanup_agent_thread_duplicates.py            # dry-run (default)
    poetry run python scripts/cleanup_agent_thread_duplicates.py --apply    # write changes

DATABASE_URL decides the target (local/.env by default; export the staging URL
to clean staging).
"""

import argparse
import asyncio

from sqlalchemy import select

from app.core.database import get_session_factory
from app.models.agent_thread import AgentThread
from app.services.brain_agent_service import BrainAgentService


async def main(apply: bool) -> None:
    factory = get_session_factory()
    async with factory() as db:
        threads = (await db.execute(select(AgentThread))).scalars().all()

        touched = 0
        for thread in threads:
            messages = thread.messages or []
            deduped = BrainAgentService._drop_consecutive_duplicates(messages)
            if len(deduped) == len(messages):
                continue
            touched += 1
            print(
                f"{'APPLY' if apply else 'DRY  '} thread={thread.id} "
                f"title={(thread.title or '')[:40]!r} "
                f"messages {len(messages)} -> {len(deduped)}"
            )
            if apply:
                thread.messages = deduped
                thread.message_count = len(deduped)

        if apply:
            await db.commit()

        print(
            f"\n{touched}/{len(threads)} threads had adjacent duplicates"
            + ("" if apply else " (dry-run — nothing written; re-run with --apply)")
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write the deduplicated messages back (default is dry-run)",
    )
    args = parser.parse_args()
    asyncio.run(main(apply=args.apply))
