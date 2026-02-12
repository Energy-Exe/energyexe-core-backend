#!/usr/bin/env python3
import asyncio, asyncpg, sys
sys.path.append(".")
from app.core.config import get_settings

async def check():
    settings = get_settings()
    dsn = str(settings.DATABASE_URL).replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(dsn)

    locks = await conn.fetch("""
        SELECT pid, state, wait_event_type, wait_event,
               query_start, LEFT(query, 120) as query_snippet
        FROM pg_stat_activity
        WHERE state != 'idle'
        AND query NOT LIKE '%pg_stat_activity%'
        ORDER BY query_start
    """)

    print(f"Active queries: {len(locks)}")
    for r in locks:
        qs = str(r["query_start"])[:19] if r["query_start"] else "NULL"
        pid = r["pid"]
        state = r["state"]
        we_type = r["wait_event_type"]
        we = r["wait_event"]
        snippet = r["query_snippet"]
        print(f"  PID={pid} state={state} wait={we_type}:{we} started={qs}")
        print(f"    {snippet}")

    # Check for lock waits
    lockwaits = await conn.fetch("""
        SELECT blocked_locks.pid AS blocked_pid,
               blocking_locks.pid AS blocking_pid
        FROM pg_catalog.pg_locks blocked_locks
        JOIN pg_catalog.pg_locks blocking_locks
            ON blocking_locks.locktype = blocked_locks.locktype
            AND blocking_locks.database IS NOT DISTINCT FROM blocked_locks.database
            AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
            AND blocking_locks.pid != blocked_locks.pid
        WHERE NOT blocked_locks.granted
        LIMIT 10
    """)
    print(f"\nBlocked queries: {len(lockwaits)}")
    for r in lockwaits:
        bp = r["blocked_pid"]
        blk = r["blocking_pid"]
        print(f"  blocked_pid={bp} by blocking_pid={blk}")

    await conn.close()

asyncio.run(check())
