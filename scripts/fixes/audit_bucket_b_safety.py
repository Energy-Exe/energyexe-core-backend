"""Safety audit before deleting Bucket B (empty-scaffolding inactive units).

Steps:
  1. Pull the candidate set: inactive units with 0 generation_data rows
     (excluding already-handled ids).
  2. Auto-discover every table that has a FK to generation_units via pg_catalog.
  3. For each FK table, count how many candidate-unit ids are referenced.
  4. Show:
     - how many candidates are 'clean' (no references anywhere)
     - how many are blocked (and by which table)
     - created_at / updated_at distribution
     - whether any candidate has an active mapping row that a cron could re-write to
"""
import asyncio
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sqlalchemy import text
from app.core.database import get_session_factory


HANDLED = {12385, 12328, 12361, 12346, 12348, 12349, 12350, 12351,
           12806, 12388, 12389, 12797, 12801, 12802}


def banner(t):
    print()
    print("=" * 100)
    print(t)
    print("=" * 100)


async def main():
    S = get_session_factory()
    async with S() as db:
        # 1. Candidate set: is_active=False, 0 rows in generation_data
        rs = await db.execute(text("""
            SELECT gu.id, gu.source, gu.name, gu.code, gu.windfarm_id,
                   gu.created_at, gu.updated_at
            FROM generation_units gu
            WHERE gu.is_active = FALSE
              AND NOT EXISTS (SELECT 1 FROM generation_data gd WHERE gd.generation_unit_id = gu.id)
              AND gu.id <> ALL(:handled)
        """), {"handled": list(HANDLED)})
        cands = list(rs)
        cand_ids = [c.id for c in cands]
        print(f"Candidate units (inactive, 0 gen_data, not handled): {len(cands)}")
        if not cands:
            return

        # 2. Discover all FK tables referencing generation_units
        banner("Tables with FK → generation_units")
        rs = await db.execute(text("""
            SELECT
              tc.table_schema, tc.table_name, kcu.column_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema    = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
              ON ccu.constraint_name = tc.constraint_name
             AND ccu.table_schema    = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND ccu.table_name = 'generation_units'
            ORDER BY tc.table_name, kcu.column_name
        """))
        fk_tables = [(r.table_schema, r.table_name, r.column_name) for r in rs]
        for s, t, c in fk_tables:
            print(f"  {s}.{t}.{c}")

        # 3. For each FK table, count how many candidate ids are referenced
        banner("Reference counts per FK table")
        blocking_per_unit = defaultdict(list)  # unit_id -> list of table refs
        for schema, tbl, col in fk_tables:
            rs = await db.execute(
                text(f"SELECT COUNT(*) FROM {schema}.{tbl} WHERE {col} = ANY(:ids)"),
                {"ids": cand_ids},
            )
            n = rs.scalar()
            print(f"  {schema}.{tbl}.{col}: {n:,} rows reference candidates")
            if n > 0:
                rs2 = await db.execute(
                    text(f"SELECT DISTINCT {col} FROM {schema}.{tbl} WHERE {col} = ANY(:ids)"),
                    {"ids": cand_ids},
                )
                for r in rs2:
                    blocking_per_unit[getattr(r, col)].append(f"{tbl}")

        clean = [c for c in cands if c.id not in blocking_per_unit]
        blocked = [c for c in cands if c.id in blocking_per_unit]
        banner(f"SUMMARY: {len(clean)} clean | {len(blocked)} have references")

        # 4. Drill on blocked: which table is blocking, and is it active?
        if blocked:
            print("\nBlocked units — what's referencing them:")
            ref_counts = defaultdict(int)
            for u in blocked:
                for t in blocking_per_unit[u.id]:
                    ref_counts[t] += 1
            for t, n in sorted(ref_counts.items(), key=lambda kv: -kv[1]):
                print(f"  blocked in {t}: {n} units")

            # Special focus: generation_unit_mapping
            print("\nMapping-row inspection (would a cron re-write to these units?):")
            rs = await db.execute(text("""
                SELECT gum.generation_unit_id, gum.source, gum.source_identifier,
                       gum.windfarm_id, gum.is_active,
                       gu.name
                FROM generation_unit_mapping gum
                JOIN generation_units gu ON gu.id = gum.generation_unit_id
                WHERE gum.generation_unit_id = ANY(:ids)
                ORDER BY gum.is_active DESC, gum.generation_unit_id
                LIMIT 40
            """), {"ids": [u.id for u in blocked]})
            mapped = list(rs)
            active_maps = sum(1 for r in mapped if r.is_active)
            print(f"  {len(mapped)} mapping rows shown (limit 40); active={active_maps}")
            for r in mapped[:20]:
                print(f"    unit={r.generation_unit_id} src={r.source} "
                      f"ident='{r.source_identifier[:30]}' active={r.is_active} "
                      f"wf={r.windfarm_id} name='{r.name[:35]}'")

        # 5. Clean-bucket characteristics
        banner("Clean-bucket profile (safe-to-delete candidates)")
        by_src = defaultdict(int)
        for u in clean:
            by_src[u.source] += 1
        for k, v in sorted(by_src.items()):
            print(f"  {k}: {v}")

        # Created_at age distribution
        print("\n  created_at year distribution:")
        years = defaultdict(int)
        for u in clean:
            y = str(u.created_at)[:4] if u.created_at else "unknown"
            years[y] += 1
        for k, v in sorted(years.items()):
            print(f"    {k}: {v}")

        print("\n  Sample of 'clean' units:")
        for u in clean[:15]:
            print(f"    {u.id:>5} {u.source:<10} '{u.name[:40]:<42}' code={(u.code or '')[:10]:<10} "
                  f"wf={u.windfarm_id}")


asyncio.run(main())
