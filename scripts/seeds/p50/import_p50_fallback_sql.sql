-- P50 fallback import — pure SQL version (runs on DB server, no round-trips).
--
-- For each operational windfarm without a P50 target, compute the mean of the
-- last 3 full calendar years of actual generation (where each year has >= 350
-- days of data). Insert into p50_targets as a fallback estimate.
--
-- Run via: psql $DATABASE_URL -f scripts/seeds/p50/import_p50_fallback_sql.sql
-- Or via: poetry run python -c "..." wrapping asyncpg.execute()
--
-- This replaces the Python per-windfarm loop that times out over remote
-- connections. The DB server computes everything in one pass.

-- Step 1: Preview (dry run) — shows what would be inserted
WITH yearly_gen AS (
    SELECT
        windfarm_id,
        EXTRACT(YEAR FROM hour)::int AS yr,
        SUM(generation_mwh) / 1000.0 AS gwh,
        COUNT(DISTINCT DATE_TRUNC('day', hour)) AS day_count
    FROM generation_data
    WHERE generation_mwh IS NOT NULL
      AND EXTRACT(YEAR FROM hour) BETWEEN (EXTRACT(YEAR FROM CURRENT_DATE) - 3)
                                       AND (EXTRACT(YEAR FROM CURRENT_DATE) - 1)
    GROUP BY windfarm_id, EXTRACT(YEAR FROM hour)
    HAVING COUNT(DISTINCT DATE_TRUNC('day', hour)) >= 350
),
windfarm_avg AS (
    SELECT
        windfarm_id,
        ROUND(AVG(gwh)::numeric, 3) AS mean_gwh,
        MAX(yr) AS latest_year,
        COUNT(*) AS years_used
    FROM yearly_gen
    GROUP BY windfarm_id
    HAVING COUNT(*) >= 2  -- need at least 2 complete years
),
missing_wf AS (
    SELECT w.id
    FROM windfarms w
    WHERE w.lat IS NOT NULL AND w.lng IS NOT NULL
      AND w.status = 'operational'
      AND NOT EXISTS (SELECT 1 FROM p50_targets p WHERE p.windfarm_id = w.id)
)
SELECT
    a.windfarm_id,
    a.mean_gwh AS p50_target_volume_gwh,
    a.latest_year,
    a.years_used
FROM windfarm_avg a
JOIN missing_wf m ON m.id = a.windfarm_id
ORDER BY a.windfarm_id;

-- Step 2: Actual insert (uncomment to apply)
-- INSERT INTO p50_targets
--     (windfarm_id, p50_target_start_date, p50_target_end_date,
--      p50_target_volume_gwh, source, comment)
-- SELECT
--     a.windfarm_id,
--     MAKE_DATE(a.latest_year, 1, 1),
--     NULL,
--     a.mean_gwh,
--     'fallback computed (3-yr historical mean)',
--     FORMAT('auto-computed from %s-%s actual generation; replace with owner-provided P50 when available',
--            a.latest_year - 2, a.latest_year)
-- FROM windfarm_avg a
-- JOIN missing_wf m ON m.id = a.windfarm_id
-- ON CONFLICT (windfarm_id, p50_target_start_date) DO NOTHING;
