-- Retention by plan tier using a space dimension (by_range, integer)
--
-- Problem: different orgs have different retention windows. Standard
-- add_retention_policy is global per table. We want tier 0 (free) to expire
-- in 2 weeks, tier 3 (long) to survive 1 year — without row-by-row DELETEs.
--
-- Solution: add retention_tier as a second dimension using by_range with
-- interval 1. Each tier value maps to its own space slice, so each
-- (time_chunk × tier_slice) is a physically separate chunk. A background job
-- queries the internal catalog to find and DROP chunks whose entire time range
-- is past the tier's cutoff — O(1) per chunk, just like partition detach.
--
-- Tiers:
--   0 = free      → 2 weeks
--   1 = short     → 6 weeks
--   2 = default   → 6 months
--   3 = long      → 1 year
--
-- Usage:
--   psql postgres://bench:bench@localhost:5433/bench -f retention_by_plan.sql

-- ── Reset ─────────────────────────────────────────────────────────────────────

SELECT delete_job(job_id)
  FROM timescaledb_information.jobs
 WHERE proc_name = 'drop_chunks_by_tier';

DROP TABLE IF EXISTS events CASCADE;
DROP PROCEDURE IF EXISTS drop_chunks_by_tier(int, jsonb);

-- ── Hypertable + space dimension ──────────────────────────────────────────────

CREATE TABLE events (
  time            TIMESTAMPTZ NOT NULL,
  organization_id INTEGER     NOT NULL,
  retention_tier  SMALLINT    NOT NULL,  -- 0=free 1=short 2=default 3=long
  value           FLOAT       NOT NULL
);

-- Primary time dimension: 1-month chunks
SELECT create_hypertable('events', by_range('time', INTERVAL '1 month'));

-- Space dimension: integer range, interval=1 → each tier value is its own slice
--   tier 0 → slice [0, 1)
--   tier 1 → slice [1, 2)
--   tier 2 → slice [2, 3)
--   tier 3 → slice [3, 4)
-- Result: 14 months × 4 tiers = 56 independent chunks
SELECT add_dimension('events', by_range('retention_tier', 1::smallint));

-- ── Seed: 14 months back so every tier has chunks past its cutoff ─────────────

DO $$
DECLARE batch INT := 50000;
BEGIN
  FOR i IN 1..10 LOOP
    INSERT INTO events (time, organization_id, retention_tier, value)
    SELECT
      now() - (random() * 420)::int * INTERVAL '1 day',  -- up to 14 months back
      (random() * 999 + 1)::int,
      (random() * 3)::int::smallint,                      -- 25% each tier
      random() * 100
    FROM generate_series(1, batch);
  END LOOP;
END $$;

-- ── Inspect chunk structure via internal catalog ───────────────────────────────
--
-- _timescaledb_catalog.dimension_slice holds the actual range boundaries for
-- both the time dimension (stored as µs epoch) and the space dimension
-- (stored as the raw integer value for by_range).

SELECT
  format('%I.%I', c.schema_name, c.table_name)    AS chunk,
  to_timestamp(ds_t.range_start / 1e6)::date       AS time_from,
  to_timestamp(ds_t.range_end   / 1e6)::date       AS time_to,
  ds_s.range_start                                  AS tier,
  pg_size_pretty(
    pg_total_relation_size(
      format('%I.%I', c.schema_name, c.table_name)::regclass
    )
  )                                                 AS size
FROM _timescaledb_catalog.chunk c
JOIN _timescaledb_catalog.hypertable h
  ON h.id = c.hypertable_id AND h.table_name = 'events'
-- time dimension slice
JOIN _timescaledb_catalog.chunk_constraint cc_t ON cc_t.chunk_id = c.id
JOIN _timescaledb_catalog.dimension_slice  ds_t ON ds_t.id = cc_t.dimension_slice_id
JOIN _timescaledb_catalog.dimension        d_t  ON d_t.id  = ds_t.dimension_id
                                               AND d_t.column_name = 'time'
-- space dimension slice
JOIN _timescaledb_catalog.chunk_constraint cc_s ON cc_s.chunk_id = c.id
JOIN _timescaledb_catalog.dimension_slice  ds_s ON ds_s.id = cc_s.dimension_slice_id
JOIN _timescaledb_catalog.dimension        d_s  ON d_s.id  = ds_s.dimension_id
                                               AND d_s.column_name = 'retention_tier'
WHERE NOT c.dropped
ORDER BY time_from, tier;

-- ── Row count per tier before drop ────────────────────────────────────────────

SELECT
  retention_tier,
  CASE retention_tier
    WHEN 0 THEN 'free    (2 weeks)'
    WHEN 1 THEN 'short   (6 weeks)'
    WHEN 2 THEN 'default (6 months)'
    WHEN 3 THEN 'long    (1 year)'
  END                 AS window,
  count(*)            AS rows,
  min(time)::date     AS oldest,
  max(time)::date     AS newest
FROM events
GROUP BY 1, 2
ORDER BY 1;

-- ── Drop procedure ────────────────────────────────────────────────────────────
--
-- For each tier, find chunks where the entire time range is older than the
-- tier's cutoff. Because the space dimension isolates tiers, dropping a chunk
-- here never touches data from another tier.
--
-- We use DROP TABLE on the physical chunk table directly — same mechanism as
-- PostgreSQL partition detach+drop, so it is O(1) regardless of row count.

CREATE OR REPLACE PROCEDURE drop_chunks_by_tier(job_id INT, config JSONB)
LANGUAGE plpgsql AS $$
DECLARE
  tier_windows JSONB := '[
    {"tier": 0, "older_than": "2 weeks"},
    {"tier": 1, "older_than": "6 weeks"},
    {"tier": 2, "older_than": "6 months"},
    {"tier": 3, "older_than": "1 year"}
  ]';
  entry      JSONB;
  tier_val   BIGINT;
  cutoff     TIMESTAMPTZ;
  cutoff_us  BIGINT;
  chunk_tbl  TEXT;
  n          INT := 0;
BEGIN
  FOR entry IN SELECT * FROM jsonb_array_elements(tier_windows) LOOP
    tier_val  := (entry->>'tier')::bigint;
    cutoff    := now() - (entry->>'older_than')::interval;
    cutoff_us := extract(epoch FROM cutoff)::bigint * 1000000;

    FOR chunk_tbl IN
      SELECT format('%I.%I', c.schema_name, c.table_name)
      FROM _timescaledb_catalog.chunk c
      JOIN _timescaledb_catalog.hypertable h
        ON h.id = c.hypertable_id AND h.table_name = 'events'
      -- time slice: chunk ends before cutoff (entire chunk is expired)
      JOIN _timescaledb_catalog.chunk_constraint cc_t ON cc_t.chunk_id = c.id
      JOIN _timescaledb_catalog.dimension_slice  ds_t ON ds_t.id = cc_t.dimension_slice_id
      JOIN _timescaledb_catalog.dimension        d_t  ON d_t.id = ds_t.dimension_id
                                                     AND d_t.column_name = 'time'
      -- space slice: matches exactly this tier
      JOIN _timescaledb_catalog.chunk_constraint cc_s ON cc_s.chunk_id = c.id
      JOIN _timescaledb_catalog.dimension_slice  ds_s ON ds_s.id = cc_s.dimension_slice_id
      JOIN _timescaledb_catalog.dimension        d_s  ON d_s.id = ds_s.dimension_id
                                                     AND d_s.column_name = 'retention_tier'
      WHERE NOT c.dropped
        AND ds_t.range_end  <= cutoff_us   -- whole chunk is past the cutoff
        AND ds_s.range_start = tier_val    -- only this tier's slice
    LOOP
      RAISE NOTICE 'tier %: dropping % (older than %)', tier_val, chunk_tbl, cutoff::date;
      EXECUTE format('DROP TABLE %s', chunk_tbl);
      n := n + 1;
    END LOOP;
  END LOOP;

  RAISE NOTICE 'Total chunks dropped: %', n;
END;
$$;

-- ── Register as a daily background job ───────────────────────────────────────

SELECT add_job('drop_chunks_by_tier', INTERVAL '1 day');

-- ── Run immediately to verify ─────────────────────────────────────────────────

CALL drop_chunks_by_tier(null, null);

-- ── Row count per tier after drop ─────────────────────────────────────────────

SELECT
  retention_tier,
  count(*)        AS rows,
  min(time)::date AS oldest,
  max(time)::date AS newest
FROM events
GROUP BY 1
ORDER BY 1;

-- ── Remaining chunks ─────────────────────────────────────────────────────────

SELECT
  format('%I.%I', c.schema_name, c.table_name) AS chunk,
  to_timestamp(ds_t.range_start / 1e6)::date    AS time_from,
  to_timestamp(ds_t.range_end   / 1e6)::date    AS time_to,
  ds_s.range_start                               AS tier
FROM _timescaledb_catalog.chunk c
JOIN _timescaledb_catalog.hypertable h
  ON h.id = c.hypertable_id AND h.table_name = 'events'
JOIN _timescaledb_catalog.chunk_constraint cc_t ON cc_t.chunk_id = c.id
JOIN _timescaledb_catalog.dimension_slice  ds_t ON ds_t.id = cc_t.dimension_slice_id
JOIN _timescaledb_catalog.dimension        d_t  ON d_t.id = ds_t.dimension_id
                                               AND d_t.column_name = 'time'
JOIN _timescaledb_catalog.chunk_constraint cc_s ON cc_s.chunk_id = c.id
JOIN _timescaledb_catalog.dimension_slice  ds_s ON ds_s.id = cc_s.dimension_slice_id
JOIN _timescaledb_catalog.dimension        d_s  ON d_s.id = ds_s.dimension_id
                                               AND d_s.column_name = 'retention_tier'
WHERE NOT c.dropped
ORDER BY time_from, tier;
