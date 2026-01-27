-- ===================================================================
-- Get Last Successful Benchmark Run ID (for Key Reuse)
-- ===================================================================
-- This query finds the most recent complete benchmark run that has
-- persisted keys in zipfian_keys_per_run (so keys can be reused).
--
-- Usage: Copy the run_id and use it with the "reuse_run_id" parameter
-- ===================================================================

-- OPTION 1: Most recent run with persisted keys (RECOMMENDED)
-- This ensures the run completed the key sampling phase
SELECT DISTINCT k.run_id,
       COUNT(*) OVER (PARTITION BY k.run_id) as key_count,
       MIN(r.run_ts) OVER (PARTITION BY k.run_id) as run_timestamp
FROM features.zipfian_keys_per_run k
LEFT JOIN features.zipfian_feature_serving_results_v5 r 
  ON k.run_id = r.run_id
GROUP BY k.run_id, k.entity_name, k.key_type
ORDER BY run_timestamp DESC NULLS LAST
LIMIT 1;

-- OPTION 2: Most recent run with complete results (all modes)
-- This ensures the run finished all execution modes
WITH run_stats AS (
  SELECT 
    run_id,
    MIN(run_ts) as first_ts,
    MAX(run_ts) as last_ts,
    COUNT(DISTINCT fetch_mode) as mode_count,
    COUNT(*) as result_rows,
    ARRAY_AGG(DISTINCT fetch_mode) as modes
  FROM features.zipfian_feature_serving_results_v5
  WHERE run_ts IS NOT NULL
  GROUP BY run_id
),
runs_with_keys AS (
  SELECT 
    rs.*,
    CASE WHEN k.run_id IS NOT NULL THEN true ELSE false END as has_keys
  FROM run_stats rs
  LEFT JOIN (
    SELECT DISTINCT run_id FROM features.zipfian_keys_per_run
  ) k ON rs.run_id = k.run_id
)
SELECT 
  run_id,
  last_ts as run_timestamp,
  mode_count,
  result_rows,
  modes,
  has_keys
FROM runs_with_keys
WHERE has_keys = true  -- Only runs with persisted keys
ORDER BY last_ts DESC
LIMIT 1;

-- OPTION 3: Quick check - just the latest run_id with keys
-- Fast query if you just need the ID
SELECT run_id
FROM features.zipfian_keys_per_run
GROUP BY run_id
ORDER BY MIN(CAST(run_id AS STRING)) DESC  -- UUID-safe ordering
LIMIT 1;

-- ===================================================================
-- VERIFICATION: Check if a specific run_id has keys
-- ===================================================================
-- Replace '<run_id>' with the actual run_id you want to check
-- Example: SELECT COUNT(*) FROM features.zipfian_keys_per_run WHERE run_id = 'a1b2c3d4';

-- Verify key counts per entity
SELECT 
  entity_name,
  key_type,
  COUNT(*) as key_count
FROM features.zipfian_keys_per_run
WHERE run_id = '<run_id>'  -- Replace with actual run_id
GROUP BY entity_name, key_type
ORDER BY entity_name, key_type;
