-- ==================================================================
-- Verify Data Load: Check row counts and table sizes
-- Run this after data load to confirm everything loaded correctly
-- ==================================================================

-- 1. Check row counts for all tables
SELECT 'fraud_reports_365d' as table_name, COUNT(*) as row_count, 
       CASE WHEN COUNT(*) = 1000000 THEN '✅ PASS' ELSE '❌ FAIL' END as status
FROM features.fraud_reports_365d
UNION ALL
SELECT 'good_rate_90d_lag_730d', COUNT(*), 
       CASE WHEN COUNT(*) = 10000000 THEN '✅ PASS' ELSE '❌ FAIL' END
FROM features.good_rate_90d_lag_730d
UNION ALL
SELECT 'request_capture_times', COUNT(*), 
       CASE WHEN COUNT(*) = 100000000 THEN '✅ PASS' ELSE '❌ FAIL' END
FROM features.request_capture_times
ORDER BY row_count;

-- 2. Check table sizes (should see actual data, not 0 bytes)
SELECT 
    tablename,
    pg_size_pretty(pg_total_relation_size('features.'||tablename)) AS total_size,
    pg_size_pretty(pg_relation_size('features.'||tablename)) AS table_size,
    pg_size_pretty(pg_indexes_size('features.'||tablename)) AS indexes_size
FROM pg_tables 
WHERE schemaname = 'features'
  AND tablename IN ('fraud_reports_365d', 'good_rate_90d_lag_730d', 'request_capture_times')
ORDER BY pg_total_relation_size('features.'||tablename) DESC;

-- 3. Sample data from each table (verify actual data exists)
SELECT 'fraud_reports_365d' as table_name, primary_key, fraud_reports_365d, updated_at
FROM features.fraud_reports_365d
LIMIT 3;

SELECT 'good_rate_90d_lag_730d' as table_name, primary_key, good_rate_90d_lag_730d, updated_at
FROM features.good_rate_90d_lag_730d
LIMIT 3;

SELECT 'request_capture_times' as table_name, primary_key, time_of_first_request, updated_at
FROM features.request_capture_times
LIMIT 3;

-- 4. Check if tables are LOGGED (should be 'LOGGED' not 'UNLOGGED')
SELECT 
    c.relname as table_name,
    CASE c.relpersistence
        WHEN 'p' THEN 'LOGGED ✅'
        WHEN 'u' THEN 'UNLOGGED ⚠️'
        WHEN 't' THEN 'TEMPORARY'
    END as persistence
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'features' 
  AND c.relkind = 'r'
  AND c.relname IN ('fraud_reports_365d', 'good_rate_90d_lag_730d', 'request_capture_times')
ORDER BY c.relname;

-- ==================================================================
-- Expected Results:
-- - Row counts: 1M, 10M, 100M
-- - Sizes: Several MB to GB (not 0 bytes)
-- - Sample data: Actual values visible
-- - Persistence: All LOGGED
-- ==================================================================

