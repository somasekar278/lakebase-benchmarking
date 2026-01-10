-- ==================================================================
-- Pre-Flight Checklist: Verify all setup before benchmarking
-- Run this to ensure everything is ready
-- ==================================================================

\echo '=================================================='
\echo 'PRE-FLIGHT CHECKLIST FOR LAKEBASE BENCHMARK'
\echo '=================================================='

\echo '\n1. Checking schema exists...'
SELECT 
    CASE 
        WHEN COUNT(*) = 1 THEN '✅ features schema exists'
        ELSE '❌ features schema missing'
    END as status
FROM information_schema.schemata 
WHERE schema_name = 'features';

\echo '\n2. Checking user exists...'
SELECT 
    CASE 
        WHEN COUNT(*) = 1 THEN '✅ fraud_benchmark_user exists'
        ELSE '❌ fraud_benchmark_user missing'
    END as status
FROM pg_user 
WHERE usename = 'fraud_benchmark_user';

\echo '\n3. Checking tables exist...'
SELECT 
    table_name,
    CASE 
        WHEN table_name IS NOT NULL THEN '✅ exists'
        ELSE '❌ missing'
    END as status
FROM information_schema.tables 
WHERE table_schema = 'features' 
  AND table_name IN ('fraud_reports_365d', 'good_rate_90d_lag_730d', 'request_capture_times', 'distinct_counts_amount_stats_365d')
ORDER BY table_name;

\echo '\n4. Checking table permissions...'
SELECT 
    table_name,
    STRING_AGG(privilege_type, ', ' ORDER BY privilege_type) as privileges
FROM information_schema.table_privileges
WHERE grantee = 'fraud_benchmark_user' 
  AND table_schema = 'features'
  AND table_name IN ('fraud_reports_365d', 'good_rate_90d_lag_730d', 'request_capture_times')
GROUP BY table_name
ORDER BY table_name;

\echo '\n5. Checking schema CREATE permission...'
SELECT 
    nspname as schema_name,
    CASE 
        WHEN has_schema_privilege('fraud_benchmark_user', nspname, 'CREATE') THEN '✅ has CREATE'
        ELSE '❌ missing CREATE'
    END as create_permission
FROM pg_namespace 
WHERE nspname = 'features';

\echo '\n6. Checking data loaded...'
SELECT 
    'fraud_reports_365d' as table_name,
    COUNT(*) as row_count,
    CASE 
        WHEN COUNT(*) = 1000000 THEN '✅ correct'
        WHEN COUNT(*) > 0 THEN '⚠️  partial'
        ELSE '❌ empty'
    END as status
FROM features.fraud_reports_365d
UNION ALL
SELECT 
    'good_rate_90d_lag_730d',
    COUNT(*),
    CASE 
        WHEN COUNT(*) = 10000000 THEN '✅ correct'
        WHEN COUNT(*) > 0 THEN '⚠️  partial'
        ELSE '❌ empty'
    END
FROM features.good_rate_90d_lag_730d
UNION ALL
SELECT 
    'request_capture_times',
    COUNT(*),
    CASE 
        WHEN COUNT(*) = 100000000 THEN '✅ correct'
        WHEN COUNT(*) > 0 THEN '⚠️  partial'
        ELSE '❌ empty'
    END
FROM features.request_capture_times;

\echo '\n7. Checking tables are LOGGED (not UNLOGGED)...'
SELECT 
    c.relname as table_name,
    CASE c.relpersistence
        WHEN 'p' THEN '✅ LOGGED'
        WHEN 'u' THEN '⚠️  UNLOGGED (data may be lost on restart!)'
        WHEN 't' THEN '❌ TEMPORARY'
    END as persistence
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'features' 
  AND c.relkind = 'r'
  AND c.relname IN ('fraud_reports_365d', 'good_rate_90d_lag_730d', 'request_capture_times')
ORDER BY c.relname;

\echo '\n=================================================='
\echo 'CHECKLIST COMPLETE'
\echo '=================================================='
\echo '\nIf all items show ✅, you are ready to:'
\echo '  1. python setup_stored_proc.py'
\echo '  2. python test_lakebase_detailed.py'
\echo '\n'

-- ==================================================================
-- Expected Results: All items should show ✅
-- If any show ❌ or ⚠️, fix those before proceeding
-- ==================================================================

