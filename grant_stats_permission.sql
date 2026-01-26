-- Grant permission to reset PostgreSQL statistics
-- Run this as a superuser (admin user) in Lakebase

-- Option 1: Grant pg_stat_reset execution to specific user
GRANT EXECUTE ON FUNCTION pg_stat_reset() TO fraud_benchmark_user;

-- Option 2: Grant read access to pg_stat functions (safer, read-only)
GRANT pg_monitor TO fraud_benchmark_user;

-- Verify the grants
SELECT 
    grantee, 
    privilege_type 
FROM information_schema.role_routine_grants 
WHERE routine_name = 'pg_stat_reset';
