-- ============================================================================
-- Setup Lakebase for Fraud Benchmarking (New Workspace)
-- ============================================================================
-- Run this in Lakebase SQL Editor with admin/superuser account
-- ============================================================================

-- Step 1: Create the benchmark user
CREATE USER fraud_benchmark_user WITH PASSWORD 'YOUR_PASSWORD_HERE';

-- Step 2: Create the database (if it doesn't exist)
CREATE DATABASE IF NOT EXISTS benchmark;

-- Step 3: Connect to benchmark database
\c benchmark

-- Step 4: Create the schema
CREATE SCHEMA IF NOT EXISTS features;

-- Step 5: Grant permissions to fraud_benchmark_user
GRANT CONNECT ON DATABASE benchmark TO fraud_benchmark_user;
GRANT USAGE ON SCHEMA features TO fraud_benchmark_user;
GRANT CREATE ON SCHEMA features TO fraud_benchmark_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA features TO fraud_benchmark_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA features TO fraud_benchmark_user;

-- Make sure future objects are also accessible
ALTER DEFAULT PRIVILEGES IN SCHEMA features 
GRANT ALL ON TABLES TO fraud_benchmark_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA features 
GRANT ALL ON SEQUENCES TO fraud_benchmark_user;

-- Step 6: Verify setup
SELECT 
    'User created' as status,
    usename,
    usesuper as is_superuser,
    usecreatedb as can_create_db
FROM pg_user 
WHERE usename = 'fraud_benchmark_user';

-- Check schema permissions
SELECT 
    nspname as schema_name,
    has_schema_privilege('fraud_benchmark_user', nspname, 'USAGE') as has_usage,
    has_schema_privilege('fraud_benchmark_user', nspname, 'CREATE') as has_create
FROM pg_namespace 
WHERE nspname = 'features';

-- ============================================================================
-- ✅ Setup complete! 
-- 
-- Next steps:
-- 1. Update databricks.yml with new workspace URL
-- 2. Update bundle variables with new Lakebase host
-- 3. Deploy: databricks bundle deploy -t dev
-- 4. Run load: databricks bundle run fraud_load_all_30_tables -t dev
-- ============================================================================
