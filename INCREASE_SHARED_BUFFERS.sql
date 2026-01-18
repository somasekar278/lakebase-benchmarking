-- ============================================================================
-- Increase PostgreSQL shared_buffers for Better Cache Performance
-- ============================================================================
-- 
-- Current: ~8-16 GB (PostgreSQL default)
-- Target:  20 GB (25-30% of 64GB RAM)
--
-- Expected Impact:
-- - Hot queries:  No change (~15ms already optimal)
-- - Cold queries: 90ms → 70-75ms (better cache for cold data)
-- - Blended P99:  84ms → 70-75ms (beats DynamoDB's 79ms!)
--
-- Requires: Cluster restart
-- ============================================================================

-- Run this in Lakebase SQL Editor:

ALTER SYSTEM SET shared_buffers = '20GB';

-- Then restart the Lakebase cluster (required for this setting)

-- To verify after restart:
SHOW shared_buffers;
-- Should show: 20GB

-- To check current effective cache size:
SHOW effective_cache_size;
-- Recommended: Set to 48GB (75% of RAM)

ALTER SYSTEM SET effective_cache_size = '48GB';

-- This tells the query planner how much total cache is available
-- (shared_buffers + OS page cache)
