-- ============================================================================
-- Test RPC Function - Run this BEFORE deploying to benchmark
-- ============================================================================

-- Step 1: Check if function exists
SELECT 
    proname AS function_name,
    pg_get_function_arguments(oid) AS arguments,
    pg_get_function_result(oid) AS returns
FROM pg_proc 
WHERE proname = 'fetch_request_features' 
  AND pronamespace = 'features'::regnamespace;

-- Step 2: Get sample keys from actual data
SELECT 'card_fingerprint' AS entity, hash_key 
FROM features.client_id_card_fingerprint__fraud_rates__30d 
LIMIT 1

UNION ALL

SELECT 'customer_email' AS entity, hash_key
FROM features.client_id_customer_email_clean__fraud_rates__30d
LIMIT 1

UNION ALL

SELECT 'cardholder_name' AS entity, hash_key
FROM features.client_id_cardholder_name__fraud_rates__30d
LIMIT 1;

-- Step 3: Test function with sample keys (REPLACE with actual keys from Step 2)
-- IMPORTANT: Replace 'SAMPLE_KEY_1', 'SAMPLE_KEY_2', 'SAMPLE_KEY_3' with real keys
SELECT features.fetch_request_features(
    'SAMPLE_KEY_1',  -- card_fingerprint key
    'SAMPLE_KEY_2',  -- customer_email key  
    'SAMPLE_KEY_3'   -- cardholder_name key
) AS result;

-- Step 4: Verify structure (should return JSONB with 3 entity keys)
WITH test_result AS (
    SELECT features.fetch_request_features(
        'SAMPLE_KEY_1',
        'SAMPLE_KEY_2',
        'SAMPLE_KEY_3'
    ) AS result
)
SELECT 
    jsonb_object_keys(result) AS entity_keys,
    jsonb_typeof(result -> jsonb_object_keys(result)) AS value_type
FROM test_result;

-- Step 5: Check payload size (should be similar to current mode)
WITH test_result AS (
    SELECT features.fetch_request_features(
        'SAMPLE_KEY_1',
        'SAMPLE_KEY_2',
        'SAMPLE_KEY_3'
    ) AS result
)
SELECT 
    length(result::text) AS payload_bytes,
    jsonb_pretty(result) AS formatted_result
FROM test_result;

-- Step 6: Performance test (run 10 times, measure timing)
EXPLAIN (ANALYZE, BUFFERS)
SELECT features.fetch_request_features(
    'SAMPLE_KEY_1',
    'SAMPLE_KEY_2',
    'SAMPLE_KEY_3'
);
