-- ============================================================================
-- RPC Request Function: Collapse entire multi-entity request into one call
-- ============================================================================
-- This function fetches all features for a 3-entity request in a single
-- server-side call, using the same bin-packing logic (UNION ALL by feature_type)
-- but with zero client-side overhead.
--
-- Returns JSONB structure:
-- {
--   "card_fingerprint": {
--     "fraud_rates": [...],
--     "time_since": [...],
--     "good_rates": [...]
--   },
--   "customer_email": {...},
--   "cardholder_name": {...}
-- }
-- ============================================================================

CREATE OR REPLACE FUNCTION features.fetch_request_features(
    p_card_key TEXT,
    p_email_key TEXT,
    p_name_key TEXT
)
RETURNS JSONB
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = features, pg_catalog
AS $$
DECLARE
    v_result JSONB := '{}'::jsonb;
    v_entity_result JSONB;
    v_group_result JSONB;
BEGIN
    -- ========================================
    -- Entity 1: card_fingerprint
    -- ========================================
    v_entity_result := '{}'::jsonb;
    
    -- fraud_rates group
    SELECT jsonb_agg(to_jsonb(t))
    INTO v_group_result
    FROM (
        SELECT *, 'client_id_card_fingerprint__fraud_rates__30d' AS _table
        FROM features.client_id_card_fingerprint__fraud_rates__30d
        WHERE hash_key = p_card_key
        UNION ALL
        SELECT *, 'client_id_card_fingerprint__fraud_rates__90d' AS _table
        FROM features.client_id_card_fingerprint__fraud_rates__90d
        WHERE hash_key = p_card_key
        UNION ALL
        SELECT *, 'client_id_card_fingerprint__fraud_rates__365d' AS _table
        FROM features.client_id_card_fingerprint__fraud_rates__365d
        WHERE hash_key = p_card_key
    ) t;
    v_entity_result := jsonb_set(v_entity_result, '{fraud_rates}', COALESCE(v_group_result, '[]'::jsonb));
    
    -- time_since group
    SELECT jsonb_agg(to_jsonb(t))
    INTO v_group_result
    FROM (
        SELECT *, 'client_id_card_fingerprint__time_since__30d' AS _table
        FROM features.client_id_card_fingerprint__time_since__30d
        WHERE hash_key = p_card_key
        UNION ALL
        SELECT *, 'client_id_card_fingerprint__time_since__90d' AS _table
        FROM features.client_id_card_fingerprint__time_since__90d
        WHERE hash_key = p_card_key
        UNION ALL
        SELECT *, 'client_id_card_fingerprint__time_since__365d' AS _table
        FROM features.client_id_card_fingerprint__time_since__365d
        WHERE hash_key = p_card_key
    ) t;
    v_entity_result := jsonb_set(v_entity_result, '{time_since}', COALESCE(v_group_result, '[]'::jsonb));
    
    -- good_rates group
    SELECT jsonb_agg(to_jsonb(t))
    INTO v_group_result
    FROM (
        SELECT *, 'client_id_card_fingerprint__good_rates__30d' AS _table
        FROM features.client_id_card_fingerprint__good_rates__30d
        WHERE hash_key = p_card_key
        UNION ALL
        SELECT *, 'client_id_card_fingerprint__good_rates__90d' AS _table
        FROM features.client_id_card_fingerprint__good_rates__90d
        WHERE hash_key = p_card_key
        UNION ALL
        SELECT *, 'client_id_card_fingerprint__good_rates__365d' AS _table
        FROM features.client_id_card_fingerprint__good_rates__365d
        WHERE hash_key = p_card_key
    ) t;
    v_entity_result := jsonb_set(v_entity_result, '{good_rates}', COALESCE(v_group_result, '[]'::jsonb));
    
    v_result := jsonb_set(v_result, '{card_fingerprint}', v_entity_result);
    
    -- ========================================
    -- Entity 2: customer_email
    -- ========================================
    v_entity_result := '{}'::jsonb;
    
    -- fraud_rates group
    SELECT jsonb_agg(to_jsonb(t))
    INTO v_group_result
    FROM (
        SELECT *, 'client_id_customer_email_clean__fraud_rates__30d' AS _table
        FROM features.client_id_customer_email_clean__fraud_rates__30d
        WHERE hash_key = p_email_key
        UNION ALL
        SELECT *, 'client_id_customer_email_clean__fraud_rates__90d' AS _table
        FROM features.client_id_customer_email_clean__fraud_rates__90d
        WHERE hash_key = p_email_key
        UNION ALL
        SELECT *, 'client_id_customer_email_clean__fraud_rates__365d' AS _table
        FROM features.client_id_customer_email_clean__fraud_rates__365d
        WHERE hash_key = p_email_key
    ) t;
    v_entity_result := jsonb_set(v_entity_result, '{fraud_rates}', COALESCE(v_group_result, '[]'::jsonb));
    
    -- time_since group
    SELECT jsonb_agg(to_jsonb(t))
    INTO v_group_result
    FROM (
        SELECT *, 'client_id_customer_email_clean__time_since__30d' AS _table
        FROM features.client_id_customer_email_clean__time_since__30d
        WHERE hash_key = p_email_key
        UNION ALL
        SELECT *, 'client_id_customer_email_clean__time_since__90d' AS _table
        FROM features.client_id_customer_email_clean__time_since__90d
        WHERE hash_key = p_email_key
        UNION ALL
        SELECT *, 'client_id_customer_email_clean__time_since__365d' AS _table
        FROM features.client_id_customer_email_clean__time_since__365d
        WHERE hash_key = p_email_key
    ) t;
    v_entity_result := jsonb_set(v_entity_result, '{time_since}', COALESCE(v_group_result, '[]'::jsonb));
    
    -- good_rates group
    SELECT jsonb_agg(to_jsonb(t))
    INTO v_group_result
    FROM (
        SELECT *, 'client_id_customer_email_clean__good_rates__30d' AS _table
        FROM features.client_id_customer_email_clean__good_rates__30d
        WHERE hash_key = p_email_key
        UNION ALL
        SELECT *, 'client_id_customer_email_clean__good_rates__90d' AS _table
        FROM features.client_id_customer_email_clean__good_rates__90d
        WHERE hash_key = p_email_key
        UNION ALL
        SELECT *, 'client_id_customer_email_clean__good_rates__365d' AS _table
        FROM features.client_id_customer_email_clean__good_rates__365d
        WHERE hash_key = p_email_key
    ) t;
    v_entity_result := jsonb_set(v_entity_result, '{good_rates}', COALESCE(v_group_result, '[]'::jsonb));
    
    v_result := jsonb_set(v_result, '{customer_email}', v_entity_result);
    
    -- ========================================
    -- Entity 3: cardholder_name
    -- ========================================
    v_entity_result := '{}'::jsonb;
    
    -- fraud_rates group
    SELECT jsonb_agg(to_jsonb(t))
    INTO v_group_result
    FROM (
        SELECT *, 'client_id_cardholder_name__fraud_rates__30d' AS _table
        FROM features.client_id_cardholder_name__fraud_rates__30d
        WHERE hash_key = p_name_key
        UNION ALL
        SELECT *, 'client_id_cardholder_name__fraud_rates__90d' AS _table
        FROM features.client_id_cardholder_name__fraud_rates__90d
        WHERE hash_key = p_name_key
        UNION ALL
        SELECT *, 'client_id_cardholder_name__fraud_rates__365d' AS _table
        FROM features.client_id_cardholder_name__fraud_rates__365d
        WHERE hash_key = p_name_key
    ) t;
    v_entity_result := jsonb_set(v_entity_result, '{fraud_rates}', COALESCE(v_group_result, '[]'::jsonb));
    
    -- time_since group
    SELECT jsonb_agg(to_jsonb(t))
    INTO v_group_result
    FROM (
        SELECT *, 'client_id_cardholder_name__time_since__30d' AS _table
        FROM features.client_id_cardholder_name__time_since__30d
        WHERE hash_key = p_name_key
        UNION ALL
        SELECT *, 'client_id_cardholder_name__time_since__90d' AS _table
        FROM features.client_id_cardholder_name__time_since__90d
        WHERE hash_key = p_name_key
        UNION ALL
        SELECT *, 'client_id_cardholder_name__time_since__365d' AS _table
        FROM features.client_id_cardholder_name__time_since__365d
        WHERE hash_key = p_name_key
    ) t;
    v_entity_result := jsonb_set(v_entity_result, '{time_since}', COALESCE(v_group_result, '[]'::jsonb));
    
    -- good_rates group
    SELECT jsonb_agg(to_jsonb(t))
    INTO v_group_result
    FROM (
        SELECT *, 'client_id_cardholder_name__good_rates__30d' AS _table
        FROM features.client_id_cardholder_name__good_rates__30d
        WHERE hash_key = p_name_key
        UNION ALL
        SELECT *, 'client_id_cardholder_name__good_rates__90d' AS _table
        FROM features.client_id_cardholder_name__good_rates__90d
        WHERE hash_key = p_name_key
        UNION ALL
        SELECT *, 'client_id_cardholder_name__good_rates__365d' AS _table
        FROM features.client_id_cardholder_name__good_rates__365d
        WHERE hash_key = p_name_key
    ) t;
    v_entity_result := jsonb_set(v_entity_result, '{good_rates}', COALESCE(v_group_result, '[]'::jsonb));
    
    v_result := jsonb_set(v_result, '{cardholder_name}', v_entity_result);
    
    RETURN v_result;
END;
$$;

-- Grant execute permission
GRANT EXECUTE ON FUNCTION features.fetch_request_features(TEXT, TEXT, TEXT) TO PUBLIC;

-- Add comment
COMMENT ON FUNCTION features.fetch_request_features IS 
'Fetches all features for a 3-entity request in a single RPC call. 
Returns JSONB with keys: card_fingerprint, customer_email, cardholder_name.
Each entity contains feature groups (fraud_rates, time_since, good_rates) as arrays.';
