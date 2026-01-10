-- ==================================================================
-- Lakebase Setup: Schema, Tables, User, and Grants
-- Run this as admin when creating a new workspace
-- ==================================================================

-- 1. Create schema
CREATE SCHEMA IF NOT EXISTS features;

-- 2. Create user
CREATE ROLE fraud_benchmark_user WITH LOGIN PASSWORD 'fraud_benchmark_user_123!';

-- 3. Create tables in features schema

-- Table 1: Fraud Reports (365d)
CREATE TABLE IF NOT EXISTS features.fraud_reports_365d (
    primary_key CHAR(64) PRIMARY KEY,
    raw_fingerprint TEXT,
    fraud_reports_365d NUMERIC,
    eligible_capture_365d NUMERIC,
    fraud_rate_365d NUMERIC,
    updated_at BIGINT
);

-- Table 2: Good Rate 90d Lag (730d)
CREATE TABLE IF NOT EXISTS features.good_rate_90d_lag_730d (
    primary_key CHAR(64) PRIMARY KEY,
    raw_fingerprint TEXT,
    eligible_90d_lag_730d NUMERIC,
    good_90d_lag_730d NUMERIC,
    good_rate_90d_lag_730d NUMERIC,
    updated_at BIGINT
);

-- Table 3: Distinct Counts & Amount Stats (365d)
CREATE TABLE IF NOT EXISTS features.distinct_counts_amount_stats_365d (
    primary_key CHAR(64) PRIMARY KEY,
    raw_fingerprint TEXT,
    avg_requested_amount_usd_365d NUMERIC,
    distinct_billing_postcode_365d NUMERIC,
    distinct_cardholder_name_365d NUMERIC,
    distinct_card_bin_365d NUMERIC,
    distinct_email_365d NUMERIC,
    distinct_customer_name_365d NUMERIC,
    distinct_issuing_bank_365d NUMERIC,
    distinct_shipping_postcode_365d NUMERIC,
    count_365d NUMERIC,
    sum_requested_amount_usd_365d NUMERIC,
    updated_at BIGINT
);

-- Table 4: First/Last Request & Capture Times
CREATE TABLE IF NOT EXISTS features.request_capture_times (
    primary_key CHAR(64) PRIMARY KEY,
    raw_fingerprint TEXT,
    time_of_first_request TIMESTAMPTZ,
    time_of_first_capture TIMESTAMPTZ,
    time_of_last_request TIMESTAMPTZ,
    time_of_last_capture TIMESTAMPTZ,
    updated_at BIGINT
);

-- Table 5: Aggregated Features (100 features for comprehensive testing)
CREATE TABLE IF NOT EXISTS features.aggregated_features_365d (
    primary_key CHAR(64) PRIMARY KEY,
    raw_fingerprint TEXT,
    -- Transaction counts by time window (6 features)
    txn_count_1d NUMERIC, txn_count_7d NUMERIC, txn_count_30d NUMERIC, txn_count_90d NUMERIC, txn_count_180d NUMERIC, txn_count_365d NUMERIC,
    -- Approved transaction counts (6 features)
    approved_count_1d NUMERIC, approved_count_7d NUMERIC, approved_count_30d NUMERIC, approved_count_90d NUMERIC, approved_count_180d NUMERIC, approved_count_365d NUMERIC,
    -- Declined transaction counts (6 features)
    declined_count_1d NUMERIC, declined_count_7d NUMERIC, declined_count_30d NUMERIC, declined_count_90d NUMERIC, declined_count_180d NUMERIC, declined_count_365d NUMERIC,
    -- Amount statistics (24 features: avg, max, min, sum x 6 time windows)
    avg_amount_1d NUMERIC, avg_amount_7d NUMERIC, avg_amount_30d NUMERIC, avg_amount_90d NUMERIC, avg_amount_180d NUMERIC, avg_amount_365d NUMERIC,
    max_amount_1d NUMERIC, max_amount_7d NUMERIC, max_amount_30d NUMERIC, max_amount_90d NUMERIC, max_amount_180d NUMERIC, max_amount_365d NUMERIC,
    min_amount_1d NUMERIC, min_amount_7d NUMERIC, min_amount_30d NUMERIC, min_amount_90d NUMERIC, min_amount_180d NUMERIC, min_amount_365d NUMERIC,
    sum_amount_1d NUMERIC, sum_amount_7d NUMERIC, sum_amount_30d NUMERIC, sum_amount_90d NUMERIC, sum_amount_180d NUMERIC, sum_amount_365d NUMERIC,
    -- Distinct entity counts (15 features)
    distinct_countries_30d NUMERIC, distinct_countries_90d NUMERIC, distinct_countries_365d NUMERIC,
    distinct_merchants_30d NUMERIC, distinct_merchants_90d NUMERIC, distinct_merchants_365d NUMERIC,
    distinct_devices_30d NUMERIC, distinct_devices_90d NUMERIC, distinct_devices_365d NUMERIC,
    distinct_ips_30d NUMERIC, distinct_ips_90d NUMERIC, distinct_ips_365d NUMERIC,
    distinct_cards_30d NUMERIC, distinct_cards_90d NUMERIC, distinct_cards_365d NUMERIC,
    -- Velocity features (5 features)
    velocity_1h NUMERIC, velocity_3h NUMERIC, velocity_6h NUMERIC, velocity_12h NUMERIC, velocity_24h NUMERIC,
    -- Ratios and rates (10 features)
    approval_rate_7d NUMERIC, approval_rate_30d NUMERIC, approval_rate_90d NUMERIC, approval_rate_365d NUMERIC,
    chargeback_rate_30d NUMERIC, chargeback_rate_90d NUMERIC, chargeback_rate_365d NUMERIC,
    refund_rate_30d NUMERIC, refund_rate_90d NUMERIC, refund_rate_365d NUMERIC,
    -- Risk scores (8 features)
    risk_score_device NUMERIC, risk_score_ip NUMERIC, risk_score_email NUMERIC, risk_score_card NUMERIC,
    risk_score_velocity NUMERIC, risk_score_amount NUMERIC, risk_score_geo NUMERIC, risk_score_behavior NUMERIC,
    -- Time-based features (5 features)
    hour_of_day NUMERIC, day_of_week NUMERIC, day_of_month NUMERIC, is_weekend NUMERIC, is_business_hours NUMERIC,
    -- Additional aggregated features (7 features)
    failed_auth_count_7d NUMERIC, failed_auth_count_30d NUMERIC, failed_auth_count_90d NUMERIC,
    cross_border_txn_count_30d NUMERIC, cross_border_txn_count_90d NUMERIC,
    high_value_txn_count_30d NUMERIC, high_value_txn_count_90d NUMERIC,
    updated_at BIGINT
);
-- Total: 92 feature columns (+ primary_key, raw_fingerprint, updated_at metadata)

-- 4. Grant permissions to user

-- Grant schema usage
GRANT USAGE ON SCHEMA features TO fraud_benchmark_user;

-- Grant table permissions (including DELETE and TRUNCATE for idempotent loads)
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON 
    features.fraud_reports_365d,
    features.good_rate_90d_lag_730d,
    features.distinct_counts_amount_stats_365d,
    features.request_capture_times,
    features.aggregated_features_365d
TO fraud_benchmark_user;

-- Ensure DELETE is granted (required by verification scripts)
GRANT DELETE ON 
    features.fraud_reports_365d,
    features.good_rate_90d_lag_730d,
    features.distinct_counts_amount_stats_365d,
    features.request_capture_times,
    features.aggregated_features_365d
TO fraud_benchmark_user;

-- Alternative if TRUNCATE still doesn't work, grant table ownership
-- ALTER TABLE features.fraud_reports_365d OWNER TO fraud_benchmark_user;
-- ALTER TABLE features.good_rate_90d_lag_730d OWNER TO fraud_benchmark_user;
-- ALTER TABLE features.distinct_counts_amount_stats_365d OWNER TO fraud_benchmark_user;
-- ALTER TABLE features.request_capture_times OWNER TO fraud_benchmark_user;

-- Grant permission to create functions (for stored procedure)
GRANT CREATE ON SCHEMA features TO fraud_benchmark_user;

-- ==================================================================
-- Verification queries (run these to check setup)
-- ==================================================================

-- Check schema exists
SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'features';

-- Check tables exist
SELECT table_name FROM information_schema.tables 
WHERE table_schema = 'features' 
ORDER BY table_name;

-- Check user exists
SELECT usename FROM pg_user WHERE usename = 'fraud_benchmark_user';

-- Check grants
SELECT grantee, table_schema, table_name, privilege_type
FROM information_schema.table_privileges
WHERE grantee = 'fraud_benchmark_user' AND table_schema = 'features'
ORDER BY table_name, privilege_type;

-- ==================================================================
-- DONE!
-- ==================================================================

