"""
Schema definitions for fraud detection feature tables.
These match the actual tables created in Lakebase.
"""

# Table 1: Fraud Reports (365d)
FRAUD_REPORTS_365D_SCHEMA = """
CREATE TABLE IF NOT EXISTS fraud_reports_365d (
    primary_key CHAR(64) PRIMARY KEY,
    raw_fingerprint TEXT,
    fraud_reports_365d NUMERIC,
    eligible_capture_365d NUMERIC,
    fraud_rate_365d NUMERIC,
    updated_at BIGINT
);

CREATE INDEX IF NOT EXISTS idx_fraud_reports_fingerprint 
ON fraud_reports_365d(raw_fingerprint);
"""

# Table 2: Good Rate 90d Lag (730d)
GOOD_RATE_90D_LAG_730D_SCHEMA = """
CREATE TABLE IF NOT EXISTS good_rate_90d_lag_730d (
    primary_key CHAR(64) PRIMARY KEY,
    raw_fingerprint TEXT,
    eligible_90d_lag_730d NUMERIC,
    good_90d_lag_730d NUMERIC,
    good_rate_90d_lag_730d NUMERIC,
    updated_at BIGINT
);

CREATE INDEX IF NOT EXISTS idx_good_rate_fingerprint 
ON good_rate_90d_lag_730d(raw_fingerprint);
"""

# Table 3: Distinct Counts & Amount Stats (365d)
DISTINCT_COUNTS_AMOUNT_STATS_365D_SCHEMA = """
CREATE TABLE IF NOT EXISTS distinct_counts_amount_stats_365d (
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

CREATE INDEX IF NOT EXISTS idx_distinct_counts_fingerprint 
ON distinct_counts_amount_stats_365d(raw_fingerprint);
"""

# Table 4: First/Last Request & Capture Times
REQUEST_CAPTURE_TIMES_SCHEMA = """
CREATE TABLE IF NOT EXISTS request_capture_times (
    primary_key CHAR(64) PRIMARY KEY,
    raw_fingerprint TEXT,
    time_of_first_request TIMESTAMPTZ,
    time_of_first_capture TIMESTAMPTZ,
    time_of_last_request TIMESTAMPTZ,
    time_of_last_capture TIMESTAMPTZ,
    updated_at BIGINT
);

CREATE INDEX IF NOT EXISTS idx_request_capture_fingerprint 
ON request_capture_times(raw_fingerprint);
"""

# Table 5: Aggregated Features (100 features for comprehensive testing)
AGGREGATED_FEATURES_365D_SCHEMA = """
CREATE TABLE IF NOT EXISTS aggregated_features_365d (
    primary_key CHAR(64) PRIMARY KEY,
    raw_fingerprint TEXT,
    -- Transaction counts by time window
    txn_count_1d NUMERIC, txn_count_7d NUMERIC, txn_count_30d NUMERIC, txn_count_90d NUMERIC, txn_count_180d NUMERIC, txn_count_365d NUMERIC,
    -- Approved transaction counts
    approved_count_1d NUMERIC, approved_count_7d NUMERIC, approved_count_30d NUMERIC, approved_count_90d NUMERIC, approved_count_180d NUMERIC, approved_count_365d NUMERIC,
    -- Declined transaction counts
    declined_count_1d NUMERIC, declined_count_7d NUMERIC, declined_count_30d NUMERIC, declined_count_90d NUMERIC, declined_count_180d NUMERIC, declined_count_365d NUMERIC,
    -- Amount statistics
    avg_amount_1d NUMERIC, avg_amount_7d NUMERIC, avg_amount_30d NUMERIC, avg_amount_90d NUMERIC, avg_amount_180d NUMERIC, avg_amount_365d NUMERIC,
    max_amount_1d NUMERIC, max_amount_7d NUMERIC, max_amount_30d NUMERIC, max_amount_90d NUMERIC, max_amount_180d NUMERIC, max_amount_365d NUMERIC,
    min_amount_1d NUMERIC, min_amount_7d NUMERIC, min_amount_30d NUMERIC, min_amount_90d NUMERIC, min_amount_180d NUMERIC, min_amount_365d NUMERIC,
    sum_amount_1d NUMERIC, sum_amount_7d NUMERIC, sum_amount_30d NUMERIC, sum_amount_90d NUMERIC, sum_amount_180d NUMERIC, sum_amount_365d NUMERIC,
    -- Distinct entity counts
    distinct_countries_30d NUMERIC, distinct_countries_90d NUMERIC, distinct_countries_365d NUMERIC,
    distinct_merchants_30d NUMERIC, distinct_merchants_90d NUMERIC, distinct_merchants_365d NUMERIC,
    distinct_devices_30d NUMERIC, distinct_devices_90d NUMERIC, distinct_devices_365d NUMERIC,
    distinct_ips_30d NUMERIC, distinct_ips_90d NUMERIC, distinct_ips_365d NUMERIC,
    distinct_cards_30d NUMERIC, distinct_cards_90d NUMERIC, distinct_cards_365d NUMERIC,
    -- Velocity features
    velocity_1h NUMERIC, velocity_3h NUMERIC, velocity_6h NUMERIC, velocity_12h NUMERIC, velocity_24h NUMERIC,
    -- Ratios and rates
    approval_rate_7d NUMERIC, approval_rate_30d NUMERIC, approval_rate_90d NUMERIC, approval_rate_365d NUMERIC,
    chargeback_rate_30d NUMERIC, chargeback_rate_90d NUMERIC, chargeback_rate_365d NUMERIC,
    refund_rate_30d NUMERIC, refund_rate_90d NUMERIC, refund_rate_365d NUMERIC,
    -- Risk scores
    risk_score_device NUMERIC, risk_score_ip NUMERIC, risk_score_email NUMERIC, risk_score_card NUMERIC,
    risk_score_velocity NUMERIC, risk_score_amount NUMERIC, risk_score_geo NUMERIC, risk_score_behavior NUMERIC,
    -- Time-based features
    hour_of_day NUMERIC, day_of_week NUMERIC, day_of_month NUMERIC, is_weekend NUMERIC, is_business_hours NUMERIC,
    -- Additional aggregated features
    failed_auth_count_7d NUMERIC, failed_auth_count_30d NUMERIC, failed_auth_count_90d NUMERIC,
    cross_border_txn_count_30d NUMERIC, cross_border_txn_count_90d NUMERIC,
    high_value_txn_count_30d NUMERIC, high_value_txn_count_90d NUMERIC,
    updated_at BIGINT
);

CREATE INDEX IF NOT EXISTS idx_aggregated_features_fingerprint 
ON aggregated_features_365d(raw_fingerprint);
"""

# Column mappings for each table
FRAUD_REPORTS_COLUMNS = [
    'primary_key', 'raw_fingerprint', 'fraud_reports_365d', 
    'eligible_capture_365d', 'fraud_rate_365d', 'updated_at'
]

GOOD_RATE_COLUMNS = [
    'primary_key', 'raw_fingerprint', 'eligible_90d_lag_730d',
    'good_90d_lag_730d', 'good_rate_90d_lag_730d', 'updated_at'
]

DISTINCT_COUNTS_COLUMNS = [
    'primary_key', 'raw_fingerprint', 'avg_requested_amount_usd_365d',
    'distinct_billing_postcode_365d', 'distinct_cardholder_name_365d',
    'distinct_card_bin_365d', 'distinct_email_365d', 'distinct_customer_name_365d',
    'distinct_issuing_bank_365d', 'distinct_shipping_postcode_365d',
    'count_365d', 'sum_requested_amount_usd_365d', 'updated_at'
]

REQUEST_CAPTURE_COLUMNS = [
    'primary_key', 'raw_fingerprint', 'time_of_first_request',
    'time_of_first_capture', 'time_of_last_request', 'time_of_last_capture',
    'updated_at'
]

AGGREGATED_FEATURES_COLUMNS = [
    'primary_key', 'raw_fingerprint',
    'txn_count_1d', 'txn_count_7d', 'txn_count_30d', 'txn_count_90d', 'txn_count_180d', 'txn_count_365d',
    'approved_count_1d', 'approved_count_7d', 'approved_count_30d', 'approved_count_90d', 'approved_count_180d', 'approved_count_365d',
    'declined_count_1d', 'declined_count_7d', 'declined_count_30d', 'declined_count_90d', 'declined_count_180d', 'declined_count_365d',
    'avg_amount_1d', 'avg_amount_7d', 'avg_amount_30d', 'avg_amount_90d', 'avg_amount_180d', 'avg_amount_365d',
    'max_amount_1d', 'max_amount_7d', 'max_amount_30d', 'max_amount_90d', 'max_amount_180d', 'max_amount_365d',
    'min_amount_1d', 'min_amount_7d', 'min_amount_30d', 'min_amount_90d', 'min_amount_180d', 'min_amount_365d',
    'sum_amount_1d', 'sum_amount_7d', 'sum_amount_30d', 'sum_amount_90d', 'sum_amount_180d', 'sum_amount_365d',
    'distinct_countries_30d', 'distinct_countries_90d', 'distinct_countries_365d',
    'distinct_merchants_30d', 'distinct_merchants_90d', 'distinct_merchants_365d',
    'distinct_devices_30d', 'distinct_devices_90d', 'distinct_devices_365d',
    'distinct_ips_30d', 'distinct_ips_90d', 'distinct_ips_365d',
    'distinct_cards_30d', 'distinct_cards_90d', 'distinct_cards_365d',
    'velocity_1h', 'velocity_3h', 'velocity_6h', 'velocity_12h', 'velocity_24h',
    'approval_rate_7d', 'approval_rate_30d', 'approval_rate_90d', 'approval_rate_365d',
    'chargeback_rate_30d', 'chargeback_rate_90d', 'chargeback_rate_365d',
    'refund_rate_30d', 'refund_rate_90d', 'refund_rate_365d',
    'risk_score_device', 'risk_score_ip', 'risk_score_email', 'risk_score_card',
    'risk_score_velocity', 'risk_score_amount', 'risk_score_geo', 'risk_score_behavior',
    'hour_of_day', 'day_of_week', 'day_of_month', 'is_weekend', 'is_business_hours',
    'failed_auth_count_7d', 'failed_auth_count_30d', 'failed_auth_count_90d',
    'cross_border_txn_count_30d', 'cross_border_txn_count_90d',
    'high_value_txn_count_30d', 'high_value_txn_count_90d',
    'updated_at'
]

# Table names
TABLE_NAMES = [
    'fraud_reports_365d',
    'good_rate_90d_lag_730d',
    'distinct_counts_amount_stats_365d',
    'request_capture_times',
    'aggregated_features_365d'
]

# Schema mapping
SCHEMAS = {
    'fraud_reports_365d': FRAUD_REPORTS_365D_SCHEMA,
    'good_rate_90d_lag_730d': GOOD_RATE_90D_LAG_730D_SCHEMA,
    'distinct_counts_amount_stats_365d': DISTINCT_COUNTS_AMOUNT_STATS_365D_SCHEMA,
    'request_capture_times': REQUEST_CAPTURE_TIMES_SCHEMA,
    'aggregated_features_365d': AGGREGATED_FEATURES_365D_SCHEMA
}

COLUMNS = {
    'fraud_reports_365d': FRAUD_REPORTS_COLUMNS,
    'good_rate_90d_lag_730d': GOOD_RATE_COLUMNS,
    'distinct_counts_amount_stats_365d': DISTINCT_COUNTS_COLUMNS,
    'request_capture_times': REQUEST_CAPTURE_COLUMNS,
    'aggregated_features_365d': AGGREGATED_FEATURES_COLUMNS
}

