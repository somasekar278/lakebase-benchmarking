# Benchmark Instructions

## Overview

Three benchmark notebooks for comparing Lakebase vs DynamoDB:

1. **`benchmark_lakebase.py`** - Test Lakebase stored procedure performance
2. **`benchmark_dynamodb.py`** - Test DynamoDB batch_get_item performance  
3. **`benchmark_comparison.py`** - Run both and create side-by-side comparison

All benchmarks run **inside Databricks** for consistent network conditions and produce identical metrics + visualizations.

## Quick Start

### 1. Upload Notebooks

Upload all benchmark notebooks to your Databricks workspace:

```bash
# Via Databricks CLI
databricks workspace import benchmark_lakebase.py \
  /Users/som.natarajan@databricks.com/benchmark_lakebase \
  --format SOURCE --language PYTHON

databricks workspace import benchmark_dynamodb.py \
  /Users/som.natarajan@databricks.com/benchmark_dynamodb \
  --format SOURCE --language PYTHON

databricks workspace import benchmark_comparison.py \
  /Users/som.natarajan@databricks.com/benchmark_comparison \
  --format SOURCE --language PYTHON
```

Or manually via Databricks UI: Workspace → Import → Upload each `.py` file

### 2. Setup DynamoDB Credentials (for DynamoDB benchmark)

Create a Databricks secret scope:

```bash
# Create secret scope
databricks secrets create-scope --scope fraud-benchmark

# Add AWS credentials
databricks secrets put --scope fraud-benchmark --key aws-access-key-id
databricks secrets put --scope fraud-benchmark --key aws-secret-access-key
```

Or use AWS IAM roles (recommended for production).

### 3. Run Benchmarks

#### Option A: Run Individual Benchmarks

1. Open `benchmark_lakebase.py` or `benchmark_dynamodb.py` in Databricks
2. Attach to a cluster
3. Run All Cells
4. View latency distribution chart and results

#### Option B: Run Side-by-Side Comparison

1. Open `benchmark_comparison.py` in Databricks
2. Run All Cells
3. Get comparative analysis with charts and recommendations

#### Option C: Deploy as Databricks Job (Automated)

```bash
cd /path/to/lakebase-benchmarking

# Initialize Terraform (if not done)
terraform init

# Deploy the benchmark job
terraform apply -target=databricks_job.lakebase_benchmark

# Outputs:
# - benchmark_job_id
# - benchmark_job_url
```

Then run the job:
- Via UI: Click the `benchmark_job_url` → Click "Run Now"
- Via CLI: `databricks jobs run-now --job-id <benchmark_job_id>`

## What It Measures

- **100 iterations** of stored procedure calls
- **25 random keys** per query (configurable)
- **Latency metrics**: P50, P95, P99, Max, Mean, StdDev, CV
- **Comparison**: vs DynamoDB 79ms P99
- **Visualization**: Histogram with P50/P99 markers

## Expected Output

### Lakebase Benchmark

```
================================================================================
BENCHMARK RESULTS
================================================================================

📊 Dataset: 111M rows across 3 tables
   - fraud_reports_365d: 1,000,000
   - good_rate_90d_lag_730d: 10,000,000
   - request_capture_times: 100,000,000

📈 Latency Statistics (n=100 iterations, 25 keys/query):
   P50 (Median):    27.39 ms
   P95:             45.12 ms
   P99:             52.17 ms
   Max:             58.23 ms
   Min:             21.45 ms
   Mean:            29.84 ms
   Std Dev:          6.32 ms
   CV:               0.212

🎯 Comparison vs DynamoDB:
   DynamoDB P99:    79.00 ms  (baseline)
   Lakebase P99:    52.17 ms
   Result:        ✅ Lakebase is 26.83ms (34.0%) FASTER

================================================================================
```

Plus a **Latency Distribution Chart** showing histogram with P50/P99 lines.

### DynamoDB Benchmark

Same format as Lakebase, but compares DynamoDB results vs Lakebase baseline.

### Comparison Benchmark

```
================================================================================
LAKEBASE vs DYNAMODB: HEAD-TO-HEAD COMPARISON
================================================================================

📊 Dataset:
   Lakebase:  111M rows
   DynamoDB:  111M rows

📈 P50 Latency (Lower is Better):
   Lakebase:    27.39 ms
   DynamoDB:    30.12 ms
   Winner:    ✅ Lakebase (2.73ms / 9.1% faster)

🎯 P99 Latency (Lower is Better):
   Lakebase:    52.17 ms
   DynamoDB:    79.00 ms
   Winner:    ✅ Lakebase (26.83ms / 34.0% faster)

🔄 Consistency (CV - Lower is Better):
   Lakebase:    0.212
   DynamoDB:    0.268
   Winner:    ✅ Lakebase (more consistent)

✅ SLA Compliance (< 120ms P99):
   Lakebase:  ✅ PASS
   DynamoDB:  ✅ PASS

================================================================================
RECOMMENDATION
================================================================================

Score: Lakebase 6 - DynamoDB 0

✅ RECOMMENDATION: Use Lakebase
   - 52.17ms P99 vs 79.00ms (DynamoDB)
   - 26.83ms (34.0%) faster

✅ Both systems meet the 120ms SLA requirement

================================================================================
```

Plus **side-by-side bar charts** comparing P50, P95, P99 latencies.

## Configuration

### Lakebase Benchmark (`benchmark_lakebase.py`)

```python
# Lakebase connection
LAKEBASE_HOST = "ep-autumn-fire-d318blbk.database.eu-west-1.cloud.databricks.com"
LAKEBASE_PORT = "5432"
LAKEBASE_DATABASE = "benchmark"
LAKEBASE_USER = "fraud_benchmark_user"
LAKEBASE_PASSWORD = "fraud_benchmark_user_123!"

# Benchmark configuration
NUM_KEYS = 25          # Keys per lookup (increase for stress test)
NUM_WARMUP = 3         # Warm-up iterations
NUM_ITERATIONS = 100   # Benchmark iterations (increase for more data)

# DynamoDB baseline for comparison
DYNAMODB_P99 = 79      # Customer's reported P99
```

### DynamoDB Benchmark (`benchmark_dynamodb.py`)

```python
# DynamoDB configuration
AWS_REGION = "us-west-2"
AWS_ACCESS_KEY_ID = dbutils.secrets.get(scope="fraud-benchmark", key="aws-access-key-id")
AWS_SECRET_ACCESS_KEY = dbutils.secrets.get(scope="fraud-benchmark", key="aws-secret-access-key")

# Table names
DYNAMODB_TABLES = {
    'fraud_reports_365d': 'fraud_reports_365d',
    'good_rate_90d_lag_730d': 'good_rate_90d_lag_730d',
    'distinct_counts_amount_stats_365d': 'distinct_counts_amount_stats_365d',
    'request_capture_times': 'request_capture_times'
}

# Benchmark configuration
NUM_KEYS = 25          # Keys per lookup
NUM_WARMUP = 3         # Warm-up iterations
NUM_ITERATIONS = 100   # Benchmark iterations

# Lakebase baseline for comparison
LAKEBASE_P99 = 52.17   # Update after running Lakebase benchmark
```

## Troubleshooting

### Lakebase Issues

**Connection Timeout**
- Increase `connect_timeout` in `get_connection()`
- Check network connectivity from Databricks to Lakebase
- Verify firewall rules allow traffic from Databricks VPC

**Query Timeout**
- Query takes > 5 minutes: Check table indexes, run `ANALYZE` on tables
- Increase `statement_timeout` in `measure_query()`
- Check if stored procedure `fraud_batch_lookup` exists

**Sample Keys Failing**
- If `TABLESAMPLE` returns empty: Tables might be too small
- Edit `get_sample_keys()` to use `ORDER BY RANDOM() LIMIT N` instead

### DynamoDB Issues

**Authentication Error**
- Verify AWS credentials in Databricks secrets
- Check IAM permissions for `dynamodb:BatchGetItem`, `dynamodb:DescribeTable`
- Ensure correct region is specified

**ProvisionedThroughputExceededException**
- DynamoDB tables may need higher read capacity
- Enable Auto Scaling on tables
- Reduce `NUM_ITERATIONS` for initial tests

**UnprocessedKeys**
- The benchmark handles retries automatically
- If this persists, reduce `NUM_KEYS` per query

### Comparison Benchmark Issues

**Notebook Not Found**
- Ensure both `benchmark_lakebase.py` and `benchmark_dynamodb.py` are uploaded
- Update notebook paths in `benchmark_comparison.py` if using different locations

## Workflow Recommendations

### Phase 1: Baseline Lakebase
1. Run `benchmark_lakebase.py` on current data
2. Record P99 latency as baseline
3. Analyze latency distribution chart

### Phase 2: Test DynamoDB
1. Update `LAKEBASE_P99` in `benchmark_dynamodb.py` with Phase 1 result
2. Run `benchmark_dynamodb.py` on equivalent DynamoDB tables
3. Compare individual results

### Phase 3: Head-to-Head Comparison
1. Run `benchmark_comparison.py` to execute both benchmarks
2. Review comparative analysis and recommendation
3. Make decision based on:
   - Performance (P99)
   - Consistency (CV)
   - Cost
   - Operational complexity

## Next Steps

After successful benchmarking:
1. **Scale testing**: Compare P99 across different table sizes (1M, 10M, 100M, 1B)
2. **Regional testing**: Run from different regions to measure network impact
3. **Load testing**: Increase `NUM_KEYS` to simulate heavier workloads (50, 100, 200 keys)
4. **Monitoring**: Schedule recurring benchmarks to track performance over time
5. **Cost analysis**: Compare DynamoDB provisioned throughput costs vs Lakebase compute costs
6. **Feature parity**: Ensure DynamoDB tables have same data and indexes as Lakebase

