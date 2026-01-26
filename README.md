# Lakebase Benchmarking Project

Production-grade benchmarking suite for Lakebase feature serving performance.

## 🎯 What This Project Does

Benchmarks Lakebase (PostgreSQL-based feature store) against DynamoDB for real-time feature serving workloads, focusing on hot/cold key access patterns and multi-entity requests.

## 📊 Current Benchmark: Zipfian V3 (Production-Ready)

**Production-grade multi-entity feature serving benchmark with:**
- ✅ Random key sampling from ALL tables (not just first keys)
- ✅ SELECT * (fetches actual data, not just index)
- ✅ Serial execution (realistic per-query latency)
- ✅ EXPLAIN sampling for precise I/O measurement
- ✅ Error handling with graceful degradation
- ✅ NaN-safe correlation calculation
- ✅ TABLESAMPLE fallback for small tables
- ✅ Key persistence for reproducibility

## 🚀 Quick Start

### 1. Upload V3 Benchmark
```bash
python3 upload_zipfian_v3.py
```

### 2. Update Job to V3
```bash
python3 update_job_to_v3.py
```

### 3. Run Benchmark
```bash
python3 run_zipfian_v3_job.py
```

**Expected runtime**: 12-18 minutes

**Monitor**: Check Databricks UI for progress

## 📁 Project Structure

```
lakebase-benchmarking/
├── README.md                                    # This file
├── databricks.yml                               # Databricks Asset Bundle config
├── requirements.txt                             # Python dependencies
├── credentials.template                         # Template for credentials
│
├── notebooks/                                   # Databricks notebooks
│   ├── benchmark_zipfian_realistic_v3.py       # ⭐ PRODUCTION: V3 benchmark
│   ├── zipfian_benchmark_visuals.py            # ⭐ PRODUCTION: Visualizations
│   ├── generate_csvs.py                        # CSV generation
│   ├── run_pipelined_load_inline.py            # Data loading
│   ├── verify_loaded_tables.py                 # Verification
│   └── verify_pk_exists.py                     # Primary key verification
│
├── resources/                                   # Databricks resources
│   └── jobs.yml                                # ⭐ Job definitions
│
├── utils/                                       # Utility modules
│   ├── __init__.py
│   ├── feature_server.py
│   ├── pipelined_load.py
│   └── csv_timestamp_validator.py
│
├── generated/                                   # Generated SQL/config
│   ├── fraud_feature_tables_30_COMPLETE.sql
│   └── fraud_tables_row_counts_30_COMPLETE.txt
│
├── upload_zipfian_v3.py                        # ⭐ Upload V3 notebook
├── update_job_to_v3.py                         # ⭐ Update job to V3
├── run_zipfian_v3_job.py                       # ⭐ Run V3 benchmark
│
├── grant_stats_permission.sql                  # Grant pg_stat_reset permission
├── SETUP_NEW_WORKSPACE.sql                     # Initial workspace setup
│
├── RUN_ZIPFIAN_BENCHMARK.md                    # How to run the benchmark
├── ZIPFIAN_V3_KEY_PERSISTENCE.md               # V3 documentation
├── COST_METHODOLOGY.md                         # Cost analysis methodology
└── CUSTOMER_DISCOVERY_QUESTIONS.md             # Customer discovery guide
```

## 🔑 Key Files

### Production Notebooks
- **`benchmark_zipfian_realistic_v3.py`**: Production-grade benchmark with all fixes
- **`zipfian_benchmark_visuals.py`**: 7 professional visualizations

### Deployment Scripts
- **`upload_zipfian_v3.py`**: Upload V3 notebook to Databricks
- **`update_job_to_v3.py`**: Update existing job to use V3
- **`run_zipfian_v3_job.py`**: Trigger benchmark job run

### Configuration
- **`databricks.yml`**: Bundle configuration (targets: dev, staging, prod)
- **`resources/jobs.yml`**: Job definitions for Databricks

### Documentation
- **`RUN_ZIPFIAN_BENCHMARK.md`**: Step-by-step guide
- **`ZIPFIAN_V3_KEY_PERSISTENCE.md`**: V3 features and improvements
- **`COST_METHODOLOGY.md`**: How we calculate cost comparisons

## 📊 What Gets Measured

### Latency Metrics
- P50, P95, P99 latency across 9 hot/cold ratios (100% → 0%)
- Per-entity latency breakdown
- Request-level cache correlation

### Cache Metrics
- Average cache score (0 = all cold, 1 = all hot)
- Fully hot request %
- Fully cold request %
- Latency-cache correlation

### I/O Metrics
- Disk blocks per request (via EXPLAIN sampling)
- Heap reads vs hits
- Per-query I/O distribution

### Cost Analysis
- Lakebase TCO (~$100/day)
- DynamoDB equivalent (~$75,000/day)
- Break-even analysis

## 🎯 Benchmark Methodology

### Request Structure
- **3 entities per request**: card_fingerprint, customer_email, cardholder_name
- **30 tables total**: 9-12 tables per entity (30d/90d/365d aggregations)
- **Independent hot/cold**: Each entity independently decides hot or cold

### Key Sampling
- Sample 10,000 keys per entity from ALL tables
- Shuffle keys randomly before hot/cold split
- Top 1% = "hot" keys (100 per entity)
- Remaining 99% = "cold" keys (9,900 per entity)
- Persist keys for reproducibility

### Execution
- Serial execution (realistic per-query latency)
- SELECT * (fetches actual data, not just index)
- EXPLAIN sampling: 1 in 100 queries for precise I/O

### Hot/Cold Matrix
- 9 ratios: 100%, 90%, 80%, 70%, 60%, 50%, 30%, 10%, 0% hot traffic
- 1,000 iterations per ratio
- Total: 9,000 multi-entity requests per run

## 📈 Visualizations (Auto-Generated)

1. **P99 Latency vs Hot Traffic %**: Core story
2. **Break-Even Skew**: Where Lakebase wins/loses vs DynamoDB
3. **Cache Locality → Latency**: Physics of cache hits
4. **Entity Contribution Heatmap**: Which entity dominates P99
5. **Request Distribution**: Fully hot vs fully cold
6. **IO Amplification Curve**: Disk blocks vs skew
7. **Worst Case Analysis**: Fully-cold fanout distribution

## 🔧 Setup

### Prerequisites
- Databricks workspace with cluster
- Lakebase instance (PostgreSQL-compatible)
- Python 3.8+
- Databricks CLI (optional, for Bundle deployment)

### Install Dependencies
```bash
pip install -r requirements.txt
```

### Configure Credentials
1. Copy `credentials.template` to `.credentials`
2. Fill in Lakebase connection details
3. Add Databricks token

### Grant PostgreSQL Permissions
```sql
-- Run as superuser
GRANT EXECUTE ON FUNCTION pg_stat_reset() TO fraud_benchmark_user;
```

## 📊 Results

Results are stored in:
- **Table**: `features.zipfian_feature_serving_results`
- **Keys**: `features.zipfian_keys_per_run`

Query results:
```sql
SELECT * FROM features.zipfian_feature_serving_results
WHERE run_id = (SELECT run_id FROM features.zipfian_feature_serving_results ORDER BY run_ts DESC LIMIT 1)
ORDER BY hot_traffic_pct DESC;
```

## 🎯 Key Insights (80% Hot Traffic)

**Realistic Production Scenario:**
- P99 latency: ~45-80ms (competitive with DynamoDB's 79ms)
- Fully hot requests: ~51% (0.8³)
- Fully cold requests: ~0.8% (0.2³)
- Mixed requests: ~48%

**Cost:**
- Lakebase: ~$100/day
- DynamoDB: ~$75,000/day (50 tables × $1,500/day)
- **Savings: $27M/year (750x cheaper)**

## 📝 Documentation

- **`RUN_ZIPFIAN_BENCHMARK.md`**: How to run the benchmark
- **`ZIPFIAN_V3_KEY_PERSISTENCE.md`**: V3 improvements and methodology
- **`COST_METHODOLOGY.md`**: Cost calculation methodology
- **`CUSTOMER_DISCOVERY_QUESTIONS.md`**: Customer discovery guide

## 🚨 Troubleshooting

### Job fails with "schema benchmark does not exist"
- Schema should be `features`, not `benchmark`
- Update job parameters: `lakebase_schema: features`

### "Could not reset stats (requires superuser)"
- Non-fatal warning
- Grant permission: `GRANT EXECUTE ON FUNCTION pg_stat_reset() TO user;`
- Or ignore (uses aggregate I/O stats instead)

### "Unable to access the notebook"
- Upload notebook first: `python3 upload_zipfian_v3.py`
- Check notebook path in job config

### TLS/SSL certificate errors
- Mac keychain issue
- Run scripts with `required_permissions: ["all"]`
- Or update Databricks CLI config

## 🏆 Version History

- **V3** (Current): Production-grade with all fixes
- **V2** (Deprecated): Had biased key sampling and SELECT 1 issues
- **V1** (Deprecated): Initial implementation

## 📧 Contact

For questions or issues, check the documentation or logs in Databricks UI.

---

**Status**: ✅ Production-Ready  
**Last Updated**: January 23, 2026  
**Current Run**: Monitor at Databricks UI
