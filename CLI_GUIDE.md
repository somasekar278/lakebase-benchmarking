# 🚀 CLI Guide - run_benchmark.py

Complete guide to using the Lakebase Benchmarking Framework CLI tool.

**Repository**: lakebase-benchmarking

---

## Quick Start

```bash
# See all available commands
python run_benchmark.py --help

# List available backends
python run_benchmark.py --list-backends

# Generate a schema
python run_benchmark.py --generate-schema --num-tables 30 --features-per-table 5
```

---

## Commands Overview

| Command | Description | Example |
|---------|-------------|---------|
| `--list-backends` | List all backends and their status | `python run_benchmark.py --list-backends` |
| `--show-config` | Show framework configuration | `python run_benchmark.py --show-config` |
| `--generate-schema` | Generate flexible schema | `python run_benchmark.py --generate-schema --num-tables 30` |
| `--backend` | Run benchmark on single backend | `python run_benchmark.py --backend lakebase` |
| `--backends` | Run benchmark on multiple backends | `python run_benchmark.py --backends lakebase dynamodb --compare` |

---

## Detailed Examples

### 1. List Available Backends

See which backends are enabled and configured:

```bash
python run_benchmark.py --list-backends
```

**Output:**
```
================================================================================
AVAILABLE BACKENDS
================================================================================

LAKEBASE
  Status: ✅ Enabled
  Config: ✅ Valid
  Host: ep-autumn-fire-d318blbk.database.eu-west-1.cloud.databricks.com
  Database: benchmark

DYNAMODB
  Status: ❌ Disabled

AURORA
  Status: ❌ Disabled

COSMOSDB
  Status: ❌ Disabled

================================================================================
✅ Enabled backends: lakebase
================================================================================
```

### 2. Show Current Configuration

View all framework settings:

```bash
python run_benchmark.py --show-config
```

Shows:
- Lakebase connection details
- Benchmark configuration (iterations, warmup, keys per lookup)
- Bulk load settings
- Backend configurations

### 3. Generate Custom Schema

Generate schema for any workload:

```bash
# Match customer's workload: 30 tables × 5 features = 150 features
python run_benchmark.py --generate-schema \
    --num-tables 30 \
    --features-per-table 5 \
    --rows-per-table 100000000 \
    --output-dir generated/fraud_30t_5f
```

**Output:**
```
================================================================================
SCHEMA GENERATION
================================================================================
Tables: 30
Features per table: 5
Total features: 150
Rows per table: 100,000,000
Output directory: generated/fraud_30t_5f
================================================================================

📝 Generating schema...
✅ Generated setup SQL: generated/fraud_30t_5f/setup.sql
✅ Generated stored procedure: generated/fraud_30t_5f/stored_procedure.sql
✅ Generated Python config: generated/fraud_30t_5f/schema_config.py

✅ Schema generated successfully!
📁 Output directory: /path/to/generated/fraud_30t_5f
📄 Files created:
   - setup.sql
   - stored_procedure.sql
   - schema_config.py
   - README.md
```

**Generated Files:**

| File | Description |
|------|-------------|
| `setup.sql` | CREATE TABLE statements, grants, indexes |
| `stored_procedure.sql` | Binpacked lookup stored procedure |
| `schema_config.py` | Python configuration for notebooks |
| `README.md` | Usage instructions |

### 4. Run Benchmark on Lakebase

```bash
python run_benchmark.py --backend lakebase \
    --num-tables 30 \
    --features-per-table 5 \
    --iterations 100 \
    --warmup 5
```

**Note:** This validates configuration. Actual benchmark execution requires data to be loaded via Databricks notebooks.

### 5. Compare Multiple Backends

```bash
python run_benchmark.py --backends lakebase dynamodb \
    --compare \
    --show-costs \
    --output comparison.json
```

Generates side-by-side comparison with:
- Performance metrics (P50, P95, P99 latency)
- Cost breakdown (loading, queries, storage)
- Cost-efficiency scores

### 6. Generate Quick Test Schema

For rapid testing with small datasets:

```bash
python run_benchmark.py --generate-schema \
    --num-tables 5 \
    --features-per-table 3 \
    --rows-per-table 1000000 \
    --output-dir test_schema_small
```

---

## Command-Line Options Reference

### Actions

| Option | Description |
|--------|-------------|
| `--list-backends` | List all available backends and their status |
| `--show-config` | Show current framework configuration |
| `--generate-schema` | Generate flexible schema for custom workload |

### Backend Selection

| Option | Description | Example |
|--------|-------------|---------|
| `--backend BACKEND` | Single backend to benchmark | `--backend lakebase` |
| `--backends BACKEND...` | Multiple backends to benchmark | `--backends lakebase dynamodb` |

### Workload Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `--num-tables N` | 30 | Number of tables |
| `--features-per-table N` | 5 | Features per table |
| `--rows-per-table N` | 100,000,000 | Rows per table |

### Benchmark Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `--iterations N` | 100 | Number of benchmark iterations |
| `--warmup N` | 5 | Number of warmup iterations |
| `--keys-per-table N` | 25 | Keys to lookup per table |

### Output Options

| Option | Description |
|--------|-------------|
| `--compare` | Generate comparison report across backends |
| `--show-costs` | Include cost analysis in results |
| `--output FILE` | Output file for benchmark results (JSON) |
| `--output-dir DIR` | Output directory for schemas (default: generated) |

---

## Complete Workflow

### Step 1: Check Available Backends

```bash
python run_benchmark.py --list-backends
```

Make sure Lakebase (or your target backend) is enabled and configured.

### Step 2: Generate Schema

```bash
python run_benchmark.py --generate-schema \
    --num-tables 30 \
    --features-per-table 5 \
    --output-dir generated/fraud_30t
```

### Step 3: Setup Database

Run the generated SQL files:

```bash
psql -h HOST -p PORT -d DATABASE -U USER \
    -f generated/fraud_30t/setup.sql

psql -h HOST -p PORT -d DATABASE -U USER \
    -f generated/fraud_30t/stored_procedure.sql
```

### Step 4: Load Data

Use Databricks notebooks to load data:

**For tables < 100M rows:**
```
notebooks/data_loading/load_flexible_schema.py
```

**For tables >= 100M rows:**
```
notebooks/data_loading/load_bulk_copy.py
```

### Step 5: Run Benchmark

Execute benchmark via Databricks notebooks:

```
notebooks/benchmarks/benchmark_flexible.py
```

Or with Data API:

```
notebooks/benchmarks/benchmark_flexible_with_data_api.py
```

### Step 6: Analyze Results

Review:
- Latency distribution (P50, P95, P99)
- Cost breakdown
- Performance vs cost trade-offs

---

## Common Use Cases

### Use Case 1: Match Customer Workload

Customer has: 30-50 tables, ~150 features, 79ms P99

```bash
# Generate 30 tables × 5 features = 150 features
python run_benchmark.py --generate-schema \
    --num-tables 30 \
    --features-per-table 5 \
    --output-dir generated/customer_workload

# Alternative: 50 tables × 3 features = 150 features
python run_benchmark.py --generate-schema \
    --num-tables 50 \
    --features-per-table 3 \
    --output-dir generated/customer_workload_alt
```

### Use Case 2: Quick Local Testing

Small dataset for rapid iteration:

```bash
python run_benchmark.py --generate-schema \
    --num-tables 5 \
    --features-per-table 3 \
    --rows-per-table 100000 \
    --output-dir test_local
```

### Use Case 3: Large-Scale Production Test

Stress test with 1B rows:

```bash
python run_benchmark.py --generate-schema \
    --num-tables 4 \
    --features-per-table 10 \
    --rows-per-table 1000000000 \
    --output-dir test_1b_rows
```

**Note:** Use `load_bulk_copy.py` with UNLOGGED tables for 1B row loads.

### Use Case 4: Multi-Backend Comparison

Compare Lakebase vs DynamoDB:

```bash
# Step 1: Enable DynamoDB in config.py
# Edit config.py and set BACKEND_CONFIGS['dynamodb']['enabled'] = True

# Step 2: Run comparison
python run_benchmark.py --backends lakebase dynamodb \
    --compare \
    --show-costs \
    --output comparison_results.json
```

---

## Configuration

All settings are in `config.py`:

### Lakebase Connection

```python
LAKEBASE_CONFIG = {
    'host': 'your-lakebase-host.cloud.databricks.com',
    'port': 5432,
    'database': 'benchmark',
    'user': 'fraud_benchmark_user',
    'password': 'your-password',
    'schema': 'features',
}
```

### Benchmark Settings

```python
BENCHMARK_CONFIG = {
    'num_warmup': 5,
    'num_iterations': 100,
    'keys_per_table': 25,
}
```

### Flexible Schema Defaults

```python
FLEXIBLE_SCHEMA_CONFIG = {
    'default_num_tables': 30,
    'default_features_per_table': 5,
    'default_rows_per_table': 100_000_000,
}
```

---

## Troubleshooting

### Issue: Backend shows as disabled

**Solution:** Edit `config.py` and set `enabled = True` for the backend:

```python
BACKEND_CONFIGS = {
    'lakebase': {
        'enabled': True,  # Set to True
        # ... other config
    }
}
```

### Issue: Invalid configuration

**Solution:** Run `--list-backends` to see specific error messages:

```bash
python run_benchmark.py --list-backends
```

Common issues:
- Missing host, database, or credentials
- Invalid region for cloud backends
- Network connectivity issues

### Issue: Generated schema not found

**Solution:** Check the output directory:

```bash
ls -la generated/
```

If missing, regenerate:

```bash
python run_benchmark.py --generate-schema \
    --num-tables 30 \
    --features-per-table 5 \
    --output-dir generated/my_schema
```

### Issue: Benchmark requires data loaded

**Solution:** The CLI validates configuration but doesn't load data. Use Databricks notebooks:

1. Upload generated SQL to database
2. Run data loading notebooks
3. Then run benchmark notebooks

---

## Advanced Usage

### Custom Row Counts per Table

Generate schema with different row counts:

```bash
# 1M rows for quick testing
python run_benchmark.py --generate-schema \
    --num-tables 10 \
    --rows-per-table 1000000 \
    --output-dir test_1m

# 100M rows for realistic benchmarking
python run_benchmark.py --generate-schema \
    --num-tables 30 \
    --rows-per-table 100000000 \
    --output-dir test_100m

# 1B rows for stress testing
python run_benchmark.py --generate-schema \
    --num-tables 4 \
    --rows-per-table 1000000000 \
    --output-dir test_1b
```

### Bulk Load Optimization

For tables >= 100M rows, use bulk load:

1. **Enable in config.py:**
```python
BULK_LOAD_CONFIG = {
    'threshold_rows': 100_000_000,
    'use_unlogged': False,  # Or True for 2-3x speedup
}
```

2. **Use bulk load notebook:**
```
notebooks/data_loading/load_bulk_copy.py
```

3. **For maximum speed (⚠️ not crash-safe!):**
```python
BULK_LOAD_CONFIG = {
    'use_unlogged': True,  # 96x faster than JDBC!
}
```

### Cost Analysis

Show detailed cost breakdown:

```bash
python run_benchmark.py --backend lakebase \
    --show-costs \
    --output results_with_costs.json
```

Includes:
- Data loading costs
- Query execution costs
- Storage costs
- Total cost
- Cost-efficiency score

---

## Next Steps

1. **Read the guides:**
   - `FLEXIBLE_BENCHMARK_GUIDE.md` - Comprehensive framework guide
   - `COST_ANALYSIS_DESIGN.md` - Cost tracking details
   - `BULK_LOAD_GUIDE.md` - High-performance data loading

2. **Configure your environment:**
   - Edit `config.py` with your credentials
   - Enable desired backends

3. **Generate your first schema:**
   ```bash
   python run_benchmark.py --generate-schema --num-tables 5 --features-per-table 3
   ```

4. **Load data and run benchmarks:**
   - Use Databricks notebooks
   - Analyze results

---

## Getting Help

```bash
# Show all options
python run_benchmark.py --help

# List backends
python run_benchmark.py --list-backends

# Show configuration
python run_benchmark.py --show-config
```

**Documentation:**
- `CLI_GUIDE.md` (this file)
- `FLEXIBLE_BENCHMARK_GUIDE.md`
- `BACKEND_DESIGN.md`
- `COST_ANALYSIS_DESIGN.md`
- `BULK_LOAD_GUIDE.md`

**Support:**
- Check inline documentation in `config.py`
- Review generated `README.md` files in output directories
- See examples in this guide

---

## Summary

The CLI provides a **unified interface** for:

✅ **Schema Generation** - Any combination of tables/features  
✅ **Backend Management** - Enable, configure, validate  
✅ **Benchmark Execution** - Single or multi-backend  
✅ **Cost Analysis** - Performance + cost tracking  
✅ **Configuration** - View and validate settings  

**Single command to get started:**

```bash
python run_benchmark.py --generate-schema --num-tables 30 --features-per-table 5
```

**That's it! You're ready to benchmark Lakebase! 🚀**

