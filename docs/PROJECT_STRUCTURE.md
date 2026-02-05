# Lakebase Benchmarking - Project Structure

## Overview
This project contains all necessary files for running Zipfian-distributed benchmarks on Lakebase feature serving.

## 📁 Directory Structure

```
lakebase-benchmarking/
├── notebooks/                          # Databricks notebooks (Python)
│   ├── generate_csvs.py                # 1️⃣ Generate synthetic CSV data
│   ├── run_pipelined_load_inline.py    # 2️⃣ Load CSVs to Lakebase
│   ├── verify_loaded_tables.py         # 2️⃣.5 Verify data loaded correctly
│   ├── benchmark_zipfian_realistic_v5.4.py  # 3️⃣ Run benchmarks
│   ├── zipfian_benchmark_visuals.py    # 4️⃣ Generate visualization reports
│   └── view_report.py                  # 5️⃣ View generated reports
├── utils/                              # Python utilities
│   ├── __init__.py
│   ├── csv_timestamp_validator.py      # Validate CSV format
│   ├── feature_server.py               # Feature serving utilities
│   └── pipelined_load.py               # Pipelined load implementation
├── data/                               # Data specifications
│   └── fraud_tables_row_counts_30_COMPLETE.txt  # Row counts per table
├── generated/                          # Generated DDL
│   └── fraud_feature_tables_30_COMPLETE.sql  # DDL for 30 feature tables
├── generated_reports/                  # Example output reports
│   └── example_report.html             # Sample benchmark report
├── templates/                          # HTML templates
│   └── report_template.html            # HTML template for reports
├── setup/                              # Setup scripts
│   └── SETUP_NEW_WORKSPACE.sql         # SQL to setup new Lakebase instance
├── sql/                                # SQL scripts
│   └── create_rpc_request_function.sql # RPC function for single-call fetching
├── docs/                               # Documentation
│   ├── PROJECT_STRUCTURE.md            # This file
│   └── RUN_ZIPFIAN_BENCHMARK.md        # Quick start guide
├── resources/                          # Databricks bundle resources
│   └── jobs.yml                        # Job definitions
├── databricks.yml                      # Databricks bundle configuration
├── requirements.txt                    # Python dependencies
└── README.md                           # Main documentation

```

## 🚀 Workflow

### Phase 1: Data Generation
1. **Generate Synthetic CSVs** (`generate_csvs.py`)
   - Creates 30 feature table CSVs based on DDL
   - Uses row counts from `fraud_tables_row_counts_30_COMPLETE.txt`
   - Outputs to UC Volume

### Phase 2: Data Loading
2. **Load CSVs to Lakebase** (`run_pipelined_load_inline.py`)
   - Creates tables in Lakebase
   - Loads CSVs using pipelined approach
   - Optimized for large datasets (16.6B rows)

3. **Verify Tables** (`verify_loaded_tables.py`)
   - Validates row counts
   - Checks data integrity

### Phase 3: Benchmarking
4. **Run Benchmarks** (`benchmark_zipfian_realistic_v5.4.py`)
   - Tests various execution modes (serial, binpacked, RPC)
   - Simulates Zipfian-distributed traffic (hot/cold keys)
   - Records latency, cache behavior, query patterns
   - Stores results in Postgres tables

### Phase 4: Visualization
5. **Generate Reports** (`zipfian_benchmark_visuals.py`)
   - Queries benchmark results from Postgres
   - Creates interactive HTML reports
   - Includes charts for latency, cache efficiency, performance

6. **View Reports** (`view_report.py`)
   - Downloads and displays generated reports

## 📊 Key Files

### Data Specification
- **`data/fraud_tables_row_counts_30_COMPLETE.txt`**: Defines row counts for each of 30 tables
  - Total: 16.6 billion rows
  - 3 entities (card_fingerprint, customer_email, cardholder_name)
  - 10 table types per entity (fraud rates, good rates, time_since features)
  - 3 time windows per type (30d, 90d, 365d)

### DDL Schema
- **`generated/fraud_feature_tables_30_COMPLETE.sql`**: Complete DDL for all 30 tables
  - Primary key: `hash_key TEXT PRIMARY KEY`
  - Feature columns specific to each table type
  - Timestamp columns for temporal features

### Configuration
- **`databricks.yml`**: Databricks Asset Bundle configuration
  - Defines targets (dev, staging, prod)
  - Lakebase connection parameters
  - Cluster configurations
  - Volume paths

- **`resources/jobs.yml`**: Databricks job definitions
  - CSV generation job
  - Data load job
  - Benchmark job
  - Visualization job

### SQL Scripts
- **`setup/SETUP_NEW_WORKSPACE.sql`**: Initial Lakebase setup
  - Creates schema
  - Sets up permissions
  - Creates benchmark results tables

- **`sql/create_rpc_request_function.sql`**: RPC function
  - Single PostgreSQL function call
  - Fetches all 30 features in one round-trip
  - Optimized for low-latency serving

## 🔧 Utilities

### `utils/pipelined_load.py`
- Implements pipelined CSV loading to Lakebase
- Handles connection pooling
- Batch processing with COPY FROM
- Error recovery and retry logic

### `utils/csv_timestamp_validator.py`
- Validates CSV format before loading
- Ensures timestamps are strings (not integers)
- Checks column counts match DDL

### `utils/feature_server.py`
- Feature serving simulation utilities
- Query generation helpers
- Timing and metrics collection

## 📝 Documentation

### README.md
Main project documentation:
- Project overview
- Setup instructions
- Architecture details
- Key concepts

### RUN_ZIPFIAN_BENCHMARK.md
Quick start guide:
- Prerequisites
- Step-by-step execution
- Configuration options
- Troubleshooting

## 🎯 Key Concepts

### Zipfian Distribution
- Models real-world access patterns
- Hot keys (frequently accessed): ~20% of keys
- Cold keys (rarely accessed): ~80% of keys
- Exponential frequency decay

### Execution Modes
1. **Serial**: Query tables sequentially
2. **Binpacked**: Batch queries together
3. **Binpacked Parallel**: Parallel execution with thread pool
4. **RPC Mode**: Single function call fetches all features

### Benchmark Metrics
- **Latency**: P50, P95, P99, avg response times
- **Cache Efficiency**: Buffer hit ratios, cache scores
- **Performance**: Queries per request, I/O blocks, planning time
- **Tail Amplification**: Slow query detection and analysis

## 🔄 Development Workflow

### Adding New Features
1. Update DDL in `generated/fraud_feature_tables_30_COMPLETE.sql`
2. Update row counts in `fraud_tables_row_counts_30_COMPLETE.txt`
3. Regenerate CSVs with `generate_csvs.py`
4. Reload tables with `run_pipelined_load_inline.py`
5. Re-run benchmarks

### Modifying Benchmarks
1. Edit `benchmark_zipfian_realistic_v5.4.py`
2. Update result table schemas if needed
3. Run benchmark with new parameters
4. Regenerate reports

### Customizing Reports
1. Edit `templates/report_template.html` for layout changes
2. Modify `zipfian_benchmark_visuals.py` for chart updates
3. Regenerate reports

## 🚦 Prerequisites

### Databricks
- Workspace with Unity Catalog
- Cluster with appropriate node types
- UC Volume for CSV storage

### Lakebase
- PostgreSQL-compatible database
- Sufficient compute for 16.6B rows
- Network connectivity from Databricks

### Dependencies
See `requirements.txt`:
- psycopg2-binary (PostgreSQL driver)
- pandas (data manipulation)
- matplotlib / seaborn (visualization)
- PySpark (data generation)

## 📞 Support

For questions or issues:
1. Check `README.md` for detailed documentation
2. Review `RUN_ZIPFIAN_BENCHMARK.md` for quick start
3. Examine example reports in `generated_reports/`

---

**Version:** 5.4  
**Last Updated:** February 5, 2026  
**Status:** Production Ready ✅
