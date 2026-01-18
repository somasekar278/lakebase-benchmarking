# 🚀 Lakebase Feature Serving Benchmark Framework

**Production-ready benchmarking framework for Lakebase feature serving with optimized bulk loading.**

[![Python](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![Databricks](https://img.shields.io/badge/databricks-runtime-orange.svg)](https://databricks.com/)

---

## 🎯 What is This?

**Comprehensive framework for:**
1. **Bulk loading** billions of rows into Lakebase (4-10x faster than JDBC)
2. **Benchmarking** feature serving performance vs DynamoDB
3. **Production optimization** with cache warming and connection pooling
4. **Publication-ready reports** with component-level analysis

**Target Use Case:** Feature store benchmarking (30 tables, 16B+ rows, 30-40ms p99 latency)

---

## 🚀 Quick Start

### **For Bulk Loading (Production Scale):**

```bash
# 1. Configure credentials
# Edit config.py with your Lakebase connection details

# 2. Deploy to Databricks
databricks bundle deploy -t dev

# 3. Run bulk load for 30 fraud tables (16.6B rows)
databricks bundle run fraud_load_all_tables -t dev

# 4. Build indexes (optimized, ~3-4 hours for 11 tables)
#    With automatic tuning: 3x faster than default PostgreSQL settings!
databricks bundle run fraud_build_indexes -t dev

# Done! ✅ Ready for benchmarking
```

**Load Performance:**
- 2B rows in ~20-30 minutes
- 8 concurrent workers
- Stage-Index-Swap pattern (no downtime)
- Automatic checkpointing (resume on failure)

### **For Feature Serving Benchmarks:**

```bash
# 🚀 AUTOMATED: Single command runs entire workflow
databricks bundle run fraud_benchmark_end_to_end -t dev

# Runs automatically:
# 1. Verify tables (10 min)
# 2. Quick baseline (30 min)  
# 3. Production benchmark (2 hours)
# 4. Generate report (1 hour)
# Total: ~3.5 hours fully automated

# Done! ✅ Complete results + publication-ready charts
```

**Or run individual steps:**
```bash
databricks bundle run fraud_verify_tables -t dev
databricks bundle run fraud_benchmark_feature_serving -t dev
databricks bundle run fraud_production_benchmark -t dev
```

**Expected Performance:**
- P50: 37ms (vs DynamoDB 30ms, +23%)
- P99: 53ms (vs DynamoDB 79ms, **-33%** ✅)
- Cache hit ratio: 99.8%
- 30 tables, single round-trip

### **For Database Migration (Workspace → Workspace):**

```bash
# 1. Dump current database to UC Volume (30-60 min)
databricks bundle run fraud_dump_database -t dev

# 2. In new workspace, configure connection to new Lakebase endpoint
# Edit config.py with new endpoint details

# 3. Restore from UC Volume (1-2 hours)
databricks bundle run fraud_restore_database -t dev

# Done! ✅ All 30 tables + indexes migrated (2-3 hours vs 30+ hours re-load)
```

**Migration Benefits:**
- ✅ Preserves all data, indexes, constraints
- ✅ ~10x faster than re-loading (2-3 hours vs 30+ hours)
- ✅ Compressed backup (~500-800 GB vs 3TB uncompressed)
- ✅ Parallel restore for faster recovery

See [MIGRATION_GUIDE.md](MIGRATION_GUIDE.md) for detailed instructions.

---

## 📊 Architecture

### **Bulk Loading (Stage-Index-Swap Pattern)**

```
Phase 1: Pure COPY (No Indexes)
  Table 1: Generate CSV → COPY (8 workers parallel)
  Table 2: Generate CSV → COPY
  ... (all 30 tables)

Phase 2: Parallel Index Builds
  Table 1: CREATE INDEX CONCURRENTLY
  Table 2: CREATE INDEX CONCURRENTLY
  ... (8 parallel builds)
```

**Key Features:**
- ✅ Checkpointing (skip completed tables)
- ✅ No duplicate loads (idempotent)
- ✅ Stage tables always dropped
- ✅ Production tables never dropped
- ✅ Handles 1B+ row tables efficiently

### **Feature Serving (Optimized Pattern)**

```python
from utils.feature_server import LakebaseFeatureServer

# Initialize with automatic optimizations
server = LakebaseFeatureServer(
    lakebase_config=config,
    table_schemas=schemas,
    pool_size=10,          # Fixed pool
    enable_warmup=True     # Auto cache warming
)

# Single round-trip for 30 tables
features = server.get_features(hashkey, table_names)
```

**Optimizations:**
- ✅ psycopg3 explicit pipelining (single network flush)
- ✅ Separated execute/fetch (minimizes round-trips)
- ✅ Connection-local prepared statements
- ✅ Fixed-size connection pool (no cache eviction)
- ✅ Automatic cache warming (prevents cold starts)

---

## 📊 Key Features

### **Bulk Loading:**
✅ **Stage-Index-Swap pattern** - No downtime, optimal performance  
✅ **Parallel COPY** - 8 concurrent workers, handles 1B+ rows  
✅ **Checkpointing** - Resume on failure, never reload completed tables  
✅ **Idempotent** - Safe to re-run, automatic skip logic  

### **Feature Serving:**
✅ **Optimized query pattern** - Single network flush for 30 tables  
✅ **37ms p50, 53ms p99** - Beats DynamoDB on tail latency  
✅ **Cache warming** - Prevents 600ms+ cold-start penalty  
✅ **Production-ready** - Connection pooling, error handling, monitoring

### **Benchmarking:**
✅ **Component-level analysis** - Network, query, transfer breakdown  
✅ **Publication-ready reports** - 13+ visualizations, executive summaries  
✅ **Cache metrics** - Hit ratios, buffer stats, index usage  
✅ **vs DynamoDB** - Direct comparison with baseline

---

## 📂 Project Structure

```
lakebase-benchmarking/
│
├── README.md                         # This file
├── requirements.txt                  # Dependencies
├── config.py                         # Configuration (gitignored)
├── databricks.yml                    # DABs deployment config
│
├── notebooks/                        # Databricks notebooks
│   ├── load_schema_from_ddl.py       # Bulk loading
│   ├── build_missing_indexes.py      # Index builder
│   ├── verify_all_tables.py          # Pre-deployment check
│   ├── generate_benchmark_report.py  # Full benchmark report
│   └── benchmarks/
│       └── benchmark_generic.py      # Feature serving benchmark
│
├── utils/                            # Core utilities
│   ├── feature_server.py             # ⭐ Optimized feature serving
│   ├── instrumented_feature_server.py# Component-level timing
│   ├── schema_loader.py              # Schema + data generation
│   ├── cache_warming.py              # Cache warming automation
│   ├── lakebase_connection.py        # Connection management
│   └── metrics.py                    # Performance metrics
│
├── backends/                         # Database backends
│   └── lakebase.py                   # Lakebase implementation
│
├── core/                             # Core abstractions
│   ├── backend.py                    # Backend interface
│   └── workload.py                   # Workload definitions
│
├── resources/                        # DABs resources
│   ├── jobs.yml                      # Job definitions
│   └── volumes.yml                   # UC volume config
│
└── Documentation/
    ├── BENCHMARKING.md               # ⭐ Benchmark guide
    ├── PRODUCTION_GUIDE.md           # ⭐ Production deployment
    ├── OPTIMIZATION_DETAILS.md       # Technical deep-dive
    └── RCA_POSTGRESQL_63_CHAR_LIMIT.md  # Lessons learned
```

---

## 📚 Documentation

| Document | Purpose |
|----------|---------|
| **[BENCHMARKING.md](BENCHMARKING.md)** | 📊 Benchmarking guide with report generation |
| **[PRODUCTION_GUIDE.md](PRODUCTION_GUIDE.md)** | 🚀 Production deployment & cache warming |
| **[MIGRATION_GUIDE.md](MIGRATION_GUIDE.md)** | 🔄 Database migration (pg_dump/restore) |
| **[INDEX_OPTIMIZATION.md](INDEX_OPTIMIZATION.md)** | ⚡ 3x faster hybrid scheduling + memory-fraction |
| **[INDEX_BUILD_BASELINE_RESULTS.md](INDEX_BUILD_BASELINE_RESULTS.md)** | 📊 Actual production build times (baseline for A/B test) |
| **[OPTIMIZATION_DETAILS.md](OPTIMIZATION_DETAILS.md)** | 🔧 Technical deep-dive on query optimizations |
| **[RCA_POSTGRESQL_63_CHAR_LIMIT.md](RCA_POSTGRESQL_63_CHAR_LIMIT.md)** | 📖 Lessons learned from 63-char bug |

---

## 🔧 Available DABs Jobs

```bash
# Bulk Loading
databricks bundle run fraud_load_single_table -t dev   # Test single table
databricks bundle run fraud_load_all_tables -t dev     # Load all 30 tables
databricks bundle run fraud_build_indexes -t dev       # Build indexes (3-4 hours)

# Verification
databricks bundle run fraud_verify_tables -t dev       # Check all tables ready

# Benchmarking (Automated)
databricks bundle run fraud_benchmark_end_to_end -t dev  # 🚀 FULL WORKFLOW (3.5h)

# Benchmarking (Individual Steps)
databricks bundle run fraud_benchmark_feature_serving -t dev       # Quick baseline (10 min)
databricks bundle run fraud_production_benchmark -t dev            # Concurrent tests (2h)
databricks bundle run fraud_generate_benchmark_report -t dev       # Report with charts (1h)

# Migration
databricks bundle run fraud_dump_database -t dev       # Backup for migration (1h)
databricks bundle run fraud_restore_database -t dev    # Restore from backup (2h)
```

**For custom schemas**, use `notebooks/load_schema_from_ddl.py` directly in Databricks.

---

## 🏥 Troubleshooting

### **Issue: High p99 Latency**
1. Check cache hit ratio (`fraud_verify_tables` output)
2. Check for autovacuum: `SELECT * FROM pg_stat_activity WHERE query LIKE '%autovacuum%'`
3. Check for sequential scans: `SELECT * FROM pg_stat_user_tables WHERE seq_scan > 0`
4. Re-run cache warmup if needed

### **Issue: Bulk Load Failure**
1. Check Databricks job logs for specific error
2. Verify Lakebase connectivity
3. Check disk space in UC volume
4. Resume with checkpointing (job will skip completed tables)

### **Issue: Index Build Takes Too Long**
- Expected: ~3-4 hours for 11 tables (sequential, batch_size=1) with optimization
- **NEW:** Automatic dynamic tuning (3x faster than default settings!)
  - `maintenance_work_mem`: 12GB (vs default 64MB)
  - `max_parallel_maintenance_workers`: 4 (vs default 2)
- Check Lakebase CU allocation (32 CU / 64 GB RAM recommended)
- Monitor with: `SELECT * FROM pg_stat_progress_create_index`
- See [INDEX_OPTIMIZATION.md](INDEX_OPTIMIZATION.md) for details

---

## 🔒 Security

**Credential Protection:**
- `config.py` is gitignored (contains credentials)
- `.gitignore` blocks all sensitive files
- `.gitattributes` provides additional safeguards
- Pre-commit hook scans for secrets

**Never commit:**
- `config.py`
- `.env` files
- Any files with passwords/secrets

---

## 📊 Performance Summary

### **Bulk Loading (Production Scale):**
| Table Size | Load Time | Throughput |
|------------|-----------|------------|
| 75M rows | 5 min | 250K rows/s |
| 1B rows | 20-30 min | 550K rows/s |
| 2B rows | 40-50 min | 660K rows/s |

**Includes:** Data generation + COPY + Index build

### **Feature Serving (30 Tables):**
| Metric | Lakebase | DynamoDB | Verdict |
|--------|----------|----------|---------|
| P50 | 37ms | 30ms | Comparable |
| P99 | 53ms | 79ms | **33% faster** ✅ |
| Cache Hit % | 99.8% | N/A | Optimal |

**Key Advantage:** Predictable p99 (no hot partitions, no throttling)

---

## ✅ Summary

**This framework provides:**
- ✅ Production-scale bulk loading (billions of rows)
- ✅ Optimized feature serving (37ms p50, 53ms p99)
- ✅ Publication-ready benchmarks (vs DynamoDB)
- ✅ Complete automation (DABs deployment)
- ✅ Battle-tested reliability (checkpointing, idempotency)

**Ready for production feature serving benchmarks!** 🚀

**See documentation for detailed guides on deployment, optimization, and benchmarking.**

---

## 🤝 Contributing

This is a private framework for Lakebase benchmarking. For questions or issues, contact the team.

---

## 📄 License

Proprietary - Internal use only
