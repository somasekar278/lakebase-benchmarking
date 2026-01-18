# Index Build Baseline Results (Default PostgreSQL Settings)

## 📊 Actual Production Build - Run Date: 2026-01-17

These are the **actual build times** from the production index build job using PostgreSQL default settings.

### Build Configuration:
- **Settings:** PostgreSQL defaults
  - `maintenance_work_mem = 64MB`
  - `max_parallel_maintenance_workers = 2`
- **Cluster:** 32 CU / 64 GB RAM Lakebase
- **Strategy:** Sequential (batch_size=1)
- **Total tables:** 11 tables (all >50 GB)

---

## 🕐 Detailed Build Times

| # | Table Name | Rows | Size (GB) | Build Time (min) | Notes |
|---|------------|------|-----------|------------------|-------|
| 1 | client_id_card_fingerprint_good_rates_90d | 1.0B | 162.2 | 71.8 | Large table |
| 2 | client_id_card_fingerprint_good_rates_365d | 1.0B | 162.2 | 70.4 | Large table |
| 3 | client_id_card_fingerprint_good_rates_30d | 1.0B | 162.2 | 70.4 | Large table |
| 4 | client_id_card_fingerprint__fraud_rates__365d | 1.0B | 146.0 | 67.1 | Large table |
| 5 | client_id_customer_email_clean__time_since__30d | 700M | 118.7 | 50.4 | Medium-large |
| 6 | client_id_customer_email_clean__time_since__90d | 700M | 118.7 | 50.4 | Medium-large |
| 7 | client_id_customer_email_clean__time_since__365d | 700M | 118.7 | 50.1 | Medium-large |
| 8 | client_id_cardholder_name_clean__good_rates_365d | 400M | 64.9 | **27.8** | **A/B test target** |
| 9 | client_id_customer_email_clean__good_rates_90d | 400M | 64.9 | 27.6 | Medium table |
| 10 | client_id_customer_email_clean__good_rates_365d | 400M | 64.9 | 27.5 | Medium table |
| 11 | client_id_customer_email_clean__good_rates_30d | 400M | 64.9 | 26.8 | Medium table |

---

## 📈 Summary Statistics

### By Table Size:

| Size Range | Count | Avg Time (min) | Total Time (min) |
|------------|-------|----------------|------------------|
| 150-165 GB | 3 | 70.9 | 212.6 |
| 145-150 GB | 1 | 67.1 | 67.1 |
| 115-120 GB | 3 | 50.3 | 150.9 |
| 64-65 GB | 4 | 27.4 | 109.7 |
| **Total** | **11** | **49.1** | **540.3** |

### Overall:
- **Total duration:** 540.3 minutes (9.0 hours)
- **Average per table:** 49.1 minutes
- **Success rate:** 11/11 (100% ✅)
- **Failed builds:** 0

---

## 🎯 A/B Test Target Table

**Selected:** `client_id_cardholder_name_clean__good_rates_365d`

### Why This Table?

1. **Medium size (64.9 GB):**
   - Large enough to show meaningful speedup
   - Not so large that rebuild takes hours
   - Ideal for A/B testing

2. **Actual baseline time: 27.8 minutes**
   - Well-documented from production run
   - Consistent with 3 other similar-sized tables (27.4 min avg)

3. **Expected optimized time: ~9 minutes**
   - Settings: 48GB memory, 1 worker
   - Memory fraction: 48GB / 42GB ≈ 114%
   - Expected speedup: **3.1x faster**
   - Time savings: ~19 minutes (68% reduction)

### A/B Test Configuration:

```yaml
test_table: "client_id_cardholder_name_clean__good_rates_365d"
original_build_time_min: "27.8"  # From this baseline
```

---

## 📊 Performance Analysis

### Time vs Size Correlation:

```
Size (GB) | Avg Time (min) | Time per GB
----------|----------------|------------
162       | 70.9          | 0.44 min/GB
146       | 67.1          | 0.46 min/GB
119       | 50.3          | 0.42 min/GB
65        | 27.4          | 0.42 min/GB
```

**Average: 0.43 minutes per GB**

This consistency suggests:
- ✅ Linear scaling with table size
- ✅ I/O bound (not CPU bound)
- ✅ Similar data distribution across tables

### Memory Fraction Analysis (Estimated):

```
Table Size | Est. Index | Default Memory | Memory Fraction | Sort Passes
-----------|------------|----------------|-----------------|-------------
162 GB     | 105 GB     | 64 MB         | 0.06%          | ~6 passes
146 GB     | 95 GB      | 64 MB         | 0.07%          | ~6 passes
119 GB     | 77 GB      | 64 MB         | 0.08%          | ~5 passes
65 GB      | 42 GB      | 64 MB         | 0.15%          | ~5 passes
```

**Key insight:** All tables have extremely low memory fraction (<0.2%), causing heavy external sorting.

---

## ⚡ Predicted Optimization Results

Using hybrid scheduling + memory-fraction strategy:

### Individual Table Predictions:

| Table Size | Baseline | Optimized Settings | Expected Time | Speedup |
|------------|----------|-------------------|---------------|---------|
| 162 GB | 70.9 min | 48GB, 1 worker | ~22 min | 3.2x |
| 146 GB | 67.1 min | 48GB, 1 worker | ~20 min | 3.4x |
| 119 GB | 50.3 min | 48GB, 1 worker | ~15 min | 3.4x |
| 65 GB | 27.4 min | 48GB, 1 worker | ~9 min | 3.0x |

### Overall Prediction:

- **Baseline (11 tables):** 540.3 minutes (9.0 hours)
- **Optimized (11 tables):** ~180 minutes (3.0 hours)
- **Overall speedup:** **3.0x faster**
- **Time saved:** ~6 hours (67% reduction)

### Why This Works:

```
Optimized settings increase memory fraction:

65 GB table example:
  Baseline: 64MB / 42GB = 0.15% memory fraction → 5 passes
  Optimized: 48GB / 42GB = 114% memory fraction → 2 passes
  
Fewer passes = less I/O = 3x faster!
```

---

## 🧪 A/B Test Execution Plan

### Test Parameters:

```python
# Table to test
test_table = "client_id_cardholder_name_clean__good_rates_365d"

# Baseline (already completed)
baseline_settings = {
    "maintenance_work_mem": "64MB",
    "max_parallel_workers": 2,
    "build_time_min": 27.8,
    "memory_fraction": 0.15%
}

# Optimized (to be tested)
optimized_settings = {
    "maintenance_work_mem": "48GB",
    "max_parallel_workers": 1,
    "expected_time_min": 9,
    "expected_memory_fraction": 114%
}

# Expected results
expected_speedup = 3.1x
expected_time_saved = 18.8 minutes
```

### Test Execution:

```bash
# After benchmarks complete, run A/B test
databricks bundle run fraud_test_index_optimization -t dev

# The job will:
# 1. Drop existing index on test table
# 2. Rebuild with optimized settings (48GB, 1 worker)
# 3. Measure actual build time
# 4. Compare to baseline (27.8 min)
# 5. Report speedup
```

### Success Criteria:

- ✅ Optimized build time: <10 minutes
- ✅ Speedup: >2.5x faster
- ✅ Index valid and ready for queries
- ✅ No errors or warnings

---

## 📝 Notes

### Build Environment:
- **Date:** 2026-01-17
- **Job:** `fraud_build_indexes`
- **Cluster:** fe-sandbox-one-env-som-workspace
- **Lakebase:** 32 CU (64 GB RAM)

### Data Characteristics:
- **Total rows across 11 tables:** ~9.4 billion
- **Total data size:** ~1,523 GB
- **Primary key:** `hashkey` (single column B-tree index)
- **Index size:** ~65% of table size (estimated)

### PostgreSQL Version:
- **Compatible with:** PostgreSQL 13+
- **Index type:** B-tree (unique)
- **Index method:** `CREATE UNIQUE INDEX CONCURRENTLY`

---

## 🔗 Related Documents

- [INDEX_OPTIMIZATION.md](INDEX_OPTIMIZATION.md) - Hybrid scheduling strategy
- [notebooks/test_index_optimization.py](notebooks/test_index_optimization.py) - A/B test notebook
- [resources/jobs.yml](resources/jobs.yml) - Job configuration

---

**This baseline data will be used to validate the 3x speedup claim from hybrid scheduling + memory-fraction optimization!** 🎯
