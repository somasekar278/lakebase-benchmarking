# Benchmark Readiness Checklist

## ✅ Pre-Flight Check (Run Once Index Build Completes)

### **Step 1: Verify All Tables Ready** (5 minutes)

```bash
databricks bundle run fraud_verify_tables -t dev
```

**Expected output:**
- ✅ 30 tables found
- ✅ All tables have PRIMARY KEYs (`has_primary_key = 1`)
- ✅ Total rows: ~16.6B
- ✅ No tables missing indexes

**If any issues:** Check job logs, re-run index build for failed tables

---

### **Step 2: Quick Baseline Benchmark** (10 minutes) - RECOMMENDED FIRST

```bash
databricks bundle run fraud_benchmark_feature_serving -t dev
```

**What it does:**
- Single-threaded latency test (1000 iterations)
- Tests 30-table fanout lookups
- Measures p50, p95, p99, max latencies
- Duration: ~5-10 minutes

**Expected results:**
- P50: 30-40ms
- P99: 50-80ms
- All queries successful

**If p99 > 100ms:** Cache may be cold, re-run or check for autovacuum

---

### **Step 3: Full Production Benchmark** (2 hours) - COMPREHENSIVE

```bash
databricks bundle run fraud_production_benchmark -t dev
```

**What it does:**
- Tests 1, 10, 50, 100 concurrent clients
- Tests 10, 20, 30 table subsets
- Checks for autovacuum interference
- Comprehensive latency breakdown
- Duration: ~1.5-2 hours

**Expected results:**
- P99 < 80ms for 1 client
- P99 < 150ms for 100 clients
- Autovacuum warnings if detected

---

### **Step 4: Generate Publication Report** (30 minutes) - OPTIONAL

```bash
databricks bundle run fraud_generate_benchmark_report -t dev
```

**What it does:**
- Re-runs single-threaded benchmark
- Generates component-level latency breakdown
- Creates publication-quality visualizations
- Duration: ~30 minutes

---

## 🎯 Recommended Workflow

### **🚀 AUTOMATED END-TO-END (3.5 hours) - RECOMMENDED:**
```bash
# Single command - runs all 4 steps automatically!
databricks bundle run fraud_benchmark_end_to_end -t dev

# What it does:
# 1. Verify tables (10 min) ✅
# 2. Quick baseline benchmark (30 min) ✅
# 3. Production benchmark (2 hours) ✅
# 4. Generate report (1 hour) ✅
# 
# Each step automatically proceeds to next on success
# Total: ~3.5 hours fully automated
# You get: Complete benchmark results + publication-ready charts
```

### **Fast Track (30 minutes) - Quick Validation:**
```bash
# 1. Verify (5 min)
databricks bundle run fraud_verify_tables -t dev

# 2. Quick baseline (10 min)
databricks bundle run fraud_benchmark_feature_serving -t dev

# 3. Check results in job output
# If good: Run automated end-to-end job ✅
# If issues: Debug before full benchmark
```

### **Manual Step-by-Step (3 hours) - For Debugging:**
```bash
# 1. Verify (5 min)
databricks bundle run fraud_verify_tables -t dev

# 2. Production benchmark (2 hours)
databricks bundle run fraud_production_benchmark -t dev

# 3. Generate report (1 hour)
databricks bundle run fraud_generate_benchmark_report -t dev

# Use this if you need to inspect results between steps
```

---

## 🔍 What Each Job Tests

### **fraud_verify_tables:**
- ✅ All 30 tables exist
- ✅ All have PRIMARY KEY indexes
- ✅ Row counts match expected
- ✅ No orphaned stage tables

### **fraud_benchmark_feature_serving:**
- ✅ Single-threaded latency (baseline)
- ✅ 30-table fanout lookups
- ✅ 1000 iterations (warm cache)
- ✅ P50, P95, P99, max latencies

### **fraud_production_benchmark:**
- ✅ Concurrent client testing (1, 10, 50, 100)
- ✅ Table subset testing (10, 20, 30 tables)
- ✅ Autovacuum detection
- ✅ Cache hit ratio monitoring
- ✅ Throughput (QPS) measurement

### **fraud_generate_benchmark_report:**
- ✅ Detailed latency breakdown (network, query, fetch)
- ✅ Publication-quality charts
- ✅ Component-level analysis
- ✅ Comparison vs DynamoDB baseline

---

## 📊 Expected Performance (Target)

| Metric | Target | DynamoDB Baseline |
|--------|--------|-------------------|
| P50 latency | 30-40ms | 30ms |
| P99 latency | **<79ms** | 79ms |
| Cache hit ratio | >99% | N/A |
| Throughput (1 client) | ~20 QPS | ~33 QPS |
| Throughput (100 clients) | ~1000 QPS | ~500 QPS |

---

## 🚨 Troubleshooting

### **Issue: High P99 latency (>100ms)**

**Possible causes:**
1. Cache is cold (first run after restart)
2. Autovacuum running in background
3. Indexes not being used (check EXPLAIN)

**Solutions:**
```bash
# Check for autovacuum
# In Lakebase: SELECT * FROM pg_stat_activity WHERE query LIKE '%autovacuum%';

# Re-run benchmark (cache should be warm now)
databricks bundle run fraud_benchmark_feature_serving -t dev

# If still slow, check index usage
# In Lakebase: SELECT * FROM pg_stat_user_tables WHERE schemaname = 'features';
```

### **Issue: Job times out**

**Possible causes:**
1. Lakebase cluster is cold/slow
2. Not enough CUs allocated

**Solutions:**
```bash
# Check CU allocation
# Expected: 24-32 CU

# If low: Scale up Lakebase cluster
# If normal: Increase timeout in jobs.yml
```

### **Issue: Tables missing PRIMARY KEYs**

**Possible causes:**
1. Index build job failed or incomplete

**Solutions:**
```bash
# Re-run index build
databricks bundle run fraud_build_indexes -t dev

# Check which tables failed
# Re-run verify after index build completes
```

---

## ✅ Success Criteria

Before presenting results, ensure:

- [ ] All 30 tables have PRIMARY KEY indexes
- [ ] P99 latency < 79ms (beats DynamoDB)
- [ ] Cache hit ratio > 99%
- [ ] No autovacuum warnings during benchmark
- [ ] All benchmark iterations successful (no errors)
- [ ] Results are reproducible (run 2-3 times)

---

## 🎯 Next Steps After Benchmarking

1. **Document results:**
   - Export charts from job output
   - Save latency distributions
   - Record cache hit ratios

2. **Compare vs DynamoDB:**
   - P99: Lakebase vs DynamoDB (79ms baseline)
   - Cost: $/1M requests
   - Scalability: Concurrent client performance

3. **Prepare presentation:**
   - Use publication-ready charts from `fraud_generate_benchmark_report`
   - Highlight P99 win vs DynamoDB
   - Show cost-performance tradeoff

4. **Migration testing (optional):**
   - Test pg_dump/restore workflow
   - Validate backup/recovery process
   - Prepare for production migration

---

**You're ready to benchmark! 🚀**
