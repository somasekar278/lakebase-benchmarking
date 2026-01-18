# ✅ Recovery Checklist - New Workspace

**Use this to track your progress through the recovery process**

---

## Phase 1: Setup (10 minutes)

- [ ] **Receive new workspace details**
  - [ ] Databricks workspace URL
  - [ ] Lakebase hostname
  - [ ] Admin credentials for Lakebase

- [ ] **Create database user** (SQL in `SETUP_NEW_WORKSPACE.sql`)
  - [ ] CREATE USER fraud_benchmark_user
  - [ ] CREATE DATABASE benchmark
  - [ ] CREATE SCHEMA features
  - [ ] GRANT permissions
  - [ ] Verify: `SELECT * FROM pg_user WHERE usename = 'fraud_benchmark_user'`

- [ ] **Create UC Volume** (Databricks SQL)
  - [ ] CREATE CATALOG benchmark
  - [ ] CREATE SCHEMA benchmark.data_load
  - [ ] CREATE VOLUME benchmark.data_load.benchmark_data_dev

- [ ] **Update local config**
  - [ ] Edit `databricks.yml` → new workspace URL
  - [ ] Fill in `credentials.template` with actual values
  - [ ] Update cluster ID in `resources/jobs.yml` (optional)

- [ ] **Deploy bundle**
  ```bash
  databricks bundle deploy -t dev
  ```
  - [ ] Verify: No errors in deployment
  - [ ] Check: `databricks bundle run -l` shows all jobs

---

## Phase 2: Data Load (23 hours)

- [ ] **Start load job**
  ```bash
  databricks bundle run fraud_load_all_30_tables -t dev
  ```
  - [ ] Job started successfully
  - [ ] Monitor in Databricks UI
  - [ ] Note start time: ___________

- [ ] **Wait for completion (~23 hours)**
  - [ ] Check job status periodically
  - [ ] Expected: SUCCESS after ~23 hours
  - [ ] Note completion time: ___________

- [ ] **Verify load completed**
  - [ ] All 30 tables loaded
  - [ ] No errors in job log
  - [ ] Expected rows: ~16.5 billion

---

## Phase 3: Index Build (3 hours)

- [ ] **Start index job**
  ```bash
  databricks bundle run fraud_build_indexes -t dev
  ```
  - [ ] Job started successfully
  - [ ] Monitor in Databricks UI
  - [ ] Note start time: ___________

- [ ] **Verify optimization is working**
  - [ ] Check logs for "Applying table-specific optimization settings"
  - [ ] Large tables show: `maintenance_work_mem=48GB, max_parallel_workers=1`
  - [ ] Medium tables show: `maintenance_work_mem=24GB, max_parallel_workers=2`
  - [ ] Small tables show: `maintenance_work_mem=12GB, max_parallel_workers=4`

- [ ] **Wait for completion (~3 hours)**
  - [ ] Check job status
  - [ ] Expected: SUCCESS after ~3 hours
  - [ ] Note completion time: ___________
  - [ ] Expected: All 30 tables with indexes

---

## Phase 4: Verification (5 minutes)

- [ ] **Run verification**
  ```bash
  databricks bundle run fraud_verify_tables -t dev
  ```

- [ ] **Check results**
  - [ ] ✅ All 30 tables present
  - [ ] ✅ All 30 tables have PRIMARY KEYs
  - [ ] ✅ Total rows: 16.5B+
  - [ ] ✅ Ready for benchmarking

---

## Phase 5: Benchmark (45 minutes)

- [ ] **Quick baseline test**
  ```bash
  databricks bundle run fraud_benchmark_feature_serving -t dev
  ```
  - [ ] Expected P99: ~14.69ms
  - [ ] Expected: Beats DynamoDB by 81%

- [ ] **Production Zipfian test**
  ```bash
  databricks bundle run fraud_benchmark_zipfian -t dev
  ```
  - [ ] Expected Hot P99: ~15ms
  - [ ] Expected Cold P99: ~90ms
  - [ ] Expected Blended P99: ~84ms

- [ ] **Full end-to-end test** (optional)
  ```bash
  databricks bundle run fraud_benchmark_end_to_end -t dev
  ```
  - [ ] Runs all tests automatically
  - [ ] Generates report with charts

---

## Phase 6: Results Documentation

- [ ] **Capture benchmark results**
  - [ ] Hot keys P99: _________ms
  - [ ] Zipfian 80/20 P99: _________ms
  - [ ] vs DynamoDB (79ms): _________% improvement

- [ ] **Verify cost savings**
  - [ ] Lakebase: $360/day ✅
  - [ ] DynamoDB: $45,000/day
  - [ ] Savings: $16.3M/year ✅

- [ ] **Prepare presentation**
  - [ ] Use numbers from `BENCHMARK_RESULTS_SUMMARY.md`
  - [ ] Show 3/4 metric wins
  - [ ] Highlight $16M savings

---

## ⏱️ Total Timeline

| Phase | Expected Duration | Actual Duration | Status |
|-------|------------------|-----------------|--------|
| Setup | 10 minutes | ____________ | ☐ |
| Load | 23 hours | ____________ | ☐ |
| Index | 3 hours | ____________ | ☐ |
| Verify | 5 minutes | ____________ | ☐ |
| Benchmark | 45 minutes | ____________ | ☐ |
| **Total** | **~27 hours** | ____________ | ☐ |

---

## 🎯 Success Criteria

At the end, you should have:

**Data:**
- ✅ 30 tables loaded (16.5B rows, 1.5TB)
- ✅ All indexes built with optimization
- ✅ All tables verified

**Performance:**
- ✅ Hot keys: P99 ~15ms (81% faster than DynamoDB)
- ✅ Zipfian: P99 ~84ms (competitive)
- ✅ Beats DynamoDB on 3/4 metrics

**Business Case:**
- ✅ $360/day vs $45,000/day
- ✅ $16.3M annual savings
- ✅ 32 CU cluster (no upgrade needed)

---

## 📞 Quick Reference

**Key Commands:**
```bash
# Deploy
databricks bundle deploy -t dev

# Load (23h)
databricks bundle run fraud_load_all_30_tables -t dev

# Index (3h)
databricks bundle run fraud_build_indexes -t dev

# Verify
databricks bundle run fraud_verify_tables -t dev

# Benchmark
databricks bundle run fraud_benchmark_zipfian -t dev
```

**Key Files:**
- `NEW_WORKSPACE_SETUP_GUIDE.md` - Complete guide
- `SETUP_NEW_WORKSPACE.sql` - Database setup SQL
- `credentials.template` - Where to store new details
- `BENCHMARK_RESULTS_SUMMARY.md` - Expected results

---

## 🚨 If Something Goes Wrong

**Job fails during load:**
- Check: UC Volume exists and is accessible
- Check: Credentials correct
- Re-run: Job is idempotent, will resume

**Job fails during index:**
- Check: Optimization logs show correct settings
- Check: No "No space left on device" errors
- Check: Large tables running sequentially
- Re-run: Job is idempotent, will skip completed indexes

**Benchmark shows poor results:**
- Check: All indexes completed successfully
- Check: Warmup completed (look for 1000 iterations)
- Check: Hot keys P99 should be ~15ms
- If >50ms: Something wrong with cache

---

**Start time:** ___________________  
**Expected completion:** ___________________ (+27 hours)  
**Actual completion:** ___________________

---

**Good luck! You've got this!** 🚀
