# 🚀 New Workspace Setup Guide

**Complete recovery checklist for new Lakebase instance**

---

## 📋 Prerequisites

You'll need from the new workspace:
- [ ] Databricks workspace URL
- [ ] Lakebase hostname
- [ ] Admin credentials for Lakebase (to create user)
- [ ] Cluster ID (or will create new one)

---

## Step 1: Create Database User (5 minutes)

### **In Lakebase SQL Editor (as admin):**

```sql
-- 1. Create user
CREATE USER fraud_benchmark_user WITH PASSWORD 'YOUR_SECURE_PASSWORD';

-- 2. Create database and schema
CREATE DATABASE IF NOT EXISTS benchmark;
\c benchmark
CREATE SCHEMA IF NOT EXISTS features;

-- 3. Grant permissions
GRANT CONNECT ON DATABASE benchmark TO fraud_benchmark_user;
GRANT USAGE, CREATE ON SCHEMA features TO fraud_benchmark_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA features TO fraud_benchmark_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA features TO fraud_benchmark_user;

-- 4. Grant future permissions
ALTER DEFAULT PRIVILEGES IN SCHEMA features 
GRANT ALL ON TABLES TO fraud_benchmark_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA features 
GRANT ALL ON SEQUENCES TO fraud_benchmark_user;

-- 5. Verify
SELECT usename, usesuper FROM pg_user WHERE usename = 'fraud_benchmark_user';
```

**Save the password!** You'll need it for the config.

---

## Step 2: Update Local Configuration (2 minutes)

### **A. Update databricks.yml**

```bash
# Edit: databricks.yml
# Change line with workspace URL
```

```yaml
workspace:
  host: https://YOUR-NEW-WORKSPACE.cloud.databricks.com
```

### **B. Update bundle variables**

Create/update: `databricks_variables.tfvars`

```hcl
# Lakebase Configuration
lakebase_host = "YOUR-LAKEBASE-HOSTNAME.cloud.databricks.com"
lakebase_database = "benchmark"
lakebase_schema = "features"
lakebase_user = "fraud_benchmark_user"
lakebase_password = "YOUR_SECURE_PASSWORD"

# Or use environment variables (more secure):
# export TF_VAR_lakebase_password="YOUR_SECURE_PASSWORD"
```

### **C. Update cluster ID (if using existing cluster)**

In `resources/jobs.yml`, search for `existing_cluster_id` and update to new cluster ID.

Or remove `existing_cluster_id` lines to auto-create clusters (slower but easier).

---

## Step 3: Create UC Volume (1 minute)

### **In Databricks SQL/Notebook:**

```sql
-- Create catalog and schema if needed
CREATE CATALOG IF NOT EXISTS benchmark;
CREATE SCHEMA IF NOT EXISTS benchmark.data_load;

-- Create volume for CSV staging
CREATE VOLUME IF NOT EXISTS benchmark.data_load.benchmark_data_dev;
```

---

## Step 4: Deploy Bundle (1 minute)

```bash
cd /Users/som.natarajan/lakebase-benchmarking

# Deploy all jobs and resources
databricks bundle deploy -t dev
```

**Expected output:**
```
Uploading bundle files...
Deploying resources...
Deployment complete!
```

---

## Step 5: Verify Deployment

```bash
# List deployed jobs
databricks bundle run -l

# Expected to see:
# - fraud_load_all_30_tables
# - fraud_build_indexes
# - fraud_benchmark_feature_serving
# - fraud_benchmark_zipfian
# - fraud_verify_tables
# - fraud_benchmark_end_to_end
```

---

## Step 6: Start Data Load (23 hours)

```bash
# Kick off the load job
databricks bundle run fraud_load_all_30_tables -t dev
```

**What this does:**
- Loads 30 tables (16.5B rows)
- Uses optimized COPY (no indexes yet)
- Checkpointed (resumable)
- ~23 hours to complete

**Monitor progress:**
- Databricks UI → Workflows → Job runs
- Check for "SUCCESS" status

---

## Step 7: Build Indexes (3 hours) 🚀

**After load completes (~23h later):**

```bash
# Build all indexes with optimization
databricks bundle run fraud_build_indexes -t dev
```

**What this does:**
- Builds PRIMARY KEY indexes on all 30 tables
- Uses adaptive optimization:
  - Large tables: 48GB RAM, 1 worker, sequential
  - Medium tables: 24GB RAM, 2 workers
  - Small tables: 12GB RAM, 4 workers
- **3 hours to complete** (vs 9 hours unoptimized!)

**Monitor progress:**
- Watch for batch progress
- Each table shows optimization settings applied
- Displays time per table

---

## Step 8: Verify Setup

```bash
# Run verification job
databricks bundle run fraud_verify_tables -t dev
```

**Expected output:**
```
✅ All 30 tables present
✅ All 30 tables have PRIMARY KEYs
✅ Total rows: 16,500,000,000+
✅ Ready for benchmarking!
```

---

## Step 9: Run Benchmarks! 🎉

### **Quick Baseline (15 minutes):**

```bash
databricks bundle run fraud_benchmark_feature_serving -t dev
```

**Expected results:**
- Hot keys P99: ~14.69ms (81% faster than DynamoDB)
- Proves system is working

### **Production Zipfian (25 minutes):**

```bash
databricks bundle run fraud_benchmark_zipfian -t dev
```

**Expected results:**
- Hot P99: ~15ms
- Cold P99: ~90ms
- Blended P99: ~84ms (competitive with DynamoDB)

### **Automated End-to-End (45 minutes):**

```bash
databricks bundle run fraud_benchmark_end_to_end -t dev
```

**This runs:**
1. Verify tables
2. Quick baseline benchmark
3. Production benchmark
4. Generate report with charts

---

## 📊 Timeline Summary

| Phase | Duration | Description |
|-------|----------|-------------|
| **Setup** | 10 minutes | User, config, deploy |
| **Load** | 23 hours | 16.5B rows (COPY only) |
| **Index** | 3 hours | Optimized index builds |
| **Verify** | 5 minutes | Check all tables ready |
| **Benchmark** | 45 minutes | End-to-end automated test |
| **━━━━━━** | **━━━━━━** | **━━━━━━━━━━━━━━━** |
| **Total** | **~27 hours** | Full recovery to benchmarked state |

**Faster than original!** (Was 32+ hours)

---

## 🔍 Troubleshooting

### **Issue: "Permission denied for function pg_stat_reset"**

This is OK! The benchmark continues without stats reset. Only affects cumulative metrics display, not actual latency measurements.

### **Issue: "Table already exists"**

Good! The load job is idempotent. It will skip existing tables and resume from where it left off.

### **Issue: "Index already exists"**

Good! The index job is idempotent. It will skip completed indexes and only build missing ones.

### **Issue: "No space left on device" during index build**

Check if optimization is applied:
- Look for "Applying table-specific optimization settings" in logs
- Should show `maintenance_work_mem=48GB` for large tables
- Hybrid scheduling should run large tables sequentially

### **Issue: Job timeout**

Increase timeout in `resources/jobs.yml`:
```yaml
timeout_seconds: 86400  # 24 hours
```

---

## 🎯 Success Criteria

After full recovery, you should have:

✅ **Data:**
- 30 tables loaded
- 16.5 billion rows
- 1.5 TB total size

✅ **Indexes:**
- All 30 tables have PRIMARY KEYs
- Built with optimization (3 hours)
- Ready for queries

✅ **Performance:**
- Hot keys: P99 = 14.69ms (proven)
- Zipfian 80/20: P99 = 83.91ms
- Competitive with DynamoDB

✅ **Cost:**
- $360/day vs $45,000/day
- $16.3M annual savings

---

## 📁 Files You'll Need

**Local files (all committed to git):**
- `databricks.yml` - Workspace config
- `resources/jobs.yml` - Job definitions
- `notebooks/*.py` - All notebooks
- `utils/*.py` - All utilities
- `*.md` - Documentation (12 files)

**To update:**
- `databricks.yml` - New workspace URL
- Cluster IDs in `resources/jobs.yml` (optional)

**To create:**
- New Lakebase user (SQL above)
- UC Volume (SQL above)

---

## 🚀 Quick Start Commands

```bash
# 1. Update config (manually edit databricks.yml)

# 2. Deploy
databricks bundle deploy -t dev

# 3. Load data (start now, ~23h)
databricks bundle run fraud_load_all_30_tables -t dev

# 4. Build indexes (after load, ~3h)
databricks bundle run fraud_build_indexes -t dev

# 5. Benchmark (after indexes, ~45min)
databricks bundle run fraud_benchmark_end_to_end -t dev
```

---

## 💡 Pro Tips

1. **Start the load job ASAP** - It's 23 hours, so get it going
2. **Monitor via Databricks UI** - Don't rely on CLI output
3. **Check for SUCCESS status** before moving to next step
4. **Save credentials securely** - Use environment variables
5. **Keep git commit** - All your work is safe in git!

---

## 📞 Need Help?

Check these files for details:
- `BENCHMARK_RESULTS_SUMMARY.md` - Full analysis
- `INDEX_OPTIMIZATION.md` - Optimization details
- `MIGRATION_GUIDE.md` - pg_dump/restore (if needed)
- `BENCHMARKING.md` - Benchmark guide

---

**Ready to start? Share the new Lakebase details and let's get rolling!** 🚀
