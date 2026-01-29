# Complete Backup Guide - Everything to /Volumes/Official

## 📋 Overview

This guide will backup **EVERYTHING** needed to continue your work after Saturday's workspace wipeout:

1. ✅ **Postgres Tables** - All benchmark results, timing data
2. ✅ **CSVs** - All 30 feature tables (for reloading)
3. ✅ **Jobs** - Databricks job definitions
4. ✅ **Notebooks** - All Python notebooks
5. ✅ **Config Files** - databricks.yml, requirements.txt, SQL scripts
6. ✅ **Reports** - Generated HTML reports (optional)

**Final Destination:** `/Volumes/Official/lakebase_backups/` (your 100GB local volume)

## 🚀 Quick Start (3 Steps)

### Step 1: Backup Postgres Tables, Jobs, Notebooks (Local)

Run this on your laptop:

```bash
cd /Users/som.natarajan/lakebase-benchmarking
./backup_all.sh
```

**What it does:**
- Dumps all `zipfian_*` tables from Lakebase Postgres
- Exports Databricks job definitions
- Backs up all notebooks (.py files)
- Backs up config files, SQL scripts, shell scripts
- Compresses everything
- **Copies to `/Volumes/Official/lakebase_backups/`**

**Time:** ~10-20 minutes  
**You'll need:** Lakebase credentials

---

### Step 2: Backup CSVs from UC Volume (Databricks)

Upload the notebook to Databricks:

```bash
databricks workspace import --profile fe-sandbox-one-env-som-workspace-v4 \
  --file notebooks/backup_csvs_from_volume.py \
  --language PYTHON \
  /Users/som.natarajan@databricks.com/backup_csvs_from_volume
```

Then **run it in Databricks UI** (or via CLI):

```bash
databricks workspace run --profile fe-sandbox-one-env-som-workspace-v4 \
  /Users/som.natarajan@databricks.com/backup_csvs_from_volume
```

**What it does:**
- Scans `/Volumes/benchmark/data_load/benchmark_data_dev`
- Compresses all CSVs into a `.tar.gz` file
- Saves to `dbfs:/FileStore/backups/`
- Ready for download

**Time:** ~30-60 minutes (depends on CSV sizes)

---

### Step 3: Download CSVs to Local Volume (Local)

Run this on your laptop:

```bash
cd /Users/som.natarajan/lakebase-benchmarking
./download_backup_to_volume.sh
```

**What it does:**
- Finds the latest CSV backup in DBFS
- Downloads to `/Volumes/Official/lakebase_backups/csvs/`
- Optionally extracts the files
- Shows progress and file sizes

**Time:** ~20-40 minutes (depends on CSV sizes and network speed)

---

## 📦 What You'll Have in /Volumes/Official

After running all 3 steps:

```
/Volumes/Official/lakebase_backups/
├── workspace_backup_20260128_120000/      # Uncompressed for easy access
│   ├── postgres/
│   │   ├── zipfian_results_v5.sql
│   │   ├── zipfian_request_timing.sql
│   │   ├── zipfian_slow_query_log.sql
│   │   └── all_zipfian_tables.sql
│   ├── jobs/
│   │   ├── jobs_list.txt
│   │   ├── benchmark_job_123.json
│   │   └── viz_job_456.json
│   ├── notebooks/
│   │   ├── benchmark_zipfian_realistic_v5.4.py
│   │   ├── zipfian_benchmark_visuals.py
│   │   ├── generate_csvs.py
│   │   └── ...
│   ├── config/
│   │   ├── databricks.yml
│   │   ├── requirements.txt
│   │   ├── report_template.html
│   │   ├── *.sql
│   │   ├── *.sh
│   │   └── *.md
│   └── reports/ (if downloaded)
│       └── benchmark_reports/
├── workspace_backup_20260128_120000.tar.gz  # Compressed copy
└── csvs/
    └── benchmark_csvs_20260128_130000.tar.gz  # All 30 feature tables
```

## 💾 Storage Requirements

Typical sizes:
- **Postgres dumps:** 100-500 MB
- **Jobs/Notebooks/Config:** < 10 MB
- **CSVs (compressed):** 1-10 GB (depends on data)
- **Reports:** < 100 MB

**Total:** ~2-15 GB (plenty of room in your 100GB volume!)

## ✅ What You Can Do With These Backups

### ✅ Continue Report Refinement (NO benchmark re-run needed)

1. Restore Postgres tables to new Lakebase instance:
   ```bash
   psql -h <NEW_LAKEBASE> -U <USER> -d <DB> \
     -f /Volumes/Official/lakebase_backups/workspace_backup_*/postgres/zipfian_results_v5.sql
   ```

2. Upload viz notebook:
   ```bash
   databricks workspace import \
     /Volumes/Official/lakebase_backups/workspace_backup_*/notebooks/zipfian_benchmark_visuals.py \
     /Users/<email>/zipfian_benchmark_visuals \
     --profile <NEW_PROFILE> --language PYTHON
   ```

3. Upload report template:
   ```bash
   databricks fs cp \
     /Volumes/Official/lakebase_backups/workspace_backup_*/config/report_template.html \
     dbfs:/FileStore/report_template.html \
     --profile <NEW_PROFILE>
   ```

4. **Generate reports with new styling!** No benchmark re-run required.

### ✅ Re-run Benchmarks (If needed)

1. Extract CSVs:
   ```bash
   cd /Volumes/Official/lakebase_backups/csvs
   tar -xzf benchmark_csvs_*.tar.gz
   ```

2. Upload to new UC Volume:
   ```bash
   databricks fs cp -r /Volumes/Official/lakebase_backups/csvs/* \
     dbfs:/Volumes/<new_catalog>/<new_schema>/<new_volume>/ \
     --profile <NEW_PROFILE>
   ```

3. Restore jobs:
   ```bash
   databricks jobs create \
     --json-file /Volumes/Official/lakebase_backups/workspace_backup_*/jobs/benchmark_job_*.json \
     --profile <NEW_PROFILE>
   ```

4. Run benchmarks on new workspace!

## 🔄 Verification Checklist

Before Saturday, verify your backups:

```bash
# Check backup exists
ls -lh /Volumes/Official/lakebase_backups/

# Check Postgres dumps are not empty
wc -l /Volumes/Official/lakebase_backups/workspace_backup_*/postgres/*.sql

# Check CSV backup size
du -h /Volumes/Official/lakebase_backups/csvs/

# Check notebooks are present
ls /Volumes/Official/lakebase_backups/workspace_backup_*/notebooks/

# Check jobs are backed up
ls /Volumes/Official/lakebase_backups/workspace_backup_*/jobs/
```

## 🆘 Troubleshooting

### "Permission denied" on /Volumes/Official
```bash
# Check volume is mounted and writable
touch /Volumes/Official/test.txt && rm /Volumes/Official/test.txt
```

### "Lakebase connection failed"
- Verify Lakebase is running
- Check credentials (host, username, password)
- Test connection: `psql -h <HOST> -U <USER> -d <DB> -c "SELECT 1;"`

### "DBFS download too slow"
- CSVs are already compressed
- Download overnight if needed
- Consider running from faster network

### "Backup incomplete"
- Re-run `backup_all.sh` - it's idempotent
- Check terminal output for specific errors
- Ensure Databricks CLI is authenticated

## 📅 Timeline

**Thursday/Friday (Before Saturday):**
- [ ] Run `backup_all.sh`
- [ ] Upload and run `backup_csvs_from_volume.py` in Databricks
- [ ] Run `download_backup_to_volume.sh`
- [ ] Verify all backups in `/Volumes/Official/lakebase_backups/`

**After Saturday:**
- [ ] Restore Postgres tables to new workspace
- [ ] Upload notebooks and templates
- [ ] Continue refining reports! 🎉

## 🎯 Key Point

**You do NOT need to re-run benchmarks to continue refining reports!**

All the data needed for visualization and report generation is in the Postgres dumps. The CSVs are only needed if you want to:
- Re-run benchmarks with different parameters
- Test new execution modes
- Collect fresh timing data

For report refinement (styling, charts, layout) → **Just restore Postgres tables** ✅

---

## 📞 Quick Reference Commands

```bash
# Full backup (Postgres, jobs, notebooks)
./backup_all.sh

# Download CSVs to local volume
./download_backup_to_volume.sh

# Check backup size
du -sh /Volumes/Official/lakebase_backups/

# List backed up tables
ls /Volumes/Official/lakebase_backups/workspace_backup_*/postgres/

# Extract CSVs if needed
cd /Volumes/Official/lakebase_backups/csvs/
tar -xzf benchmark_csvs_*.tar.gz
```

---

## ✅ Summary

After following this guide, you'll have:

1. ✅ **All benchmark results** (Postgres dumps)
2. ✅ **All feature table CSVs** (for reloading)
3. ✅ **All jobs, notebooks, configs**
4. ✅ **Everything in `/Volumes/Official`** (100GB local volume)
5. ✅ **Ability to continue work** without re-running benchmarks

**Total time:** ~1-2 hours (mostly waiting for downloads)  
**Storage used:** ~2-15 GB  
**Peace of mind:** Priceless! 🎉
