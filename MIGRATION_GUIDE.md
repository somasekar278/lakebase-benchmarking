# Lakebase Migration Guide: pg_dump/pg_restore

## Overview

This guide explains how to migrate your entire Lakebase database (30 tables, 16.6B rows, indexes, constraints) from one workspace to another using PostgreSQL's native backup/restore tools.

## Why This Approach?

- ✅ **Preserves everything:** Tables, indexes, constraints, statistics
- ✅ **Much faster than re-load:** ~2-3 hours vs. 30+ hours
- ✅ **Compressed format:** Reduces storage/transfer costs
- ✅ **Parallel restore:** Can use multiple cores for faster restore

## Prerequisites

1. **Access to both workspaces:**
   - Current workspace: `fe-sandbox-one-env-som-workspace`
   - New workspace: (to be determined)

2. **Lakebase connection details:**
   - Host, port, database name, schema, credentials
   - Get from `utils/lakebase_connection.py` or Databricks secrets

3. **UC Volume for backup storage:**
   - Already exists: `/Volumes/benchmark/data_load/benchmark_data_dev/`
   - Or create a new one for migrations

## Migration Process

### Phase 1: Dump from Current Workspace (Source)

**Duration:** ~30-60 minutes for 16.6B rows + indexes

```bash
# On Databricks notebook or job
pg_dump \
  --host=<lakebase_host> \
  --port=<lakebase_port> \
  --username=<lakebase_user> \
  --dbname=<lakebase_db> \
  --schema=features \
  --format=custom \
  --compress=9 \
  --verbose \
  --file=/Volumes/benchmark/data_load/benchmark_data_dev/lakebase_backup.dump
```

**Key options:**
- `--format=custom` (`-Fc`): Binary format, compressed, supports parallel restore
- `--compress=9`: Maximum compression (reduces file size by ~70-80%)
- `--schema=features`: Only dump the `features` schema (not system tables)
- `--verbose`: Show progress

**Expected dump size:** ~500-800 GB compressed (from ~3TB uncompressed data + indexes)

### Phase 2: Restore to New Workspace (Target)

**Duration:** ~1-2 hours with parallel restore

```bash
# On new workspace
pg_restore \
  --host=<new_lakebase_host> \
  --port=<new_lakebase_port> \
  --username=<new_lakebase_user> \
  --dbname=<new_lakebase_db> \
  --schema=features \
  --jobs=4 \
  --verbose \
  --no-owner \
  --no-privileges \
  /Volumes/benchmark/data_load/benchmark_data_dev/lakebase_backup.dump
```

**Key options:**
- `--jobs=4` (`-j 4`): Parallel restore using 4 cores (adjust based on Lakebase capacity)
- `--no-owner`: Don't try to restore ownership (use target DB's default owner)
- `--no-privileges`: Don't restore privileges (use target DB's defaults)

### Phase 3: Verify Migration

Run the verification notebook to ensure all tables, indexes, and constraints were migrated correctly:

```bash
databricks bundle run fraud_verify_tables -t dev
```

**Expected output:**
- 30 tables with correct row counts
- All PRIMARY KEY constraints present
- All indexes valid and ready

## Automation with Databricks Jobs

### Job 1: Dump Current Database

```yaml
# resources/jobs.yml
fraud_dump_database:
  name: "[${bundle.target}] Fraud Detection - Dump Lakebase Database"
  tasks:
    - task_key: dump_database
      notebook_task:
        notebook_path: ./notebooks/dump_lakebase_database.py
      timeout_seconds: 7200  # 2 hours
```

### Job 2: Restore to New Database

```yaml
# resources/jobs.yml
fraud_restore_database:
  name: "[${bundle.target}] Fraud Detection - Restore Lakebase Database"
  tasks:
    - task_key: restore_database
      notebook_task:
        notebook_path: ./notebooks/restore_lakebase_database.py
      timeout_seconds: 7200  # 2 hours
```

## Selective Restore Options

The custom format dump supports **selective restore** - you don't have to restore everything!

### Option 1: Restore Specific Tables

**Use case:** Test with a subset of tables, or only migrate tables you need.

```bash
# Restore only 5 tables (fastest test)
pg_restore \
  --host=<new_lakebase_host> \
  --port=<new_lakebase_port> \
  --username=<new_lakebase_user> \
  --dbname=<new_lakebase_db> \
  --table=client_id_card_fingerprint__time_since__365d \
  --table=client_id_card_fingerprint_good_rates_365d \
  --table=client_id_customer_email_clean__fraud_rates__365d \
  --table=client_id_cardholder_name_clean__fraud_rates__365d \
  --table=client_id_customer_email_clean__time_since__90d \
  /Volumes/benchmark/data_load/benchmark_data_dev/lakebase_backup.dump
```

**Duration:** ~10-15 minutes for 5 largest tables (~7.5B rows)

### Option 2: List Dump Contents First

**Use case:** See what's in the dump before restoring.

```bash
# List all objects in the dump
pg_restore --list /Volumes/benchmark/data_load/benchmark_data_dev/lakebase_backup.dump > toc.txt

# Output shows:
# - All tables
# - All indexes  
# - All constraints
# - All sequences
# - Dependencies

# Then selectively restore by editing toc.txt (comment out items you DON'T want)
pg_restore --use-list=toc.txt /Volumes/benchmark/data_load/benchmark_data_dev/lakebase_backup.dump
```

### Option 3: Schema-Only Restore

**Use case:** Restore table definitions and indexes, but not data (for testing schema).

```bash
pg_restore \
  --host=<new_lakebase_host> \
  --port=<new_lakebase_port> \
  --username=<new_lakebase_user> \
  --dbname=<new_lakebase_db> \
  --schema-only \
  /Volumes/benchmark/data_load/benchmark_data_dev/lakebase_backup.dump
```

**Duration:** ~1-2 minutes (no data copied)

### Option 4: Data-Only Restore

**Use case:** Tables already exist, just reload data.

```bash
pg_restore \
  --host=<new_lakebase_host> \
  --port=<new_lakebase_port> \
  --username=<new_lakebase_user> \
  --dbname=<new_lakebase_db> \
  --data-only \
  /Volumes/benchmark/data_load/benchmark_data_dev/lakebase_backup.dump
```

### Option 5: Automated Test Restore (5 Tables)

**Use case:** Quick validation that restore works before full migration.

```bash
# Test with 5 largest tables to features_test schema
databricks bundle run fraud_restore_database_test -t dev
```

**Duration:** ~10-15 minutes  
**Output:** Restores 5 tables to `features_test` schema (won't affect production)

## Alternative: Schema-Only Dump

If you want to migrate just the schema (tables, indexes, constraints) without data:

```bash
pg_dump \
  --host=<lakebase_host> \
  --port=<lakebase_port> \
  --username=<lakebase_user> \
  --dbname=<lakebase_db> \
  --schema=features \
  --schema-only \
  --file=/Volumes/benchmark/data_load/benchmark_data_dev/lakebase_schema.sql
```

Then restore with:

```bash
psql \
  --host=<new_lakebase_host> \
  --port=<new_lakebase_port> \
  --username=<new_lakebase_user> \
  --dbname=<new_lakebase_db> \
  --file=/Volumes/benchmark/data_load/benchmark_data_dev/lakebase_schema.sql
```

This is useful for testing or when you want to reload data differently.

## Troubleshooting

### Issue: "permission denied" during dump

**Solution:** Ensure the Databricks job has the correct Lakebase credentials. Check `{{secrets/fraud-benchmark/lakebase_*}}` in your DABs config.

### Issue: "out of disk space" during dump

**Solution:** 
- Increase UC Volume capacity
- Or dump to DBFS: `/dbfs/mnt/...`
- Or dump directly to S3: `s3://your-bucket/lakebase_backup.dump`

### Issue: Restore is very slow

**Solution:**
- Increase `--jobs` parameter (e.g., `--jobs=8` for faster restore)
- Disable autovacuum during restore: `ALTER SYSTEM SET autovacuum = off;` (remember to re-enable after!)
- Increase `maintenance_work_mem` on target Lakebase

### Issue: Index rebuild during restore is slow

**Solution:** Indexes are automatically restored by `pg_restore`. If this is slow, consider:
- `pg_dump --data-only` first (fast)
- Then `pg_dump --schema-only` and manually create indexes with `CREATE INDEX CONCURRENTLY`

## Best Practices

1. **Test the migration first:**
   - Dump a single table: `pg_dump --table=features.client_id_card_fingerprint__time_since__365d`
   - Restore to a test schema: `pg_restore --schema=features_test`
   - Verify it works before full migration

2. **Monitor progress:**
   - Dump: Watch file size grow in UC Volume
   - Restore: Check `pg_stat_progress_create_index` for index rebuild progress

3. **Plan for downtime:**
   - Dump is online (doesn't lock tables)
   - But ensure no writes during dump to maintain consistency
   - Stop any benchmark jobs before dumping

4. **Checksum verification:**
   - After restore, compare row counts: `SELECT COUNT(*) FROM features.<table>`
   - Compare table sizes: `SELECT pg_size_pretty(pg_total_relation_size('features.<table>'))`

## Timeline Summary

| Phase | Duration | Notes |
|-------|----------|-------|
| Dump | 30-60 min | Depends on Lakebase I/O |
| Transfer | 0 min | UC Volume accessible from both workspaces |
| Restore | 1-2 hours | With parallel restore (--jobs=4) |
| Verify | 5 min | Quick counts and index checks |
| **Total** | **~2-3 hours** | **vs. 30+ hours for re-load** |

## Recommended Testing Strategy

### Phase 1: Create Backup (Current Workspace)

```bash
# 1. Dump entire database to UC Volume (30-60 min)
databricks bundle run fraud_dump_database -t dev

# Expected output:
# ✅ Backup file: /Volumes/.../lakebase_backup.dump
# ✅ Backup size: ~500-800 GB (compressed)
# ✅ Tables backed up: 30
# ✅ Rows backed up: 16,600,000,000+
```

### Phase 2: Test Restore (Current Workspace - Optional but Recommended)

```bash
# 2. Test restore with 5 tables to features_test schema (10-15 min)
databricks bundle run fraud_restore_database_test -t dev

# This validates:
# ✅ Dump file is valid
# ✅ Restore process works
# ✅ Indexes and constraints preserved
# ✅ No impact on production (uses test schema)

# Cleanup test:
# DROP SCHEMA features_test CASCADE;
```

### Phase 3: Full Restore (New Workspace)

```bash
# 3. Set up new workspace
# - Create new Lakebase endpoint
# - Configure connection details in config.py
# - Deploy DABs: databricks bundle deploy -t dev

# 4. Run full restore (1-2 hours)
databricks bundle run fraud_restore_database -t dev

# Expected output:
# ✅ Tables restored: 30
# ✅ Rows restored: 16,600,000,000+
# ✅ Indexes: 30+
# ✅ PRIMARY KEYs: 30
```

### Phase 4: Verify and Benchmark

```bash
# 5. Verify migration
databricks bundle run fraud_verify_tables -t dev

# 6. Run benchmarks to confirm performance
databricks bundle run fraud_production_benchmark -t dev
```

## Next Steps

Choose your migration path:

### **Path A: Full Migration (All 30 Tables)**

1. **Run the dump job in the current workspace:**
   ```bash
   databricks bundle run fraud_dump_database -t dev
   ```

2. **(Optional) Test restore in current workspace:**
   ```bash
   databricks bundle run fraud_restore_database_test -t dev
   ```

3. **Set up the new workspace:**
   - Create a new Lakebase endpoint
   - Configure connection details in secrets
   - Deploy DABs: `databricks bundle deploy -t dev`

4. **Run the restore job in the new workspace:**
   ```bash
   databricks bundle run fraud_restore_database -t dev
   ```

5. **Verify migration:**
   ```bash
   databricks bundle run fraud_verify_tables -t dev
   ```

6. **Run benchmarks to confirm performance:**
   ```bash
   databricks bundle run fraud_production_benchmark -t dev
   ```

### **Path B: Selective Migration (Only Tables You Need)**

1. **Dump current database** (same as Path A)

2. **Create selective restore notebook** by modifying `restore_lakebase_database_test.py`:
   - Update `TEST_TABLES` list with tables you need
   - Change `TARGET_SCHEMA` from `features_test` to `features`

3. **Run selective restore:**
   ```bash
   databricks bundle run fraud_restore_database_test -t dev
   ```

4. **Verify and benchmark** (same as Path A)

---

**You're now ready to migrate your Lakebase database without losing any work!** 🎉
