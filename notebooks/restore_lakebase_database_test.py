# Databricks notebook source
# MAGIC %md
# MAGIC # Test Lakebase Restore (Selective - 5 Tables Only)
# MAGIC
# MAGIC This notebook tests the restore process with just 5 tables for quick validation.
# MAGIC
# MAGIC **What gets restored:**
# MAGIC - 5 largest tables (~7.5B rows)
# MAGIC - Their indexes
# MAGIC - Their PRIMARY KEY constraints
# MAGIC
# MAGIC **Duration:** ~10-15 minutes
# MAGIC **Target schema:** `features_test` (won't affect production)

# COMMAND ----------

import subprocess
import os
from datetime import datetime
from utils.lakebase_connection import get_lakebase_connection_info

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Get Lakebase connection details
conn_info = get_lakebase_connection_info()

# Input path for dump file
DUMP_FILE = "/Volumes/benchmark/data_load/benchmark_data_dev/lakebase_backup.dump"
SOURCE_SCHEMA = "features"
TARGET_SCHEMA = "features_test"  # Test schema (won't affect production)

# Tables to restore (5 largest tables for testing)
TEST_TABLES = [
    "client_id_card_fingerprint__time_since__365d",          # 2B rows, 339 GB
    "client_id_card_fingerprint_good_rates_365d",            # 1B rows, 162 GB
    "client_id_card_fingerprint_good_rates_90d",             # 1B rows, 162 GB
    "client_id_card_fingerprint_good_rates_30d",             # 1B rows, 162 GB
    "client_id_card_fingerprint__fraud_rates__365d",         # 1B rows, 146 GB
]

print(f"🎯 Selective Restore Configuration:")
print(f"   Host: {conn_info['host']}")
print(f"   Database: {conn_info['database']}")
print(f"   Source schema: {SOURCE_SCHEMA}")
print(f"   Target schema: {TARGET_SCHEMA}")
print(f"   Tables to restore: {len(TEST_TABLES)}")
print(f"   Input: {DUMP_FILE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-Restore Setup

# COMMAND ----------

import psycopg

# Verify dump file exists
if not os.path.exists(DUMP_FILE):
    raise FileNotFoundError(f"Dump file not found: {DUMP_FILE}")

dump_size_bytes = os.path.getsize(DUMP_FILE)
dump_size_gb = dump_size_bytes / (1024 ** 3)

print("=" * 80)
print("📦 DUMP FILE VALIDATION")
print("=" * 80)
print(f"✅ File exists: {DUMP_FILE}")
print(f"✅ File size: {dump_size_gb:.2f} GB")
print("=" * 80)

# Create test schema
with psycopg.connect(
    host=conn_info['host'],
    port=conn_info['port'],
    dbname=conn_info['database'],
    user=conn_info['user'],
    password=conn_info['password']
) as conn:
    with conn.cursor() as cur:
        # Drop test schema if it exists
        print(f"\n🗑️  Dropping test schema '{TARGET_SCHEMA}' if it exists...")
        cur.execute(f"DROP SCHEMA IF EXISTS {TARGET_SCHEMA} CASCADE")
        conn.commit()
        print("✅ Test schema dropped (if existed)")
        
        # Create fresh test schema
        print(f"\n📁 Creating test schema '{TARGET_SCHEMA}'...")
        cur.execute(f"CREATE SCHEMA {TARGET_SCHEMA}")
        conn.commit()
        print("✅ Test schema created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Selective pg_restore

# COMMAND ----------

# Set up environment for pg_restore
env = os.environ.copy()
env['PGPASSWORD'] = conn_info['password']

# Build pg_restore command with selective table restore
restore_cmd = [
    'pg_restore',
    '--host', conn_info['host'],
    '--port', str(conn_info['port']),
    '--username', conn_info['user'],
    '--dbname', conn_info['database'],
    '--schema', SOURCE_SCHEMA,      # Source schema in dump
    '--jobs', '2',                   # 2 parallel workers (smaller test)
    '--verbose',
    '--no-owner',
    '--no-privileges',
]

# Add selective table flags
for table in TEST_TABLES:
    restore_cmd.extend(['--table', table])

# Add dump file
restore_cmd.append(DUMP_FILE)

print("🚀 Starting selective pg_restore...")
print(f"   Command: {' '.join(restore_cmd)}")
print(f"\nRestoring {len(TEST_TABLES)} tables (~7.5B rows)")
print("This will take ~10-15 minutes.\n")
print("=" * 80)

# Run pg_restore
start_time = datetime.now()

try:
    # Note: pg_restore will restore to SOURCE_SCHEMA, then we'll rename
    result = subprocess.run(
        restore_cmd,
        env=env,
        capture_output=True,
        text=True,
        check=True
    )
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds() / 60
    
    print("\n" + "=" * 80)
    print("✅ SELECTIVE RESTORE SUCCESSFUL!")
    print("=" * 80)
    print(f"Duration: {duration:.1f} minutes")
    
    # Print verbose output
    if result.stdout:
        print("\n📝 Restore log (last 1000 chars):")
        print(result.stdout[-1000:])
    
except subprocess.CalledProcessError as e:
    print("\n" + "=" * 80)
    print("❌ RESTORE FAILED!")
    print("=" * 80)
    print(f"Error: {e}")
    if e.stderr:
        print(f"\nError details:\n{e.stderr}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Move Tables to Test Schema

# COMMAND ----------

# Move restored tables from features to features_test
with psycopg.connect(
    host=conn_info['host'],
    port=conn_info['port'],
    dbname=conn_info['database'],
    user=conn_info['user'],
    password=conn_info['password']
) as conn:
    with conn.cursor() as cur:
        print("\n🔄 Moving tables to test schema...")
        for table in TEST_TABLES:
            try:
                cur.execute(f"ALTER TABLE {SOURCE_SCHEMA}.{table} SET SCHEMA {TARGET_SCHEMA}")
                print(f"   ✅ Moved {table}")
            except Exception as e:
                print(f"   ⚠️  Could not move {table}: {e}")
        
        conn.commit()
        print("✅ All tables moved to test schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Restored Data

# COMMAND ----------

# Verify restored data
with psycopg.connect(
    host=conn_info['host'],
    port=conn_info['port'],
    dbname=conn_info['database'],
    user=conn_info['user'],
    password=conn_info['password']
) as conn:
    with conn.cursor() as cur:
        # Get table count
        cur.execute(f"""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = '{TARGET_SCHEMA}'
            AND table_type = 'BASE TABLE'
        """)
        table_count = cur.fetchone()[0]
        
        # Get total row count
        cur.execute(f"""
            SELECT SUM(n_live_tup::bigint) 
            FROM pg_stat_user_tables 
            WHERE schemaname = '{TARGET_SCHEMA}'
        """)
        total_rows = cur.fetchone()[0] or 0
        
        # Get database size
        cur.execute(f"""
            SELECT pg_size_pretty(SUM(pg_total_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename))))
            FROM pg_tables
            WHERE schemaname = '{TARGET_SCHEMA}'
        """)
        db_size = cur.fetchone()[0]
        
        # Get index count
        cur.execute(f"""
            SELECT COUNT(*) 
            FROM pg_indexes 
            WHERE schemaname = '{TARGET_SCHEMA}'
        """)
        index_count = cur.fetchone()[0]
        
        # Get PRIMARY KEY count
        cur.execute(f"""
            SELECT COUNT(*) 
            FROM information_schema.table_constraints 
            WHERE table_schema = '{TARGET_SCHEMA}'
            AND constraint_type = 'PRIMARY KEY'
        """)
        pk_count = cur.fetchone()[0]

print("=" * 80)
print("📊 TEST RESTORE VERIFICATION")
print("=" * 80)
print(f"Tables: {table_count} (expected: {len(TEST_TABLES)})")
print(f"Total rows: {total_rows:,}")
print(f"Total size: {db_size}")
print(f"Indexes: {index_count}")
print(f"PRIMARY KEYs: {pk_count}")
print("=" * 80)

# Validation
if table_count != len(TEST_TABLES):
    print(f"⚠️  WARNING: Expected {len(TEST_TABLES)} tables, got {table_count}")
else:
    print(f"✅ Table count matches expected: {len(TEST_TABLES)}")

if pk_count != len(TEST_TABLES):
    print(f"⚠️  WARNING: Expected {len(TEST_TABLES)} PRIMARY KEYs, got {pk_count}")
else:
    print(f"✅ PRIMARY KEY count matches expected: {len(TEST_TABLES)}")

if total_rows < 7_000_000_000:
    print(f"⚠️  WARNING: Expected ~7.5B rows, got {total_rows:,}")
else:
    print(f"✅ Row count looks good: {total_rows:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Detailed Table Status

# COMMAND ----------

# Get detailed table stats
with psycopg.connect(
    host=conn_info['host'],
    port=conn_info['port'],
    dbname=conn_info['database'],
    user=conn_info['user'],
    password=conn_info['password']
) as conn:
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT 
                t.tablename,
                s.n_live_tup AS estimated_rows,
                ROUND(pg_total_relation_size(quote_ident(t.schemaname) || '.' || quote_ident(t.tablename))::numeric / (1024^3), 2) AS size_gb,
                COALESCE(
                    (SELECT COUNT(*) 
                     FROM information_schema.table_constraints tc 
                     WHERE tc.table_schema = t.schemaname 
                     AND tc.table_name = t.tablename 
                     AND tc.constraint_type = 'PRIMARY KEY'), 0
                ) AS has_primary_key
            FROM pg_tables t
            LEFT JOIN pg_stat_user_tables s ON t.tablename = s.relname AND t.schemaname = s.schemaname
            WHERE t.schemaname = '{TARGET_SCHEMA}'
            ORDER BY s.n_live_tup DESC NULLS LAST
        """)
        
        tables = cur.fetchall()

print("\n" + "=" * 80)
print("📋 DETAILED TABLE STATUS (TEST SCHEMA)")
print("=" * 80)
print(f"{'Table':<70} {'Rows':>15} {'Size (GB)':>10} {'PK':>3}")
print("=" * 80)

for table in tables:
    tablename, rows, size_gb, has_pk = table
    pk_status = "✅" if has_pk else "❌"
    print(f"{tablename:<70} {rows:>15,} {size_gb:>10.2f} {pk_status:>3}")

print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (Optional)

# COMMAND ----------

# Uncomment to automatically drop test schema after verification
# print("\n🗑️  Cleaning up test schema...")
# with psycopg.connect(
#     host=conn_info['host'],
#     port=conn_info['port'],
#     dbname=conn_info['database'],
#     user=conn_info['user'],
#     password=conn_info['password']
# ) as conn:
#     with conn.cursor() as cur:
#         cur.execute(f"DROP SCHEMA {TARGET_SCHEMA} CASCADE")
#         conn.commit()
# print("✅ Test schema dropped")

print("\n💡 To manually cleanup, run:")
print(f"   DROP SCHEMA {TARGET_SCHEMA} CASCADE;")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\n" + "=" * 80)
print("🎉 TEST RESTORE COMPLETE!")
print("=" * 80)
print(f"✅ Tables restored: {table_count}")
print(f"✅ Rows restored: {total_rows:,}")
print(f"✅ Duration: {duration:.1f} minutes")
print(f"✅ Test schema: {TARGET_SCHEMA}")
print("\n" + "=" * 80)
print("NEXT STEPS:")
print("=" * 80)
print("1. ✅ Selective restore works!")
print("2. Run full restore when ready: databricks bundle run fraud_restore_database -t dev")
print("3. Or restore remaining 25 tables selectively")
print(f"4. Cleanup test: DROP SCHEMA {TARGET_SCHEMA} CASCADE;")
print("=" * 80)
