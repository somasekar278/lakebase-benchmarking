# Databricks notebook source
# MAGIC %md
# MAGIC # Restore Lakebase Database from UC Volume
# MAGIC
# MAGIC This notebook restores a complete backup of the Lakebase `features` schema using `pg_restore`.
# MAGIC
# MAGIC **What gets restored:**
# MAGIC - All 30 tables
# MAGIC - All indexes
# MAGIC - All PRIMARY KEY constraints
# MAGIC - All data (~16.6 billion rows)
# MAGIC
# MAGIC **Duration:** ~1-2 hours (with parallel restore)
# MAGIC **Input:** `/Volumes/benchmark/data_load/benchmark_data_dev/lakebase_backup.dump`

# COMMAND ----------

import subprocess
import os
from datetime import datetime
from utils.lakebase_connection import get_lakebase_connection_info

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Get Lakebase connection details (NEW workspace)
conn_info = get_lakebase_connection_info()

# Input path for dump file
DUMP_FILE = "/Volumes/benchmark/data_load/benchmark_data_dev/lakebase_backup.dump"
SCHEMA = "features"

# Restore configuration
PARALLEL_JOBS = 4  # Number of parallel restore workers (adjust based on Lakebase capacity)

# Restore metadata
RESTORE_TIMESTAMP = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

print(f"🎯 Restore Configuration:")
print(f"   Host: {conn_info['host']}")
print(f"   Database: {conn_info['database']}")
print(f"   Schema: {SCHEMA}")
print(f"   Input: {DUMP_FILE}")
print(f"   Parallel jobs: {PARALLEL_JOBS}")
print(f"   Timestamp: {RESTORE_TIMESTAMP}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-Restore Validation

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

# Check if target database is empty
with psycopg.connect(
    host=conn_info['host'],
    port=conn_info['port'],
    dbname=conn_info['database'],
    user=conn_info['user'],
    password=conn_info['password']
) as conn:
    with conn.cursor() as cur:
        # Check if schema exists
        cur.execute(f"""
            SELECT COUNT(*) 
            FROM information_schema.schemata 
            WHERE schema_name = '{SCHEMA}'
        """)
        schema_exists = cur.fetchone()[0] > 0
        
        if schema_exists:
            # Get table count
            cur.execute(f"""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = '{SCHEMA}'
                AND table_type = 'BASE TABLE'
            """)
            table_count = cur.fetchone()[0]
            
            if table_count > 0:
                print(f"\n⚠️  WARNING: Schema '{SCHEMA}' already has {table_count} tables!")
                print("   Restore will fail if tables already exist.")
                print("   Consider dropping the schema first:")
                print(f"   DROP SCHEMA {SCHEMA} CASCADE;")
                print(f"   CREATE SCHEMA {SCHEMA};")
                
                # Uncomment to auto-drop (DANGEROUS!)
                # print(f"\n🗑️  Auto-dropping schema '{SCHEMA}'...")
                # cur.execute(f"DROP SCHEMA {SCHEMA} CASCADE")
                # cur.execute(f"CREATE SCHEMA {SCHEMA}")
                # conn.commit()
                # print("✅ Schema dropped and recreated")
        else:
            # Create schema if it doesn't exist
            print(f"\n📁 Creating schema '{SCHEMA}'...")
            cur.execute(f"CREATE SCHEMA {SCHEMA}")
            conn.commit()
            print("✅ Schema created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run pg_restore

# COMMAND ----------

# Set up environment for pg_restore
env = os.environ.copy()
env['PGPASSWORD'] = conn_info['password']

# Build pg_restore command
restore_cmd = [
    'pg_restore',
    '--host', conn_info['host'],
    '--port', str(conn_info['port']),
    '--username', conn_info['user'],
    '--dbname', conn_info['database'],
    '--schema', SCHEMA,
    '--jobs', str(PARALLEL_JOBS),  # Parallel restore
    '--verbose',                    # Show progress
    '--no-owner',                   # Don't restore ownership
    '--no-privileges',              # Don't restore privileges
    DUMP_FILE
]

print("🚀 Starting pg_restore...")
print(f"   Command: {' '.join(restore_cmd)}")
print(f"\nThis will take ~1-2 hours with {PARALLEL_JOBS} parallel workers.")
print("Watch for progress below:\n")
print("=" * 80)

# Run pg_restore
start_time = datetime.now()

try:
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
    print("✅ RESTORE SUCCESSFUL!")
    print("=" * 80)
    print(f"Duration: {duration:.1f} minutes")
    
    # Print verbose output
    if result.stdout:
        print("\n📝 Restore log:")
        print(result.stdout[-2000:])  # Last 2000 chars
    
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
# MAGIC ## Post-Restore Verification

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
            WHERE table_schema = '{SCHEMA}'
            AND table_type = 'BASE TABLE'
        """)
        table_count = cur.fetchone()[0]
        
        # Get total row count
        cur.execute(f"""
            SELECT SUM(n_live_tup::bigint) 
            FROM pg_stat_user_tables 
            WHERE schemaname = '{SCHEMA}'
        """)
        total_rows = cur.fetchone()[0] or 0
        
        # Get database size
        cur.execute(f"""
            SELECT pg_size_pretty(SUM(pg_total_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename))))
            FROM pg_tables
            WHERE schemaname = '{SCHEMA}'
        """)
        db_size = cur.fetchone()[0]
        
        # Get index count
        cur.execute(f"""
            SELECT COUNT(*) 
            FROM pg_indexes 
            WHERE schemaname = '{SCHEMA}'
        """)
        index_count = cur.fetchone()[0]
        
        # Get PRIMARY KEY count
        cur.execute(f"""
            SELECT COUNT(*) 
            FROM information_schema.table_constraints 
            WHERE table_schema = '{SCHEMA}'
            AND constraint_type = 'PRIMARY KEY'
        """)
        pk_count = cur.fetchone()[0]

print("=" * 80)
print("📊 DATABASE STATE (POST-RESTORE)")
print("=" * 80)
print(f"Tables: {table_count}")
print(f"Total rows: {total_rows:,}")
print(f"Total size: {db_size}")
print(f"Indexes: {index_count}")
print(f"PRIMARY KEYs: {pk_count}")
print("=" * 80)

# Validation
expected_tables = 30
expected_pks = 30

if table_count != expected_tables:
    print(f"⚠️  WARNING: Expected {expected_tables} tables, got {table_count}")
else:
    print(f"✅ Table count matches expected: {expected_tables}")

if pk_count != expected_pks:
    print(f"⚠️  WARNING: Expected {expected_pks} PRIMARY KEYs, got {pk_count}")
else:
    print(f"✅ PRIMARY KEY count matches expected: {expected_pks}")

if total_rows < 16_000_000_000:
    print(f"⚠️  WARNING: Expected ~16.6B rows, got {total_rows:,}")
else:
    print(f"✅ Row count looks good: {total_rows:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Detailed Table Verification

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
            WHERE t.schemaname = '{SCHEMA}'
            ORDER BY s.n_live_tup DESC NULLS LAST
        """)
        
        tables = cur.fetchall()

print("\n" + "=" * 80)
print("📋 DETAILED TABLE STATUS")
print("=" * 80)
print(f"{'Table':<70} {'Rows':>15} {'Size (GB)':>10} {'PK':>3}")
print("=" * 80)

tables_without_pk = []
for table in tables:
    tablename, rows, size_gb, has_pk = table
    pk_status = "✅" if has_pk else "❌"
    print(f"{tablename:<70} {rows:>15,} {size_gb:>10.2f} {pk_status:>3}")
    
    if not has_pk:
        tables_without_pk.append(tablename)

print("=" * 80)

if tables_without_pk:
    print(f"\n⚠️  Tables missing PRIMARY KEY ({len(tables_without_pk)}):")
    for table in tables_without_pk:
        print(f"   - {table}")
else:
    print("\n✅ All tables have PRIMARY KEYs!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\n" + "=" * 80)
print("🎉 LAKEBASE RESTORE COMPLETE!")
print("=" * 80)
print(f"✅ Tables restored: {table_count}")
print(f"✅ Rows restored: {total_rows:,}")
print(f"✅ Indexes restored: {index_count}")
print(f"✅ PRIMARY KEYs: {pk_count}")
print(f"✅ Duration: {duration:.1f} minutes")
print("\n" + "=" * 80)
print("NEXT STEPS:")
print("=" * 80)
print("1. Run verification job: databricks bundle run fraud_verify_tables -t dev")
print("2. Test benchmarks: databricks bundle run fraud_benchmark_feature_serving -t dev")
print("3. Run production benchmarks: databricks bundle run fraud_production_benchmark -t dev")
print("=" * 80)
