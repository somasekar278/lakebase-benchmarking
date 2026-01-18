# Databricks notebook source
# MAGIC %md
# MAGIC # Dump Lakebase Database to UC Volume
# MAGIC
# MAGIC This notebook creates a complete backup of the Lakebase `features` schema using `pg_dump`.
# MAGIC
# MAGIC **What gets backed up:**
# MAGIC - All 30 tables
# MAGIC - All indexes
# MAGIC - All PRIMARY KEY constraints
# MAGIC - All data (~16.6 billion rows)
# MAGIC
# MAGIC **Duration:** ~30-60 minutes
# MAGIC **Output:** `/Volumes/benchmark/data_load/benchmark_data_dev/lakebase_backup.dump`

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

# Output path for dump file
DUMP_FILE = "/Volumes/benchmark/data_load/benchmark_data_dev/lakebase_backup.dump"
SCHEMA = "features"

# Backup metadata
BACKUP_TIMESTAMP = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

print(f"🎯 Dump Configuration:")
print(f"   Host: {conn_info['host']}")
print(f"   Database: {conn_info['database']}")
print(f"   Schema: {SCHEMA}")
print(f"   Output: {DUMP_FILE}")
print(f"   Timestamp: {BACKUP_TIMESTAMP}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-Dump Validation

# COMMAND ----------

import psycopg

# Connect and get current state
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

print("=" * 80)
print("📊 DATABASE STATE (PRE-DUMP)")
print("=" * 80)
print(f"Tables: {table_count}")
print(f"Total rows: {total_rows:,}")
print(f"Total size: {db_size}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run pg_dump

# COMMAND ----------

# Set up environment for pg_dump
env = os.environ.copy()
env['PGPASSWORD'] = conn_info['password']

# Build pg_dump command
dump_cmd = [
    'pg_dump',
    '--host', conn_info['host'],
    '--port', str(conn_info['port']),
    '--username', conn_info['user'],
    '--dbname', conn_info['database'],
    '--schema', SCHEMA,
    '--format', 'custom',  # Custom format (compressed, parallel restore)
    '--compress', '9',     # Maximum compression
    '--verbose',           # Show progress
    '--file', DUMP_FILE
]

print("🚀 Starting pg_dump...")
print(f"   Command: {' '.join(dump_cmd)}")
print("\nThis will take ~30-60 minutes. Watch for progress below:\n")
print("=" * 80)

# Run pg_dump
start_time = datetime.now()

try:
    result = subprocess.run(
        dump_cmd,
        env=env,
        capture_output=True,
        text=True,
        check=True
    )
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds() / 60
    
    print("\n" + "=" * 80)
    print("✅ DUMP SUCCESSFUL!")
    print("=" * 80)
    print(f"Duration: {duration:.1f} minutes")
    print(f"Output file: {DUMP_FILE}")
    
    # Get dump file size
    dump_size_bytes = os.path.getsize(DUMP_FILE)
    dump_size_gb = dump_size_bytes / (1024 ** 3)
    print(f"Dump size: {dump_size_gb:.2f} GB")
    
    # Print verbose output
    if result.stdout:
        print("\n📝 Dump log:")
        print(result.stdout[-2000:])  # Last 2000 chars
    
except subprocess.CalledProcessError as e:
    print("\n" + "=" * 80)
    print("❌ DUMP FAILED!")
    print("=" * 80)
    print(f"Error: {e}")
    if e.stderr:
        print(f"\nError details:\n{e.stderr}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Post-Dump Verification

# COMMAND ----------

# Verify dump file exists and is readable
if os.path.exists(DUMP_FILE):
    dump_size_bytes = os.path.getsize(DUMP_FILE)
    dump_size_gb = dump_size_bytes / (1024 ** 3)
    
    print("=" * 80)
    print("📦 DUMP FILE VERIFICATION")
    print("=" * 80)
    print(f"✅ File exists: {DUMP_FILE}")
    print(f"✅ File size: {dump_size_gb:.2f} GB ({dump_size_bytes:,} bytes)")
    print(f"✅ Readable: {os.access(DUMP_FILE, os.R_OK)}")
    print("=" * 80)
    
    # Write metadata file
    metadata_file = DUMP_FILE.replace('.dump', '_metadata.txt')
    with open(metadata_file, 'w') as f:
        f.write(f"Lakebase Backup Metadata\n")
        f.write(f"========================\n\n")
        f.write(f"Timestamp: {BACKUP_TIMESTAMP}\n")
        f.write(f"Source Host: {conn_info['host']}\n")
        f.write(f"Source Database: {conn_info['database']}\n")
        f.write(f"Schema: {SCHEMA}\n")
        f.write(f"Table Count: {table_count}\n")
        f.write(f"Total Rows: {total_rows:,}\n")
        f.write(f"Source Size: {db_size}\n")
        f.write(f"Dump Size: {dump_size_gb:.2f} GB\n")
        f.write(f"Duration: {duration:.1f} minutes\n")
    
    print(f"✅ Metadata saved to: {metadata_file}")
    
else:
    print("❌ ERROR: Dump file not found!")
    raise FileNotFoundError(f"Dump file not created: {DUMP_FILE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\n" + "=" * 80)
print("🎉 LAKEBASE DUMP COMPLETE!")
print("=" * 80)
print(f"✅ Backup file: {DUMP_FILE}")
print(f"✅ Backup size: {dump_size_gb:.2f} GB (compressed)")
print(f"✅ Tables backed up: {table_count}")
print(f"✅ Rows backed up: {total_rows:,}")
print(f"✅ Duration: {duration:.1f} minutes")
print("\n" + "=" * 80)
print("NEXT STEPS:")
print("=" * 80)
print("1. Set up new workspace with Lakebase endpoint")
print("2. Deploy DABs to new workspace: databricks bundle deploy -t dev")
print("3. Run restore job: databricks bundle run fraud_restore_database -t dev")
print("4. Verify: databricks bundle run fraud_verify_tables -t dev")
print("=" * 80)
