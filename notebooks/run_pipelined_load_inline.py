# Databricks notebook source
# MAGIC %md
# MAGIC # Pipelined Load - All 30 Tables
# MAGIC 
# MAGIC Loads CSVs → Lakebase using pipelined approach:
# MAGIC - Phase 1: COPY to staging tables
# MAGIC - Phase 2: Build indexes sequentially  
# MAGIC - Phase 3: Atomic swap to production

# COMMAND ----------

import os
import sys
import logging

# Silence psycopg logging FIRST (before any imports that use psycopg)
# Reference: https://kb.databricks.com/notebooks/cmd-c-on-object-id-p0
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def silence_psycopg_logging(level=logging.ERROR):
    """
    Databricks-safe suppression of psycopg / psycopg_pool logging.
    
    Based on Databricks KB: https://kb.databricks.com/notebooks/cmd-c-on-object-id-p0
    - Some libraries set log level to INFO, causing spam
    - Must set propagate=False and explicitly set handler levels
    """
    # Suppress py4j gateway messages (standard Databricks fix)
    logging.getLogger("py4j").setLevel(logging.ERROR)
    
    # Suppress psycopg library messages (same principle)
    for name in (
        "psycopg",
        "psycopg.pool",
        "psycopg_pool",
        "psycopg_pool.pool",
    ):
        logger = logging.getLogger(name)

        # Stop propagation to Databricks root handlers
        logger.propagate = False

        # Raise logger level
        logger.setLevel(level)

        # ALSO raise level on any pre-existing handlers (CRITICAL for Databricks)
        for h in logger.handlers:
            h.setLevel(level)

silence_psycopg_logging()

# Add UC Volume python_modules to path
sys.path.insert(0, '/Volumes/benchmark/data_load/benchmark_data_dev/python_modules')

# COMMAND ----------

# Get parameters
try:
    lakebase_host = dbutils.widgets.get("lakebase_host")
except:
    lakebase_host = "ep-gentle-sea-d3ir52og.database.eu-west-1.cloud.databricks.com"

try:
    lakebase_database = dbutils.widgets.get("lakebase_database")
except:
    lakebase_database = "benchmark"

try:
    lakebase_user = dbutils.widgets.get("lakebase_user")
except:
    lakebase_user = "fraud_benchmark_user"

try:
    lakebase_password = dbutils.widgets.get("lakebase_password")
except:
    lakebase_password = ""  # Set via Databricks widget or environment variable

try:
    ddl_file_path = dbutils.widgets.get("ddl_file_path")
except:
    ddl_file_path = "/Volumes/benchmark/data_load/benchmark_data_dev/fraud_feature_tables.sql"

try:
    uc_volume_path = dbutils.widgets.get("uc_volume_path")
except:
    uc_volume_path = "/Volumes/benchmark/data_load/benchmark_data_dev"

try:
    rows_per_table_file = dbutils.widgets.get("rows_per_table_file")
except:
    rows_per_table_file = "/Volumes/benchmark/data_load/benchmark_data_dev/fraud_tables_row_counts_30_COMPLETE.txt"

# Build connection string
conninfo = (
    f"host={lakebase_host} "
    f"dbname={lakebase_database} "
    f"user={lakebase_user} "
    f"password={lakebase_password} "
    f"sslmode=require "
    f"connect_timeout=30 "
    f"keepalives=1 "
    f"keepalives_idle=30 "
    f"keepalives_interval=10 "
    f"keepalives_count=5"
)

print(f"✓ Lakebase connection configured: {lakebase_host}/{lakebase_database}")

# COMMAND ----------

import psycopg

# COMMAND ----------

# Create production tables from DDL (if they don't exist)
print(f"\n{'='*72}")
print("CREATING PRODUCTION TABLES")
print(f"{'='*72}")
print(f"DDL File: {ddl_file_path}")

with open(ddl_file_path, 'r') as f:
    ddl_content = f.read()

with psycopg.connect(conninfo, autocommit=True) as conn:
    with conn.cursor() as cur:
        statements = [s.strip() for s in ddl_content.split(';') if s.strip()]
        
        created = 0
        skipped = 0
        
        for stmt in statements:
            if 'CREATE TABLE' in stmt.upper():
                # Ensure schema prefix
                if 'features.' not in stmt.lower():
                    stmt = stmt.replace('CREATE TABLE ', 'CREATE TABLE features.', 1)
                
                try:
                    cur.execute(stmt)
                    table_name = stmt.split('CREATE TABLE', 1)[1].strip().split('(')[0].strip()
                    print(f"  ✓ Created: {table_name}")
                    created += 1
                except Exception as e:
                    if 'already exists' in str(e).lower():
                        table_name = stmt.split('CREATE TABLE', 1)[1].strip().split('(')[0].strip()
                        print(f"  ⏭️  Skipped (exists): {table_name}")
                        skipped += 1
                    else:
                        print(f"  ✗ Error creating table: {e}")
                        print(f"  Statement: {stmt[:200]}...")
                        raise

print(f"\n✓ Production tables ready: {created} created, {skipped} already existed")

# COMMAND ----------

# Configure COPY streams for 32CU Lakebase instance
os.environ["COPY_STREAMS_PER_TABLE"] = "4"  # 4 parallel streams (reduced to prevent connection timeouts)
os.environ["TABLES_PER_WAVE"] = "1"          # Not used in sequential mode, but set to avoid warning

print("⚙️  COPY Configuration:")
print("   COPY_STREAMS_PER_TABLE = 4 (reduced from 8 to improve stability)")
print("   TABLES_PER_WAVE = 1 (sequential mode - one table at a time)")
print("   Total concurrent streams: 4")
print("   Pool size: 1 * 4 + 4 = 8 max connections (conservative, stable)\n")

# COMMAND ----------

# Import pipelined_load from UC Volume (it's a real Python file there)
from pipelined_load import LakebaseAdaptor

# COMMAND ----------

# Extract table names from DDL (strip features. prefix for adaptor)
TABLES = []
for line in ddl_content.split('\n'):
    if 'CREATE TABLE' in line.upper():
        parts = line.split('CREATE TABLE', 1)[1].strip().split('(')[0].strip()
        table_name = parts.replace('features.', '')
        if table_name:
            TABLES.append(table_name)

CHECKPOINT_FILE = f"{uc_volume_path}/.checkpoint_pipelined_load.json"

print(f"\n{'='*72}")
print(f"PIPELINED LOAD: {len(TABLES)} Tables")
print(f"{'='*72}")
print(f"UC Volume: {uc_volume_path}")
print(f"Checkpoint: {CHECKPOINT_FILE}")
print(f"Schema: features")
print(f"\nFirst 3 table names (for debugging):")
for i, t in enumerate(TABLES[:3]):
    print(f"  [{i+1}] {t}")
    print(f"      Prod:  features.{t}")
    print(f"      Stage: features.{t}__stage")
print()

# COMMAND ----------

# Verify production tables are accessible
print(f"\n{'='*72}")
print("VERIFYING PRODUCTION TABLES")
print(f"{'='*72}")

with psycopg.connect(conninfo) as conn:
    with conn.cursor() as cur:
        # Test first table
        test_table = f"features.{TABLES[0]}"
        try:
            cur.execute(f"SELECT 1 FROM {test_table} LIMIT 0;")
            print(f"✅ Production tables accessible (tested: {test_table})")
        except Exception as e:
            print(f"❌ ERROR: Cannot access production table: {test_table}")
            print(f"   Error: {e}")
            print(f"\n   Possible issues:")
            print(f"   1. Table doesn't exist (but we saw it was created above)")
            print(f"   2. User doesn't have SELECT permission")
            print(f"   3. Schema 'features' not in search_path")
            raise

# COMMAND ----------

# Create adaptor and run the pipeline
adaptor = LakebaseAdaptor(
    conninfo=conninfo,
    tables=TABLES,
    uc_volume_path=uc_volume_path,
    checkpoint_file=CHECKPOINT_FILE,
    schema="features",
    rows_per_table_file=rows_per_table_file
)

try:
    # Sequential load: Each table completes COPY → INDEX → SWAP → VALIDATE before next table starts
    print(f"\n{'='*72}")
    print("SEQUENTIAL LOAD: COPY → INDEX → SWAP → VALIDATE (per table)")
    print(f"{'='*72}")
    print("This avoids small tables waiting for large tables to finish.")
    print("Each table is validated after completion before proceeding.")
    print()
    
    adaptor.load_tables_sequential()
    
    print(f"\n{'='*72}")
    print("✓ PIPELINED LOAD COMPLETE!")
    print(f"{'='*72}")
    
    # Only cleanup checkpoint on success
    adaptor.checkpoint_mgr.cleanup()
    print("✅ Checkpoint removed (all tables completed)")
    
except Exception as e:
    print(f"\n❌ Load failed: {e}")
    print("⚠️  Checkpoint preserved for resume")
    raise
    
finally:
    # Always print metrics and close connections
    adaptor.metrics.print_summary()
    adaptor.pool.close()

# COMMAND ----------

print("\n" + "="*72)
print("✓ ALL 30 TABLES LOADED SUCCESSFULLY!")
print("="*72)
print("\nNext step:")
print("  → Run benchmarks to compare Lakebase vs DynamoDB")
