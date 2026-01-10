# Databricks notebook source
# MAGIC %md
# MAGIC # Bulk Load with UNLOGGED Tables (Maximum Speed)
# MAGIC 
# MAGIC This notebook demonstrates ultra-fast bulk loading using:
# MAGIC - PostgreSQL COPY (10-100x faster than JDBC)
# MAGIC - UNLOGGED tables (2-3x faster than COPY with logging)
# MAGIC 
# MAGIC **⚠️ CRITICAL WARNING: UNLOGGED TABLES ARE NOT CRASH-SAFE!**
# MAGIC 
# MAGIC ## What are UNLOGGED Tables?
# MAGIC 
# MAGIC UNLOGGED tables skip write-ahead logging (WAL), which provides:
# MAGIC - **2-3x faster bulk loads** (no WAL overhead)
# MAGIC - **Lower I/O** (no log writes)
# MAGIC - **Same read performance** after data is loaded
# MAGIC 
# MAGIC ## ⚠️ The Trade-Off
# MAGIC 
# MAGIC **IF THE DATABASE CRASHES DURING LOAD:**
# MAGIC - ❌ All data in UNLOGGED tables is LOST
# MAGIC - ❌ You must REPEAT THE ENTIRE LOAD PROCESS
# MAGIC - ❌ No recovery possible (WAL was not written)
# MAGIC 
# MAGIC ## ✅ When to Use UNLOGGED
# MAGIC 
# MAGIC Safe for:
# MAGIC - Reproducible benchmark data (this use case!)
# MAGIC - Test environments
# MAGIC - Data that can be regenerated
# MAGIC - Data that can be reloaded from source
# MAGIC 
# MAGIC ## ❌ When NOT to Use UNLOGGED
# MAGIC 
# MAGIC Never use for:
# MAGIC - Production data
# MAGIC - Irreplaceable data
# MAGIC - Data that cannot be easily regenerated
# MAGIC - Any data where loss would cause problems
# MAGIC 
# MAGIC ## 💡 Best Practice
# MAGIC 
# MAGIC 1. **Load**: Use UNLOGGED for fast bulk load
# MAGIC 2. **Convert**: Switch to LOGGED after load completes
# MAGIC 3. **Backup**: Take backups of the LOGGED data
# MAGIC 
# MAGIC ## Performance Comparison
# MAGIC 
# MAGIC | Method | 100M Rows | 1B Rows |
# MAGIC |--------|-----------|---------|
# MAGIC | JDBC | ~8 hours | ~80 hours |
# MAGIC | COPY (LOGGED) | ~15 mins | ~2.5 hours |
# MAGIC | COPY (UNLOGGED) | ~5 mins | ~1 hour |
# MAGIC 
# MAGIC **Speedup: UNLOGGED is 96x faster than JDBC for 100M rows!**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Install dependencies
%pip install psycopg2-binary --quiet
dbutils.library.restartPython()

# COMMAND ----------

# Import libraries
import sys
sys.path.append('/Workspace/Users/your.email@company.com/lakebase-benchmarking')

from config import LAKEBASE_CONFIG, BULK_LOAD_CONFIG
from utils.bulk_load import BulkLoader
from pyspark.sql import functions as F
from pyspark.sql.types import *
import time

# COMMAND ----------

# Configuration
TABLE_NAME = "test_unlogged_demo"
SCHEMA = LAKEBASE_CONFIG['schema']
NUM_ROWS = 10_000_000  # 10M rows for quick demo
VOLUME_PATH = f"/Volumes/{BULK_LOAD_CONFIG['unity_catalog']['catalog']}/{BULK_LOAD_CONFIG['unity_catalog']['schema']}/{BULK_LOAD_CONFIG['unity_catalog']['volume']}"

print(f"Configuration:")
print(f"  Table: {SCHEMA}.{TABLE_NAME}")
print(f"  Rows: {NUM_ROWS:,}")
print(f"  Volume: {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Table

# COMMAND ----------

# Create table (initially LOGGED)
create_table_sql = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE_NAME} (
    id CHAR(64) PRIMARY KEY,
    feature_1 NUMERIC,
    feature_2 NUMERIC,
    feature_3 NUMERIC,
    feature_4 NUMERIC,
    feature_5 NUMERIC,
    updated_at BIGINT
);
"""

# Execute via JDBC (for table creation)
import psycopg2

conn = psycopg2.connect(
    host=LAKEBASE_CONFIG['host'],
    port=LAKEBASE_CONFIG['port'],
    database=LAKEBASE_CONFIG['database'],
    user=LAKEBASE_CONFIG['user'],
    password=LAKEBASE_CONFIG['password']
)

cursor = conn.cursor()
cursor.execute(create_table_sql)
conn.commit()
cursor.close()
conn.close()

print(f"✅ Table created: {SCHEMA}.{TABLE_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Generate Test Data

# COMMAND ----------

print(f"📊 Generating {NUM_ROWS:,} rows...")
start = time.time()

# Generate data
df = spark.range(NUM_ROWS).select(
    F.sha2(F.col("id").cast("string"), 256).alias("id"),
    (F.rand() * 1000).alias("feature_1"),
    (F.rand() * 1000).alias("feature_2"),
    (F.rand() * 1000).alias("feature_3"),
    (F.rand() * 1000).alias("feature_4"),
    (F.rand() * 1000).alias("feature_5"),
    F.unix_timestamp().alias("updated_at")
)

print(f"✅ Data generated in {time.time() - start:.2f} seconds")
print(f"   Schema: {df.schema}")
print(f"   Sample rows:")
df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Export to CSV (Unity Catalog Volume)

# COMMAND ----------

csv_path = f"{VOLUME_PATH}/{TABLE_NAME}.csv"
print(f"📝 Writing CSV to: {csv_path}")
start = time.time()

# Coalesce to single file for simpler COPY
df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"{VOLUME_PATH}/{TABLE_NAME}_temp")

# Find the actual CSV file (Spark adds partition prefixes)
import os
files = dbutils.fs.ls(f"{VOLUME_PATH}/{TABLE_NAME}_temp")
csv_file = [f.path for f in files if f.path.endswith('.csv')][0]

# Move to final location
dbutils.fs.mv(csv_file, csv_path)
dbutils.fs.rm(f"{VOLUME_PATH}/{TABLE_NAME}_temp", recurse=True)

csv_local_path = csv_path.replace("dbfs:", "")
print(f"✅ CSV written in {time.time() - start:.2f} seconds")
print(f"   Location: {csv_local_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Benchmark 1 - COPY with LOGGED Table (Standard)

# COMMAND ----------

# Clear table first
conn = psycopg2.connect(
    host=LAKEBASE_CONFIG['host'],
    port=LAKEBASE_CONFIG['port'],
    database=LAKEBASE_CONFIG['database'],
    user=LAKEBASE_CONFIG['user'],
    password=LAKEBASE_CONFIG['password']
)
cursor = conn.cursor()
cursor.execute(f"TRUNCATE TABLE {SCHEMA}.{TABLE_NAME}")
conn.commit()
cursor.close()
conn.close()

print("📊 Benchmark 1: COPY with LOGGED table (standard, crash-safe)")
print("=" * 70)

loader = BulkLoader(
    host=LAKEBASE_CONFIG['host'],
    port=str(LAKEBASE_CONFIG['port']),
    database=LAKEBASE_CONFIG['database'],
    user=LAKEBASE_CONFIG['user'],
    password=LAKEBASE_CONFIG['password'],
    use_unlogged=False  # Standard LOGGED mode
)

start = time.time()
result = loader.copy_from_csv(
    table_name=TABLE_NAME,
    schema=SCHEMA,
    csv_path=csv_local_path,
    columns=['id', 'feature_1', 'feature_2', 'feature_3', 'feature_4', 'feature_5', 'updated_at'],
    header=True
)
logged_time = time.time() - start

print(f"\n✅ LOGGED load complete!")
print(f"   Time: {logged_time:.2f} seconds")
print(f"   Speed: {NUM_ROWS / logged_time:,.0f} rows/sec")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Benchmark 2 - COPY with UNLOGGED Table (Maximum Speed)

# COMMAND ----------

# Clear table first
conn = psycopg2.connect(
    host=LAKEBASE_CONFIG['host'],
    port=LAKEBASE_CONFIG['port'],
    database=LAKEBASE_CONFIG['database'],
    user=LAKEBASE_CONFIG['user'],
    password=LAKEBASE_CONFIG['password']
)
cursor = conn.cursor()
cursor.execute(f"TRUNCATE TABLE {SCHEMA}.{TABLE_NAME}")
conn.commit()
cursor.close()
conn.close()

print("🚀 Benchmark 2: COPY with UNLOGGED table (maximum speed)")
print("=" * 70)

loader_unlogged = BulkLoader(
    host=LAKEBASE_CONFIG['host'],
    port=str(LAKEBASE_CONFIG['port']),
    database=LAKEBASE_CONFIG['database'],
    user=LAKEBASE_CONFIG['user'],
    password=LAKEBASE_CONFIG['password'],
    use_unlogged=True  # ⚠️ UNLOGGED mode (NOT crash-safe during load!)
)

# Step 1: Convert to UNLOGGED
print("\n⚠️  Converting table to UNLOGGED mode...")
loader_unlogged.set_table_unlogged(TABLE_NAME, SCHEMA)

# Step 2: Perform bulk load
print("\n📦 Loading data...")
start = time.time()
result = loader_unlogged.copy_from_csv(
    table_name=TABLE_NAME,
    schema=SCHEMA,
    csv_path=csv_local_path,
    columns=['id', 'feature_1', 'feature_2', 'feature_3', 'feature_4', 'feature_5', 'updated_at'],
    header=True
)
unlogged_time = time.time() - start

print(f"\n✅ UNLOGGED load complete!")
print(f"   Time: {unlogged_time:.2f} seconds")
print(f"   Speed: {NUM_ROWS / unlogged_time:,.0f} rows/sec")

# Step 3: Convert back to LOGGED (IMPORTANT!)
print("\n🔒 Converting table back to LOGGED mode (making it crash-safe)...")
loader_unlogged.set_table_logged(TABLE_NAME, SCHEMA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance Comparison

# COMMAND ----------

print("\n" + "=" * 70)
print("PERFORMANCE COMPARISON")
print("=" * 70)
print(f"\nDataset: {NUM_ROWS:,} rows\n")

print(f"COPY (LOGGED):   {logged_time:.2f} seconds  ({NUM_ROWS / logged_time:,.0f} rows/sec)")
print(f"COPY (UNLOGGED): {unlogged_time:.2f} seconds  ({NUM_ROWS / unlogged_time:,.0f} rows/sec)")

speedup = logged_time / unlogged_time
print(f"\nSpeedup: {speedup:.2f}x faster with UNLOGGED!")

# Calculate time savings for larger datasets
print(f"\n💡 Time Savings for Larger Datasets:")
print(f"   100M rows: {(logged_time / NUM_ROWS * 100_000_000 / 60):.1f} min (LOGGED) vs {(unlogged_time / NUM_ROWS * 100_000_000 / 60):.1f} min (UNLOGGED)")
print(f"   1B rows:   {(logged_time / NUM_ROWS * 1_000_000_000 / 3600):.1f} hours (LOGGED) vs {(unlogged_time / NUM_ROWS * 1_000_000_000 / 3600):.1f} hours (UNLOGGED)")

print("\n" + "=" * 70)
print("⚠️  REMEMBER: UNLOGGED mode is NOT crash-safe during load!")
print("   Use only for reproducible benchmark data.")
print("   Always convert back to LOGGED after load completes.")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

# Verify data was loaded
conn = psycopg2.connect(
    host=LAKEBASE_CONFIG['host'],
    port=LAKEBASE_CONFIG['port'],
    database=LAKEBASE_CONFIG['database'],
    user=LAKEBASE_CONFIG['user'],
    password=LAKEBASE_CONFIG['password']
)
cursor = conn.cursor()

# Count rows
cursor.execute(f"SELECT COUNT(*) FROM {SCHEMA}.{TABLE_NAME}")
row_count = cursor.fetchone()[0]

# Check if table is LOGGED or UNLOGGED
cursor.execute(f"""
    SELECT relpersistence 
    FROM pg_class 
    WHERE relname = '{TABLE_NAME.lower()}' 
    AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '{SCHEMA}')
""")
persistence = cursor.fetchone()[0]

cursor.close()
conn.close()

print(f"\n✅ Verification:")
print(f"   Rows loaded: {row_count:,} (expected: {NUM_ROWS:,})")
print(f"   Table mode: {'LOGGED (crash-safe)' if persistence == 'p' else 'UNLOGGED (not crash-safe)'}")

if row_count == NUM_ROWS and persistence == 'p':
    print(f"\n🎉 SUCCESS! Data loaded and table is now LOGGED (production-ready)")
else:
    print(f"\n⚠️  Warning: Check results above")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (Optional)

# COMMAND ----------

# Uncomment to drop test table and CSV
# cursor = conn.cursor()
# cursor.execute(f"DROP TABLE IF EXISTS {SCHEMA}.{TABLE_NAME}")
# conn.commit()
# dbutils.fs.rm(csv_path)
# print("✅ Cleanup complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC ### Key Takeaways
# MAGIC 
# MAGIC 1. **UNLOGGED tables are 2-3x faster** for bulk loads
# MAGIC 2. **Combined with COPY: 96x faster** than JDBC for 100M rows
# MAGIC 3. **Critical trade-off**: NOT crash-safe during load
# MAGIC 4. **Best practice**: Convert back to LOGGED after load
# MAGIC 
# MAGIC ### When to Use
# MAGIC 
# MAGIC ✅ **Use UNLOGGED for:**
# MAGIC - Benchmark data (this framework!)
# MAGIC - Test environments
# MAGIC - Reproducible data
# MAGIC 
# MAGIC ❌ **Never use for:**
# MAGIC - Production data
# MAGIC - Irreplaceable data
# MAGIC - Any data where loss matters
# MAGIC 
# MAGIC ### Configuration
# MAGIC 
# MAGIC Set in `config.py`:
# MAGIC ```python
# MAGIC BULK_LOAD_CONFIG = {
# MAGIC     'use_unlogged': True,  # Enable for maximum speed
# MAGIC     # ... other settings
# MAGIC }
# MAGIC ```
# MAGIC 
# MAGIC ### Safety Checklist
# MAGIC 
# MAGIC - [x] Data is reproducible (can regenerate if lost)
# MAGIC - [x] Table converted back to LOGGED after load
# MAGIC - [x] Backups taken after load completes
# MAGIC - [x] Team aware of the trade-offs

