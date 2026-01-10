# Databricks notebook source
# MAGIC %md
# MAGIC # Bulk Load to Lakebase (PostgreSQL COPY)
# MAGIC 
# MAGIC **For tables with > 100M rows, COPY is 10-100x faster than JDBC writes**
# MAGIC 
# MAGIC ## Process:
# MAGIC 1. Generate data in Spark (parallelized)
# MAGIC 2. Write to Unity Catalog Volume as CSV
# MAGIC 3. Use PostgreSQL COPY command to bulk load
# MAGIC 
# MAGIC ## Benefits:
# MAGIC - **10-100x faster** than JDBC writes for large tables
# MAGIC - Handles 1B+ rows reliably
# MAGIC - No networking issues (single bulk operation)
# MAGIC - Lower memory footprint

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

import os
import sys
import time
from pyspark.sql import functions as F
from pyspark.sql.types import *
import hashlib

# Add project root to path
sys.path.append('/Workspace/Repos/lakebase-benchmarking')

# Import configuration
from config import (
    LAKEBASE_HOST, LAKEBASE_PORT, LAKEBASE_DATABASE, 
    LAKEBASE_USER, LAKEBASE_PASSWORD, LAKEBASE_SCHEMA
)

from utils.bulk_load import BulkLoader, estimate_load_method

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Parameters

# COMMAND ----------

# Get parameters from job (or use defaults)
dbutils.widgets.text("table_index", "0", "Table Index")
dbutils.widgets.text("num_tables", "4", "Number of Tables")
dbutils.widgets.text("rows", "100000000", "Number of Rows")
dbutils.widgets.text("volume_name", "benchmark_data", "Unity Catalog Volume Name")
dbutils.widgets.text("catalog", "main", "Unity Catalog Name")
dbutils.widgets.text("schema_uc", "default", "Unity Catalog Schema")

TABLE_INDEX = int(dbutils.widgets.get("table_index"))
NUM_TABLES = int(dbutils.widgets.get("num_tables"))
NUM_ROWS = int(dbutils.widgets.get("rows"))
VOLUME_NAME = dbutils.widgets.get("volume_name")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA_UC = dbutils.widgets.get("schema_uc")

# Table configuration
TABLE_NAME = f"feature_table_{TABLE_INDEX:02d}"
FEATURES_PER_TABLE = 5

# Unity Catalog Volume path
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA_UC}/{VOLUME_NAME}"
CSV_PATH = f"{VOLUME_PATH}/{TABLE_NAME}.csv"

print(f"📊 Bulk Load Configuration")
print(f"   Table: {LAKEBASE_SCHEMA}.{TABLE_NAME}")
print(f"   Rows: {NUM_ROWS:,}")
print(f"   Method: {estimate_load_method(NUM_ROWS)}")
print(f"   CSV Path: {CSV_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create Unity Catalog Volume (if needed)

# COMMAND ----------

# Create volume if it doesn't exist
try:
    spark.sql(f"""
        CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA_UC}.{VOLUME_NAME}
        COMMENT 'Volume for Lakebase bulk loading'
    """)
    print(f"✅ Volume ready: {VOLUME_PATH}")
except Exception as e:
    print(f"⚠️  Volume creation: {e}")
    print(f"   Continuing (volume may already exist)...")

# Test volume access
try:
    dbutils.fs.ls(VOLUME_PATH)
    print(f"✅ Volume accessible")
except Exception as e:
    print(f"❌ Cannot access volume: {e}")
    dbutils.notebook.exit(f"ERROR: Cannot access volume {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Generate Data with Spark

# COMMAND ----------

print(f"🔨 Generating {NUM_ROWS:,} rows in Spark...")
start_gen = time.time()

# Create DataFrame
df = spark.range(NUM_ROWS).repartition(200)  # Parallel generation

# Add primary_key (SHA256 hash)
df = df.withColumn(
    "primary_key",
    F.sha2(F.concat(F.lit(f"table_{TABLE_INDEX}_"), F.col("id").cast("string")), 256)
)

# Add raw_fingerprint
df = df.withColumn(
    "raw_fingerprint",
    F.concat(F.lit("fingerprint_"), (F.col("id") % 100000).cast("string"))
)

# Add feature columns (NUMERIC values)
for i in range(FEATURES_PER_TABLE):
    df = df.withColumn(
        f"feature_{i+1}_t{TABLE_INDEX:02d}",
        (F.rand() * 1000000).cast("decimal(18,2)")
    )

# Add updated_at (unix timestamp)
df = df.withColumn(
    "updated_at",
    (F.lit(1704067200) + (F.col("id") % 31536000)).cast("bigint")  # Random within a year
)

# Drop the temporary id column
df = df.drop("id")

gen_duration = time.time() - start_gen
print(f"✅ Data generated in {gen_duration:.2f}s")
print(f"   Schema: {df.schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Write to Unity Catalog Volume (CSV)

# COMMAND ----------

print(f"💾 Writing to CSV: {CSV_PATH}")
start_write = time.time()

# Write as single CSV file (coalesce to 1 partition for COPY command)
# For very large files, you might use multiple partitions and COPY each
df.coalesce(1).write.mode("overwrite").option("header", "true").csv(CSV_PATH)

write_duration = time.time() - start_write
print(f"✅ CSV written in {write_duration:.2f}s")

# Find the actual CSV file (Spark creates a directory with part files)
csv_files = [f.path for f in dbutils.fs.ls(CSV_PATH) if f.path.endswith('.csv')]
if not csv_files:
    print(f"❌ No CSV files found in {CSV_PATH}")
    dbutils.notebook.exit("ERROR: No CSV files generated")

actual_csv_path = csv_files[0].replace('dbfs:', '/dbfs')  # Convert to local path
print(f"   Actual CSV file: {actual_csv_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Bulk Load with PostgreSQL COPY

# COMMAND ----------

print(f"📦 Bulk loading to Lakebase...")

# Initialize bulk loader
loader = BulkLoader(
    host=LAKEBASE_HOST,
    port=LAKEBASE_PORT,
    database=LAKEBASE_DATABASE,
    user=LAKEBASE_USER,
    password=LAKEBASE_PASSWORD
)

# Define columns (must match table schema)
columns = ["primary_key", "raw_fingerprint"]
columns.extend([f"feature_{i+1}_t{TABLE_INDEX:02d}" for i in range(FEATURES_PER_TABLE)])
columns.append("updated_at")

# Truncate table first (idempotent)
import psycopg2
try:
    conn = psycopg2.connect(
        host=LAKEBASE_HOST,
        port=LAKEBASE_PORT,
        database=LAKEBASE_DATABASE,
        user=LAKEBASE_USER,
        password=LAKEBASE_PASSWORD
    )
    cursor = conn.cursor()
    cursor.execute(f"TRUNCATE TABLE {LAKEBASE_SCHEMA}.{TABLE_NAME}")
    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ Table truncated (idempotent)")
except Exception as e:
    print(f"⚠️  Truncate: {e}")

# Bulk load
result = loader.copy_from_csv(
    table_name=TABLE_NAME,
    schema=LAKEBASE_SCHEMA,
    csv_path=actual_csv_path,
    columns=columns,
    delimiter=',',
    header=True
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Summary & Comparison

# COMMAND ----------

if result['success']:
    total_duration = time.time() - start_gen
    
    print("\n" + "=" * 70)
    print("📊 BULK LOAD SUMMARY")
    print("=" * 70)
    print(f"Table:               {LAKEBASE_SCHEMA}.{TABLE_NAME}")
    print(f"Rows:                {result['rows_loaded']:,}")
    print(f"")
    print(f"Timing Breakdown:")
    print(f"  1. Data Generation: {gen_duration:>8.2f}s")
    print(f"  2. Write CSV:       {write_duration:>8.2f}s")
    print(f"  3. COPY Load:       {result['duration_seconds']:>8.2f}s")
    print(f"  Total:              {total_duration:>8.2f}s")
    print(f"")
    print(f"Performance:")
    print(f"  Throughput:         {result['throughput_rows_per_sec']:>8,.0f} rows/s")
    print(f"  CSV Size:           ~{NUM_ROWS * 0.0001:.1f} MB (estimated)")
    print(f"")
    print(f"Method:               PostgreSQL COPY (bulk)")
    print("=" * 70)
    
    # Comparison with JDBC (estimated)
    estimated_jdbc_duration = NUM_ROWS / 50000  # ~50K rows/s typical for JDBC
    speedup = estimated_jdbc_duration / total_duration
    
    print(f"\n📈 COMPARISON")
    print(f"   JDBC (estimated):  ~{estimated_jdbc_duration:.0f}s")
    print(f"   COPY (actual):     {total_duration:.0f}s")
    print(f"   Speedup:           ~{speedup:.1f}x faster")
    
    if NUM_ROWS >= 100_000_000:
        print(f"\n✅ COPY method recommended for {NUM_ROWS:,} rows")
    
    # Clean up CSV file (optional)
    print(f"\n🧹 Cleaning up CSV files...")
    dbutils.fs.rm(CSV_PATH, True)
    print(f"✅ Done!")
    
else:
    print("\n" + "=" * 70)
    print("❌ BULK LOAD FAILED")
    print("=" * 70)
    print(f"Error: {result.get('error', 'Unknown error')}")
    print(f"File:  {result.get('file', 'N/A')}")
    print("=" * 70)
    
    dbutils.notebook.exit(f"ERROR: {result.get('error')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Verify Load

# COMMAND ----------

# Quick verification query
import psycopg2

conn = psycopg2.connect(
    host=LAKEBASE_HOST,
    port=LAKEBASE_PORT,
    database=LAKEBASE_DATABASE,
    user=LAKEBASE_USER,
    password=LAKEBASE_PASSWORD
)
cursor = conn.cursor()

# Count rows
cursor.execute(f"SELECT COUNT(*) FROM {LAKEBASE_SCHEMA}.{TABLE_NAME}")
row_count = cursor.fetchone()[0]

# Sample data
cursor.execute(f"SELECT * FROM {LAKEBASE_SCHEMA}.{TABLE_NAME} LIMIT 5")
sample_rows = cursor.fetchall()

cursor.close()
conn.close()

print(f"✅ Verification")
print(f"   Rows in table: {row_count:,}")
print(f"   Expected:      {NUM_ROWS:,}")
print(f"   Match:         {'✅ Yes' if row_count == NUM_ROWS else '❌ No'}")
print(f"\n   Sample rows loaded successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notes
# MAGIC 
# MAGIC ### Why COPY is Faster
# MAGIC 
# MAGIC 1. **Batch Processing**: COPY loads all data in one transaction vs thousands of small transactions
# MAGIC 2. **Reduced Overhead**: No per-row parsing/planning overhead
# MAGIC 3. **Optimized I/O**: Direct file → table transfer
# MAGIC 4. **Less Network**: Single operation vs continuous streaming
# MAGIC 
# MAGIC ### When to Use
# MAGIC 
# MAGIC - ✅ **COPY**: Tables > 100M rows, bulk loads, initial data loading
# MAGIC - ✅ **JDBC**: Tables < 100M rows, incremental updates, need Spark parallelism
# MAGIC 
# MAGIC ### Unity Catalog Volumes
# MAGIC 
# MAGIC - Volumes provide POSIX filesystem access
# MAGIC - Files persist across cluster restarts
# MAGIC - Can be accessed by external tools (like psql COPY)
# MAGIC - Managed through Unity Catalog for governance

