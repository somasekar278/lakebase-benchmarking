# Databricks notebook source
# MAGIC %md
# MAGIC # Load Test Data to Lakebase (1M, 10M, 100M only)
# MAGIC 
# MAGIC Generates and loads test data using Spark (NO 1B table)

# COMMAND ----------

# MAGIC %md ## Install dependencies

# COMMAND ----------

%pip install psycopg2-binary

# COMMAND ----------

# Restart Python to use the newly installed package
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md ## Configuration

# COMMAND ----------

# Import required libraries
import psycopg2
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta

# Optional parameter: run specific table only (for job tasks)
dbutils.widgets.text("run_table", "ALL", "Run specific table")
run_table = dbutils.widgets.get("run_table")
print(f"🎯 Run mode: {run_table}")

# COMMAND ----------

# Lakebase connection details
LAKEBASE_HOST = "ep-autumn-fire-d318blbk.database.eu-west-1.cloud.databricks.com"
LAKEBASE_PORT = "5432"
LAKEBASE_DATABASE = "benchmark"
LAKEBASE_USER = "fraud_benchmark_user"
LAKEBASE_PASSWORD = "fraud_benchmark_user_123!"
LAKEBASE_SCHEMA = "features"

# JDBC URL with connection tuning for large writes
jdbc_url = f"jdbc:postgresql://{LAKEBASE_HOST}:{LAKEBASE_PORT}/{LAKEBASE_DATABASE}?tcpKeepAlive=true&socketTimeout=300&connectTimeout=30"

# Table sizes (NO 1B table)
SIZES = {
    'fraud_reports_365d': 1_000_000,
    'good_rate_90d_lag_730d': 10_000_000,
    'request_capture_times': 100_000_000
}

# COMMAND ----------

# MAGIC %md ## Load Tables

# COMMAND ----------

loaded = False

# ============================================================================
# 1. fraud_reports_365d (1M rows)
# ============================================================================
if run_table == "ALL" or run_table == "fraud_reports_365d":
    table_name = 'fraud_reports_365d'
    print(f"\n{'='*60}")
    print(f"Loading {table_name} ({SIZES[table_name]:,} rows)...")
    print(f"{'='*60}\n")
    
    # Clear existing data
    print("  Clearing existing data...")
    conn = psycopg2.connect(host=LAKEBASE_HOST, port=LAKEBASE_PORT, database=LAKEBASE_DATABASE, 
                            user=LAKEBASE_USER, password=LAKEBASE_PASSWORD)
    conn.cursor().execute(f"TRUNCATE TABLE {LAKEBASE_SCHEMA}.{table_name}")
    conn.commit()
    conn.close()
    
    # Generate data
    print("  Generating data...")
    df = (
        spark.range(SIZES[table_name])
        .withColumn("primary_key", F.sha2(F.concat(F.lit("key_"), F.col("id") + 1_000_000), 256))
        .withColumn("raw_fingerprint", F.concat(F.lit("fingerprint_"), (F.col("id") % 100000).cast("string")))
        .withColumn("fraud_reports_365d", (F.rand() * 50).cast("int"))
        .withColumn("eligible_capture_365d", (F.rand() * 190 + 10).cast("int"))
        .withColumn("fraud_rate_365d", F.col("fraud_reports_365d") / F.col("eligible_capture_365d"))
        .withColumn("updated_at", F.unix_timestamp())
        .drop("id")
    )
    
    # Write to Lakebase (optimized for reliability)
    print("  Writing to Lakebase...")
    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", f"{LAKEBASE_SCHEMA}.{table_name}") \
        .option("user", LAKEBASE_USER) \
        .option("password", LAKEBASE_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .option("batchsize", 5000) \
        .option("numPartitions", 8) \
        .option("isolationLevel", "NONE") \
        .mode("append") \
        .save()
    
    print(f"✅ Loaded {SIZES[table_name]:,} rows\n")
    loaded = True

# ============================================================================
# 2. good_rate_90d_lag_730d (10M rows)
# ============================================================================
if run_table == "ALL" or run_table == "good_rate_90d_lag_730d":
    table_name = 'good_rate_90d_lag_730d'
    print(f"\n{'='*60}")
    print(f"Loading {table_name} ({SIZES[table_name]:,} rows)...")
    print(f"{'='*60}\n")
    
    # Clear existing data
    print("  Clearing existing data...")
    conn = psycopg2.connect(host=LAKEBASE_HOST, port=LAKEBASE_PORT, database=LAKEBASE_DATABASE, 
                            user=LAKEBASE_USER, password=LAKEBASE_PASSWORD)
    conn.cursor().execute(f"TRUNCATE TABLE {LAKEBASE_SCHEMA}.{table_name}")
    conn.commit()
    conn.close()
    
    # Generate data
    print("  Generating data...")
    df = (
        spark.range(SIZES[table_name])
        .withColumn("primary_key", F.sha2(F.concat(F.lit("key_"), F.col("id") + 11_000_000), 256))
        .withColumn("raw_fingerprint", F.concat(F.lit("fingerprint_"), (F.col("id") % 100000).cast("string")))
        .withColumn("eligible_90d_lag_730d", (F.rand() * 190 + 10).cast("int"))
        .withColumn("good_90d_lag_730d", F.col("eligible_90d_lag_730d") - (F.rand() * 50).cast("int"))
        .withColumn("good_rate_90d_lag_730d", F.col("good_90d_lag_730d") / F.col("eligible_90d_lag_730d"))
        .withColumn("updated_at", F.unix_timestamp())
        .drop("id")
    )
    
    # Write to Lakebase (optimized for reliability)
    print("  Writing to Lakebase...")
    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", f"{LAKEBASE_SCHEMA}.{table_name}") \
        .option("user", LAKEBASE_USER) \
        .option("password", LAKEBASE_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .option("batchsize", 5000) \
        .option("numPartitions", 8) \
        .option("isolationLevel", "NONE") \
        .mode("append") \
        .save()
    
    print(f"✅ Loaded {SIZES[table_name]:,} rows\n")
    loaded = True

# ============================================================================
# 3. request_capture_times (100M rows)
# ============================================================================
if run_table == "ALL" or run_table == "request_capture_times":
    table_name = 'request_capture_times'
    print(f"\n{'='*60}")
    print(f"Loading {table_name} ({SIZES[table_name]:,} rows)...")
    print(f"{'='*60}\n")
    
    # Clear existing data
    print("  Clearing existing data...")
    conn = psycopg2.connect(host=LAKEBASE_HOST, port=LAKEBASE_PORT, database=LAKEBASE_DATABASE, 
                            user=LAKEBASE_USER, password=LAKEBASE_PASSWORD)
    conn.cursor().execute(f"TRUNCATE TABLE {LAKEBASE_SCHEMA}.{table_name}")
    conn.commit()
    conn.close()
    
    # Generate data
    print("  Generating data...")
    df = (
        spark.range(SIZES[table_name])
        .withColumn("primary_key", F.sha2(F.concat(F.lit("key_"), F.col("id") + 1_011_000_000), 256))
        .withColumn("raw_fingerprint", F.concat(F.lit("fingerprint_"), (F.col("id") % 100000).cast("string")))
        .withColumn("time_of_first_request", 
            F.from_unixtime(F.unix_timestamp() - (F.rand() * 31536000).cast("int")).cast("timestamp"))
        .withColumn("time_of_first_capture", 
            F.from_unixtime(F.unix_timestamp("time_of_first_request") + (F.rand() * 172800).cast("int")).cast("timestamp"))
        .withColumn("time_of_last_request", 
            F.from_unixtime(F.unix_timestamp() - (F.rand() * 2592000).cast("int")).cast("timestamp"))
        .withColumn("time_of_last_capture", 
            F.from_unixtime(F.unix_timestamp("time_of_last_request") + (F.rand() * 86400).cast("int")).cast("timestamp"))
        .withColumn("updated_at", F.unix_timestamp())
        .drop("id")
    )
    
    # Write to Lakebase (optimized for reliability)
    print("  Writing to Lakebase...")
    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", f"{LAKEBASE_SCHEMA}.{table_name}") \
        .option("user", LAKEBASE_USER) \
        .option("password", LAKEBASE_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .option("batchsize", 5000) \
        .option("numPartitions", 8) \
        .option("isolationLevel", "NONE") \
        .mode("append") \
        .save()
    
    print(f"✅ Loaded {SIZES[table_name]:,} rows\n")
    loaded = True

# COMMAND ----------

# Verify something was loaded
if not loaded:
    raise ValueError(f"Invalid run_table parameter: '{run_table}'. Must be one of: {list(SIZES.keys())} or 'ALL'")

print(f"\n{'='*60}")
print(f"✅ Complete! Successfully loaded data for run_table='{run_table}'")
print(f"{'='*60}")

