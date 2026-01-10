# Databricks notebook source
# MAGIC %md
# MAGIC # Flexible Benchmark - N Tables with M Features
# MAGIC 
# MAGIC Tests Lakebase performance with configurable number of tables and features.
# MAGIC 
# MAGIC **Purpose:** Match customer's actual workload (30-50 tables, ~150 features)
# MAGIC 
# MAGIC **Configuration:**
# MAGIC - `NUM_TABLES`: Number of tables to query (default: 30)
# MAGIC - `FEATURES_PER_TABLE`: Features per table (default: 5)
# MAGIC - `KEYS_PER_TABLE`: Keys to lookup per table (default: 25)

# COMMAND ----------

# MAGIC %md ## Install Dependencies

# COMMAND ----------

# MAGIC %pip install psycopg2-binary matplotlib

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md ## Configuration

# COMMAND ----------

import psycopg2
import time
import statistics
import numpy as np
import matplotlib.pyplot as plt
from typing import Dict, List

# Lakebase connection
LAKEBASE_HOST = 'ep-autumn-fire-d318blbk.database.eu-west-1.cloud.databricks.com'
LAKEBASE_PORT = 5432
LAKEBASE_DATABASE = 'benchmark'
LAKEBASE_USER = 'fraud_benchmark_user'
LAKEBASE_PASSWORD = 'fraud_benchmark_user_123!'

# Benchmark configuration
NUM_TABLES = 30  # Number of tables to query
FEATURES_PER_TABLE = 5  # Features per table
KEYS_PER_TABLE = 25  # Number of keys to lookup per table
NUM_WARMUP = 5  # Warm-up iterations
NUM_ITERATIONS = 100  # Benchmark iterations

# DynamoDB baseline for comparison
DYNAMODB_P99 = 79  # ms (customer's baseline)

# Calculated values
TOTAL_FEATURES = NUM_TABLES * FEATURES_PER_TABLE
TOTAL_KEYS = NUM_TABLES * KEYS_PER_TABLE

print(f"📋 Benchmark Configuration:")
print(f"   Tables: {NUM_TABLES}")
print(f"   Features per table: {FEATURES_PER_TABLE}")
print(f"   Keys per table: {KEYS_PER_TABLE}")
print(f"   ─────────────────────────")
print(f"   Total features: {TOTAL_FEATURES}")
print(f"   Total keys: {TOTAL_KEYS}")
print(f"   ─────────────────────────")
print(f"   Warm-up: {NUM_WARMUP} iterations")
print(f"   Benchmark: {NUM_ITERATIONS} iterations")
print(f"   ─────────────────────────")
print(f"   Comparison: DynamoDB P99 = {DYNAMODB_P99}ms")

# COMMAND ----------

# MAGIC %md ## Create Flexible Stored Procedure

# COMMAND ----------

def get_table_name(table_idx):
    """Get table name for given index"""
    return f"feature_table_{table_idx:02d}"

print(f"Creating flexible stored procedure for {NUM_TABLES} tables...")

conn = psycopg2.connect(
    host=LAKEBASE_HOST,
    port=LAKEBASE_PORT,
    database=LAKEBASE_DATABASE,
    user=LAKEBASE_USER,
    password=LAKEBASE_PASSWORD
)
cursor = conn.cursor()

# Build stored procedure dynamically
proc_sql = "CREATE OR REPLACE FUNCTION features.fraud_batch_lookup_flexible(\n"

# Parameters (one TEXT[] per table)
params = [f"    keys_{i:02d} TEXT[]" for i in range(NUM_TABLES)]
proc_sql += ",\n".join(params) + "\n"

proc_sql += """)\nRETURNS TABLE(
    table_name TEXT,
    data JSONB
)
LANGUAGE plpgsql
STABLE PARALLEL SAFE
AS $$
BEGIN
    -- CRITICAL: Cast TEXT[] to CHAR(64)[] to enable index usage
    
"""

# Add RETURN QUERY for each table
for i in range(NUM_TABLES):
    table_name = get_table_name(i)
    proc_sql += f"""    RETURN QUERY
    SELECT 
        '{table_name}'::TEXT,
        to_jsonb(t.*) AS data
    FROM features.{table_name} t
    WHERE primary_key = ANY(keys_{i:02d}::CHAR(64)[]);
    
"""

proc_sql += """    RETURN;
END;
$$;
"""

cursor.execute(proc_sql)
conn.commit()
cursor.close()
conn.close()

print(f"✅ Stored procedure created: features.fraud_batch_lookup_flexible()")
print(f"   Parameters: {NUM_TABLES} TEXT[] arrays")
print(f"   Queries: {NUM_TABLES} tables in single call (binpacking)")

# COMMAND ----------

# MAGIC %md ## Helper Functions

# COMMAND ----------

def get_connection():
    """Create optimized database connection"""
    return psycopg2.connect(
        host=LAKEBASE_HOST,
        port=LAKEBASE_PORT,
        database=LAKEBASE_DATABASE,
        user=LAKEBASE_USER,
        password=LAKEBASE_PASSWORD,
        connect_timeout=30,
        options='-c work_mem=256MB'
    )

def get_sample_keys(conn, num_keys_per_table):
    """Get random sample keys from each table"""
    cursor = conn.cursor()
    cursor.execute("SET statement_timeout = '60000'")
    
    all_keys = {}
    
    for table_idx in range(NUM_TABLES):
        table_name = get_table_name(table_idx)
        cursor.execute(f"""
            SELECT primary_key 
            FROM features.{table_name}
            TABLESAMPLE SYSTEM (0.001)
            LIMIT {num_keys_per_table}
        """)
        keys = [row[0] for row in cursor.fetchall()]
        all_keys[table_idx] = keys
    
    return all_keys

def measure_query(conn, keys_dict):
    """
    Measure a single query execution using flexible stored procedure.
    
    Binpacks all N tables into a single database call.
    """
    cursor = conn.cursor()
    cursor.execute("SET statement_timeout = '300000'")
    
    start = time.time()
    
    # Build parameter list for stored procedure
    params = [keys_dict.get(i, []) for i in range(NUM_TABLES)]
    
    # Execute SINGLE stored procedure call that queries ALL tables
    placeholders = ", ".join(["%s"] * NUM_TABLES)
    cursor.execute(
        f"SELECT * FROM features.fraud_batch_lookup_flexible({placeholders})",
        tuple(params)
    )
    
    # Fetch results
    results = cursor.fetchall()
    
    end = time.time()
    latency_ms = (end - start) * 1000
    
    cursor.close()
    
    return latency_ms

def calculate_percentiles(values):
    """Calculate latency statistics"""
    return {
        'p50': np.percentile(values, 50),
        'p95': np.percentile(values, 95),
        'p99': np.percentile(values, 99),
        'max': max(values),
        'min': min(values),
        'mean': statistics.mean(values),
        'stdev': statistics.stdev(values) if len(values) > 1 else 0,
        'cv': (statistics.stdev(values) / statistics.mean(values)) if len(values) > 1 and statistics.mean(values) > 0 else 0
    }

def plot_latency_distribution(latencies, stats, config_label):
    """Create latency distribution chart"""
    plt.figure(figsize=(12, 6))
    
    # Histogram
    n, bins, patches = plt.hist(latencies, bins=50, color='skyblue', alpha=0.7, edgecolor='white', linewidth=0.5)
    
    # P50 line
    plt.axvline(stats['p50'], color='orange', linestyle='--', linewidth=2, label=f"P50: {stats['p50']:.2f} ms")
    
    # P99 line
    plt.axvline(stats['p99'], color='red', linestyle='--', linewidth=2, label=f"P99: {stats['p99']:.2f} ms")
    
    # DynamoDB baseline
    plt.axvline(DYNAMODB_P99, color='purple', linestyle=':', linewidth=2, alpha=0.6, label=f"DynamoDB P99: {DYNAMODB_P99} ms")
    
    # 120ms SLA
    plt.axvline(120, color='darkred', linestyle=':', linewidth=1.5, alpha=0.5, label="120ms SLA")
    
    plt.xlabel('Latency (ms)', fontsize=12)
    plt.ylabel('Frequency', fontsize=12)
    plt.title(f'Lakebase Latency Distribution: {config_label}', fontsize=14, fontweight='bold')
    plt.legend(fontsize=11, loc='upper right')
    plt.grid(axis='y', alpha=0.3)
    
    max_x = min(stats['max'] * 1.1, stats['p99'] * 2, 500)
    plt.xlim(0, max_x)
    
    plt.tight_layout()
    return plt.gcf()

# COMMAND ----------

# MAGIC %md ## Run Benchmark

# COMMAND ----------

print("=" * 80)
print("FLEXIBLE BENCHMARK - LAKEBASE vs DYNAMODB")
print("=" * 80)

# Test connection
print("\n1️⃣  Testing connection...")
try:
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT current_database(), current_user, version()")
    db, user, version = cursor.fetchone()
    print(f"   ✅ Connected to database: {db}")
    print(f"   ✅ User: {user}")
    print(f"   ✅ PostgreSQL: {version.split(',')[0]}")
    conn.close()
except Exception as e:
    print(f"   ❌ Connection failed: {e}")
    raise

# Get sample keys
print(f"\n2️⃣  Loading sample keys ({KEYS_PER_TABLE} per table)...")
conn = get_connection()
sample_keys = get_sample_keys(conn, KEYS_PER_TABLE)
print(f"   ✅ Loaded keys for {len(sample_keys)} tables")
print(f"   📦 BINPACKING: {NUM_TABLES} tables x {KEYS_PER_TABLE} keys = {TOTAL_KEYS} total keys")
print(f"   📦 All {NUM_TABLES} tables queried in SINGLE stored procedure call")
print(f"   ⚡ Using CHAR(64)[] casting for index scans")
conn.close()

# Warm-up
print(f"\n3️⃣  Warming up ({NUM_WARMUP} iterations)...")
for i in range(NUM_WARMUP):
    conn = get_connection()
    try:
        latency = measure_query(conn, sample_keys)
        print(f"   Warm-up {i+1}/{NUM_WARMUP}: {latency:.1f}ms")
    except Exception as e:
        print(f"   ⚠️  Warm-up {i+1} failed: {e}")
    finally:
        conn.close()
print("   ✅ Warm-up complete")

# Run benchmark
print(f"\n4️⃣  Running benchmark ({NUM_ITERATIONS} iterations)...")
latencies = []

for i in range(NUM_ITERATIONS):
    conn = get_connection()
    try:
        latency = measure_query(conn, sample_keys)
        latencies.append(latency)
        
        if (i + 1) % 10 == 0:
            current_p99 = np.percentile(latencies, 99)
            print(f"   Progress: {i+1}/{NUM_ITERATIONS} - Current P99: {current_p99:.2f}ms")
    except Exception as e:
        print(f"   ⚠️  Iteration {i+1} failed: {e}")
    finally:
        conn.close()

print(f"   ✅ Completed {len(latencies)} iterations")

# COMMAND ----------

# MAGIC %md ## Results

# COMMAND ----------

if latencies:
    stats = calculate_percentiles(latencies)
    
    print("\n" + "=" * 80)
    print(f"BENCHMARK RESULTS - {NUM_TABLES} TABLES × {FEATURES_PER_TABLE} FEATURES = {TOTAL_FEATURES} TOTAL")
    print("=" * 80)
    
    # Get actual table counts from database
    conn = get_connection()
    cursor = conn.cursor()
    
    table_info = []
    for i in range(NUM_TABLES):
        table_name = get_table_name(i)
        cursor.execute(f"SELECT COUNT(*) FROM features.{table_name}")
        count = cursor.fetchone()[0]
        table_info.append((table_name, count))
    
    total_rows = sum(count for _, count in table_info)
    conn.close()
    
    print(f"\n📊 Dataset:")
    print(f"   Total tables: {NUM_TABLES}")
    print(f"   Total features: {TOTAL_FEATURES} ({FEATURES_PER_TABLE} per table)")
    print(f"   Total rows: {total_rows:,}")
    print(f"   Sample: First 3 tables:")
    for table_name, count in table_info[:3]:
        print(f"      - {table_name}: {count:,} rows")
    if NUM_TABLES > 3:
        print(f"      ... and {NUM_TABLES - 3} more tables")
    
    print(f"\n⚡ Optimizations Applied:")
    print(f"   ✅ Binpacking via stored procedure (single DB call)")
    print(f"   ✅ CHAR(64)[] casting for index usage")
    print(f"   ✅ {NUM_WARMUP} warm-up iterations for cache priming")
    print(f"   ✅ work_mem=256MB for query optimization")
    
    print(f"\n📈 Latency Statistics (n={len(latencies)} iterations, {TOTAL_KEYS} keys/query):")
    print(f"   P50 (Median):  {stats['p50']:>7.2f} ms")
    print(f"   P95:           {stats['p95']:>7.2f} ms")
    print(f"   P99:           {stats['p99']:>7.2f} ms")
    print(f"   Max:           {stats['max']:>7.2f} ms")
    print(f"   Min:           {stats['min']:>7.2f} ms")
    print(f"   Mean:          {stats['mean']:>7.2f} ms")
    print(f"   Std Dev:       {stats['stdev']:>7.2f} ms")
    print(f"   CV:            {stats['cv']:>7.3f}")
    
    print(f"\n🎯 Comparison vs Customer's DynamoDB:")
    print(f"   Customer DynamoDB:  {DYNAMODB_P99:>7.2f} ms  (P99)")
    print(f"   Lakebase:           {stats['p99']:>7.2f} ms  (P99)")
    
    if stats['p99'] < DYNAMODB_P99:
        diff = DYNAMODB_P99 - stats['p99']
        pct = (diff / DYNAMODB_P99) * 100
        speedup = DYNAMODB_P99 / stats['p99']
        print(f"   Result:             ✅ Lakebase is {diff:.2f}ms ({pct:.1f}%) FASTER")
        print(f"                       🚀 {speedup:.1f}x speedup!")
    elif stats['p99'] < 120:
        diff = stats['p99'] - DYNAMODB_P99
        pct = (diff / DYNAMODB_P99) * 100
        print(f"   Result:             ⚠️  Lakebase is {diff:.2f}ms ({pct:.1f}%) slower")
        print(f"                       ✅ But still within 120ms SLA")
    else:
        diff = stats['p99'] - DYNAMODB_P99
        pct = (diff / DYNAMODB_P99) * 100
        print(f"   Result:             ❌ Lakebase is {diff:.2f}ms ({pct:.1f}%) slower")
        print(f"                       ❌ Exceeds 120ms SLA")
    
    print(f"\n120ms SLA Assessment:")
    if stats['p99'] < 120:
        margin = 120 - stats['p99']
        pct = (margin / 120) * 100
        print(f"   ✅ PASSES - {margin:.2f}ms ({pct:.1f}%) under SLA")
    else:
        overage = stats['p99'] - 120
        pct = (overage / 120) * 100
        print(f"   ❌ FAILS - {overage:.2f}ms ({pct:.1f}%) over SLA")
    
    print("\n" + "=" * 80)
    
    # Create summary
    config_label = f"{NUM_TABLES} tables, {TOTAL_FEATURES} features"
    
    summary = {
        "num_tables": NUM_TABLES,
        "features_per_table": FEATURES_PER_TABLE,
        "total_features": TOTAL_FEATURES,
        "total_rows": total_rows,
        "keys_per_query": TOTAL_KEYS,
        "num_iterations": len(latencies),
        "p50_ms": round(stats['p50'], 2),
        "p95_ms": round(stats['p95'], 2),
        "p99_ms": round(stats['p99'], 2),
        "max_ms": round(stats['max'], 2),
        "mean_ms": round(stats['mean'], 2),
        "stdev_ms": round(stats['stdev'], 2),
        "cv": round(stats['cv'], 3),
        "dynamodb_p99_ms": DYNAMODB_P99,
        "vs_dynamodb_ms": round(stats['p99'] - DYNAMODB_P99, 2),
        "vs_dynamodb_pct": round(((stats['p99'] - DYNAMODB_P99) / DYNAMODB_P99) * 100, 1),
        "speedup": round(DYNAMODB_P99 / stats['p99'], 2),
        "within_sla": stats['p99'] < 120,
        "beats_dynamodb": stats['p99'] < DYNAMODB_P99
    }
    
    print(f"\n📊 Summary: {summary}")
else:
    print("❌ No successful iterations completed!")
    print("Check logs above for errors.")

# COMMAND ----------

# MAGIC %md ## Latency Distribution Chart

# COMMAND ----------

if latencies:
    print("\n" + "=" * 80)
    print("LATENCY DISTRIBUTION CHART")
    print("=" * 80)
    
    config_label = f"{NUM_TABLES} tables, {TOTAL_FEATURES} features"
    fig = plot_latency_distribution(latencies, stats, config_label)
    
    try:
        display(fig)
    except:
        plt.show()
    
    plt.close()
    
    print("\n✅ Benchmark complete!")
    print(f"   Lakebase P99: {stats['p99']:.2f}ms")
    print(f"   DynamoDB P99: {DYNAMODB_P99}ms")
    print(f"   Result: {'✅ FASTER' if stats['p99'] < DYNAMODB_P99 else '⚠️ SLOWER'}")

# COMMAND ----------



