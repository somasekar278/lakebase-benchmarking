# Databricks notebook source
# MAGIC %md
# MAGIC # Generic Lakebase Benchmark
# MAGIC 
# MAGIC **Automatically discovers and benchmarks all tables in the schema**
# MAGIC - Finds all tables
# MAGIC - Runs warmup queries
# MAGIC - Measures latency (avg, p50, p95, p99)
# MAGIC - Reports results

# COMMAND ----------

# MAGIC %md ## Install Dependencies

# COMMAND ----------

%pip install psycopg2-binary
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md ## Configuration

# COMMAND ----------

import psycopg2
import time

# Get parameters from job
dbutils.widgets.text("lakebase_host", "", "Lakebase Host")
dbutils.widgets.text("lakebase_database", "", "Database")
dbutils.widgets.text("lakebase_schema", "", "Schema")
dbutils.widgets.text("lakebase_user", "", "User")
dbutils.widgets.text("lakebase_password", "", "Password")
dbutils.widgets.text("num_warmup", "5", "Warmup Iterations")
dbutils.widgets.text("num_iterations", "100", "Benchmark Iterations")

# Get config
LAKEBASE_CONFIG = {
    'host': dbutils.widgets.get("lakebase_host"),
    'port': 5432,
    'database': dbutils.widgets.get("lakebase_database"),
    'user': dbutils.widgets.get("lakebase_user"),
    'password': dbutils.widgets.get("lakebase_password"),
    'sslmode': 'require'
}

SCHEMA = dbutils.widgets.get("lakebase_schema")
NUM_WARMUP = int(dbutils.widgets.get("num_warmup"))
NUM_ITERATIONS = int(dbutils.widgets.get("num_iterations"))

print("✅ Configuration loaded")
print(f"   Host: {LAKEBASE_CONFIG['host']}")
print(f"   Database: {LAKEBASE_CONFIG['database']}")
print(f"   Schema: {SCHEMA}")
print(f"   Warmup: {NUM_WARMUP}")
print(f"   Iterations: {NUM_ITERATIONS}")

# COMMAND ----------

# MAGIC %md ## Discover Tables

# COMMAND ----------

print("🔍 Discovering tables in schema...")

conn = psycopg2.connect(**LAKEBASE_CONFIG)
cursor = conn.cursor()

# Set schema
cursor.execute(f"SET search_path TO {SCHEMA}")

# Get all tables in schema
cursor.execute("""
    SELECT tablename 
    FROM pg_tables 
    WHERE schemaname = %s 
    ORDER BY tablename
""", (SCHEMA,))

tables = [row[0] for row in cursor.fetchall()]

print(f"\n✅ Found {len(tables)} tables:")
for table in tables:
    # Get row count
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    count = cursor.fetchone()[0]
    print(f"   - {table}: {count:,} rows")

cursor.close()
conn.close()

if not tables:
    print("\n❌ No tables found! Cannot run benchmark.")
    dbutils.notebook.exit("No tables found")

print("\n" + "="*80)

# COMMAND ----------

# MAGIC %md ## Run Benchmark

# COMMAND ----------

print("🎯 Starting benchmark...\n")
print(f"Query: SELECT COUNT(*) FROM <table>")
print(f"Warmup: {NUM_WARMUP} iterations per table")
print(f"Benchmark: {NUM_ITERATIONS} iterations per table")
print("="*80)

conn = psycopg2.connect(**LAKEBASE_CONFIG)
cursor = conn.cursor()
cursor.execute(f"SET search_path TO {SCHEMA}")

results = {}

for table in tables:
    print(f"\n📊 Benchmarking: {table}")
    
    # Warmup
    print(f"   🔥 Warmup ({NUM_WARMUP} iterations)...")
    for _ in range(NUM_WARMUP):
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        cursor.fetchone()
    
    # Benchmark
    print(f"   ⏱️  Running benchmark ({NUM_ITERATIONS} iterations)...")
    latencies = []
    
    for i in range(NUM_ITERATIONS):
        start = time.perf_counter()
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        result = cursor.fetchone()
        latency_ms = (time.perf_counter() - start) * 1000
        latencies.append(latency_ms)
        
        if (i + 1) % 20 == 0:
            print(f"      Progress: {i+1}/{NUM_ITERATIONS}")
    
    # Calculate statistics
    latencies_sorted = sorted(latencies)
    results[table] = {
        'count': result[0],
        'avg': sum(latencies) / len(latencies),
        'p50': latencies_sorted[50],
        'p95': latencies_sorted[95],
        'p99': latencies_sorted[99],
        'min': min(latencies),
        'max': max(latencies)
    }
    
    print(f"   ✅ Complete:")
    print(f"      Avg: {results[table]['avg']:.2f}ms")
    print(f"      P50: {results[table]['p50']:.2f}ms")
    print(f"      P95: {results[table]['p95']:.2f}ms")
    print(f"      P99: {results[table]['p99']:.2f}ms")

cursor.close()
conn.close()

print("\n" + "="*80)
print("✅ Benchmark Complete!")
print("="*80)

# COMMAND ----------

# MAGIC %md ## Results Summary

# COMMAND ----------

print("\n📊 BENCHMARK RESULTS SUMMARY")
print("="*80)

for table, metrics in results.items():
    print(f"\n📋 {table} ({metrics['count']:,} rows)")
    print(f"   Avg latency: {metrics['avg']:>8.2f}ms")
    print(f"   P50 latency: {metrics['p50']:>8.2f}ms")
    print(f"   P95 latency: {metrics['p95']:>8.2f}ms")
    print(f"   P99 latency: {metrics['p99']:>8.2f}ms")
    print(f"   Min latency: {metrics['min']:>8.2f}ms")
    print(f"   Max latency: {metrics['max']:>8.2f}ms")

print("\n" + "="*80)
print("🎉 Done!")
print("="*80)

# COMMAND ----------
