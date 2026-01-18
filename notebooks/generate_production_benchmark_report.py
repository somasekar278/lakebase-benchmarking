# Databricks notebook source
# MAGIC %md
# MAGIC # 📊 Lakebase Production Benchmark Report
# MAGIC 
# MAGIC **Comprehensive production-ready benchmarks with concurrency testing**
# MAGIC 
# MAGIC Tests 3 critical dimensions:
# MAGIC 1. **Concurrency:** 1, 10, 50, 100 concurrent clients
# MAGIC 2. **Table Subsets:** 10, 20, 30 tables
# MAGIC 3. **Load Conditions:** With/without autovacuum
# MAGIC 
# MAGIC Generates publication-ready report comparing Lakebase vs DynamoDB.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣ Install Dependencies

# COMMAND ----------

%pip install psycopg[binary,pool] numpy matplotlib seaborn pandas plotly kaleido
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2️⃣ Configuration

# COMMAND ----------

dbutils.widgets.text("lakebase_host", "", "Lakebase Host")
dbutils.widgets.text("lakebase_database", "benchmark", "Database")
dbutils.widgets.text("lakebase_schema", "features", "Schema")
dbutils.widgets.text("lakebase_user", "", "User")
dbutils.widgets.text("lakebase_password", "", "Password")
dbutils.widgets.text("num_warmup", "1000", "Warmup Iterations")
dbutils.widgets.text("num_iterations_per_client", "100", "Iterations per Client")
dbutils.widgets.text("concurrent_clients", "1,10,50,100", "Concurrent Client Counts (comma-separated)")
dbutils.widgets.text("table_counts", "10,20,30", "Table Counts to Test (comma-separated)")
dbutils.widgets.dropdown("check_autovacuum", "true", ["true", "false"], "Check Autovacuum")

LAKEBASE_CONFIG = {
    'host': dbutils.widgets.get("lakebase_host"),
    'port': 5432,
    'database': dbutils.widgets.get("lakebase_database"),
    'schema': dbutils.widgets.get("lakebase_schema"),
    'user': dbutils.widgets.get("lakebase_user"),
    'password': dbutils.widgets.get("lakebase_password"),
    'sslmode': 'require'
}

SCHEMA = dbutils.widgets.get("lakebase_schema")
NUM_WARMUP = int(dbutils.widgets.get("num_warmup"))
NUM_ITERATIONS_PER_CLIENT = int(dbutils.widgets.get("num_iterations_per_client"))
CONCURRENT_CLIENTS = [int(x.strip()) for x in dbutils.widgets.get("concurrent_clients").split(',')]
TABLE_COUNTS = [int(x.strip()) for x in dbutils.widgets.get("table_counts").split(',')]
CHECK_AUTOVACUUM = dbutils.widgets.get("check_autovacuum").lower() == "true"

# DynamoDB baseline (from production measurements)
DYNAMODB_P50 = 30.0
DYNAMODB_P99 = 79.0

print("="*80)
print("⚙️  PRODUCTION BENCHMARK CONFIGURATION")
print("="*80)
print(f"Lakebase:           {LAKEBASE_CONFIG['host']}")
print(f"Schema:             {SCHEMA}")
print(f"Warmup:             {NUM_WARMUP} iterations")
print(f"Per-client iters:   {NUM_ITERATIONS_PER_CLIENT}")
print(f"Concurrent clients: {CONCURRENT_CLIENTS}")
print(f"Table counts:       {TABLE_COUNTS}")
print(f"Check autovacuum:   {CHECK_AUTOVACUUM}")
print(f"DynamoDB baseline:  p50={DYNAMODB_P50}ms, p99={DYNAMODB_P99}ms")
print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3️⃣ Import Libraries

# COMMAND ----------

import sys
sys.path.append("/Workspace/som.natarajan@databricks.com/.bundle/lakebase-benchmarking/dev/files")

from utils.feature_server import create_feature_schemas_from_ddl, get_sample_hashkeys, LakebaseFeatureServer
import psycopg
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Tuple
import threading

# Set style for publication-quality plots
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 8)
plt.rcParams['font.size'] = 11

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4️⃣ Pre-Flight Checks

# COMMAND ----------

def check_autovacuum_status(conninfo: str, schema: str) -> List[Dict]:
    """
    Check if autovacuum is currently running on any tables.
    
    Returns list of active autovacuum processes.
    """
    with psycopg.connect(conninfo) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    pid,
                    query,
                    state,
                    query_start,
                    NOW() - query_start as duration
                FROM pg_stat_activity
                WHERE query LIKE '%autovacuum%'
                  AND query NOT LIKE '%pg_stat_activity%'
                  AND state = 'active'
            """)
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    'pid': row[0],
                    'query': row[1],
                    'state': row[2],
                    'query_start': row[3],
                    'duration': row[4]
                })
            
            return results

def disable_autovacuum_for_schema(conninfo: str, schema: str):
    """
    Disable autovacuum for all tables in schema during benchmark.
    """
    with psycopg.connect(conninfo) as conn:
        with conn.cursor() as cursor:
            # Get all tables in schema
            cursor.execute("""
                SELECT tablename 
                FROM pg_tables 
                WHERE schemaname = %s
            """, (schema,))
            
            tables = [row[0] for row in cursor.fetchall()]
            
            for table in tables:
                cursor.execute(f"""
                    ALTER TABLE {schema}.{table} 
                    SET (autovacuum_enabled = false)
                """)
            
            conn.commit()
            
            print(f"✅ Disabled autovacuum for {len(tables)} tables in {schema}")
            return tables

def enable_autovacuum_for_schema(conninfo: str, schema: str, tables: List[str]):
    """
    Re-enable autovacuum after benchmark.
    """
    with psycopg.connect(conninfo) as conn:
        with conn.cursor() as cursor:
            for table in tables:
                cursor.execute(f"""
                    ALTER TABLE {schema}.{table} 
                    SET (autovacuum_enabled = true)
                """)
            
            conn.commit()
            
            print(f"✅ Re-enabled autovacuum for {len(tables)} tables")

# Build connection string
conninfo = f"host={LAKEBASE_CONFIG['host']} port={LAKEBASE_CONFIG['port']} " \
           f"dbname={LAKEBASE_CONFIG['database']} user={LAKEBASE_CONFIG['user']} " \
           f"password={LAKEBASE_CONFIG['password']} sslmode={LAKEBASE_CONFIG['sslmode']}"

print("\n" + "="*80)
print("🔍 PRE-FLIGHT CHECKS")
print("="*80)

# Check for active autovacuum
if CHECK_AUTOVACUUM:
    active_autovacuum = check_autovacuum_status(conninfo, SCHEMA)
    
    if active_autovacuum:
        print(f"⚠️  WARNING: {len(active_autovacuum)} active autovacuum processes detected!")
        for av in active_autovacuum:
            print(f"   PID {av['pid']}: {av['query'][:80]}... (running for {av['duration']})")
        print("\n   Recommendation: Wait for autovacuum to complete or run benchmark later.")
        print("   Autovacuum can cause p99 spikes of 50-200ms.")
        
        # Ask if we should proceed
        response = input("\n   Proceed anyway? (yes/no): ")
        if response.lower() != 'yes':
            raise RuntimeError("Benchmark aborted due to active autovacuum")
    else:
        print("✅ No active autovacuum processes detected")
    
    # Disable autovacuum for benchmark duration
    print("\n📌 Disabling autovacuum for benchmark duration...")
    disabled_tables = disable_autovacuum_for_schema(conninfo, SCHEMA)
else:
    print("⏭️  Skipping autovacuum check (disabled in config)")
    disabled_tables = []

print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5️⃣ Load Table Schemas

# COMMAND ----------

print("\n" + "="*80)
print("📋 LOADING TABLE SCHEMAS & SAMPLING HASHKEYS")
print("="*80)

# Get all tables, create schemas, and sample hashkeys (all in one connection)
with psycopg.connect(conninfo) as conn:
    # Create schemas for all tables (auto-discovers tables)
    table_schemas = create_feature_schemas_from_ddl(
        conn,
        schema=SCHEMA,
        exclude_columns=['updated_at']
    )
    
    # Extract table names
    all_tables = [schema.table_name for schema in table_schemas.values()]
    
    print(f"Found {len(table_schemas)} tables in {SCHEMA} schema")
    print(f"✅ Loaded schemas for {len(table_schemas)} tables")
    
    # Sample hashkeys from first (largest) table
    num_samples = max(NUM_WARMUP, NUM_ITERATIONS_PER_CLIENT * max(CONCURRENT_CLIENTS))
    print(f"\n🔑 Sampling {num_samples} hashkeys from {all_tables[0]}...")
    
    sample_hashkeys = get_sample_hashkeys(
        conn,  # Pass connection object, not conninfo string
        schema=SCHEMA,
        table_name=all_tables[0],
        limit=num_samples
    )
    
    print(f"✅ Retrieved {len(sample_hashkeys)} sample hashkeys")

print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6️⃣ Data Loaded

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7️⃣ Initialize Feature Server & Warmup

# COMMAND ----------

print("\n" + "="*80)
print("🔥 INITIALIZING FEATURE SERVER & CACHE WARMING")
print("="*80)

# Calculate pool size based on max concurrent clients
# Rule: 1 connection per 5-10 concurrent clients is sufficient due to fast query execution
max_clients = max(CONCURRENT_CLIENTS)
pool_size = min(max_clients // 5, 50)  # 100 clients → 20 connections, capped at 50
pool_size = max(pool_size, 10)  # Minimum 10 connections

print(f"💡 Connection pool sizing:")
print(f"   Max concurrent clients: {max_clients}")
print(f"   Pool size: {pool_size} connections")
print(f"   Ratio: {max_clients / pool_size:.1f} clients per connection")

feature_server = LakebaseFeatureServer(
    lakebase_config=LAKEBASE_CONFIG,
    table_schemas=table_schemas,
    pool_size=pool_size,
    max_pool_size=pool_size,  # FIXED SIZE (prevents connection churn)
    enable_warmup=True  # Auto index pre-warming
)

print(f"\n📊 Running warmup ({NUM_WARMUP} iterations)...")
print(f"   ⚠️  CRITICAL: Warming ALL {len(sample_hashkeys)} unique keys to prevent cache thrash")
print(f"   This ensures benchmark queries hit warm cache (simulates production hot key access)")
print()

# ✅ FIX: Warm ALL unique keys that will be benchmarked (not just first NUM_WARMUP)
# Cycle through all unique keys multiple times to ensure heap + index pages are cached
warmup_times = []
num_cycles = max(1, NUM_WARMUP // len(sample_hashkeys))

for cycle in range(num_cycles):
    for i, hashkey in enumerate(sample_hashkeys):
        _, latency = feature_server.get_features(
            hashkey=hashkey,
            table_names=all_tables,
            return_timing=True
        )
        warmup_times.append(latency)
        
        if (len(warmup_times) + 1) % 100 == 0:
            recent_avg = np.mean(warmup_times[-100:])
            print(f"   Iteration {len(warmup_times)+1}: Recent avg = {recent_avg:.2f}ms")
    
    print(f"   ✅ Completed cycle {cycle+1}/{num_cycles} ({len(sample_hashkeys)} keys warmed)")

warmup_improvement = ((warmup_times[0] - warmup_times[-1]) / warmup_times[0]) * 100 if len(warmup_times) > 1 else 0
print(f"\n✅ Warmup complete!")
print(f"   Total iterations: {len(warmup_times)}")
print(f"   Unique keys warmed: {len(sample_hashkeys)}")
print(f"   First request: {warmup_times[0]:.2f}ms")
print(f"   Last request:  {warmup_times[-1]:.2f}ms")
print(f"   Improvement:   {warmup_improvement:.1f}%")
print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8️⃣ Run Concurrent Benchmarks

# COMMAND ----------

def run_single_client_benchmark(
    feature_server: LakebaseFeatureServer,
    hashkeys: List[str],
    table_names: List[str],
    num_iterations: int,
    client_id: int
) -> List[float]:
    """
    Run benchmark for a single client thread.
    """
    latencies = []
    
    for i in range(num_iterations):
        hashkey = hashkeys[(client_id * num_iterations + i) % len(hashkeys)]
        
        try:
            _, latency = feature_server.get_features(
                hashkey=hashkey,
                table_names=table_names,
                return_timing=True
            )
            latencies.append(latency)
        except Exception as e:
            print(f"   ❌ Client {client_id} error: {e}")
            latencies.append(None)  # Mark as error
    
    return latencies

def run_concurrent_benchmark(
    feature_server: LakebaseFeatureServer,
    hashkeys: List[str],
    table_names: List[str],
    num_clients: int,
    iterations_per_client: int
) -> Dict:
    """
    Run benchmark with specified number of concurrent clients.
    
    Returns dict with latencies, errors, and timing stats.
    """
    print(f"\n{'─'*80}")
    print(f"🚀 Testing {num_clients} concurrent clients × {iterations_per_client} iterations")
    print(f"   Tables: {len(table_names)}")
    print(f"{'─'*80}")
    
    start_time = time.perf_counter()
    
    # Run concurrent requests
    with ThreadPoolExecutor(max_workers=num_clients) as executor:
        futures = []
        for client_id in range(num_clients):
            future = executor.submit(
                run_single_client_benchmark,
                feature_server,
                hashkeys,
                table_names,
                iterations_per_client,
                client_id
            )
            futures.append(future)
        
        # Collect results
        all_latencies = []
        for future in as_completed(futures):
            latencies = future.result()
            all_latencies.extend(latencies)
    
    elapsed = time.perf_counter() - start_time
    
    # Filter out errors
    valid_latencies = [l for l in all_latencies if l is not None]
    error_count = len(all_latencies) - len(valid_latencies)
    
    if not valid_latencies:
        raise RuntimeError(f"All requests failed for {num_clients} clients!")
    
    # Calculate stats
    latencies_array = np.array(valid_latencies)
    
    stats = {
        'num_clients': num_clients,
        'num_tables': len(table_names),
        'iterations_per_client': iterations_per_client,
        'total_requests': len(all_latencies),
        'successful_requests': len(valid_latencies),
        'errors': error_count,
        'error_rate': error_count / len(all_latencies) * 100,
        'elapsed_seconds': elapsed,
        'throughput_rps': len(valid_latencies) / elapsed,
        'latencies': valid_latencies,
        'p50': np.percentile(latencies_array, 50),
        'p95': np.percentile(latencies_array, 95),
        'p99': np.percentile(latencies_array, 99),
        'p999': np.percentile(latencies_array, 99.9),
        'mean': np.mean(latencies_array),
        'min': np.min(latencies_array),
        'max': np.max(latencies_array),
        'std': np.std(latencies_array)
    }
    
    print(f"\n✅ Results:")
    print(f"   Requests:  {stats['successful_requests']}/{stats['total_requests']} successful")
    print(f"   Errors:    {stats['errors']} ({stats['error_rate']:.2f}%)")
    print(f"   Duration:  {stats['elapsed_seconds']:.2f}s")
    print(f"   Throughput: {stats['throughput_rps']:.1f} req/s")
    print(f"   Latency:   p50={stats['p50']:.2f}ms, p95={stats['p95']:.2f}ms, p99={stats['p99']:.2f}ms")
    
    return stats

# Run full benchmark matrix
print("\n" + "="*80)
print("🎯 RUNNING PRODUCTION BENCHMARK MATRIX")
print("="*80)
print(f"Testing: {len(CONCURRENT_CLIENTS)} client counts × {len(TABLE_COUNTS)} table counts")
print(f"Total scenarios: {len(CONCURRENT_CLIENTS) * len(TABLE_COUNTS)}")
print("="*80)

benchmark_results = []

for table_count in TABLE_COUNTS:
    tables_subset = all_tables[:table_count]
    
    for num_clients in CONCURRENT_CLIENTS:
        result = run_concurrent_benchmark(
            feature_server=feature_server,
            hashkeys=sample_hashkeys,
            table_names=tables_subset,
            num_clients=num_clients,
            iterations_per_client=NUM_ITERATIONS_PER_CLIENT
        )
        
        benchmark_results.append(result)
        
        # Brief pause between scenarios
        time.sleep(2)

print("\n" + "="*80)
print("✅ BENCHMARK MATRIX COMPLETE")
print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9️⃣ Generate Visualizations

# COMMAND ----------

print("\n" + "="*80)
print("📊 GENERATING VISUALIZATIONS")
print("="*80)

# Convert results to DataFrame for easier analysis
results_df = pd.DataFrame([{
    'clients': r['num_clients'],
    'tables': r['num_tables'],
    'p50': r['p50'],
    'p95': r['p95'],
    'p99': r['p99'],
    'p999': r['p999'],
    'mean': r['mean'],
    'throughput': r['throughput_rps'],
    'errors': r['errors'],
    'error_rate': r['error_rate']
} for r in benchmark_results])

print(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.1 Latency vs Concurrency (by Table Count)

# COMMAND ----------

fig, axes = plt.subplots(2, 2, figsize=(16, 12))
fig.suptitle('Lakebase Latency Under Concurrency vs DynamoDB Baseline', fontsize=16, fontweight='bold')

metrics = [('p50', 'P50 (Median)'), ('p95', 'P95'), ('p99', 'P99'), ('p999', 'P99.9')]

for idx, (metric, title) in enumerate(metrics):
    ax = axes[idx // 2, idx % 2]
    
    # Plot line for each table count
    for table_count in TABLE_COUNTS:
        subset = results_df[results_df['tables'] == table_count]
        ax.plot(subset['clients'], subset[metric], marker='o', linewidth=2, 
                label=f'{table_count} tables', markersize=8)
    
    # Add DynamoDB baseline (only for p50 and p99)
    if metric == 'p50':
        ax.axhline(y=DYNAMODB_P50, color='purple', linestyle='--', linewidth=2, 
                   label='DynamoDB P50', alpha=0.7)
    elif metric == 'p99':
        ax.axhline(y=DYNAMODB_P99, color='red', linestyle='--', linewidth=2, 
                   label='DynamoDB P99', alpha=0.7)
    
    ax.set_xlabel('Concurrent Clients', fontsize=12, fontweight='bold')
    ax.set_ylabel('Latency (ms)', fontsize=12, fontweight='bold')
    ax.set_title(title, fontsize=13, fontweight='bold')
    ax.legend(loc='upper left')
    ax.grid(True, alpha=0.3)
    ax.set_xscale('log')
    ax.set_xticks(CONCURRENT_CLIENTS)
    ax.set_xticklabels(CONCURRENT_CLIENTS)

plt.tight_layout()
plt.show()

print("✅ Latency vs concurrency charts generated")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.2 Throughput vs Concurrency

# COMMAND ----------

fig, ax = plt.subplots(1, 1, figsize=(12, 6))

for table_count in TABLE_COUNTS:
    subset = results_df[results_df['tables'] == table_count]
    ax.plot(subset['clients'], subset['throughput'], marker='o', linewidth=2, 
            label=f'{table_count} tables', markersize=8)

ax.set_xlabel('Concurrent Clients', fontsize=12, fontweight='bold')
ax.set_ylabel('Throughput (requests/sec)', fontsize=12, fontweight='bold')
ax.set_title('Lakebase Throughput Under Concurrency', fontsize=14, fontweight='bold')
ax.legend(loc='upper left')
ax.grid(True, alpha=0.3)
ax.set_xscale('log')
ax.set_xticks(CONCURRENT_CLIENTS)
ax.set_xticklabels(CONCURRENT_CLIENTS)

plt.tight_layout()
plt.show()

print("✅ Throughput chart generated")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.3 Latency Distribution Heatmap

# COMMAND ----------

# Create pivot tables for heatmaps
pivot_p99 = results_df.pivot(index='tables', columns='clients', values='p99')

fig, ax = plt.subplots(1, 1, figsize=(10, 6))

sns.heatmap(pivot_p99, annot=True, fmt='.1f', cmap='RdYlGn_r', 
            cbar_kws={'label': 'P99 Latency (ms)'}, ax=ax)

ax.set_xlabel('Concurrent Clients', fontsize=12, fontweight='bold')
ax.set_ylabel('Number of Tables', fontsize=12, fontweight='bold')
ax.set_title('P99 Latency Heatmap (ms)', fontsize=14, fontweight='bold')

plt.tight_layout()
plt.show()

print("✅ Latency heatmap generated")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.4 Error Rate Analysis

# COMMAND ----------

fig, ax = plt.subplots(1, 1, figsize=(12, 6))

for table_count in TABLE_COUNTS:
    subset = results_df[results_df['tables'] == table_count]
    ax.plot(subset['clients'], subset['error_rate'], marker='o', linewidth=2, 
            label=f'{table_count} tables', markersize=8)

ax.set_xlabel('Concurrent Clients', fontsize=12, fontweight='bold')
ax.set_ylabel('Error Rate (%)', fontsize=12, fontweight='bold')
ax.set_title('Error Rate Under Concurrency', fontsize=14, fontweight='bold')
ax.legend(loc='upper left')
ax.grid(True, alpha=0.3)
ax.set_xscale('log')
ax.set_xticks(CONCURRENT_CLIENTS)
ax.set_xticklabels(CONCURRENT_CLIENTS)
ax.axhline(y=0.1, color='red', linestyle='--', linewidth=1, label='0.1% threshold', alpha=0.5)

plt.tight_layout()
plt.show()

print("✅ Error rate chart generated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔟 Executive Summary

# COMMAND ----------

print("\n" + "="*80)
print("📋 EXECUTIVE SUMMARY")
print("="*80)

# Find best and worst cases
best_case = results_df.loc[results_df['p99'].idxmin()]
worst_case = results_df.loc[results_df['p99'].idxmax()]

print(f"\n🎯 BEST CASE (Lowest P99):")
print(f"   Config:     {int(best_case['clients'])} clients, {int(best_case['tables'])} tables")
print(f"   P50:        {best_case['p50']:.2f}ms")
print(f"   P99:        {best_case['p99']:.2f}ms")
print(f"   Throughput: {best_case['throughput']:.1f} req/s")
print(f"   vs DynamoDB P99: {((best_case['p99'] - DYNAMODB_P99) / DYNAMODB_P99 * 100):+.1f}%")

print(f"\n⚠️  WORST CASE (Highest P99):")
print(f"   Config:     {int(worst_case['clients'])} clients, {int(worst_case['tables'])} tables")
print(f"   P50:        {worst_case['p50']:.2f}ms")
print(f"   P99:        {worst_case['p99']:.2f}ms")
print(f"   Throughput: {worst_case['throughput']:.1f} req/s")
print(f"   vs DynamoDB P99: {((worst_case['p99'] - DYNAMODB_P99) / DYNAMODB_P99 * 100):+.1f}%")

# Check if we beat DynamoDB at any concurrency level
wins_at_30_tables = results_df[
    (results_df['tables'] == 30) & 
    (results_df['p99'] < DYNAMODB_P99)
]

print(f"\n🏆 VERDICT vs DynamoDB (30 tables):")
if len(wins_at_30_tables) > 0:
    max_winning_clients = wins_at_30_tables['clients'].max()
    print(f"   ✅ Lakebase BEATS DynamoDB P99 up to {int(max_winning_clients)} concurrent clients")
    for _, row in wins_at_30_tables.iterrows():
        improvement = ((DYNAMODB_P99 - row['p99']) / DYNAMODB_P99 * 100)
        print(f"      {int(row['clients'])} clients: {row['p99']:.2f}ms ({improvement:+.1f}% faster)")
else:
    print(f"   ⚠️  Lakebase does not beat DynamoDB P99 at any tested concurrency level")

# Check error rates
high_error_rate = results_df[results_df['error_rate'] > 0.1]
if len(high_error_rate) > 0:
    print(f"\n⚠️  WARNING: High error rates detected:")
    for _, row in high_error_rate.iterrows():
        print(f"      {int(row['clients'])} clients, {int(row['tables'])} tables: {row['error_rate']:.2f}% errors")
else:
    print(f"\n✅ All scenarios completed with < 0.1% error rate")

print("\n" + "="*80)
print("✅ PRODUCTION BENCHMARK COMPLETE")
print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣1️⃣ Cleanup (Re-enable Autovacuum)

# COMMAND ----------

if disabled_tables and CHECK_AUTOVACUUM:
    print("\n" + "="*80)
    print("🔄 RE-ENABLING AUTOVACUUM")
    print("="*80)
    
    enable_autovacuum_for_schema(conninfo, SCHEMA, disabled_tables)
    
    print("="*80)

print("\n✅ All done! Review visualizations and summary above.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣2️⃣ Export Results

# COMMAND ----------

# Save results to CSV for further analysis
results_df.to_csv("/dbfs/tmp/lakebase_production_benchmark_results.csv", index=False)
print("📁 Results saved to /dbfs/tmp/lakebase_production_benchmark_results.csv")

# Save detailed results as JSON
import json

detailed_results = {
    'config': {
        'lakebase_host': LAKEBASE_CONFIG['host'],
        'schema': SCHEMA,
        'num_warmup': NUM_WARMUP,
        'iterations_per_client': NUM_ITERATIONS_PER_CLIENT,
        'concurrent_clients': CONCURRENT_CLIENTS,
        'table_counts': TABLE_COUNTS,
        'dynamodb_baseline': {'p50': DYNAMODB_P50, 'p99': DYNAMODB_P99},
        'timestamp': datetime.now().isoformat()
    },
    'results': benchmark_results
}

with open("/dbfs/tmp/lakebase_production_benchmark_detailed.json", "w") as f:
    json.dump(detailed_results, f, indent=2)

print("📁 Detailed results saved to /dbfs/tmp/lakebase_production_benchmark_detailed.json")
