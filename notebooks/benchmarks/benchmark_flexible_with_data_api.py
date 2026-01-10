# Databricks notebook source
# MAGIC %md
# MAGIC # Flexible Benchmark - Direct Connection vs Data API
# MAGIC 
# MAGIC Compares two approaches for querying Lakebase:
# MAGIC 1. **Direct Connection** - psycopg2 with PostgreSQL binary protocol
# MAGIC 2. **Data API** - PostgREST-compatible HTTP/REST API
# MAGIC 
# MAGIC **Purpose:** Determine which approach is faster for fraud detection workload
# MAGIC 
# MAGIC **Configuration:**
# MAGIC - `NUM_TABLES`: Number of tables to query (default: 30)
# MAGIC - `FEATURES_PER_TABLE`: Features per table (default: 5)
# MAGIC - `KEYS_PER_TABLE`: Keys to lookup per table (default: 25)
# MAGIC - `TEST_MODE`: "both", "direct", or "data_api"

# COMMAND ----------

# MAGIC %md ## Install Dependencies

# COMMAND ----------

# MAGIC %pip install psycopg2-binary matplotlib requests

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md ## Configuration

# COMMAND ----------

import psycopg2
import requests
import time
import statistics
import numpy as np
import matplotlib.pyplot as plt
from typing import Dict, List
import json

# Lakebase connection (for direct connection)
LAKEBASE_HOST = 'ep-autumn-fire-d318blbk.database.eu-west-1.cloud.databricks.com'
LAKEBASE_PORT = 5432
LAKEBASE_DATABASE = 'benchmark'
LAKEBASE_USER = 'fraud_benchmark_user'
LAKEBASE_PASSWORD = 'fraud_benchmark_user_123!'

# Data API configuration
# Format: https://<project-id>.<region>.cloud.databricks.com/api/v1
DATA_API_BASE_URL = 'https://your-project-id.eu-west-1.cloud.databricks.com/api/v1'
DATABRICKS_TOKEN = 'your-databricks-oauth-token'  # Get from Databricks workspace

# Benchmark configuration
NUM_TABLES = 30
FEATURES_PER_TABLE = 5
KEYS_PER_TABLE = 25
NUM_WARMUP = 5
NUM_ITERATIONS = 100

# Test mode: "both", "direct", or "data_api"
TEST_MODE = "both"

# DynamoDB baseline
DYNAMODB_P99 = 79  # ms

# Calculated values
TOTAL_FEATURES = NUM_TABLES * FEATURES_PER_TABLE
TOTAL_KEYS = NUM_TABLES * KEYS_PER_TABLE

print(f"📋 Benchmark Configuration:")
print(f"   Tables: {NUM_TABLES}")
print(f"   Features per table: {FEATURES_PER_TABLE}")
print(f"   Keys per table: {KEYS_PER_TABLE}")
print(f"   Total features: {TOTAL_FEATURES}")
print(f"   Total keys: {TOTAL_KEYS}")
print(f"   Test mode: {TEST_MODE}")
print(f"   Comparison baseline: DynamoDB P99 = {DYNAMODB_P99}ms")

# COMMAND ----------

# MAGIC %md ## Helper Functions

# COMMAND ----------

def get_table_name(table_idx):
    """Get table name for given index"""
    return f"feature_table_{table_idx:02d}"

def get_connection():
    """Create optimized PostgreSQL connection"""
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

def measure_direct_connection(conn, keys_dict):
    """
    Measure query via direct PostgreSQL connection.
    Uses stored procedure for binpacking.
    """
    cursor = conn.cursor()
    cursor.execute("SET statement_timeout = '300000'")
    
    start = time.time()
    
    # Build parameter list for stored procedure
    params = [keys_dict.get(i, []) for i in range(NUM_TABLES)]
    
    # Execute stored procedure
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
    
    return latency_ms, len(results)

def measure_data_api(keys_dict):
    """
    Measure query via Lakebase Data API (PostgREST).
    Uses RPC endpoint to call stored procedure.
    """
    # Build payload (convert keys_dict to Data API format)
    payload = {f"keys_{i:02d}": keys_dict.get(i, []) for i in range(NUM_TABLES)}
    
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Prefer": "params=single-object"  # PostgREST convention for stored procedures
    }
    
    start = time.time()
    
    # Call stored procedure via /rpc/ endpoint
    response = requests.post(
        f"{DATA_API_BASE_URL}/rpc/fraud_batch_lookup_flexible",
        headers=headers,
        json=payload,
        timeout=30
    )
    
    end = time.time()
    latency_ms = (end - start) * 1000
    
    if response.status_code != 200:
        raise Exception(f"Data API error: {response.status_code} - {response.text}")
    
    results = response.json()
    result_count = len(results) if isinstance(results, list) else 1
    
    return latency_ms, result_count

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

def plot_comparison(direct_latencies, api_latencies, direct_stats, api_stats):
    """Create side-by-side comparison chart"""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    
    # Direct Connection histogram
    ax1.hist(direct_latencies, bins=50, color='skyblue', alpha=0.7, edgecolor='white', linewidth=0.5)
    ax1.axvline(direct_stats['p50'], color='orange', linestyle='--', linewidth=2, label=f"P50: {direct_stats['p50']:.2f}ms")
    ax1.axvline(direct_stats['p99'], color='red', linestyle='--', linewidth=2, label=f"P99: {direct_stats['p99']:.2f}ms")
    ax1.axvline(DYNAMODB_P99, color='purple', linestyle=':', linewidth=2, alpha=0.6, label=f"DynamoDB: {DYNAMODB_P99}ms")
    ax1.axvline(120, color='darkred', linestyle=':', linewidth=1.5, alpha=0.5, label="120ms SLA")
    ax1.set_xlabel('Latency (ms)', fontsize=11)
    ax1.set_ylabel('Frequency', fontsize=11)
    ax1.set_title('Direct Connection (psycopg2)', fontsize=13, fontweight='bold')
    ax1.legend(fontsize=10)
    ax1.grid(axis='y', alpha=0.3)
    
    # Data API histogram
    ax2.hist(api_latencies, bins=50, color='lightgreen', alpha=0.7, edgecolor='white', linewidth=0.5)
    ax2.axvline(api_stats['p50'], color='orange', linestyle='--', linewidth=2, label=f"P50: {api_stats['p50']:.2f}ms")
    ax2.axvline(api_stats['p99'], color='red', linestyle='--', linewidth=2, label=f"P99: {api_stats['p99']:.2f}ms")
    ax2.axvline(DYNAMODB_P99, color='purple', linestyle=':', linewidth=2, alpha=0.6, label=f"DynamoDB: {DYNAMODB_P99}ms")
    ax2.axvline(120, color='darkred', linestyle=':', linewidth=1.5, alpha=0.5, label="120ms SLA")
    ax2.set_xlabel('Latency (ms)', fontsize=11)
    ax2.set_ylabel('Frequency', fontsize=11)
    ax2.set_title('Data API (PostgREST)', fontsize=13, fontweight='bold')
    ax2.legend(fontsize=10)
    ax2.grid(axis='y', alpha=0.3)
    
    plt.suptitle(f'Lakebase Performance Comparison: {NUM_TABLES} Tables, {TOTAL_FEATURES} Features', 
                 fontsize=15, fontweight='bold', y=1.02)
    plt.tight_layout()
    
    return fig

# COMMAND ----------

# MAGIC %md ## Create Stored Procedure (if not exists)

# COMMAND ----------

print(f"Ensuring stored procedure exists for {NUM_TABLES} tables...")

conn = get_connection()
cursor = conn.cursor()

# Check if procedure exists
cursor.execute("""
    SELECT EXISTS (
        SELECT 1 FROM pg_proc 
        WHERE proname = 'fraud_batch_lookup_flexible'
    )
""")
exists = cursor.fetchone()[0]

if not exists:
    print("Creating stored procedure...")
    
    # Build stored procedure dynamically
    proc_sql = "CREATE OR REPLACE FUNCTION features.fraud_batch_lookup_flexible(\n"
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
    print("✅ Stored procedure created")
else:
    print("✅ Stored procedure already exists")

cursor.close()
conn.close()

# COMMAND ----------

# MAGIC %md ## Test Connections

# COMMAND ----------

print("=" * 80)
print("CONNECTION TESTS")
print("=" * 80)

# Test direct connection
if TEST_MODE in ["both", "direct"]:
    print("\n1️⃣  Testing direct PostgreSQL connection...")
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

# Test Data API
if TEST_MODE in ["both", "data_api"]:
    print("\n2️⃣  Testing Lakebase Data API...")
    try:
        headers = {
            "Authorization": f"Bearer {DATABRICKS_TOKEN}",
            "Content-Type": "application/json"
        }
        # Test with a simple query
        response = requests.get(
            f"{DATA_API_BASE_URL}/",
            headers=headers,
            timeout=10
        )
        if response.status_code in [200, 401, 404]:  # 401/404 means endpoint is reachable
            print(f"   ✅ Data API endpoint reachable")
            print(f"   ✅ Base URL: {DATA_API_BASE_URL}")
        else:
            print(f"   ⚠️  Unexpected status: {response.status_code}")
    except Exception as e:
        print(f"   ❌ Data API test failed: {e}")
        print(f"   💡 Check your DATA_API_BASE_URL and DATABRICKS_TOKEN")
        if TEST_MODE == "data_api":
            raise

# COMMAND ----------

# MAGIC %md ## Load Sample Keys

# COMMAND ----------

print(f"\n{'='*80}")
print(f"LOADING SAMPLE KEYS")
print(f"{'='*80}")

conn = get_connection()
sample_keys = get_sample_keys(conn, KEYS_PER_TABLE)
conn.close()

print(f"   ✅ Loaded {len(sample_keys)} tables")
print(f"   📦 Total keys: {sum(len(v) for v in sample_keys.values())}")
print(f"   📦 Binpacking: {NUM_TABLES} tables in single call")

# COMMAND ----------

# MAGIC %md ## Run Benchmarks

# COMMAND ----------

print("\n" + "=" * 80)
print("RUNNING BENCHMARKS")
print("=" * 80)

results = {}

# Benchmark Direct Connection
if TEST_MODE in ["both", "direct"]:
    print(f"\n🔵 DIRECT CONNECTION BENCHMARK")
    print(f"{'='*80}")
    
    # Warm-up
    print(f"\n   Warming up ({NUM_WARMUP} iterations)...")
    for i in range(NUM_WARMUP):
        conn = get_connection()
        try:
            latency, _ = measure_direct_connection(conn, sample_keys)
            print(f"      Warm-up {i+1}/{NUM_WARMUP}: {latency:.1f}ms")
        except Exception as e:
            print(f"      ⚠️  Warm-up {i+1} failed: {e}")
        finally:
            conn.close()
    
    # Benchmark
    print(f"\n   Running benchmark ({NUM_ITERATIONS} iterations)...")
    direct_latencies = []
    
    for i in range(NUM_ITERATIONS):
        conn = get_connection()
        try:
            latency, result_count = measure_direct_connection(conn, sample_keys)
            direct_latencies.append(latency)
            
            if (i + 1) % 20 == 0:
                current_p99 = np.percentile(direct_latencies, 99)
                print(f"      Progress: {i+1}/{NUM_ITERATIONS} - Current P99: {current_p99:.2f}ms")
        except Exception as e:
            print(f"      ⚠️  Iteration {i+1} failed: {e}")
        finally:
            conn.close()
    
    print(f"   ✅ Completed {len(direct_latencies)} iterations")
    results['direct'] = {
        'latencies': direct_latencies,
        'stats': calculate_percentiles(direct_latencies)
    }

# Benchmark Data API
if TEST_MODE in ["both", "data_api"]:
    print(f"\n🟢 DATA API BENCHMARK")
    print(f"{'='*80}")
    
    # Warm-up
    print(f"\n   Warming up ({NUM_WARMUP} iterations)...")
    for i in range(NUM_WARMUP):
        try:
            latency, _ = measure_data_api(sample_keys)
            print(f"      Warm-up {i+1}/{NUM_WARMUP}: {latency:.1f}ms")
        except Exception as e:
            print(f"      ⚠️  Warm-up {i+1} failed: {e}")
    
    # Benchmark
    print(f"\n   Running benchmark ({NUM_ITERATIONS} iterations)...")
    api_latencies = []
    
    for i in range(NUM_ITERATIONS):
        try:
            latency, result_count = measure_data_api(sample_keys)
            api_latencies.append(latency)
            
            if (i + 1) % 20 == 0:
                current_p99 = np.percentile(api_latencies, 99)
                print(f"      Progress: {i+1}/{NUM_ITERATIONS} - Current P99: {current_p99:.2f}ms")
        except Exception as e:
            print(f"      ⚠️  Iteration {i+1} failed: {e}")
    
    print(f"   ✅ Completed {len(api_latencies)} iterations")
    results['data_api'] = {
        'latencies': api_latencies,
        'stats': calculate_percentiles(api_latencies)
    }

# COMMAND ----------

# MAGIC %md ## Results & Comparison

# COMMAND ----------

print("\n" + "=" * 80)
print("BENCHMARK RESULTS")
print("=" * 80)

# Print individual results
if 'direct' in results:
    stats = results['direct']['stats']
    print(f"\n🔵 DIRECT CONNECTION (psycopg2)")
    print(f"   P50: {stats['p50']:>7.2f} ms")
    print(f"   P95: {stats['p95']:>7.2f} ms")
    print(f"   P99: {stats['p99']:>7.2f} ms")
    print(f"   Max: {stats['max']:>7.2f} ms")
    print(f"   Mean: {stats['mean']:>7.2f} ms")
    print(f"   CV: {stats['cv']:>7.3f}")

if 'data_api' in results:
    stats = results['data_api']['stats']
    print(f"\n🟢 DATA API (PostgREST)")
    print(f"   P50: {stats['p50']:>7.2f} ms")
    print(f"   P95: {stats['p95']:>7.2f} ms")
    print(f"   P99: {stats['p99']:>7.2f} ms")
    print(f"   Max: {stats['max']:>7.2f} ms")
    print(f"   Mean: {stats['mean']:>7.2f} ms")
    print(f"   CV: {stats['cv']:>7.3f}")

# Comparison
if len(results) == 2:
    direct_p99 = results['direct']['stats']['p99']
    api_p99 = results['data_api']['stats']['p99']
    diff = api_p99 - direct_p99
    pct = (diff / direct_p99) * 100
    
    print(f"\n{'='*80}")
    print(f"COMPARISON")
    print(f"{'='*80}")
    print(f"\n📊 P99 Latency:")
    print(f"   Direct Connection: {direct_p99:>7.2f} ms")
    print(f"   Data API:          {api_p99:>7.2f} ms")
    print(f"   Difference:        {diff:>+7.2f} ms ({pct:+.1f}%)")
    
    if abs(diff) < 5:
        print(f"   Result: ⚖️  Virtually identical performance")
    elif api_p99 < direct_p99:
        print(f"   Result: 🟢 Data API is FASTER!")
    elif diff < 10:
        print(f"   Result: 🟡 Data API is slightly slower (acceptable)")
    else:
        print(f"   Result: 🔵 Direct connection is notably faster")
    
    print(f"\n🎯 vs DynamoDB ({DYNAMODB_P99}ms):")
    
    for name, p99 in [("Direct", direct_p99), ("Data API", api_p99)]:
        if p99 < DYNAMODB_P99:
            speedup = DYNAMODB_P99 / p99
            print(f"   {name}: ✅ {speedup:.2f}x FASTER ({DYNAMODB_P99 - p99:.1f}ms faster)")
        else:
            slowdown = p99 / DYNAMODB_P99
            print(f"   {name}: ⚠️  {slowdown:.2f}x slower ({p99 - DYNAMODB_P99:.1f}ms slower)")
    
    print(f"\n🎯 vs 120ms SLA:")
    for name, p99 in [("Direct", direct_p99), ("Data API", api_p99)]:
        if p99 < 120:
            margin = 120 - p99
            pct = (margin / 120) * 100
            print(f"   {name}: ✅ PASS ({margin:.1f}ms / {pct:.0f}% under SLA)")
        else:
            overage = p99 - 120
            pct = (overage / 120) * 100
            print(f"   {name}: ❌ FAIL ({overage:.1f}ms / {pct:.0f}% over SLA)")

print("\n" + "=" * 80)

# COMMAND ----------

# MAGIC %md ## Visualization

# COMMAND ----------

if len(results) == 2:
    print("\n" + "=" * 80)
    print("LATENCY DISTRIBUTION COMPARISON")
    print("=" * 80)
    
    fig = plot_comparison(
        results['direct']['latencies'],
        results['data_api']['latencies'],
        results['direct']['stats'],
        results['data_api']['stats']
    )
    
    try:
        display(fig)
    except:
        plt.show()
    
    plt.close()
    
    print("\n✅ Benchmark complete!")

# COMMAND ----------

# MAGIC %md ## Recommendation

# COMMAND ----------

if len(results) == 2:
    direct_p99 = results['direct']['stats']['p99']
    api_p99 = results['data_api']['stats']['p99']
    diff = api_p99 - direct_p99
    
    print("\n" + "=" * 80)
    print("RECOMMENDATION")
    print("=" * 80)
    
    both_beat_dynamo = direct_p99 < DYNAMODB_P99 and api_p99 < DYNAMODB_P99
    both_meet_sla = direct_p99 < 120 and api_p99 < 120
    
    print(f"\n✅ **Both approaches meet requirements!**" if both_beat_dynamo and both_meet_sla else "")
    
    print(f"\n💡 **Choose Direct Connection (psycopg2) if:**")
    print(f"   • You need absolute minimum latency ({direct_p99:.1f}ms vs {api_p99:.1f}ms)")
    print(f"   • You're running in traditional server environment")
    print(f"   • You can manage connection pools")
    print(f"   • Every millisecond counts")
    
    print(f"\n💡 **Choose Data API (PostgREST) if:**")
    print(f"   • {diff:.1f}ms extra latency is acceptable")
    print(f"   • You want simpler deployment (no connection management)")
    print(f"   • You're using serverless/edge functions")
    print(f"   • You want better OAuth security & audit trail")
    print(f"   • You need multi-language support (not just Python)")
    
    if diff < 5:
        print(f"\n🎯 **Strong Recommendation:** Use Data API")
        print(f"   • Performance is virtually identical ({diff:.1f}ms difference)")
        print(f"   • Operational simplicity is worth it")
    elif diff < 10:
        print(f"\n🎯 **Recommendation:** Consider Data API")
        print(f"   • Small overhead ({diff:.1f}ms) for significant operational benefits")
        print(f"   • Use Direct if you really need every millisecond")
    else:
        print(f"\n🎯 **Recommendation:** Use Direct Connection")
        print(f"   • Notable performance difference ({diff:.1f}ms)")
        print(f"   • Worth the operational complexity")

print("\n" + "=" * 80)

# COMMAND ----------



