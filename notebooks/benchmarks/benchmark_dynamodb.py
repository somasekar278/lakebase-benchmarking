# Databricks notebook source
# MAGIC %md
# MAGIC # DynamoDB Fraud Detection Benchmark
# MAGIC 
# MAGIC Tests P50, P95, P99 latencies for fraud feature lookups using DynamoDB batch_get_item

# COMMAND ----------

# MAGIC %md ## Install dependencies

# COMMAND ----------

%pip install boto3 numpy matplotlib

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md ## Configuration

# COMMAND ----------

import boto3
import time
import statistics
import numpy as np
import matplotlib.pyplot as plt
from typing import Dict, List, Tuple

# DynamoDB configuration
AWS_REGION = "us-west-2"  # Update to match your DynamoDB region
AWS_ACCESS_KEY_ID = dbutils.secrets.get(scope="fraud-benchmark", key="aws-access-key-id")
AWS_SECRET_ACCESS_KEY = dbutils.secrets.get(scope="fraud-benchmark", key="aws-secret-access-key")

# Table names (update to match your DynamoDB tables)
DYNAMODB_TABLES = {
    'fraud_reports_365d': 'fraud_reports_365d',
    'good_rate_90d_lag_730d': 'good_rate_90d_lag_730d',
    'distinct_counts_amount_stats_365d': 'distinct_counts_amount_stats_365d',
    'request_capture_times': 'request_capture_times'
}

# Benchmark configuration
NUM_KEYS = 25  # Number of keys to lookup per query
NUM_WARMUP = 3  # Warm-up iterations
NUM_ITERATIONS = 100  # Benchmark iterations

# Lakebase baseline for comparison
LAKEBASE_P99 = 52.17  # ms (update after running Lakebase benchmark)

# COMMAND ----------

# MAGIC %md ## Helper Functions

# COMMAND ----------

def get_dynamodb_client():
    """Create DynamoDB client"""
    return boto3.client(
        'dynamodb',
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

def get_sample_keys(dynamodb_client, num_keys=25):
    """Get random sample keys from each table"""
    keys = {}
    
    for table_name in DYNAMODB_TABLES.values():
        try:
            # Scan with limit to get sample keys
            response = dynamodb_client.scan(
                TableName=table_name,
                Limit=num_keys,
                ProjectionExpression='primary_key'
            )
            keys[table_name] = [item['primary_key']['S'] for item in response.get('Items', [])]
        except Exception as e:
            print(f"Warning: Could not get keys from {table_name}: {e}")
            keys[table_name] = []
    
    return keys

def measure_query(dynamodb_client, keys):
    """
    Measure a single batch lookup execution using DynamoDB batch_get_item.
    
    This is the BINPACKING strategy - all 4 tables are queried in a single
    batch_get_item request, equivalent to Lakebase's stored procedure approach.
    
    DynamoDB batch_get_item can fetch up to 100 items across multiple tables
    in a single request, minimizing network round-trips.
    """
    start = time.time()
    
    # Build batch_get_item request for all tables (BINPACKING)
    # This queries all 4 tables in a SINGLE request
    request_items = {}
    total_keys = 0
    
    for table_name, table_keys in keys.items():
        if table_keys:
            request_items[table_name] = {
                'Keys': [{'primary_key': {'S': key}} for key in table_keys]
            }
            total_keys += len(table_keys)
    
    # Validate we're querying multiple tables (binpacking)
    if len(request_items) == 0:
        raise ValueError("No keys provided for batch_get_item")
    
    # Execute SINGLE batch_get_item across ALL tables
    response = dynamodb_client.batch_get_item(RequestItems=request_items)
    
    # Track total items retrieved
    items_retrieved = sum(len(items) for items in response.get('Responses', {}).values())
    
    # Process any unprocessed keys (retry if needed due to throttling)
    retry_count = 0
    while response.get('UnprocessedKeys'):
        retry_count += 1
        response = dynamodb_client.batch_get_item(RequestItems=response['UnprocessedKeys'])
        items_retrieved += sum(len(items) for items in response.get('Responses', {}).values())
    
    end = time.time()
    
    # Return latency and metadata
    return (end - start) * 1000  # Convert to ms

def calculate_percentiles(values):
    """Calculate percentiles from values"""
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

def plot_latency_distribution(latencies, stats, table_info):
    """Create latency distribution histogram with P50 and P99 markers"""
    plt.figure(figsize=(10, 6))
    
    # Create histogram
    n, bins, patches = plt.hist(latencies, bins=50, color='skyblue', alpha=0.7, edgecolor='white', linewidth=0.5)
    
    # Add P50 line
    plt.axvline(stats['p50'], color='orange', linestyle='--', linewidth=2, label=f"P50: {stats['p50']:.2f} ms")
    
    # Add P99 line
    plt.axvline(stats['p99'], color='red', linestyle='--', linewidth=2, label=f"P99: {stats['p99']:.2f} ms")
    
    # Labels and title
    plt.xlabel('Latency (ms)', fontsize=12)
    plt.ylabel('Frequency', fontsize=12)
    plt.title(f'Latency Distribution: {table_info}', fontsize=14, fontweight='bold')
    plt.legend(fontsize=11, loc='upper right')
    plt.grid(axis='y', alpha=0.3)
    
    # Set x-axis limit to focus on relevant range
    max_x = min(stats['max'] * 1.1, stats['p99'] * 2)
    plt.xlim(0, max_x)
    
    plt.tight_layout()
    return plt.gcf()

# COMMAND ----------

# MAGIC %md ## Run Benchmark

# COMMAND ----------

print("=" * 80)
print("DYNAMODB FRAUD DETECTION BENCHMARK")
print("=" * 80)

# Test connection
print("\n1️⃣  Testing connection...")
try:
    dynamodb = get_dynamodb_client()
    # Test by listing tables
    response = dynamodb.list_tables(Limit=1)
    print(f"   ✅ Connected to DynamoDB in {AWS_REGION}")
except Exception as e:
    print(f"   ❌ Connection failed: {e}")
    raise

# Get sample keys
print("\n2️⃣  Loading sample keys for BINPACKED batch_get_item...")
sample_keys = get_sample_keys(dynamodb, num_keys=NUM_KEYS)
total_keys = sum(len(v) for v in sample_keys.values())
num_tables = len([v for v in sample_keys.values() if v])
print(f"   ✅ Loaded {total_keys} keys across {num_tables} tables")
for table, keys in sample_keys.items():
    if keys:
        print(f"      - {table}: {len(keys)} keys")

print(f"\n   📦 BINPACKING: {num_tables} tables x ~{NUM_KEYS} keys = {total_keys} total keys")
print(f"   📦 All {num_tables} tables will be queried in a SINGLE batch_get_item request")

# Warm-up
print(f"\n3️⃣  Warming up ({NUM_WARMUP} iterations)...")
for i in range(NUM_WARMUP):
    try:
        latency = measure_query(dynamodb, sample_keys)
        print(f"   Warm-up {i+1}/{NUM_WARMUP}: {latency:.1f}ms")
    except Exception as e:
        print(f"   ⚠️  Warm-up {i+1} failed: {e}")
print("   ✅ Warm-up complete")

# Run benchmark
print(f"\n4️⃣  Running benchmark ({NUM_ITERATIONS} iterations)...")
latencies = []

for i in range(NUM_ITERATIONS):
    try:
        latency = measure_query(dynamodb, sample_keys)
        latencies.append(latency)
        
        if (i + 1) % 10 == 0:
            print(f"   Progress: {i+1}/{NUM_ITERATIONS} - Current: {latency:.1f}ms")
    except Exception as e:
        print(f"   ❌ Iteration {i+1} failed: {e}")

print(f"   ✅ Completed {len(latencies)} iterations")

# COMMAND ----------

# MAGIC %md ## Results

# COMMAND ----------

if latencies:
    stats = calculate_percentiles(latencies)
    
    print("\n" + "=" * 80)
    print("BENCHMARK RESULTS")
    print("=" * 80)
    
    # Get table item counts
    item_counts = {}
    for table_name in DYNAMODB_TABLES.values():
        try:
            response = dynamodb.describe_table(TableName=table_name)
            item_counts[table_name] = response['Table'].get('ItemCount', 0)
        except:
            item_counts[table_name] = 0
    
    total_rows = sum(item_counts.values())
    
    # Format row count for display
    if total_rows >= 1_000_000_000:
        row_display = f"{total_rows / 1_000_000_000:.1f}B rows"
    elif total_rows >= 1_000_000:
        row_display = f"{total_rows / 1_000_000:.0f}M rows"
    else:
        row_display = f"{total_rows:,} rows"
    
    print(f"\n📊 Dataset: {row_display} across {len([c for c in item_counts.values() if c > 0])} tables")
    for table, count in item_counts.items():
        if count > 0:
            print(f"   - {table}: {count:,}")
    
    print(f"\n📈 Latency Statistics (n={len(latencies)} iterations, {total_keys} keys/query):")
    print(f"   P50 (Median):  {stats['p50']:>7.2f} ms")
    print(f"   P95:           {stats['p95']:>7.2f} ms")
    print(f"   P99:           {stats['p99']:>7.2f} ms")
    print(f"   Max:           {stats['max']:>7.2f} ms")
    print(f"   Min:           {stats['min']:>7.2f} ms")
    print(f"   Mean:          {stats['mean']:>7.2f} ms")
    print(f"   Std Dev:       {stats['stdev']:>7.2f} ms")
    print(f"   CV:            {stats['cv']:>7.3f}")
    
    print(f"\n🎯 Comparison vs Lakebase:")
    print(f"   Lakebase P99:  {LAKEBASE_P99:>7.2f} ms  (baseline)")
    print(f"   DynamoDB P99:  {stats['p99']:>7.2f} ms")
    
    if stats['p99'] < LAKEBASE_P99:
        diff = LAKEBASE_P99 - stats['p99']
        pct = (diff / LAKEBASE_P99) * 100
        print(f"   Result:        ✅ DynamoDB is {diff:.2f}ms ({pct:.1f}%) FASTER")
    elif stats['p99'] < 120:
        diff = stats['p99'] - LAKEBASE_P99
        pct = (diff / LAKEBASE_P99) * 100
        print(f"   Result:        ⚠️  DynamoDB is {diff:.2f}ms ({pct:.1f}%) slower, but within 120ms SLA")
    else:
        diff = stats['p99'] - LAKEBASE_P99
        pct = (diff / LAKEBASE_P99) * 100
        print(f"   Result:        ❌ DynamoDB is {diff:.2f}ms ({pct:.1f}%) slower and exceeds 120ms SLA")
    
    print("\n" + "=" * 80)
    
    # Create summary for job output
    summary = {
        "total_rows": total_rows,
        "row_display": row_display,
        "num_keys": total_keys,
        "num_iterations": len(latencies),
        "p50_ms": round(stats['p50'], 2),
        "p95_ms": round(stats['p95'], 2),
        "p99_ms": round(stats['p99'], 2),
        "max_ms": round(stats['max'], 2),
        "mean_ms": round(stats['mean'], 2),
        "stdev_ms": round(stats['stdev'], 2),
        "cv": round(stats['cv'], 3),
        "lakebase_p99_ms": LAKEBASE_P99,
        "vs_lakebase_ms": round(stats['p99'] - LAKEBASE_P99, 2),
        "vs_lakebase_pct": round(((stats['p99'] - LAKEBASE_P99) / LAKEBASE_P99) * 100, 1),
        "within_sla": stats['p99'] < 120,
        "beats_lakebase": stats['p99'] < LAKEBASE_P99
    }
    
    print(f"\n📊 Summary: {summary}")
else:
    print("❌ No successful iterations completed!")
    dbutils.notebook.exit("FAILED")

# COMMAND ----------

# MAGIC %md ## Latency Distribution Chart

# COMMAND ----------

if latencies:
    # Create and display the latency distribution chart
    fig = plot_latency_distribution(latencies, stats, row_display + " (DynamoDB)")
    plt.show()
    
    print("\n✅ Benchmark complete!")
    print(f"   DynamoDB P99: {stats['p99']:.2f}ms vs Lakebase {LAKEBASE_P99}ms")
    
    # Store results for job
    dbutils.notebook.exit(str(summary))

# COMMAND ----------



