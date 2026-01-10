# Databricks notebook source
# MAGIC %md
# MAGIC # Lakebase vs DynamoDB: Side-by-Side Comparison
# MAGIC 
# MAGIC Runs both benchmarks and creates comparative analysis

# COMMAND ----------

# MAGIC %md ## Install dependencies

# COMMAND ----------

%pip install psycopg2-binary boto3 numpy matplotlib

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md ## Run Lakebase Benchmark

# COMMAND ----------

lakebase_result = dbutils.notebook.run(
    "/Users/som.natarajan@databricks.com/benchmark_lakebase",
    timeout_seconds=3600
)

print("✅ Lakebase benchmark complete!")
print(f"Result: {lakebase_result}")

# COMMAND ----------

# MAGIC %md ## Run DynamoDB Benchmark

# COMMAND ----------

dynamodb_result = dbutils.notebook.run(
    "/Users/som.natarajan@databricks.com/benchmark_dynamodb",
    timeout_seconds=3600
)

print("✅ DynamoDB benchmark complete!")
print(f"Result: {dynamodb_result}")

# COMMAND ----------

# MAGIC %md ## Comparative Analysis

# COMMAND ----------

import ast
import matplotlib.pyplot as plt
import numpy as np

# Parse results
lakebase = ast.literal_eval(lakebase_result)
dynamodb = ast.literal_eval(dynamodb_result)

print("=" * 80)
print("LAKEBASE vs DYNAMODB: HEAD-TO-HEAD COMPARISON")
print("=" * 80)

print("\n⚡ BINPACKING STRATEGY:")
print("   Both systems use optimized binpacking for multi-table lookups:")
print("   • Lakebase: Stored procedure (fraud_batch_lookup) - single DB call")
print("   • DynamoDB: batch_get_item - single API call across all tables")
print("   • Fair comparison: Both minimize network round-trips")

print(f"\n📊 Dataset:")
print(f"   Lakebase:  {lakebase['row_display']}")
print(f"   DynamoDB:  {dynamodb['row_display']}")

print(f"\n📈 P50 Latency (Lower is Better):")
print(f"   Lakebase:  {lakebase['p50_ms']:>7.2f} ms")
print(f"   DynamoDB:  {dynamodb['p50_ms']:>7.2f} ms")
if lakebase['p50_ms'] < dynamodb['p50_ms']:
    diff = dynamodb['p50_ms'] - lakebase['p50_ms']
    pct = (diff / dynamodb['p50_ms']) * 100
    print(f"   Winner:    ✅ Lakebase ({diff:.2f}ms / {pct:.1f}% faster)")
else:
    diff = lakebase['p50_ms'] - dynamodb['p50_ms']
    pct = (diff / lakebase['p50_ms']) * 100
    print(f"   Winner:    ✅ DynamoDB ({diff:.2f}ms / {pct:.1f}% faster)")

print(f"\n🎯 P99 Latency (Lower is Better):")
print(f"   Lakebase:  {lakebase['p99_ms']:>7.2f} ms")
print(f"   DynamoDB:  {dynamodb['p99_ms']:>7.2f} ms")
if lakebase['p99_ms'] < dynamodb['p99_ms']:
    diff = dynamodb['p99_ms'] - lakebase['p99_ms']
    pct = (diff / dynamodb['p99_ms']) * 100
    print(f"   Winner:    ✅ Lakebase ({diff:.2f}ms / {pct:.1f}% faster)")
else:
    diff = lakebase['p99_ms'] - dynamodb['p99_ms']
    pct = (diff / lakebase['p99_ms']) * 100
    print(f"   Winner:    ✅ DynamoDB ({diff:.2f}ms / {pct:.1f}% faster)")

print(f"\n🔄 Consistency (CV - Lower is Better):")
print(f"   Lakebase:  {lakebase['cv']:>7.3f}")
print(f"   DynamoDB:  {dynamodb['cv']:>7.3f}")
if lakebase['cv'] < dynamodb['cv']:
    print(f"   Winner:    ✅ Lakebase (more consistent)")
else:
    print(f"   Winner:    ✅ DynamoDB (more consistent)")

print(f"\n✅ SLA Compliance (< 120ms P99):")
print(f"   Lakebase:  {'✅ PASS' if lakebase['within_sla'] else '❌ FAIL'}")
print(f"   DynamoDB:  {'✅ PASS' if dynamodb['within_sla'] else '❌ FAIL'}")

print("\n" + "=" * 80)

# COMMAND ----------

# MAGIC %md ## Latency Distribution Comparison

# COMMAND ----------

# Get raw latency data from each benchmark
# Re-run a smaller sample to get distribution data
print("📊 Re-running benchmarks to capture latency distributions...")
print("   (Running 50 iterations each for visualization)")

import psycopg2
import boto3
import time

# Helper function to get connection and measure
def quick_lakebase_sample(num_samples=50):
    """Quick sample of Lakebase latencies"""
    conn = psycopg2.connect(
        host="ep-autumn-fire-d318blbk.database.eu-west-1.cloud.databricks.com",
        port="5432",
        database="benchmark",
        user="fraud_benchmark_user",
        password="fraud_benchmark_user_123!",
        connect_timeout=30
    )
    cursor = conn.cursor()
    cursor.execute("SET statement_timeout = '300000'")
    
    # Get sample keys
    cursor.execute("SELECT primary_key FROM features.fraud_reports_365d TABLESAMPLE SYSTEM (0.001) LIMIT 25")
    keys = [row[0] for row in cursor.fetchall()]
    
    latencies = []
    for _ in range(num_samples):
        start = time.time()
        cursor.execute("SELECT * FROM features.fraud_batch_lookup(%s, %s, %s, %s)", 
                      (keys, [], [], []))
        cursor.fetchall()
        latencies.append((time.time() - start) * 1000)
    
    conn.close()
    return latencies

def quick_dynamodb_sample(num_samples=50):
    """Quick sample of DynamoDB latencies"""
    dynamodb_client = boto3.client(
        'dynamodb',
        region_name="us-west-2",
        aws_access_key_id=dbutils.secrets.get(scope="fraud-benchmark", key="aws-access-key-id"),
        aws_secret_access_key=dbutils.secrets.get(scope="fraud-benchmark", key="aws-secret-access-key")
    )
    
    # Get sample keys
    response = dynamodb_client.scan(
        TableName='fraud_reports_365d',
        Limit=25,
        ProjectionExpression='primary_key'
    )
    keys = [item['primary_key']['S'] for item in response.get('Items', [])]
    
    latencies = []
    for _ in range(num_samples):
        start = time.time()
        request_items = {
            'fraud_reports_365d': {
                'Keys': [{'primary_key': {'S': key}} for key in keys]
            }
        }
        response = dynamodb_client.batch_get_item(RequestItems=request_items)
        latencies.append((time.time() - start) * 1000)
    
    return latencies

# Get distribution samples
lakebase_dist = quick_lakebase_sample(50)
dynamodb_dist = quick_dynamodb_sample(50)

print(f"✅ Captured {len(lakebase_dist)} Lakebase samples")
print(f"✅ Captured {len(dynamodb_dist)} DynamoDB samples")

# COMMAND ----------

# Create side-by-side latency distributions
fig, axes = plt.subplots(1, 2, figsize=(16, 6))

# Lakebase distribution
ax1 = axes[0]
n1, bins1, patches1 = ax1.hist(lakebase_dist, bins=30, color='skyblue', alpha=0.7, edgecolor='white', linewidth=0.5)
ax1.axvline(lakebase['p50_ms'], color='orange', linestyle='--', linewidth=2, label=f"P50: {lakebase['p50_ms']:.2f} ms")
ax1.axvline(lakebase['p99_ms'], color='red', linestyle='--', linewidth=2, label=f"P99: {lakebase['p99_ms']:.2f} ms")
ax1.set_xlabel('Latency (ms)', fontsize=12)
ax1.set_ylabel('Frequency', fontsize=12)
ax1.set_title(f'Lakebase - {lakebase["row_display"]}', fontsize=14, fontweight='bold')
ax1.legend(fontsize=11, loc='upper right')
ax1.grid(axis='y', alpha=0.3)

# DynamoDB distribution
ax2 = axes[1]
n2, bins2, patches2 = ax2.hist(dynamodb_dist, bins=30, color='lightcoral', alpha=0.7, edgecolor='white', linewidth=0.5)
ax2.axvline(dynamodb['p50_ms'], color='orange', linestyle='--', linewidth=2, label=f"P50: {dynamodb['p50_ms']:.2f} ms")
ax2.axvline(dynamodb['p99_ms'], color='red', linestyle='--', linewidth=2, label=f"P99: {dynamodb['p99_ms']:.2f} ms")
ax2.set_xlabel('Latency (ms)', fontsize=12)
ax2.set_ylabel('Frequency', fontsize=12)
ax2.set_title(f'DynamoDB - {dynamodb["row_display"]}', fontsize=14, fontweight='bold')
ax2.legend(fontsize=11, loc='upper right')
ax2.grid(axis='y', alpha=0.3)

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md ## Percentile Bar Charts

# COMMAND ----------

# Create comparative bar chart
fig, axes = plt.subplots(1, 3, figsize=(15, 5))

# P50 comparison
metrics = ['P50', 'P95', 'P99']
lakebase_values = [lakebase['p50_ms'], lakebase['p95_ms'], lakebase['p99_ms']]
dynamodb_values = [dynamodb['p50_ms'], dynamodb['p95_ms'], dynamodb['p99_ms']]

x = np.arange(len(metrics))
width = 0.35

for i, (metric, lb_val, db_val) in enumerate(zip(metrics, lakebase_values, dynamodb_values)):
    ax = axes[i]
    bars = ax.bar(['Lakebase', 'DynamoDB'], [lb_val, db_val], color=['#1f77b4', '#ff7f0e'], alpha=0.8)
    ax.set_ylabel('Latency (ms)', fontsize=11)
    ax.set_title(f'{metric} Latency', fontsize=12, fontweight='bold')
    ax.grid(axis='y', alpha=0.3)
    
    # Add value labels on bars
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.1f}ms',
                ha='center', va='bottom', fontsize=10)
    
    # Add SLA line for P99
    if metric == 'P99':
        ax.axhline(y=120, color='red', linestyle=':', linewidth=2, alpha=0.5, label='120ms SLA')
        ax.legend(fontsize=9)

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md ## Intelligent Analysis

# COMMAND ----------

print("\n" + "=" * 80)
print("INTELLIGENT ANALYSIS")
print("=" * 80)

# Performance Analysis
print("\n📊 PERFORMANCE BREAKDOWN")
print("-" * 80)

p99_diff = lakebase['p99_ms'] - dynamodb['p99_ms']
p99_diff_pct = (p99_diff / dynamodb['p99_ms']) * 100

if abs(p99_diff) < 5:
    perf_verdict = "🤝 Virtually identical"
    perf_detail = f"Less than 5ms difference ({abs(p99_diff):.2f}ms)"
elif p99_diff < 0:
    perf_verdict = "✅ Lakebase is faster"
    perf_detail = f"{abs(p99_diff):.2f}ms ({abs(p99_diff_pct):.1f}%) lower P99"
else:
    perf_verdict = "✅ DynamoDB is faster"
    perf_detail = f"{abs(p99_diff):.2f}ms ({abs(p99_diff_pct):.1f}%) lower P99"

print(f"P99 Latency: {perf_verdict}")
print(f"   {perf_detail}")
print(f"   Lakebase: {lakebase['p99_ms']:.2f}ms | DynamoDB: {dynamodb['p99_ms']:.2f}ms")

# Consistency Analysis
print(f"\n🔄 CONSISTENCY ANALYSIS")
print("-" * 80)

cv_diff = lakebase['cv'] - dynamodb['cv']
cv_diff_pct = (cv_diff / dynamodb['cv']) * 100 if dynamodb['cv'] > 0 else 0

if abs(cv_diff) < 0.05:
    consistency_verdict = "🤝 Both equally consistent"
elif cv_diff < 0:
    consistency_verdict = "✅ Lakebase is more predictable"
    consistency_detail = f"{abs(cv_diff_pct):.1f}% lower variance"
else:
    consistency_verdict = "✅ DynamoDB is more predictable"
    consistency_detail = f"{abs(cv_diff_pct):.1f}% lower variance"

print(f"{consistency_verdict}")
if abs(cv_diff) >= 0.05:
    print(f"   {consistency_detail}")
print(f"   Lakebase CV: {lakebase['cv']:.3f} | DynamoDB CV: {dynamodb['cv']:.3f}")

if lakebase['cv'] < 0.2 and dynamodb['cv'] < 0.2:
    print(f"   ✅ Both show excellent consistency (CV < 0.2)")
elif lakebase['cv'] < 0.3 and dynamodb['cv'] < 0.3:
    print(f"   ✅ Both show good consistency (CV < 0.3)")
else:
    print(f"   ⚠️  High variance detected - investigate outliers")

# Tail Latency Analysis
print(f"\n📈 TAIL LATENCY ANALYSIS")
print("-" * 80)

lakebase_tail_gap = lakebase['p99_ms'] - lakebase['p50_ms']
dynamodb_tail_gap = dynamodb['p99_ms'] - dynamodb['p50_ms']

print(f"P50 to P99 gap (smaller is better):")
print(f"   Lakebase: {lakebase_tail_gap:.2f}ms gap ({lakebase['p50_ms']:.2f} → {lakebase['p99_ms']:.2f}ms)")
print(f"   DynamoDB: {dynamodb_tail_gap:.2f}ms gap ({dynamodb['p50_ms']:.2f} → {dynamodb['p99_ms']:.2f}ms)")

if lakebase_tail_gap < dynamodb_tail_gap:
    tail_winner = "Lakebase"
    tail_diff = dynamodb_tail_gap - lakebase_tail_gap
    print(f"   ✅ {tail_winner} has {tail_diff:.2f}ms smaller tail - better worst-case performance")
else:
    tail_winner = "DynamoDB"
    tail_diff = lakebase_tail_gap - dynamodb_tail_gap
    print(f"   ✅ {tail_winner} has {tail_diff:.2f}ms smaller tail - better worst-case performance")

# SLA Compliance
print(f"\n✅ SLA COMPLIANCE (< 120ms P99)")
print("-" * 80)

lakebase_headroom = 120 - lakebase['p99_ms']
dynamodb_headroom = 120 - dynamodb['p99_ms']

print(f"Lakebase: {lakebase['p99_ms']:.2f}ms → {'✅ PASS' if lakebase['within_sla'] else '❌ FAIL'}")
if lakebase['within_sla']:
    print(f"   {lakebase_headroom:.2f}ms headroom ({lakebase_headroom/120*100:.1f}% buffer)")
else:
    print(f"   ⚠️  Exceeds SLA by {abs(lakebase_headroom):.2f}ms")

print(f"\nDynamoDB: {dynamodb['p99_ms']:.2f}ms → {'✅ PASS' if dynamodb['within_sla'] else '❌ FAIL'}")
if dynamodb['within_sla']:
    print(f"   {dynamodb_headroom:.2f}ms headroom ({dynamodb_headroom/120*100:.1f}% buffer)")
else:
    print(f"   ⚠️  Exceeds SLA by {abs(dynamodb_headroom):.2f}ms")

# COMMAND ----------

# MAGIC %md ## Final Recommendation

# COMMAND ----------

print("\n" + "=" * 80)
print("FINAL RECOMMENDATION")
print("=" * 80)

# Determine winner with scoring
lakebase_score = 0
dynamodb_score = 0
reasoning = []

# Score P99 (most important - 3 points)
if lakebase['p99_ms'] < dynamodb['p99_ms']:
    lakebase_score += 3
    reasoning.append(f"✅ Lakebase P99 is {abs(p99_diff):.2f}ms faster (+3 pts)")
elif dynamodb['p99_ms'] < lakebase['p99_ms']:
    dynamodb_score += 3
    reasoning.append(f"✅ DynamoDB P99 is {abs(p99_diff):.2f}ms faster (+3 pts)")

# Score P50 (2 points)
if lakebase['p50_ms'] < dynamodb['p50_ms']:
    lakebase_score += 2
    reasoning.append(f"✅ Lakebase P50 is lower (+2 pts)")
elif dynamodb['p50_ms'] < lakebase['p50_ms']:
    dynamodb_score += 2
    reasoning.append(f"✅ DynamoDB P50 is lower (+2 pts)")

# Score consistency (1 point)
if lakebase['cv'] < dynamodb['cv']:
    lakebase_score += 1
    reasoning.append(f"✅ Lakebase is more consistent (+1 pt)")
elif dynamodb['cv'] < lakebase['cv']:
    dynamodb_score += 1
    reasoning.append(f"✅ DynamoDB is more consistent (+1 pt)")

print(f"\n📊 SCORING BREAKDOWN")
print("-" * 80)
for reason in reasoning:
    print(f"   {reason}")

print(f"\n🏆 Final Score: Lakebase {lakebase_score} - DynamoDB {dynamodb_score}")
print("=" * 80)

# Final verdict
if lakebase_score > dynamodb_score:
    print(f"\n✅ RECOMMENDATION: **Use Lakebase**")
    print(f"\n   Key Advantages:")
    print(f"   • {lakebase['p99_ms']:.2f}ms P99 vs {dynamodb['p99_ms']:.2f}ms (DynamoDB)")
    print(f"   • {abs(p99_diff):.2f}ms ({abs(p99_diff_pct):.1f}%) faster at tail")
    print(f"   • {lakebase_headroom:.2f}ms SLA headroom")
    if lakebase['cv'] < dynamodb['cv']:
        print(f"   • More consistent performance")
    
    print(f"\n   Additional Benefits:")
    print(f"   • Unified data platform (same as analytics)")
    print(f"   • SQL interface (easier development)")
    print(f"   • No provisioned capacity management")
    print(f"   • Better for complex queries")
    
elif dynamodb_score > lakebase_score:
    print(f"\n⚠️  RECOMMENDATION: **DynamoDB still leads**")
    print(f"\n   Key Advantages:")
    print(f"   • {dynamodb['p99_ms']:.2f}ms P99 vs {lakebase['p99_ms']:.2f}ms (Lakebase)")
    print(f"   • {abs(p99_diff):.2f}ms ({abs(p99_diff_pct):.1f}%) faster at tail")
    print(f"   • {dynamodb_headroom:.2f}ms SLA headroom")
    if dynamodb['cv'] < lakebase['cv']:
        print(f"   • More consistent performance")
    
    print(f"\n   Lakebase Path Forward:")
    print(f"   • Consider read replicas for lower latency")
    print(f"   • Optimize stored procedure")
    print(f"   • Test with connection pooling tuning")
    print(f"   • Evaluate in same AWS region as application")
    
else:
    print(f"\n🤝 RECOMMENDATION: **Both are viable - consider other factors**")
    print(f"\n   Performance is virtually identical:")
    print(f"   • P99 difference: {abs(p99_diff):.2f}ms ({abs(p99_diff_pct):.1f}%)")
    print(f"   • Both meet SLA: {'✅' if lakebase['within_sla'] and dynamodb['within_sla'] else '⚠️'}")
    
    print(f"\n   Decision Factors:")
    print(f"   • **Cost**: Compare DynamoDB RCU costs vs Lakebase compute")
    print(f"   • **Operations**: DynamoDB is fully managed, Lakebase requires Databricks")
    print(f"   • **Flexibility**: Lakebase better for complex queries, DynamoDB for simple KV")
    print(f"   • **Integration**: Lakebase better if already using Databricks")

# SLA Check
print(f"\n{'='*80}")
if lakebase['within_sla'] and dynamodb['within_sla']:
    print(f"✅ Both systems meet the 120ms P99 SLA requirement")
    print(f"   Either option is technically viable for production")
elif lakebase['within_sla']:
    print(f"⚠️  Only Lakebase meets the 120ms SLA requirement")
    print(f"   DynamoDB must be optimized or ruled out")
elif dynamodb['within_sla']:
    print(f"⚠️  Only DynamoDB meets the 120ms SLA requirement")
    print(f"   Lakebase must be optimized or ruled out")
else:
    print(f"❌ Neither system meets the 120ms SLA requirement")
    print(f"   Further optimization is REQUIRED before production use")

print("=" * 80)

# COMMAND ----------



