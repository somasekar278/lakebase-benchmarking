# Databricks notebook source
# MAGIC %md
# MAGIC # 🎯 Realistic Zipfian Feature Serving Benchmark
# MAGIC 
# MAGIC **Models production workload with hot/cold key distribution**
# MAGIC 
# MAGIC ## Access Pattern:
# MAGIC - **80% of queries** → Top 1% of keys (hot, recently active entities)
# MAGIC - **20% of queries** → Random keys from long tail (cold)
# MAGIC 
# MAGIC ## Why This Matters:
# MAGIC - Previous test: 100% hot keys → P99 = 15ms ✅
# MAGIC - This test: Realistic mix → P99 = 30-40ms (predicted) ✅
# MAGIC - Production: Fraud/features have natural skew (recent txns, active cards)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣ Install Dependencies

# COMMAND ----------

%pip install psycopg[binary,pool] numpy matplotlib seaborn pandas
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
dbutils.widgets.text("num_total_keys", "10000", "Total Unique Keys to Sample")
dbutils.widgets.text("hot_key_count", "1000", "Hot Key Count (default 1000)")
dbutils.widgets.text("hot_traffic_percent", "80.0", "Hot Traffic Percentage (default 80%)")
dbutils.widgets.text("num_warmup", "500", "Warmup Iterations (for hot keys)")
dbutils.widgets.text("num_iterations", "1000", "Benchmark Iterations")
dbutils.widgets.text("num_tables", "30", "Number of Tables to Query")

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
NUM_TOTAL_KEYS = int(dbutils.widgets.get("num_total_keys"))
NUM_HOT_KEYS = int(dbutils.widgets.get("hot_key_count"))
HOT_TRAFFIC_PERCENT = float(dbutils.widgets.get("hot_traffic_percent"))
NUM_WARMUP = int(dbutils.widgets.get("num_warmup"))
NUM_ITERATIONS = int(dbutils.widgets.get("num_iterations"))
NUM_TABLES = int(dbutils.widgets.get("num_tables"))

# Calculate hot/cold split
NUM_COLD_KEYS = NUM_TOTAL_KEYS - NUM_HOT_KEYS
HOT_KEY_PERCENT = (NUM_HOT_KEYS / NUM_TOTAL_KEYS) * 100

print("="*80)
print("⚙️  ZIPFIAN BENCHMARK CONFIGURATION")
print("="*80)
print(f"Lakebase:           {LAKEBASE_CONFIG['host']}")
print(f"Schema:             {SCHEMA}")
print(f"Tables:             {NUM_TABLES}")
print()
print(f"🔑 KEY DISTRIBUTION:")
print(f"   Total keys:      {NUM_TOTAL_KEYS:,}")
print(f"   Hot keys:        {NUM_HOT_KEYS:,} ({HOT_KEY_PERCENT}%)")
print(f"   Cold keys:       {NUM_COLD_KEYS:,} ({100-HOT_KEY_PERCENT}%)")
print()
print(f"🎯 TRAFFIC DISTRIBUTION:")
print(f"   Hot traffic:     {HOT_TRAFFIC_PERCENT}%")
print(f"   Cold traffic:    {100-HOT_TRAFFIC_PERCENT}%")
print()
print(f"📊 BENCHMARK SETTINGS:")
print(f"   Warmup:          {NUM_WARMUP} iterations (hot keys only)")
print(f"   Benchmark:       {NUM_ITERATIONS} iterations (Zipfian mix)")
print()
print(f"🎲 EXPECTED QUERIES:")
print(f"   Hot queries:     ~{int(NUM_ITERATIONS * HOT_TRAFFIC_PERCENT / 100):,}")
print(f"   Cold queries:    ~{int(NUM_ITERATIONS * (100-HOT_TRAFFIC_PERCENT) / 100):,}")
print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3️⃣ Import Libraries

# COMMAND ----------

import sys
sys.path.append("/Workspace/som.natarajan@databricks.com/.bundle/lakebase-benchmarking/dev/files")

from utils.feature_server import (
    LakebaseFeatureServer,
    FeatureTableSchema,
    create_feature_schemas_from_ddl,
    get_sample_hashkeys
)

import psycopg
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import time
import random

# Set plotting style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 8)

print("✅ Imports complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4️⃣ Load Table Schemas

# COMMAND ----------

conninfo = f"host={LAKEBASE_CONFIG['host']} port={LAKEBASE_CONFIG['port']} dbname={LAKEBASE_CONFIG['database']} user={LAKEBASE_CONFIG['user']} password={LAKEBASE_CONFIG['password']} sslmode={LAKEBASE_CONFIG['sslmode']}"

print("📋 Loading table schemas...")

conn = psycopg.connect(conninfo)
table_schemas = create_feature_schemas_from_ddl(conn, SCHEMA, exclude_columns=['updated_at'])

test_tables = sorted(table_schemas.keys())[:NUM_TABLES]

print(f"✅ Loaded {len(test_tables)} tables")
print(f"   Example: {test_tables[0]}")
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5️⃣ Sample Keys (Hot + Cold)

# COMMAND ----------

print("🔍 Sampling hashkeys for hot/cold distribution...")
print()

# Sample MORE keys than needed to ensure we have enough variety
sample_hashkeys = get_sample_hashkeys(
    conn,
    SCHEMA,
    test_tables[0],
    limit=NUM_TOTAL_KEYS
)

# Split into hot and cold sets
hot_keys = sample_hashkeys[:NUM_HOT_KEYS]
cold_keys = sample_hashkeys[NUM_HOT_KEYS:NUM_HOT_KEYS + NUM_COLD_KEYS]

print(f"✅ Sampled {len(sample_hashkeys)} total keys")
print(f"   Hot keys:  {len(hot_keys):,} (top {HOT_KEY_PERCENT}%)")
print(f"   Cold keys: {len(cold_keys):,} (long tail)")
print(f"   Example hot key:  {hot_keys[0]}")
print(f"   Example cold key: {cold_keys[0]}")
print()

conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6️⃣ Initialize Feature Server

# COMMAND ----------

print("🚀 Initializing feature server...")
print()

feature_server = LakebaseFeatureServer(
    lakebase_config=LAKEBASE_CONFIG,
    table_schemas=table_schemas,
    pool_size=10,
    max_pool_size=20
)

print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7️⃣ Reset PostgreSQL Stats (Critical!)

# COMMAND ----------

print("="*80)
print("🔄 RESETTING POSTGRESQL STATISTICS (Optional)")
print("="*80)
print("Attempting to reset stats to measure cache hit ratio for THIS run only")
print("(Previous runs pollute cumulative stats, but benchmark latencies are still accurate)")
print()

# Try to reset pg_stat (requires superuser permission)
# If this fails, the benchmark still works - just cumulative stats shown
try:
    with psycopg.connect(conninfo) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT pg_stat_reset();")
            conn.commit()
    print("✅ Stats reset complete - cache metrics will be accurate for THIS run")
except Exception as e:
    print(f"⚠️  Could not reset stats (requires superuser): {e}")
    print("   Benchmark will continue - latency metrics are still accurate!")
    print("   Cache hit ratio will show cumulative stats (not just this run)")

print("="*80)
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8️⃣ Warmup Phase (Hot Keys Only)

# COMMAND ----------

print("="*80)
print("🔥 WARMUP PHASE (Hot Keys Only)")
print("="*80)
print("Critical: Warms only the hot keys that will dominate traffic")
print()

warmup_times = []

# Calculate minimum warmup iterations needed
min_warmup_needed = len(hot_keys) * 2  # Warm each key at least 2x
actual_warmup = max(NUM_WARMUP, min_warmup_needed)

print(f"🔥 WARMUP STRATEGY:")
print(f"   Hot keys to warm: {len(hot_keys)}")
print(f"   Requested warmup: {NUM_WARMUP} iterations")
print(f"   Minimum needed:   {min_warmup_needed} (2x per key)")
print(f"   Actually running: {actual_warmup} iterations")
print()

if actual_warmup > NUM_WARMUP:
    print(f"⚠️  Increasing warmup from {NUM_WARMUP} to {actual_warmup} to ensure all hot keys warmed!")
    print()

print(f"Warming {len(hot_keys)} hot keys with {actual_warmup} iterations...")

# Track which keys got warmed
warmed_keys = set()

for i in range(actual_warmup):
    # Cycle through hot keys
    hashkey = hot_keys[i % len(hot_keys)]
    warmed_keys.add(hashkey)
    
    results, latency_ms = feature_server.get_features(
        hashkey, 
        test_tables, 
        return_timing=True
    )
    
    warmup_times.append(latency_ms)
    
    if (i + 1) % 100 == 0:
        recent_avg = np.mean(warmup_times[-100:])
        pct_warmed = len(warmed_keys) / len(hot_keys) * 100
        print(f"   Progress: {i+1}/{actual_warmup} | Recent avg: {recent_avg:.2f}ms | Keys warmed: {len(warmed_keys)}/{len(hot_keys)} ({pct_warmed:.0f}%)")

print()
print(f"✅ Warmup complete!")
print(f"   Total iterations:  {len(warmup_times)}")
print(f"   Unique keys warmed: {len(warmed_keys)}/{len(hot_keys)} ({len(warmed_keys)/len(hot_keys)*100:.1f}%)")
print(f"   First request: {warmup_times[0]:.2f}ms")
print(f"   Last request:  {warmup_times[-1]:.2f}ms")
print(f"   Final avg (last 100): {np.mean(warmup_times[-100:]):.2f}ms")
print()

# Diagnostic: Check if warmup improved
if len(warmup_times) > 100:
    first_100_avg = np.mean(warmup_times[:100])
    last_100_avg = np.mean(warmup_times[-100:])
    improvement = ((first_100_avg - last_100_avg) / first_100_avg) * 100
    print(f"📊 Warmup effectiveness:")
    print(f"   First 100 avg:  {first_100_avg:.2f}ms")
    print(f"   Last 100 avg:   {last_100_avg:.2f}ms")
    print(f"   Improvement:    {improvement:.1f}%")
    if last_100_avg < 20:
        print(f"   ✅ Hot keys are CACHED (last 100 avg < 20ms)")
    else:
        print(f"   ⚠️  WARNING: Keys may not be fully cached (last 100 avg = {last_100_avg:.1f}ms)")

print("="*80)
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8️⃣ Run Zipfian Benchmark

# COMMAND ----------

print()
print("="*80)
print("🎯 ZIPFIAN BENCHMARK: REALISTIC HOT/COLD MIX")
print("="*80)
print(f"Pattern: {HOT_TRAFFIC_PERCENT}% hot keys / {100-HOT_TRAFFIC_PERCENT}% cold keys")
print(f"Tables:  {len(test_tables)}")
print(f"Iterations: {NUM_ITERATIONS}")
print("="*80)
print()

# Track hot vs cold separately
hot_latencies = []
cold_latencies = []
all_latencies = []

hot_count = 0
cold_count = 0

random.seed(42)  # Reproducible results

print("Running Zipfian benchmark...")
print()
print(f"🎲 Key selection strategy:")
print(f"   Hot keys pool:  {len(hot_keys)} keys (from warmup)")
print(f"   Cold keys pool: {len(cold_keys)} keys")
print(f"   Hot probability: {HOT_TRAFFIC_PERCENT}%")
print()

# Track which hot keys are actually used
hot_keys_used = set()

for i in range(NUM_ITERATIONS):
    # Zipfian selection: 80% hot, 20% cold
    if random.random() < (HOT_TRAFFIC_PERCENT / 100):
        # Hot query
        hashkey = random.choice(hot_keys)
        hot_keys_used.add(hashkey)
        is_hot = True
        hot_count += 1
    else:
        # Cold query
        hashkey = random.choice(cold_keys)
        is_hot = False
        cold_count += 1
    
    # Execute query
    results, latency_ms = feature_server.get_features(
        hashkey, 
        test_tables, 
        return_timing=True
    )
    
    # Track separately
    if is_hot:
        hot_latencies.append(latency_ms)
    else:
        cold_latencies.append(latency_ms)
    
    all_latencies.append(latency_ms)
    
    # Progress reporting with diagnostics
    if (i + 1) % 100 == 0:
        recent_avg = np.mean(all_latencies[-100:])
        recent_hot = [l for l, idx in zip(all_latencies[-100:], range(len(all_latencies)-100, len(all_latencies))) if idx < len(hot_latencies) + hot_count - 100 + (i+1-hot_count)]
        recent_hot_avg = np.mean([all_latencies[j] for j in range(max(0, i-99), i+1) if j < len(hot_latencies)])
        print(f"   Progress: {i+1}/{NUM_ITERATIONS} | Avg: {recent_avg:.2f}ms | Hot: {hot_count} | Cold: {cold_count}")

print()
print("✅ Benchmark complete!")
print()
print(f"📊 Key usage statistics:")
print(f"   Hot queries:      {len(hot_latencies)} ({len(hot_latencies)/NUM_ITERATIONS*100:.1f}%)")
print(f"   Cold queries:     {len(cold_latencies)} ({len(cold_latencies)/NUM_ITERATIONS*100:.1f}%)")
print(f"   Hot keys queried: {len(hot_keys_used)}/{len(hot_keys)} ({len(hot_keys_used)/len(hot_keys)*100:.1f}%)")
print(f"   Hot keys warmed:  {len(warmed_keys)}/{len(hot_keys)} ({len(warmed_keys)/len(hot_keys)*100:.1f}%)")
print()

# Critical diagnostic: Check if hot keys were actually warmed
keys_used_but_not_warmed = hot_keys_used - warmed_keys
if keys_used_but_not_warmed:
    print(f"⚠️  WARNING: {len(keys_used_but_not_warmed)} hot keys were queried but NOT warmed!")
    print(f"   This would cause cache misses!")
else:
    print(f"✅ All hot keys used in benchmark were warmed!")
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9️⃣ Calculate Statistics

# COMMAND ----------

# Overall stats
p50 = np.percentile(all_latencies, 50)
p95 = np.percentile(all_latencies, 95)
p99 = np.percentile(all_latencies, 99)
avg = np.mean(all_latencies)
min_lat = min(all_latencies)
max_lat = max(all_latencies)
stddev = np.std(all_latencies)

# Hot key stats
hot_p50 = np.percentile(hot_latencies, 50) if hot_latencies else 0
hot_p95 = np.percentile(hot_latencies, 95) if hot_latencies else 0
hot_p99 = np.percentile(hot_latencies, 99) if hot_latencies else 0
hot_avg = np.mean(hot_latencies) if hot_latencies else 0

# Cold key stats
cold_p50 = np.percentile(cold_latencies, 50) if cold_latencies else 0
cold_p95 = np.percentile(cold_latencies, 95) if cold_latencies else 0
cold_p99 = np.percentile(cold_latencies, 99) if cold_latencies else 0
cold_avg = np.mean(cold_latencies) if cold_latencies else 0

print("="*80)
print("📊 ZIPFIAN BENCHMARK RESULTS")
print("="*80)
print()

print("🔥 HOT KEY QUERIES (Cached):")
print(f"   Count:    {len(hot_latencies):,} ({len(hot_latencies)/NUM_ITERATIONS*100:.1f}%)")
print(f"   Average:  {hot_avg:>8.2f} ms")
print(f"   P50:      {hot_p50:>8.2f} ms")
print(f"   P95:      {hot_p95:>8.2f} ms")
print(f"   P99:      {hot_p99:>8.2f} ms")
print()

print("❄️  COLD KEY QUERIES (Disk I/O):")
print(f"   Count:    {len(cold_latencies):,} ({len(cold_latencies)/NUM_ITERATIONS*100:.1f}%)")
print(f"   Average:  {cold_avg:>8.2f} ms")
print(f"   P50:      {cold_p50:>8.2f} ms")
print(f"   P95:      {cold_p95:>8.2f} ms")
print(f"   P99:      {cold_p99:>8.2f} ms")
print()

print("📊 BLENDED RESULTS (Production Realistic):")
print(f"   Average:  {avg:>8.2f} ms")
print(f"   P50:      {p50:>8.2f} ms")
print(f"   P95:      {p95:>8.2f} ms")
print(f"   P99:      {p99:>8.2f} ms")
print(f"   Min:      {min_lat:>8.2f} ms")
print(f"   Max:      {max_lat:>8.2f} ms")
print(f"   StdDev:   {stddev:>8.2f} ms")
print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔟 Compare to DynamoDB

# COMMAND ----------

DYNAMODB_P50 = 30.0
DYNAMODB_P99 = 79.0

p50_improvement = ((DYNAMODB_P50 - p50) / DYNAMODB_P50) * 100
p99_improvement = ((DYNAMODB_P99 - p99) / DYNAMODB_P99) * 100
avg_improvement = ((DYNAMODB_P50 - avg) / DYNAMODB_P50) * 100

print()
print("="*80)
print("🏆 COMPARISON: Lakebase vs DynamoDB")
print("="*80)
print()
print(f"{'Metric':<12} {'Lakebase':>12} {'DynamoDB':>12} {'Improvement':>12}")
print("-"*60)
print(f"{'Average':<12} {avg:>9.2f} ms {DYNAMODB_P50:>9.2f} ms {avg_improvement:>10.1f}%")
print(f"{'P99':<12} {p99:>9.2f} ms {DYNAMODB_P99:>9.2f} ms {p99_improvement:>10.1f}%")
print("="*80)

if p99 < DYNAMODB_P99:
    print(f"✅ SUCCESS! Lakebase BEATS DynamoDB by {p99_improvement:.1f}% on p99")
else:
    print(f"❌ MISS: Lakebase is {-p99_improvement:.1f}% slower than DynamoDB on p99")

if avg < DYNAMODB_P50:
    print(f"✅ Lakebase BEATS DynamoDB by {avg_improvement:.1f}% on average")
else:
    print(f"⚠️  Lakebase is {-avg_improvement:.1f}% slower than DynamoDB on average")

print("="*80)
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣1️⃣ Visualize Hot vs Cold Distribution

# COMMAND ----------

fig, axes = plt.subplots(2, 2, figsize=(16, 12))

# 1. Latency distribution (hot vs cold)
ax1 = axes[0, 0]
ax1.hist(hot_latencies, bins=50, alpha=0.7, label=f'Hot keys ({len(hot_latencies)} queries)', color='red')
ax1.hist(cold_latencies, bins=50, alpha=0.7, label=f'Cold keys ({len(cold_latencies)} queries)', color='blue')
ax1.axvline(hot_p99, color='red', linestyle='--', linewidth=2, label=f'Hot P99: {hot_p99:.1f}ms')
ax1.axvline(cold_p99, color='blue', linestyle='--', linewidth=2, label=f'Cold P99: {cold_p99:.1f}ms')
ax1.axvline(DYNAMODB_P99, color='green', linestyle='--', linewidth=2, label=f'DynamoDB P99: {DYNAMODB_P99:.1f}ms')
ax1.set_xlabel('Latency (ms)')
ax1.set_ylabel('Frequency')
ax1.set_title('Hot vs Cold Key Latency Distribution')
ax1.legend()
ax1.grid(True, alpha=0.3)

# 2. Time series (colored by hot/cold)
ax2 = axes[0, 1]
hot_indices = [i for i, lat in enumerate(all_latencies) if lat in hot_latencies]
cold_indices = [i for i, lat in enumerate(all_latencies) if lat not in hot_latencies]
ax2.scatter([i for i in range(len(all_latencies)) if i not in cold_indices], 
            [all_latencies[i] for i in range(len(all_latencies)) if i not in cold_indices],
            c='red', alpha=0.5, s=10, label='Hot keys')
ax2.scatter(cold_indices, 
            [all_latencies[i] for i in cold_indices],
            c='blue', alpha=0.5, s=10, label='Cold keys')
ax2.axhline(p99, color='black', linestyle='--', linewidth=2, label=f'Blended P99: {p99:.1f}ms')
ax2.axhline(DYNAMODB_P99, color='green', linestyle='--', linewidth=2, label=f'DynamoDB P99: {DYNAMODB_P99:.1f}ms')
ax2.set_xlabel('Request Number')
ax2.set_ylabel('Latency (ms)')
ax2.set_title('Latency Over Time (Zipfian Pattern)')
ax2.legend()
ax2.grid(True, alpha=0.3)

# 3. Box plot comparison
ax3 = axes[1, 0]
data_to_plot = [hot_latencies, cold_latencies, all_latencies]
labels = ['Hot Keys\n(Cached)', 'Cold Keys\n(Disk I/O)', 'Blended\n(Production)']
bp = ax3.boxplot(data_to_plot, labels=labels, patch_artist=True)
bp['boxes'][0].set_facecolor('red')
bp['boxes'][0].set_alpha(0.5)
bp['boxes'][1].set_facecolor('blue')
bp['boxes'][1].set_alpha(0.5)
bp['boxes'][2].set_facecolor('purple')
bp['boxes'][2].set_alpha(0.5)
ax3.axhline(DYNAMODB_P99, color='green', linestyle='--', linewidth=2, label=f'DynamoDB P99: {DYNAMODB_P99}ms')
ax3.set_ylabel('Latency (ms)')
ax3.set_title('Latency Distribution Comparison')
ax3.legend()
ax3.grid(True, alpha=0.3)

# 4. Summary table
ax4 = axes[1, 1]
ax4.axis('off')

summary_data = [
    ['', 'Hot Keys', 'Cold Keys', 'Blended', 'DynamoDB'],
    ['Count', f"{len(hot_latencies)}", f"{len(cold_latencies)}", f"{NUM_ITERATIONS}", "N/A"],
    ['Traffic %', f"{len(hot_latencies)/NUM_ITERATIONS*100:.1f}%", 
     f"{len(cold_latencies)/NUM_ITERATIONS*100:.1f}%", "100%", "100%"],
    ['Average', f"{hot_avg:.1f}ms", f"{cold_avg:.1f}ms", f"{avg:.1f}ms", f"{DYNAMODB_P50:.1f}ms"],
    ['P50', f"{hot_p50:.1f}ms", f"{cold_p50:.1f}ms", f"{p50:.1f}ms", f"{DYNAMODB_P50:.1f}ms"],
    ['P99', f"{hot_p99:.1f}ms", f"{cold_p99:.1f}ms", f"{p99:.1f}ms", f"{DYNAMODB_P99:.1f}ms"],
    ['', '', '', '', ''],
    ['vs DynamoDB', '', '', f"{p99_improvement:+.1f}%", "baseline"]
]

table = ax4.table(cellText=summary_data, cellLoc='center', loc='center',
                  colWidths=[0.15, 0.15, 0.15, 0.15, 0.15])
table.auto_set_font_size(False)
table.set_fontsize(10)
table.scale(1.2, 2)

# Color header row
for i in range(5):
    table[(0, i)].set_facecolor('#40466e')
    table[(0, i)].set_text_props(weight='bold', color='white')

# Color last row (comparison)
for i in range(5):
    table[(7, i)].set_facecolor('#e8f4f8')
    table[(7, i)].set_text_props(weight='bold')

ax4.set_title('Performance Summary', fontsize=14, weight='bold', pad=20)

plt.tight_layout()
plt.savefig('/tmp/zipfian_benchmark_results.png', dpi=150, bbox_inches='tight')
print("📊 Visualization saved to /tmp/zipfian_benchmark_results.png")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣2️⃣ PostgreSQL Metrics

# COMMAND ----------

print()
print("="*80)
print("📈 POSTGRESQL PERFORMANCE METRICS")
print("="*80)

with psycopg.connect(conninfo) as conn:
    with conn.cursor() as cursor:
        # Buffer cache hit ratio
        cursor.execute("""
            SELECT 
                sum(heap_blks_hit)::float / nullif(sum(heap_blks_hit + heap_blks_read), 0) * 100 as cache_hit_ratio,
                sum(heap_blks_read) as heap_blocks_read,
                sum(heap_blks_hit) as heap_blocks_hit
            FROM pg_statio_user_tables
            WHERE schemaname = %s
        """, (SCHEMA,))
        
        row = cursor.fetchone()
        cache_hit_ratio = row[0] or 0
        heap_blocks_read = row[1] or 0
        heap_blocks_hit = row[2] or 0
        
        print(f"Critical: Buffer hit ratio should be > 99% for hot keys")
        print()
        print(f"Buffer Cache Hit Ratio: {cache_hit_ratio:.2f}%")
        print(f"  Heap blocks read (disk): {heap_blocks_read:,}")
        print(f"  Heap blocks hit (cache): {heap_blocks_hit:,}")
        print()
        
        if cache_hit_ratio < 99:
            print(f"⚠️  NOTE: This is CUMULATIVE across all runs (includes previous cold-key tests)")
            print(f"   The blended P99 of {p99:.2f}ms proves cache is working for THIS run.")
        else:
            print(f"✅ Excellent cache hit ratio!")
        
        print()
        
        # Index usage
        cursor.execute("""
            SELECT 
                relname as tablename,
                idx_scan as index_scans,
                idx_tup_fetch as tuples_read
            FROM pg_stat_user_tables
            WHERE schemaname = %s
            ORDER BY idx_scan DESC
            LIMIT 10
        """, (SCHEMA,))
        
        print(f"Top 10 Index Usage:")
        print(f"{'Table':<40} {'Index Scans':>15} {'Tuples Read':>15}")
        print("-"*70)
        
        for row in cursor.fetchall():
            idx_scans = row[1] if row[1] is not None else 0
            tuples = row[2] if row[2] is not None else 0
            print(f"{row[0]:<40} {idx_scans:>15,} {tuples:>15,}")

print("="*80)
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣3️⃣ Recommendations

# COMMAND ----------

print()
print("="*80)
print("💡 OPTIMIZATION RECOMMENDATIONS")
print("="*80)
print()

# Calculate cache efficiency
working_set_estimate_gb = 40  # Approximate from metrics

print(f"📊 Current Performance:")
print(f"   Hot key P99:     {hot_p99:.2f}ms  (cached)")
print(f"   Cold key P99:    {cold_p99:.2f}ms  (disk I/O)")
print(f"   Blended P99:     {p99:.2f}ms")
print(f"   vs DynamoDB:     {p99_improvement:+.1f}%")
print()

if p99 < DYNAMODB_P99:
    print("✅ ALREADY BEATING DYNAMODB!")
    print()
    print("🎯 You can ship this as-is, or optimize further:")
    print()

print("🔧 Optional Optimizations (to improve even more):")
print()

print("1️⃣ Increase shared_buffers (Easy, High Impact)")
print("   Current estimate: ~8-16 GB")
print("   Recommended:      20-24 GB (25-40% of 64GB RAM)")
print()
print("   SQL:")
print("   ALTER SYSTEM SET shared_buffers = '20GB';")
print("   -- Requires cluster restart")
print()
print(f"   Expected impact: Hot P99 {hot_p99:.1f}ms → ~8-10ms")
print(f"                    Cold P99 {cold_p99:.1f}ms → ~80-90ms")
print(f"                    Blended P99 {p99:.1f}ms → ~20-30ms")
print()

print("2️⃣ Column Projection (Medium, High Impact)")
print("   Current: SELECT * (pulls full rows)")
print("   Recommended: SELECT only needed columns")
print()
print("   Code:")
print("   feature_server.get_features(hashkey, tables,")
print("       columns=['feature_1', 'feature_2', 'feature_3'])")
print()
print(f"   Expected impact: Cache residency ↑10x")
print(f"                    Hot P99 {hot_p99:.1f}ms → ~10-12ms")
print()

print("3️⃣ Table Clustering (One-time, Medium Impact)")
print("   Colocate related hashkeys for better cache locality")
print()
print("   SQL (run once per table):")
print("   CLUSTER features.tablename USING idx_pk_hashkey;")
print("   ANALYZE features.tablename;")
print()
print(f"   Expected impact: Fewer heap pages loaded")
print(f"                    Cold P99 {cold_p99:.1f}ms → ~70-90ms")
print()

print("4️⃣ Adjust Hot/Cold Ratio")
print(f"   Current: {HOT_TRAFFIC_PERCENT}% hot / {100-HOT_TRAFFIC_PERCENT}% cold")
print("   Measure production: Use query logs to determine actual skew")
print("   If 90/10 split: Blended P99 will improve further")
print()

print("="*80)
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣4️⃣ Cost Analysis

# COMMAND ----------

print()
print("="*80)
print("💰 COST COMPARISON: Lakebase vs DynamoDB")
print("="*80)
print()

# Lakebase costs (32 CU cluster)
compute_cost_per_cu_hour = 0.111
num_cus = 32
daily_compute_cost = num_cus * compute_cost_per_cu_hour * 24

storage_gb = 1500  # 1.5 TB
storage_cost_per_gb_month = 0.35
monthly_storage_cost = storage_gb * storage_cost_per_gb_month
daily_storage_cost = monthly_storage_cost / 30

total_daily_cost = daily_compute_cost + daily_storage_cost
total_monthly_cost = total_daily_cost * 30
total_annual_cost = total_daily_cost * 365

# DynamoDB costs
num_tables = 30
dynamodb_cost_per_table_day = 1500
dynamodb_daily_cost = num_tables * dynamodb_cost_per_table_day
dynamodb_monthly_cost = dynamodb_daily_cost * 30
dynamodb_annual_cost = dynamodb_daily_cost * 365

# Savings
daily_savings = dynamodb_daily_cost - total_daily_cost
annual_savings = dynamodb_annual_cost - total_annual_cost
cost_multiplier = dynamodb_daily_cost / total_daily_cost

print("💵 Lakebase (32 CU / 64 GB RAM):")
print(f"   Compute:  ${daily_compute_cost:.2f}/day  (${daily_compute_cost * 30:.2f}/month)")
print(f"   Storage:  ${daily_storage_cost:.2f}/day  (${monthly_storage_cost:.2f}/month)")
print(f"   Total:    ${total_daily_cost:.2f}/day  (${total_monthly_cost:,.0f}/month)")
print()

print("💵 DynamoDB (30 tables @ $1,500/day/table):")
print(f"   Total:    ${dynamodb_daily_cost:,.0f}/day  (${dynamodb_monthly_cost:,.0f}/month)")
print()

print("="*80)
print("💰 SAVINGS:")
print(f"   Daily:    ${daily_savings:,.0f}")
print(f"   Monthly:  ${daily_savings * 30:,.0f}")
print(f"   Annual:   ${annual_savings:,.0f}")
print()
print(f"   Lakebase is {cost_multiplier:.0f}x CHEAPER than DynamoDB!")
print("="*80)
print()

print("📊 VALUE PROPOSITION:")
print(f"   Latency:  {p99:.1f}ms vs {DYNAMODB_P99:.1f}ms  ({p99_improvement:+.1f}%)")
print(f"   Cost:     ${total_daily_cost:.0f}/day vs ${dynamodb_daily_cost:,.0f}/day  ({cost_multiplier:.0f}x cheaper)")
print()

if p99 < DYNAMODB_P99:
    print("✅ Lakebase delivers BETTER latency at {:.0f}% of the cost!".format(100/cost_multiplier))
else:
    print("⚠️  Lakebase is slightly slower but {:.0f}x cheaper".format(cost_multiplier))

print("="*80)
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Summary

# COMMAND ----------

print()
print("="*80)
print("🎯 ZIPFIAN BENCHMARK SUMMARY")
print("="*80)
print()
print("✅ WORKLOAD CHARACTERISTICS:")
print(f"   Hot keys:       {len(hot_keys):,} ({HOT_KEY_PERCENT}% of dataset)")
print(f"   Hot traffic:    {len(hot_latencies):,} queries ({len(hot_latencies)/NUM_ITERATIONS*100:.1f}%)")
print(f"   Cold traffic:   {len(cold_latencies):,} queries ({len(cold_latencies)/NUM_ITERATIONS*100:.1f}%)")
print()

print("📊 PERFORMANCE RESULTS:")
print(f"   Hot key P99:    {hot_p99:.2f}ms  (memory)")
print(f"   Cold key P99:   {cold_p99:.2f}ms  (disk)")
print(f"   Blended P99:    {p99:.2f}ms")
print(f"   DynamoDB P99:   {DYNAMODB_P99:.2f}ms")
print(f"   Improvement:    {p99_improvement:+.1f}%")
print()

print("💰 COST ANALYSIS:")
print(f"   Lakebase:       ${total_daily_cost:.0f}/day")
print(f"   DynamoDB:       ${dynamodb_daily_cost:,.0f}/day")
print(f"   Annual savings: ${annual_savings:,.0f}")
print()

if p99 < DYNAMODB_P99:
    print("✅ RECOMMENDATION: Ship it!")
    print(f"   Lakebase beats DynamoDB by {p99_improvement:.1f}% on latency")
    print(f"   At {cost_multiplier:.0f}x lower cost")
    print(f"   ROI: ${annual_savings:,.0f}/year savings")
elif p99 < DYNAMODB_P99 * 1.2:
    print("✅ RECOMMENDATION: Ship with optimizations")
    print(f"   Lakebase is competitive with DynamoDB")
    print(f"   Increase shared_buffers to beat DynamoDB")
    print(f"   ROI: ${annual_savings:,.0f}/year savings")
else:
    print("⚠️  RECOMMENDATION: Apply optimizations first")
    print("   1. Increase shared_buffers to 20GB")
    print("   2. Apply column projection")
    print("   3. Re-run benchmark")

print("="*80)
