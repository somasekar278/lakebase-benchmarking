# Databricks notebook source
# MAGIC %md
# MAGIC # 🔍 Diagnose Tail Amplification Spike
# MAGIC 
# MAGIC Investigates why the tail amplification chart shows a spike in the hot zone.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("run_id", "latest", "Run ID")

import psycopg
import pandas as pd
import numpy as np

LAKEBASE_CONFIG = {
    "host": "ep-gentle-silence-d346r2cf.database.eu-west-1.cloud.databricks.com",
    "port": 5432,
    "dbname": "benchmark",
    "user": "fraud_benchmark_user",
    "password": "fraud-benchmark-user123!",
    "sslmode": "require",
}

RUN_ID = dbutils.widgets.get("run_id")

# Resolve 'latest' to actual RUN_ID
conn = psycopg.connect(**LAKEBASE_CONFIG)
if RUN_ID == "latest":
    run_query = """
        SELECT run_id 
        FROM features.zipfian_feature_serving_results_v5 
        ORDER BY run_timestamp DESC 
        LIMIT 1
    """
    RUN_ID = pd.read_sql(run_query, conn)['run_id'][0]
    print(f"📊 Resolved 'latest' to run_id: {RUN_ID}")
else:
    print(f"📊 Using run_id: {RUN_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Sanity Check: Correct Probability Computation

# COMMAND ----------

print("=" * 80)
print("1️⃣ SANITY CHECK: P(request has ≥1 slow query) by hot_traffic_pct")
print("=" * 80)

sanity_query = f"""
WITH base AS (
  SELECT hot_traffic_pct, request_id
  FROM features.zipfian_request_timing
  WHERE run_id = '{RUN_ID}'
    AND mode = 'serial'
),
slow AS (
  SELECT hot_traffic_pct, request_id
  FROM features.zipfian_slow_query_log
  WHERE run_id = '{RUN_ID}'
    AND mode = 'serial'
    AND query_latency_ms > 100
)
SELECT
  b.hot_traffic_pct,
  COUNT(*) AS total_requests,
  COUNT(DISTINCT s.request_id) AS requests_with_slow,
  100.0 * COUNT(DISTINCT s.request_id) / NULLIF(COUNT(*),0) AS p_request_has_slow_pct
FROM base b
LEFT JOIN slow s
  ON s.hot_traffic_pct = b.hot_traffic_pct
 AND s.request_id = b.request_id
GROUP BY 1
ORDER BY 1 DESC
"""

sanity_df = pd.read_sql(sanity_query, conn)
print("\n📊 Results:")
print(sanity_df.to_string(index=False))

# Identify spike
spike_pct = sanity_df.loc[sanity_df['p_request_has_slow_pct'].idxmax(), 'hot_traffic_pct']
print(f"\n⚠️ SPIKE detected at hot_traffic_pct = {spike_pct}%")
print(f"   P(≥1 slow) = {sanity_df[sanity_df['hot_traffic_pct'] == spike_pct]['p_request_has_slow_pct'].values[0]:.1f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Detailed Analysis of Spike Bucket

# COMMAND ----------

print("=" * 80)
print(f"2️⃣ DETAILED ANALYSIS: hot_traffic_pct = {spike_pct}%")
print("=" * 80)

# Get affected requests stats
affected_query = f"""
SELECT 
    COUNT(DISTINCT request_id) as affected_requests,
    COUNT(*) as total_slow_queries,
    COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT request_id), 0) as avg_slow_per_request,
    MIN(query_latency_ms) as min_latency_ms,
    AVG(query_latency_ms) as avg_latency_ms,
    MAX(query_latency_ms) as max_latency_ms
FROM features.zipfian_slow_query_log
WHERE run_id = '{RUN_ID}'
  AND mode = 'serial'
  AND hot_traffic_pct = {spike_pct}
  AND query_latency_ms > 100
"""

affected_df = pd.read_sql(affected_query, conn)
print("\n📊 Affected Requests Summary:")
print(affected_df.to_string(index=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Top Offender Tables

# COMMAND ----------

print("=" * 80)
print(f"3️⃣ TOP OFFENDER TABLES: hot_traffic_pct = {spike_pct}%")
print("=" * 80)

offender_query = f"""
SELECT 
    table_name, 
    COUNT(*) AS slow_hits, 
    AVG(query_latency_ms) avg_ms, 
    MAX(query_latency_ms) max_ms,
    COUNT(DISTINCT hash_key) as distinct_keys,
    COUNT(DISTINCT request_id) as distinct_requests
FROM features.zipfian_slow_query_log
WHERE run_id = '{RUN_ID}'
  AND mode = 'serial'
  AND hot_traffic_pct = {spike_pct}
  AND query_latency_ms > 100
GROUP BY 1
ORDER BY slow_hits DESC
LIMIT 10
"""

offender_df = pd.read_sql(offender_query, conn)
print("\n📊 Top 10 Tables:")
print(offender_df.to_string(index=False))

# Check if dominated by one table
if len(offender_df) > 0:
    top_table = offender_df.iloc[0]
    total_slow = offender_df['slow_hits'].sum()
    dominance = (top_table['slow_hits'] / total_slow * 100) if total_slow > 0 else 0
    print(f"\n⚠️ Top table '{top_table['table_name']}' accounts for {dominance:.1f}% of slow queries")
    
    if dominance > 50:
        print("   → LIKELY: Single hotspot table")
    else:
        print("   → Distributed across multiple tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Top Hash Keys (Hotspot Detection)

# COMMAND ----------

print("=" * 80)
print(f"4️⃣ TOP HASH KEYS: hot_traffic_pct = {spike_pct}%")
print("=" * 80)

hashkey_query = f"""
SELECT 
    hash_key,
    table_name,
    COUNT(*) AS slow_hits, 
    AVG(query_latency_ms) avg_ms, 
    MAX(query_latency_ms) max_ms,
    COUNT(DISTINCT request_id) as distinct_requests
FROM features.zipfian_slow_query_log
WHERE run_id = '{RUN_ID}'
  AND mode = 'serial'
  AND hot_traffic_pct = {spike_pct}
  AND query_latency_ms > 100
GROUP BY 1, 2
ORDER BY slow_hits DESC
LIMIT 10
"""

hashkey_df = pd.read_sql(hashkey_query, conn)
print("\n📊 Top 10 Hash Keys:")
print(hashkey_df.to_string(index=False))

# Check for key repetition
if len(hashkey_df) > 0:
    top_key = hashkey_df.iloc[0]
    print(f"\n⚠️ Top hash_key: {top_key['hash_key']}")
    print(f"   Table: {top_key['table_name']}")
    print(f"   Slow hits: {top_key['slow_hits']}")
    print(f"   Avg latency: {top_key['avg_ms']:.1f}ms")
    
    if top_key['slow_hits'] > 10:
        print("   → LIKELY: Genuine hotspot contention (same key repeatedly slow)")
    else:
        print("   → Keys distributed (not a single-key hotspot)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Compare to Other Buckets

# COMMAND ----------

print("=" * 80)
print("5️⃣ COMPARISON ACROSS ALL HOT_TRAFFIC_PCT BUCKETS")
print("=" * 80)

comparison_query = f"""
WITH slow_counts AS (
    SELECT 
        hot_traffic_pct,
        COUNT(*) as total_slow_queries,
        COUNT(DISTINCT request_id) as affected_requests,
        COUNT(DISTINCT table_name) as affected_tables,
        AVG(query_latency_ms) as avg_latency_ms
    FROM features.zipfian_slow_query_log
    WHERE run_id = '{RUN_ID}'
      AND mode = 'serial'
      AND query_latency_ms > 100
    GROUP BY 1
),
total_counts AS (
    SELECT 
        hot_traffic_pct,
        COUNT(DISTINCT request_id) as total_requests
    FROM features.zipfian_request_timing
    WHERE run_id = '{RUN_ID}'
      AND mode = 'serial'
    GROUP BY 1
)
SELECT 
    t.hot_traffic_pct,
    t.total_requests,
    COALESCE(s.total_slow_queries, 0) as total_slow_queries,
    COALESCE(s.affected_requests, 0) as affected_requests,
    COALESCE(s.affected_tables, 0) as affected_tables,
    COALESCE(s.avg_latency_ms, 0) as avg_latency_ms,
    100.0 * COALESCE(s.affected_requests, 0) / NULLIF(t.total_requests, 0) as p_request_has_slow_pct
FROM total_counts t
LEFT JOIN slow_counts s ON t.hot_traffic_pct = s.hot_traffic_pct
ORDER BY t.hot_traffic_pct DESC
"""

comparison_df = pd.read_sql(comparison_query, conn)
print("\n📊 Full Comparison:")
print(comparison_df.to_string(index=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Timing Pattern Analysis

# COMMAND ----------

print("=" * 80)
print(f"6️⃣ TIMING PATTERN ANALYSIS: hot_traffic_pct = {spike_pct}%")
print("=" * 80)

# Check iteration pattern - are slow queries clustered in time?
timing_query = f"""
SELECT 
    iteration,
    COUNT(*) as slow_queries,
    COUNT(DISTINCT request_id) as affected_requests,
    AVG(query_latency_ms) as avg_latency_ms,
    MAX(query_latency_ms) as max_latency_ms
FROM features.zipfian_slow_query_log
WHERE run_id = '{RUN_ID}'
  AND mode = 'serial'
  AND hot_traffic_pct = {spike_pct}
  AND query_latency_ms > 100
GROUP BY 1
ORDER BY slow_queries DESC
LIMIT 20
"""

timing_df = pd.read_sql(timing_query, conn)
print("\n📊 Top 20 Iterations with Slow Queries:")
print(timing_df.to_string(index=False))

if len(timing_df) > 0:
    # Check for clustering
    high_count = timing_df['slow_queries'].max()
    median_count = timing_df['slow_queries'].median()
    
    if high_count > median_count * 3:
        print(f"\n⚠️ CLUSTERING DETECTED: Some iterations have {high_count} slow queries vs median {median_count}")
        print("   → LIKELY: Background event (checkpoint, vacuum, connection refresh)")
    else:
        print("\n✅ No obvious clustering - slow queries distributed across iterations")

conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 Summary & Diagnosis

# COMMAND ----------

print("=" * 80)
print("🎯 DIAGNOSIS SUMMARY")
print("=" * 80)
print(f"\nSpike occurs at: hot_traffic_pct = {spike_pct}%")
print("\nPossible causes:")
print("1. ✅ NOT a computation bug (using correct DISTINCT request_id)")
print("2. Check table dominance above - if >50%, likely a single hotspot")
print("3. Check hash key repetition - if >10 hits, genuine contention")
print("4. Check iteration clustering - if high variance, likely background events")
print("5. Remember: Even at 80% hot, only ~51% of requests are fully hot")
print("\nNext steps:")
print("- Review the 'Top Offender Tables' section")
print("- Review the 'Top Hash Keys' section")
print("- Review the 'Timing Pattern Analysis' section")
print("- If dominated by 1-2 tables + same keys → real hotspot contention")
print("- If distributed + iteration spikes → background noise (checkpoint/vacuum)")
print("=" * 80)
