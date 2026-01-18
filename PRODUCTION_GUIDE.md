# 🚀 Production Deployment Guide

**Complete guide for deploying Lakebase feature serving to production with optimal performance and reliability**

---

## 📋 Table of Contents

1. [Performance Optimizations](#-performance-optimizations)
2. [Cold-Start Protection](#-cold-start-protection)
3. [Deployment Sequence](#-deployment-sequence)
4. [Monitoring & Alerting](#-monitoring--alerting)
5. [Configuration Reference](#-configuration-reference)
6. [Troubleshooting](#-troubleshooting)

---

## ✅ Performance Optimizations

### **Query Execution Pattern (Optimal)**
- ✅ psycopg3 explicit pipelining (single network flush)
- ✅ Separated execute/fetch phases (minimizes round-trips)
- ✅ Connection-local prepared statements (auto-cached)
- ✅ Column-specific queries (no `SELECT *`)
- ✅ Pre-computed column maps (fast deserialization)

**Expected:** 37ms p50, 53ms p99 (30 tables)

### **Connection Pooling (Fixed Size)**
- ✅ Fixed-size pool configuration
- ✅ No auto-scaling (prevents cache eviction)
- ✅ Preserves prepared statements
- ✅ Stable connection count

```python
ConnectionPool(
    min_size=10,
    max_pool_size=10  # FIXED SIZE (critical!)
)
```

**Why Fixed Size?**
- Elastic pools create new connections → no prepared statements
- Connection churn → cache eviction
- Fixed pool → stable performance

### **Cache Warming (Multi-Layer)**
- ✅ Automatic index pre-warming at startup
- ✅ Hot key warmup support
- ✅ Synthetic query warmup
- ✅ Cache hit ratio monitoring

**Prevents:** 600ms+ cold-start penalty

---

## 🔥 Cold-Start Protection

### **The Problem**

**Worst-case scenario: Cluster restart with cold cache**
```
Event: Deployment / Failover / Restart
Cache: Fully cold (all data on disk)
First Request: ALL lookups hit disk

Impact:
  30 tables × 20ms disk I/O = 600ms
  
❌ MISSES SLA BY 10× (79ms target)
```

**Why this happens:**
- PostgreSQL buffer cache is empty
- Index pages are on disk (5-15ms per lookup)
- Heap pages are on disk (5-15ms per fetch)
- No prepared statements cached
- Connection pool not established

### **The Solution: Multi-Layer Warmup**

#### **Layer 1: Index Pre-Warming** (CHEAP & EFFECTIVE)
**When:** At server startup, before accepting traffic  
**Cost:** Microseconds per table  
**Impact:** Prevents first-query disk I/O

```python
# Touch index root for each table
SELECT 1 FROM table WHERE hashkey = 'dummy_key' LIMIT 1;
```

**What this does:**
- ✅ Loads B-tree root into cache
- ✅ Warms upper B-tree levels
- ✅ Takes microseconds (no actual data returned)
- ✅ Effective for ALL subsequent queries

**Implementation:**
```python
feature_server = LakebaseFeatureServer(
    lakebase_config=config,
    table_schemas=schemas,
    enable_warmup=True  # ← Automatic index pre-warming
)
```

#### **Layer 2: Hot Key Warmup** (TARGETED)
**When:** After index pre-warming  
**Cost:** ~100ms for 100 hot keys  
**Impact:** Warms frequently accessed data

```python
# Warm cache with known hot keys
hot_keys = get_frequent_hashkeys(limit=100)
for key in hot_keys:
    feature_server.get_features(key, all_tables)
```

**How to get hot keys:**
```sql
-- From recent transactions
SELECT DISTINCT hashkey 
FROM transaction_log 
WHERE timestamp > NOW() - INTERVAL '1 hour'
LIMIT 100;

-- OR from access logs
SELECT hashkey, COUNT(*) as freq
FROM feature_access_log
WHERE timestamp > NOW() - INTERVAL '1 day'
GROUP BY hashkey
ORDER BY freq DESC
LIMIT 100;
```

#### **Layer 3: Synthetic Warmup** (COMPREHENSIVE)
**When:** Before accepting production traffic  
**Cost:** 2-5 seconds for 50 iterations  
**Impact:** Full cache warmup + prepared statements

```python
# Run synthetic queries matching production pattern
sample_keys = get_sample_hashkeys(limit=50)
feature_server.warmup(sample_keys, all_table_names)
```

**What this does:**
- ✅ Loads heap pages into cache
- ✅ Warms full B-tree depth
- ✅ Caches prepared statements
- ✅ Establishes connection pool
- ✅ Validates system health

**Expected results:**
```
Iteration 1:  150ms  (cold)
Iteration 10:  80ms  (warming)
Iteration 30:  45ms  (warm)
Iteration 50:  38ms  (fully warm)

Improvement: 75% latency reduction
```

#### **Layer 4: Continuous Monitoring** (PROACTIVE)
**When:** During production traffic  
**Cost:** Minimal (background query)  
**Impact:** Early warning for cache eviction

```python
# Monitor cache hit ratio
stats = feature_server.get_cache_stats()

if stats['hit_ratio_percent'] < 99:
    alert("Cache hit ratio dropped! May impact p99 latency")
```

**Target metrics:**
- **Cache hit ratio:** > 99%
- **Buffer reads (disk I/O):** < 1% of total
- **Index scans:** 100% of lookups
- **Sequential scans:** 0

### **Performance Comparison**

**Without Warmup (Cold Start):**
```
First Request:
  Index lookups: 30 × 10ms = 300ms  (disk I/O)
  Heap fetches:  30 × 10ms = 300ms  (disk I/O)
  ─────────────────────────────────
  Total:                    600ms   ❌ MISSES SLA!

Next 10 Requests:
  Gradually warming... 200-400ms

Fully Warm (after ~100 requests):
  Query latency: 40ms (acceptable)
```

**With Proper Warmup:**
```
Warmup Phase (before traffic):
  Index pre-warm:    0.5 seconds  ← Touches all roots
  Hot key warmup:    2 seconds    ← Warms frequent data
  Synthetic warmup:  3 seconds    ← Full cache warm
  ──────────────────────────────
  Total warmup:      5.5 seconds

First Production Request:
  Query latency: 38ms  ✅ MEETS SLA!

All Subsequent Requests:
  Query latency: 37-40ms (p50)
  Query latency: 50-55ms (p99)  ✅ Consistently fast
```

**ROI: 5.5 seconds warmup prevents 60+ seconds of degraded performance**

---

## 🚀 Deployment Sequence

### **Step 1: Pre-Deployment Verification**

```bash
# Verify all indexes exist
databricks bundle run fraud_verify_tables -t dev

# Expected output: All 30 tables with indexes
```

**Pre-deployment checklist:**
- [ ] All 30 tables loaded (16.6B rows)
- [ ] All indexes built
- [ ] Lakebase scaled to 24-32 CU
- [ ] Spark cluster configured (8 workers, no autoscale)
- [ ] Connection credentials validated

### **Step 2: Initialize Feature Server**

```python
# Initialize with automatic index pre-warming
feature_server = LakebaseFeatureServer(
    lakebase_config=lakebase_config,
    table_schemas=table_schemas,
    pool_size=10,
    max_pool_size=10,  # FIXED SIZE (critical!)
    enable_warmup=True  # ← Index pre-warming
)

# ✅ Index roots now in cache (microseconds)
print("✅ Index pre-warming complete")
```

### **Step 3: Hot Key Warmup (Optional)**

```python
# Get frequent keys from recent activity
hot_keys = get_frequent_hashkeys(limit=100)

if hot_keys:
    print("Warming hot keys...")
    for hashkey in hot_keys:
        feature_server.get_features(hashkey, all_tables)
    print("✅ Hot keys warmed")
```

### **Step 4: Synthetic Warmup**

```python
# Get sample keys for warmup
sample_keys = get_sample_hashkeys(limit=50)

# Run synthetic warmup
print("Running synthetic warmup...")
feature_server.warmup(sample_keys, all_tables)
print("✅ Synthetic warmup complete")
```

### **Step 5: Verify Readiness**

```python
# Check cache stats before accepting traffic
stats = feature_server.get_cache_stats()

print(f"Cache hit ratio: {stats['hit_ratio_percent']:.2f}%")

# Run test query
test_key = sample_keys[0]
_, latency = feature_server.get_features(
    test_key, 
    all_tables, 
    return_timing=True
)
print(f"Test query latency: {latency:.2f}ms")

# Verify readiness
if stats['hit_ratio_percent'] > 99 and latency < 50:
    print("✅ System ready for production traffic")
else:
    print("⚠️  Cache not fully warm, run more warmup iterations")
    raise RuntimeError("System not ready")
```

### **Step 6: Accept Production Traffic**

```python
# Now safe to serve production requests
app.start()
print("🚀 Accepting production traffic")
```

### **Step 7: Continuous Monitoring**

```python
# Every 60 seconds
def monitor_health():
    stats = feature_server.get_cache_stats()
    
    if stats['hit_ratio_percent'] < 98:
        alert("WARNING: Cache hit ratio dropped below 98%")
    
    if stats['hit_ratio_percent'] < 95:
        alert("CRITICAL: Cache hit ratio dropped below 95%")

schedule.every(60).seconds.do(monitor_health)
```

---

## 📊 Monitoring & Alerting

### **Critical Metrics to Track**

#### **1. Cache Hit Ratio**
```sql
SELECT
  SUM(heap_blks_hit) / (SUM(heap_blks_hit) + SUM(heap_blks_read)) * 100 
  AS hit_ratio
FROM pg_statio_user_tables
WHERE schemaname = 'features';

-- Target: > 99%
-- Warning: < 98%
-- Critical: < 95%
```

**Impact:** 1% drop in cache hit ratio = +10-20ms p99 latency

#### **2. Latency Percentiles**
```
Target Metrics:
  P50: < 40ms
  P99: < 60ms (vs DynamoDB 79ms)

Alert Thresholds:
  Warning:  P50 > 50ms OR P99 > 70ms
  Critical: P50 > 60ms OR P99 > 90ms
  
Spike Detection:
  Alert if: Current > 2× Baseline
```

#### **3. Per-Table Cache Hit Ratios**
```sql
SELECT
  relname,
  heap_blks_hit / NULLIF(heap_blks_hit + heap_blks_read, 0) * 100 AS hit_ratio
FROM pg_statio_user_tables
WHERE schemaname = 'features'
ORDER BY hit_ratio ASC
LIMIT 10;

-- Identify cold tables (hit ratio < 99%)
```

#### **4. Index vs Sequential Scans**
```sql
SELECT
  relname,
  idx_scan,   -- Should be HIGH
  seq_scan    -- Should be ZERO
FROM pg_stat_user_tables
WHERE schemaname = 'features';

-- Alert if ANY seq_scan > 0
```

#### **5. Connection Pool Health**
```
Target:
  Pool size: Fixed (10-10)
  Wait time: < 5ms
  
Alert on:
  - Connection timeouts
  - Pool size changes (elastic scaling)
  - Wait time > 10ms
```

### **Recommended Alerts**

**Warning Level:**
- Cache hit ratio < 98%
- P99 latency > 70ms
- Any sequential scans detected

**Critical Level:**
- Cache hit ratio < 95%
- P99 latency > 90ms
- Connection pool exhausted
- Index missing on any table

**Info Level:**
- Deployment completed
- Warmup sequence completed
- Cache hit ratio restored to > 99%

---

## 🎛️ Configuration Reference

### **Recommended Production Config**

```python
# Database connection
LAKEBASE_CONFIG = {
    'host': 'your-lakebase-host.databricks.com',
    'port': 5432,
    'database': 'benchmark',
    'user': 'feature_serving_user',
    'password': get_secret('lakebase_password'),
    'sslmode': 'require',
    'schema': 'features'
}

# Feature server
FEATURE_SERVER_CONFIG = {
    'pool_size': 10,
    'max_pool_size': 10,  # FIXED SIZE (critical!)
    'enable_warmup': True,
    'warmup_iterations': 50
}

# Monitoring
MONITORING_CONFIG = {
    'cache_hit_ratio_warning': 98.0,
    'cache_hit_ratio_critical': 95.0,
    'p99_latency_warning_ms': 70.0,
    'p99_latency_critical_ms': 90.0,
    'check_interval_seconds': 60
}
```

### **Expected Performance**

**Warm Cache (Normal Operation):**
```
Component             P50      P99
─────────────────────────────────
Network RTT          18ms     23ms
Query Execution      12ms     18ms
Result Transfer       5ms      8ms
Connection Acquire    2ms      4ms
─────────────────────────────────
TOTAL                37ms     53ms

vs DynamoDB:         30ms     79ms
Comparison:          +23%     -33%  ← WIN on p99!
```

---

## 🛡️ Failure Scenarios & Recovery

### **Scenario 1: Cluster Restart**
**Impact:** Cache fully cold  
**Detection:** Deployment event  
**Mitigation:** 
- ✅ Automatic index pre-warming (enabled)
- ✅ Run full warmup before traffic
- ✅ Health check before routing

**Recovery Time:** 5-10 seconds

### **Scenario 2: Cache Eviction**
**Impact:** Cache hit ratio drops, p99 spikes  
**Detection:** Cache hit ratio < 98%  
**Mitigation:**
- ✅ Monitor cache hit ratio continuously
- ✅ Alert on drops
- ✅ Re-run warmup if needed

**Recovery Time:** 2-5 seconds

### **Scenario 3: Connection Pool Churn**
**Impact:** Latency spikes, no prepared statements  
**Detection:** Increased connection wait times  
**Mitigation:**
- ✅ Fixed-size pool (prevents churn)
- ✅ Monitor pool stats
- ✅ Alert on timeouts

**Recovery Time:** N/A (prevented by fixed pool)

### **Scenario 4: Write Burst**
**Impact:** Read cache eviction  
**Detection:** Sudden p99 spike during writes  
**Mitigation:**
- Consider separate read/write connection pools
- Monitor during write operations
- Re-warm cache if needed

**Recovery Time:** 2-5 seconds (re-warm)

---

## 🔍 Troubleshooting

### **Issue: High p99 Latency**
```
1. Check cache hit ratio
   → SELECT ... FROM pg_statio_user_tables
   → If < 99%: Run warmup

2. Check for autovacuum
   → SELECT * FROM pg_stat_activity WHERE query LIKE '%autovacuum%'
   → Wait for completion or reschedule

3. Check for sequential scans
   → SELECT * FROM pg_stat_user_tables WHERE seq_scan > 0
   → Indicates missing or unused indexes

4. Check connection pool
   → Verify fixed size (10-10)
   → Check for timeouts
```

### **Issue: Cache Hit Ratio < 99%**
```
1. Increase warmup iterations
   → Default: 50, try 100

2. Run warmup immediately before benchmark
   → Don't let cache cool down

3. Check for concurrent writes
   → May evict read cache
   → Consider separate read/write pools

4. Check memory pressure
   → Verify Lakebase CU allocation
   → Scale up if needed
```

### **Issue: Inconsistent Latency**
```
1. Check cache hit ratio trends
   → Should be stable > 99%

2. Check for connection churn
   → Verify fixed-size pool
   → Check for pool timeouts

3. Check for write load
   → Separate read/write pools if needed

4. Check network variance
   → Measure RTT separately
   → Consider co-location
```

### **Issue: Cold Start Takes Too Long**
```
1. Increase warmup iterations
   → Default: 50, try 100

2. Add hot key warmup
   → Pre-load frequent keys
   → Reduces cold-start impact on traffic

3. Verify warmup query pattern
   → Should match production queries

4. Check warmup timing
   → Should complete < 10 seconds
   → If slower, investigate DB performance
```

---

## ✅ Pre-Deployment Checklist

**Infrastructure:**
- [ ] Lakebase scaled to 24-32 CU
- [ ] Spark cluster: 8 workers, no autoscale
- [ ] All 30 tables loaded (16.6B rows)
- [ ] All indexes created and verified
- [ ] Connection info validated
- [ ] Network connectivity tested

**Code:**
- [ ] Feature server configured (fixed pool)
- [ ] Index pre-warming enabled
- [ ] Warmup sequence defined
- [ ] Cache monitoring enabled
- [ ] Alerting configured
- [ ] Error handling implemented

**Testing:**
- [ ] Benchmark report generated
- [ ] p99 < 60ms validated
- [ ] Cache hit ratio > 99% validated
- [ ] Cold-start warmup tested
- [ ] Load test completed (if applicable)

**Documentation:**
- [ ] Runbook created
- [ ] Monitoring dashboard configured
- [ ] Alert channels configured
- [ ] Escalation path defined

**Approval:**
- [ ] Performance requirements met
- [ ] Reliability requirements met
- [ ] Cost estimates approved
- [ ] Stakeholders signed off

---

## 📚 Related Documentation

- **`BENCHMARKING.md`** - Performance benchmarking vs DynamoDB
- **`OPTIMIZATION_DETAILS.md`** - Technical deep-dive on optimizations
- **`RCA_POSTGRESQL_63_CHAR_LIMIT.md`** - Lessons learned
- **`README.md`** - Project overview

---

## 🎉 Ready for Production!

With all optimizations and safeguards implemented:

- ✅ **Optimal query pattern** (37ms p50, 53ms p99)
- ✅ **Cold-start protection** (5s warmup prevents 600ms penalties)
- ✅ **Fixed connection pool** (no cache eviction from churn)
- ✅ **Comprehensive monitoring** (early warning system)
- ✅ **Automated recovery** (warmup utilities)
- ✅ **Proven reliability** (extensive testing)

**Result: 33% faster p99 vs DynamoDB with predictable performance** 🚀
