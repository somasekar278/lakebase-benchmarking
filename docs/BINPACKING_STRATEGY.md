# Binpacking Strategy: Fair Comparison

## Overview

Both Lakebase and DynamoDB benchmarks use **binpacking** - fetching data from multiple tables in a **single request** to minimize network round-trips. This ensures a fair, apples-to-apples performance comparison.

## Why Binpacking Matters

### Without Binpacking (Sequential)
```
Request 1: fraud_reports_365d         → 20ms
Request 2: good_rate_90d_lag_730d     → 20ms
Request 3: distinct_counts_...        → 20ms
Request 4: request_capture_times      → 20ms
----------------------------------------
Total:                                  80ms
```

### With Binpacking (Parallel)
```
Single Request: All 4 tables          → 25ms
----------------------------------------
Total:                                  25ms
```

**3x faster** by eliminating network round-trips!

## Implementation

### Lakebase: Stored Procedure

```sql
-- Single database call
SELECT * FROM features.fraud_batch_lookup(
    array_of_keys_table1,  -- fraud_reports_365d
    array_of_keys_table2,  -- good_rate_90d_lag_730d
    array_of_keys_table3,  -- distinct_counts_amount_stats_365d
    array_of_keys_table4   -- request_capture_times
);
```

The stored procedure executes internally:
1. Parse input arrays
2. Query all 4 tables in parallel (PostgreSQL can parallelize)
3. Combine results
4. Return as single result set

**Benefits:**
- ✅ Single network round-trip (client → Lakebase)
- ✅ Server-side execution (no data transfer until final results)
- ✅ Connection pooling optimization
- ✅ Query plan caching

### DynamoDB: batch_get_item

```python
# Single API call
response = dynamodb_client.batch_get_item(
    RequestItems={
        'fraud_reports_365d': {
            'Keys': [{'primary_key': {'S': key}} for key in keys1]
        },
        'good_rate_90d_lag_730d': {
            'Keys': [{'primary_key': {'S': key}} for key in keys2]
        },
        'distinct_counts_amount_stats_365d': {
            'Keys': [{'primary_key': {'S': key}} for key in keys3]
        },
        'request_capture_times': {
            'Keys': [{'primary_key': {'S': key}} for key in keys4]
        }
    }
)
```

The batch_get_item API:
1. Accepts up to 100 items across multiple tables
2. Fetches all items in parallel
3. Returns combined results in single response

**Benefits:**
- ✅ Single network round-trip (client → DynamoDB)
- ✅ DynamoDB handles parallel fetching internally
- ✅ Automatic retry for throttled keys
- ✅ Optimized for key-value lookups

## Benchmark Configuration

Both benchmarks use identical configuration:

| Parameter | Value | Description |
|-----------|-------|-------------|
| **NUM_KEYS** | 25 | Keys per table per query |
| **Tables** | 4 | fraud_reports, good_rate, distinct_counts, request_capture |
| **Total Keys** | ~100 | 25 keys × 4 tables |
| **Iterations** | 100 | Number of test runs |
| **Warm-up** | 3 | Initial runs to prime cache |

## What We Measure

### Single Request Latency
- **Start time**: Before binpacked request
- **End time**: After all results returned
- **Measured**: Total time including:
  - Network round-trip
  - Query execution (all 4 tables)
  - Data transfer
  - Result parsing

### Metrics
- **P50**: Median latency (typical case)
- **P95**: 95th percentile (catching outliers)
- **P99**: 99th percentile (worst case within SLA)
- **CV**: Coefficient of variation (consistency)

## Why This Is Fair

1. **Same workload**: Both fetch ~100 keys across 4 tables
2. **Same strategy**: Both use single-request binpacking
3. **Same environment**: Both run from Databricks (consistent network)
4. **Same metrics**: P50, P95, P99, CV measured identically
5. **Same optimization**: Both leverage system-specific optimizations

## Real-World Relevance

This mirrors actual fraud detection workloads:

```
Incoming transaction → Feature lookup required:
  ✓ Historical fraud reports (1 table)
  ✓ Good transaction rates (1 table)  
  ✓ Amount/count statistics (1 table)
  ✓ Timing patterns (1 table)
  
→ Model inference with all features
→ Fraud score returned
```

**Requirement**: Must complete in < 120ms P99 (SLA)

## Optimization Notes

### Lakebase Optimizations
- ✅ Stored procedure pre-compiled
- ✅ B-tree indexes on primary_key
- ✅ Connection pooling
- ✅ Query plan caching
- 🔄 Consider: Read replicas for geo-distribution

### DynamoDB Optimizations
- ✅ Hash-based partitioning
- ✅ batch_get_item optimized for parallel fetch
- ✅ Auto-scaling for throughput
- ✅ Global tables for geo-distribution
- 🔄 Consider: DAX (caching layer) if needed

## Expected Results

Based on typical configurations:

| System | P99 Target | Typical Range |
|--------|------------|---------------|
| **Lakebase** | < 60ms | 40-60ms (co-located) |
| **DynamoDB** | < 80ms | 50-80ms (optimized) |
| **SLA Requirement** | < 120ms | Must meet for production |

Both systems should comfortably meet the 120ms SLA with binpacking.

## Anti-Patterns (What We Avoid)

❌ **Sequential queries** (4 separate requests)
❌ **Client-side joins** (fetch all data, join locally)  
❌ **Overfetching** (fetching unnecessary columns)
❌ **No connection pooling** (new connection per request)
❌ **Cold starts** (no warm-up phase)

## Conclusion

The binpacking strategy ensures:
- ✅ **Fair comparison**: Both systems optimized equivalently
- ✅ **Real-world relevance**: Mirrors production patterns
- ✅ **Performance**: Minimizes network overhead
- ✅ **Scalability**: Works at high request rates

This is the **industry standard** for multi-table feature lookups in production ML systems.

