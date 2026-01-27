# RPC Request JSON Mode - Implementation Guide

## Overview

The `rpc_request_json` mode is a new benchmark execution strategy that collapses the **entire multi-entity request fan-out** into a **single server-side function call** returning JSONB.

### Key Innovation
Instead of making 30 individual queries (serial) or 10 UNION queries (bin-packed), the benchmark makes **1 RPC call** that:
1. Executes all feature lookups server-side
2. Uses the same bin-packing logic (UNION ALL by feature_type)
3. Returns structured JSON with all results
4. Minimizes client-side overhead to near-zero

---

## Architecture

### Performance Hierarchy (Expected)
```
1. Serial            → 30 queries sequential (baseline)
2. Binpacked         → 10 queries sequential (3× reduction)
3. Binpacked_parallel → 10 queries, 3 workers (critical path)
4. RPC Request JSON  → 1 function call (minimal overhead) ✨ BEST
```

### How It Works

**Client Side (Python):**
```python
# Extract 3 keys in entity order
card_key = entities["card_fingerprint"]["hashkey"]
email_key = entities["customer_email"]["hashkey"]
name_key = entities["cardholder_name"]["hashkey"]

# Single RPC call
SELECT features.fetch_request_features(card_key, email_key, name_key)
```

**Server Side (PostgreSQL):**
```sql
-- Function executes:
-- 1. Card fingerprint: 3 UNION groups (fraud_rates, time_since, good_rates)
-- 2. Customer email: 3 UNION groups
-- 3. Cardholder name: 3 UNION groups
-- Returns: JSONB structure with all results
```

---

## Deployment

### Step 1: Deploy SQL Function

```bash
# From repo root
psql -h <lakebase_host> -U <user> -d benchmark -f sql/create_rpc_request_function.sql
```

Or execute directly in SQL editor:
```sql
-- See sql/create_rpc_request_function.sql for full function definition
CREATE OR REPLACE FUNCTION features.fetch_request_features(
    p_card_key TEXT,
    p_email_key TEXT,
    p_name_key TEXT
) RETURNS JSONB ...
```

### Step 2: Verify Function Exists

```sql
-- Test the function
SELECT features.fetch_request_features(
    'test_card_key',
    'test_email_key',
    'test_name_key'
);

-- Should return JSONB structure:
-- {
--   "card_fingerprint": {...},
--   "customer_email": {...},
--   "cardholder_name": {...}
-- }
```

### Step 3: Run Benchmark

**Option A: Run RPC mode only**
```python
# In Databricks widget
fetch_mode = "rpc_request_json"
run_all_modes = "false"
```

**Option B: Run all modes (includes RPC)**
```python
run_all_modes = "true"  # Will run: serial, binpacked, parallel (1,2,3,4 workers), rpc_request_json
```

---

## Results & Metrics

### What Gets Measured

| Metric | Value for RPC Mode |
|--------|-------------------|
| `queries_per_request` | **1** (single function call) |
| `fetch_mode` | `rpc_request_json` |
| `parallel_workers` | `NULL` (not applicable) |
| `request_latency_ms` | Total RPC call time |
| `avg_payload_bytes` | Size of returned JSONB |
| `entity_timings` | Single entry: `{"rpc_call": latency_ms}` |

### Expected Performance

**At 0% hot (fully cold):**
- RPC should be **10-20ms faster** than binpacked_parallel due to:
  - No Python GIL overhead
  - No ThreadPool coordination
  - No connection pool waits
  - Single network round-trip

**At 100% hot (fully warm):**
- RPC should be **20-30ms faster** than binpacked_parallel due to:
  - Zero planning overhead (function pre-compiled)
  - Reduced client CPU usage
  - No per-query marshalling

**Critical path still dominates:**
- RPC doesn't make disk reads faster
- Cold-heavy workloads will still see high latency
- But overhead is minimized to nearly zero

---

## Comparison Table

| Feature | Serial | Binpacked | Parallel | **RPC JSON** |
|---------|--------|-----------|----------|-------------|
| Queries/request | 30 | 10 | 10 | **1** |
| Network RTT | 30× | 10× | 10× | **1×** |
| Planning overhead | 30× | 10× | 10× | **0** (pre-compiled) |
| Client CPU | High | Medium | High (threads) | **Minimal** |
| Critical path | Sum | Sum | Max | **Server-optimized** |
| Parallelism | No | No | Yes (client) | **Yes (server)** |
| Observability | Per-query | Per-group | Per-group | **Aggregate** |

---

## Limitations & Trade-offs

### ✅ Advantages
1. **Minimal overhead** - Single function call, zero client coordination
2. **Pre-compiled** - Function is parsed/planned once, executed many times
3. **Server-side optimization** - PostgreSQL can optimize internal UNION queries
4. **Lower network usage** - One request/response instead of 10-30

### ❌ Disadvantages
1. **No per-query timing** - Can't measure individual table latencies
2. **Fixed structure** - Function must be updated if schema changes
3. **Harder debugging** - All execution happens server-side (less visibility)
4. **No parallelism control** - Can't tune worker count like parallel mode

### When to Use RPC Mode

**Best for:**
- Warm-cache workloads (latency dominated by overhead, not IO)
- High-QPS scenarios (minimize client CPU + network)
- Production serving (consistent, predictable performance)

**Not ideal for:**
- Deep diagnostics (need per-query visibility)
- Schema changes (requires function updates)
- Experimentation (parallel mode more flexible)

---

## Visualization Updates

The RPC mode is automatically included in:
- P99 vs Hot% charts (alongside serial/binpacked/parallel)
- Mode comparison tables
- Query count analysis

**Chart labels:**
- `"RPC Request JSON (1 server-side function call)"`

---

## Troubleshooting

### Function Not Found
```sql
-- Check if function exists
SELECT proname, proargnames, prosrc 
FROM pg_proc 
WHERE proname = 'fetch_request_features' 
  AND pronamespace = 'features'::regnamespace;
```

### Permission Denied
```sql
-- Grant execute to your user
GRANT EXECUTE ON FUNCTION features.fetch_request_features(TEXT, TEXT, TEXT) TO <username>;
```

### Slow RPC Performance
```sql
-- Check if function is being planned correctly
EXPLAIN (ANALYZE, BUFFERS) 
SELECT features.fetch_request_features('key1', 'key2', 'key3');
```

### Schema Changes
If feature tables change, update the function:
```bash
# Edit sql/create_rpc_request_function.sql
# Then redeploy
psql -h <host> -U <user> -d benchmark -f sql/create_rpc_request_function.sql
```

---

## Future Enhancements

### Potential Improvements
1. **Dynamic feature groups** - Function accepts feature list as parameter
2. **Parallel UNION execution** - Use PostgreSQL parallel query features
3. **Prepared statement caching** - Further reduce planning overhead
4. **Compression** - Return compressed JSONB for large payloads

### Advanced Mode: `rpc_request_parallel`
Could create parallel variant that:
- Spawns 3 separate functions (one per entity)
- Executes in parallel using client-side ThreadPool
- Combines benefits of RPC + parallelism
- Would require 3 entity-specific functions

---

## Summary

The `rpc_request_json` mode represents the **most optimized execution strategy** for feature serving workloads where:
1. Request structure is known and stable
2. Overhead minimization is critical
3. Server-side execution is acceptable

**Key Metric:** Reduces from **30 queries/10 queries** → **1 RPC call**

**Expected Win:** 10-30ms latency reduction vs. binpacked_parallel, depending on cache state.

This mode makes the benchmark comprehensive by testing the full spectrum from "client does everything" (serial) to "server does everything" (RPC).
