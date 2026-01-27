# Testing RPC Mode - Step by Step

⚠️ **IMPORTANT**: Test before running full benchmark! This validates the implementation.

---

## Quick Test Path (15 minutes total)

### ✅ **Test 1: Deploy SQL Function** (5 min)

```bash
cd /Users/som.natarajan/lakebase-benchmarking

# Deploy the function
./deploy_rpc_function.sh <lakebase_host> <lakebase_user> benchmark

# Enter password when prompted
```

**Expected output:**
```
✅ Function deployed successfully!
✅ RPC Function Ready!
```

---

### ✅ **Test 2: Run Isolated Python Test** (5 min)

```bash
# Test the Python integration without running full benchmark
python test_rpc_mode.py \
  --host <lakebase_host> \
  --user <lakebase_user> \
  --password <password> \
  --iterations 10
```

**What it tests:**
1. ✅ SQL function exists
2. ✅ Can get sample keys from database
3. ✅ Function call works (measures latency)
4. ✅ JSON structure is valid (3 entities × 3 groups)
5. ✅ Payload size is reasonable
6. ✅ Performance is consistent (10 iterations)

**Expected output:**
```
✅ ALL TESTS PASSED!
Function:     features.fetch_request_features()
Latency:      45.23ms (first call)
Performance:  42.15ms avg, 48.67ms max
Payload:      12,345 bytes
Structure:    ✅ Valid (3 entities × 3 groups each)
```

**If tests pass:** ✅ Safe to proceed to Test 3

**If tests fail:** ❌ Fix issues before running benchmark

---

### ✅ **Test 3: Small Benchmark Run** (5 min)

Run a **mini benchmark** (10 iterations instead of 1000) to validate end-to-end:

```python
# In Databricks notebook, set widgets to:
fetch_mode = "rpc_request_json"
run_all_modes = "false"
iterations_per_run = "10"  # ← SMALL test run
hot_key_percent = "1"
```

**Expected results:**
- ✅ No errors during execution
- ✅ `queries_per_request = 1.0`
- ✅ Latency is logged correctly
- ✅ Results persisted to `zipfian_feature_serving_results_v5`

**Check results:**
```sql
SELECT 
    fetch_mode,
    hot_traffic_pct,
    p99_ms,
    queries_per_request,
    parallel_workers,
    avg_payload_bytes
FROM features.zipfian_feature_serving_results_v5
WHERE fetch_mode = 'rpc_request_json'
ORDER BY run_ts DESC
LIMIT 5;
```

---

## If All Tests Pass ✅

Run the **full benchmark**:

```python
# Option 1: RPC mode only (fast)
fetch_mode = "rpc_request_json"
run_all_modes = "false"
iterations_per_run = "1000"

# Option 2: Full sweep including RPC (comprehensive)
run_all_modes = "true"  # Runs all 4 modes
iterations_per_run = "1000"
```

---

## Troubleshooting

### ❌ **Function not found**
```
ERROR: function features.fetch_request_features does not exist
```

**Fix:** Deploy function first
```bash
./deploy_rpc_function.sh <host> <user> benchmark
```

---

### ❌ **Permission denied**
```
ERROR: permission denied for function fetch_request_features
```

**Fix:** Grant execute permission
```sql
GRANT EXECUTE ON FUNCTION features.fetch_request_features(TEXT, TEXT, TEXT) TO <your_user>;
```

---

### ❌ **Wrong JSON structure**
```
❌ Structure validation FAILED:
   • Missing group 'fraud_rates' in entity 'card_fingerprint'
```

**Fix:** Function may have wrong table names. Check:
```sql
-- Verify tables exist
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'features' 
  AND table_name LIKE '%fraud_rates%'
ORDER BY table_name;
```

---

### ❌ **Python import error**
```
ModuleNotFoundError: No module named 'psycopg'
```

**Fix:** Install dependencies
```bash
pip install psycopg[binary]
```

---

### ❌ **Benchmark fails with RPC mode**
```
KeyError: 'rpc_call' in entity_timings
```

**Fix:** Make sure you're using the updated `benchmark_zipfian_realistic_v5.3.py`
```bash
git pull  # Get latest changes
# Or re-upload to Databricks workspace
```

---

## Performance Expectations

### **Latency by Cache State**

| Cache State | Serial | Binpacked | Parallel (w=3) | **RPC JSON** |
|-------------|--------|-----------|----------------|--------------|
| 100% hot | ~50ms | ~25ms | ~20ms | **~15ms** ⚡ |
| 50% hot | ~150ms | ~80ms | ~60ms | **~50ms** ⚡ |
| 0% hot | ~300ms | ~200ms | ~150ms | **~140ms** ⚡ |

**Key metrics:**
- `queries_per_request`: Should be exactly **1.0**
- `avg_payload_bytes`: Should be similar to other modes (~10-50KB)
- `latency_per_query_ms`: Should equal total request latency (since queries=1)

---

## Safety Notes

✅ **Safe to test:**
- SQL function is `SECURITY DEFINER` with explicit `search_path`
- Read-only operation (no writes)
- Uses same keys as other modes

✅ **Can rollback commits if needed:**
```bash
git log --oneline -5
git reset --soft HEAD~3  # Undo last 3 commits, keep changes
```

✅ **Isolated testing:**
- `test_rpc_mode.py` doesn't run full benchmark
- Can test without affecting existing results

---

## What Gets Committed

Already committed (but not deployed yet):
1. ✅ SQL function definition (`sql/create_rpc_request_function.sql`)
2. ✅ Python integration (`notebooks/benchmark_zipfian_realistic_v5.3.py`)
3. ✅ Documentation (`RPC_MODE_GUIDE.md`)
4. ✅ Deployment script (`deploy_rpc_function.sh`)

Not yet committed (testing files):
- `test_rpc_function.sql` (manual SQL tests)
- `test_rpc_mode.py` (automated Python tests)
- `TESTING_RPC_MODE.md` (this guide)

---

## Success Criteria

Before running full benchmark, verify:

- [ ] SQL function deploys without errors
- [ ] `test_rpc_mode.py` passes all 6 tests
- [ ] Mini benchmark (10 iterations) completes successfully
- [ ] Results show `queries_per_request = 1.0`
- [ ] Latency is reasonable (lower than parallel mode)

**If all checkboxes pass:** ✅ Safe to run full benchmark!

**If any fail:** ❌ Debug before proceeding
