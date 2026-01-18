# RCA: PostgreSQL 63-Character Identifier Limit Failures

**Date:** 2026-01-16  
**Severity:** CRITICAL (Data Loss Risk)  
**Impact:** 3 tables (1.175B rows, 255 GB) stuck in stage tables, 2 tables missing primary key indexes  
**Status:** MITIGATED (manual rescue), ROOT CAUSE IDENTIFIED, PERMANENT FIX PENDING

---

## Executive Summary

During a 16.6 billion row bulk load operation, **5 out of 30 tables experienced failures** due to PostgreSQL's 63-character limit on identifier names. Three tables had data loaded to staging tables but failed to swap to production (255 GB of data inaccessible). Two tables loaded successfully but failed to create primary key indexes (degraded query performance).

**Root Cause:** PostgreSQL silently truncates identifiers longer than 63 characters, causing name mismatches and silent failures in swap and index operations.

**Detection:** Issue discovered post-job completion when querying production tables returned zero rows despite "successful" job status.

**Resolution:** Manual SQL rescue of 3 tables + manual index creation for 5 tables required (3-4 hours of manual work).

---

## Timeline

| Time | Event | Status |
|------|-------|--------|
| T+0h | Job started: Load 30 tables, 16.6B rows | Running |
| T+23h | Job completed with "Success" status | ✅ Appeared successful |
| T+23h | User verification: Found production tables empty | ❌ Issue detected |
| T+23h | SQL query revealed 3 tables stuck in stage | 🚨 Data loss risk |
| T+23h | Manual rescue initiated | 🔧 Mitigating |
| T+24h | All data rescued, 5 indexes still missing | ⚠️ Degraded performance |

---

## Root Cause Analysis

### 1. PostgreSQL 63-Character Limit

PostgreSQL enforces a **63-character maximum** on all identifiers:
- Table names
- Index names
- Column names
- Constraint names

**When violated:**
- PostgreSQL **silently truncates** to 63 characters
- No error, no warning
- Truncation is deterministic but unpredictable for humans

### 2. Affected Table Names

**Example:**
```
Original: client_id_cardholder_name_clean__tesseract_velocities__365d__stage
Length:   67 characters

Truncated: client_id_cardholder_name_clean__tesseract_velocities__365d__st
Length:    63 characters (lost "age")
```

**Impact:** Code expected `...__stage`, but PostgreSQL created `...__st`

### 3. Failure Mode 1: Stage-to-Production Swap Failed

**Code Logic:**
```python
# schema_loader.py, line ~850
staging_table = f"{table_name}__stage"  # 67 chars
production_table = f"{table_name}"      # 63 chars

# Attempt swap
cursor.execute(f"""
    ALTER TABLE {staging_table} RENAME TO {production_table}
""")
```

**What Happened:**
- PostgreSQL created table: `...__365d__st` (truncated)
- Code tried to rename: `...__365d__stage` (original)
- PostgreSQL error: "relation does not exist"
- Exception caught, logged as "⚠️ Index rename: relation does not exist"
- **Job continued, marked as SUCCESS**
- Production table remained empty

**Affected Tables:**
1. `client_id_cardholder_name_clean__tesseract_velocities__365d` (900M rows, 196 GB)
2. `client_id_cardholder_name_clean__tesseract_velocities__90d` (200M rows, 43.6 GB)
3. `client_id_cardholder_name_clean__tesseract_velocities__30d` (75M rows, 16.3 GB)

### 4. Failure Mode 2: Index Creation Collisions

**Code Logic:**
```python
# schema_loader.py, line ~759
index_name = f"{staging_table}_pkey"  # 73 chars!

cursor.execute(f"""
    CREATE UNIQUE INDEX CONCURRENTLY {index_name} 
    ON {staging_table} (hashkey)
""")
```

**What Happened:**
- Multiple long table names truncate to **same 63 characters**
- First index created successfully
- Second index creation fails: "relation already exists"
- Exception caught, logged as "❌ Index build failed"
- **Job continued, marked as SUCCESS**
- Table has no primary key index

**Example Collision:**
```
client_id_cardholder_name_clean__tesseract_velocities__365d__sta_pkey (73 chars)
client_id_cardholder_name_clean__tesseract_velocities__90d__stage_pkey (73 chars)

Both truncate to:
client_id_cardholder_name_clean__tesseract_velocities__365d__ (63 chars)
                                                         ^^^^^^ SAME!
```

**Affected Tables:**
1. `client_id_card_fingerprint__fraud_rates__90d` (250M rows)
2. `client_id_card_fingerprint__fraud_rates__30d` (80M rows)
3. Plus 3 tesseract tables from swap failure

### 5. Why It Wasn't Detected Earlier

**Multiple Detection Failures:**

1. **No Pre-Creation Validation**
   - No check for name length before `CREATE TABLE`
   - No check that actual table name matches expected name

2. **Silent Exception Handling**
   ```python
   except Exception as e:
       print(f"⚠️ Index rename: relation does not exist")
       # But job continues as if nothing happened!
   ```

3. **No Post-Swap Verification**
   - No check that production table exists after swap
   - No check that production table has data after swap
   - No check that row count matches expected

4. **Job Reported Success Despite Failures**
   - Databricks job marked as "SUCCESS"
   - User email notification: "✅ SUCCESS!"
   - Actual state: 3 tables empty, 5 indexes missing

5. **Testing Gaps**
   - Single table test used short name (75M rows table)
   - Longest table name in single test: 63 chars (no truncation)
   - Issue only surfaced with full 30-table run

---

## Impact Assessment

### Data Impact
- **Data Loss Risk:** HIGH (255 GB in inaccessible stage tables)
- **Data Corruption:** NONE (data intact, just misplaced)
- **Recovery Time:** 4 hours (manual rescue + index builds)

### Performance Impact
- **5 tables without indexes:** Full table scans on every query
- **Expected query latency:**
  - Without index: 10-30 seconds (250M rows)
  - With index: <10ms (hash lookup)
- **Benchmark validity:** INVALID until indexes built

### Operational Impact
- **Job reliability:** BROKEN (25% failure rate not detected)
- **Monitoring:** INADEQUATE (no alerts on silent failures)
- **Trust:** DAMAGED (success != success)

---

## Immediate Mitigations (Completed)

1. ✅ Manual SQL rescue of 3 stage tables (2 hours)
2. 🔄 Manual index creation for 5 tables (pending, ~2 hours)
3. ✅ RCA documentation (this document)

---

## Permanent Fixes Required

### Fix 1: Table Name Length Validation (CRITICAL)

**Location:** `utils/schema_loader.py`, `_parse_ddl_statements()`

**Change:**
```python
def _parse_ddl_statements(self, ddl_content: str, schema: Optional[str] = None):
    # ... existing parsing logic ...
    
    for table_name, table_def in self.tables.items():
        # CRITICAL: Validate table name length
        stage_table_name = f"{table_name}__stage"
        if len(stage_table_name) > 63:
            raise ValueError(
                f"Table name too long for PostgreSQL (max 63 chars): "
                f"{stage_table_name} ({len(stage_table_name)} chars). "
                f"Use shorter table names or implement hash-based naming."
            )
        
        # Validate production table name
        if schema:
            full_table_name = f"{schema}.{table_name}"
        else:
            full_table_name = table_name
            
        if len(table_name) > 63:
            raise ValueError(
                f"Table name too long for PostgreSQL (max 63 chars): "
                f"{table_name} ({len(table_name)} chars)"
            )
```

**Impact:** Fail fast at DDL parsing, before any data generation or loading

### Fix 2: Use Short, Deterministic Stage Table Names (HIGH PRIORITY)

**Instead of:** `client_id_cardholder_name_clean__tesseract_velocities__365d__stage`

**Use one of:**

**Option A: Hash-Based Names**
```python
import hashlib

def get_stage_table_name(table_name: str) -> str:
    """Generate short, unique stage table name"""
    table_hash = hashlib.md5(table_name.encode()).hexdigest()[:8]
    return f"stg_{table_hash}"
    
# Example:
# "tesseract_velocities__365d" → "stg_a3f2b9c1"
```

**Option B: Sequential Names**
```python
def get_stage_table_name(table_name: str, table_index: int) -> str:
    """Generate short, sequential stage table name"""
    return f"stg_{table_index:03d}"
    
# Example:
# Table 1 → "stg_001"
# Table 2 → "stg_002"
```

**Option C: Truncate + Hash Suffix**
```python
def get_stage_table_name(table_name: str) -> str:
    """Truncate table name and add unique hash"""
    table_hash = hashlib.md5(table_name.encode()).hexdigest()[:6]
    max_len = 63 - 7  # Reserve 7 chars for "_stg_" + hash
    truncated = table_name[:max_len]
    return f"{truncated}_s_{table_hash}"
    
# Example:
# "client_id_cardholder_name_clean__tesseract_velocities__365d"
# → "client_id_cardholder_name_clean__tesseract_velocities_s_a3f2b9"
```

**Recommendation:** Option A (hash-based) for simplicity and guaranteed uniqueness

### Fix 3: Post-Swap Verification (CRITICAL)

**Location:** `utils/schema_loader.py`, `_atomic_swap()`

**Add verification:**
```python
def _atomic_swap(self, table_name: str, staging_table: str, schema: str):
    """Atomically swap stage table with production table"""
    
    # ... existing swap logic ...
    
    # CRITICAL: Verify swap succeeded
    with self.lakebase_config.get_connection() as conn:
        with conn.cursor() as cursor:
            # Check production table exists
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = '{schema}' 
                  AND table_name = '{table_name}'
            """)
            prod_exists = cursor.fetchone()[0] > 0
            
            if not prod_exists:
                raise RuntimeError(
                    f"CRITICAL: Production table {table_name} does not exist after swap! "
                    f"Data may be lost. Check for stage table: {staging_table}"
                )
            
            # Check production table has data
            cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table_name}")
            row_count = cursor.fetchone()[0]
            
            if row_count == 0:
                raise RuntimeError(
                    f"CRITICAL: Production table {table_name} is empty after swap! "
                    f"Expected rows from stage table: {staging_table}"
                )
            
            print(f"   ✅ Swap verified: {row_count:,} rows in production")
```

### Fix 4: Short Index Names (HIGH PRIORITY)

**Location:** `utils/schema_loader.py`, `_build_primary_key_index()`

**Change:**
```python
def _build_primary_key_index(self, staging_table: str, table_def: Dict, schema: str):
    """Build primary key index on staging table"""
    
    # Use short, unique index name
    table_hash = hashlib.md5(staging_table.encode()).hexdigest()[:8]
    index_name = f"idx_{table_hash}"
    
    # Ensure index name is unique and < 63 chars
    if len(index_name) > 63:
        raise ValueError(f"Index name too long: {index_name}")
    
    sql = f"""
        CREATE UNIQUE INDEX CONCURRENTLY {index_name} 
        ON {schema}.{staging_table} (hashkey)
    """
    
    # ... rest of logic ...
```

### Fix 5: Fail-Fast on Unexpected Errors (CRITICAL)

**Location:** `utils/schema_loader.py`, `generate_and_load_data()`

**Change exception handling:**
```python
# BEFORE (Silent failure):
except Exception as e:
    print(f"⚠️ Index rename: {e}")
    # Job continues!

# AFTER (Fail fast):
except Exception as e:
    print(f"❌ CRITICAL ERROR: {e}")
    raise RuntimeError(
        f"Operation failed for table {table_name}. "
        f"Stopping job to prevent data loss. Error: {e}"
    ) from e
```

### Fix 6: Comprehensive Post-Job Validation (MEDIUM PRIORITY)

**New utility:** `utils/validate_load.py`

```python
def validate_all_tables(lakebase_config, expected_tables: Dict[str, int]):
    """
    Validate all tables loaded successfully
    
    Args:
        lakebase_config: Lakebase connection config
        expected_tables: Dict of {table_name: expected_row_count}
        
    Returns:
        Dict with validation results
        
    Raises:
        RuntimeError if any validation fails
    """
    results = {
        "tables_ok": [],
        "tables_missing": [],
        "tables_empty": [],
        "tables_wrong_count": [],
        "indexes_missing": []
    }
    
    with lakebase_config.get_connection() as conn:
        with conn.cursor() as cursor:
            for table_name, expected_rows in expected_tables.items():
                # Check table exists
                cursor.execute(f"""
                    SELECT COUNT(*) FROM information_schema.tables 
                    WHERE table_name = '{table_name}'
                """)
                exists = cursor.fetchone()[0] > 0
                
                if not exists:
                    results["tables_missing"].append(table_name)
                    continue
                
                # Check row count
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                actual_rows = cursor.fetchone()[0]
                
                if actual_rows == 0:
                    results["tables_empty"].append(table_name)
                elif actual_rows != expected_rows:
                    results["tables_wrong_count"].append({
                        "table": table_name,
                        "expected": expected_rows,
                        "actual": actual_rows
                    })
                else:
                    results["tables_ok"].append(table_name)
                
                # Check index exists
                cursor.execute(f"""
                    SELECT COUNT(*) FROM pg_indexes 
                    WHERE tablename = '{table_name}'
                """)
                index_count = cursor.fetchone()[0]
                
                if index_count == 0:
                    results["indexes_missing"].append(table_name)
    
    # Fail if any issues found
    issues = (
        results["tables_missing"] + 
        results["tables_empty"] + 
        results["tables_wrong_count"] + 
        results["indexes_missing"]
    )
    
    if issues:
        raise RuntimeError(
            f"Validation failed! Issues found:\n"
            f"  Missing tables: {len(results['tables_missing'])}\n"
            f"  Empty tables: {len(results['tables_empty'])}\n"
            f"  Wrong row counts: {len(results['tables_wrong_count'])}\n"
            f"  Missing indexes: {len(results['indexes_missing'])}\n"
            f"See results for details."
        )
    
    return results
```

### Fix 7: Enhanced Monitoring & Alerting

**Add to job:**
- Post-job validation step (calls `validate_all_tables()`)
- Slack/email alert on validation failure
- Row count metrics to Databricks job metrics
- Index existence metrics

### Fix 8: Smart Checkpoint with Row Count Validation (MEDIUM PRIORITY)

**Location:** `utils/schema_loader.py`, `generate_and_load_data()`

**Problem:** Current checkpoint logic skips any table with data, even if incomplete.

**Example:** 
- Expected: 250M rows
- Actual: 187M rows (partial load from timeout)
- Current behavior: Skip (treat as complete)
- Desired behavior: Drop and reload

**Change:**
```python
def generate_and_load_data(
    self, 
    rows_per_table: int, 
    rows_per_table_dict: Dict[str, int],
    uc_volume_path: Optional[str] = None,
    skip_tables_with_data: bool = True,
    checkpoint_tolerance: float = 0.01  # NEW: Allow 1% variance
):
    """Generate and load data with smart checkpoint validation"""
    
    for table_name, table_def in self.tables.items():
        # Get expected row count
        expected_rows = rows_per_table_dict.get(table_name, rows_per_table)
        
        # Check existing data
        if skip_tables_with_data:
            try:
                with self.lakebase_config.get_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(f"""
                            SELECT COUNT(*) 
                            FROM {schema}.{table_name}
                        """)
                        existing_rows = cursor.fetchone()[0]
                        
                        if existing_rows > 0:
                            # Calculate completeness
                            min_acceptable = int(expected_rows * (1 - checkpoint_tolerance))
                            max_acceptable = int(expected_rows * (1 + checkpoint_tolerance))
                            
                            if existing_rows >= min_acceptable:
                                if existing_rows <= max_acceptable:
                                    # Within acceptable range
                                    print(f"   ✅ SKIPPING - Table complete: {existing_rows:,} / {expected_rows:,} rows")
                                    tables_skipped += 1
                                    continue
                                else:
                                    # Has MORE data than expected (user scaled down?)
                                    print(f"   ⚠️  WARNING - Table has MORE data than expected!")
                                    print(f"      Actual: {existing_rows:,}, Expected: {expected_rows:,}")
                                    print(f"      Keeping existing data (not safe to drop)")
                                    tables_skipped += 1
                                    continue
                            else:
                                # Incomplete load detected
                                shortfall_pct = (expected_rows - existing_rows) / expected_rows * 100
                                print(f"   ⚠️  INCOMPLETE - Table has {existing_rows:,} / {expected_rows:,} rows ({shortfall_pct:.1f}% short)")
                                print(f"   🔄 Dropping and reloading to reach target...")
                                
                                cursor.execute(f"DROP TABLE {schema}.{table_name} CASCADE")
                                conn.commit()
                                
                                print(f"   ✅ Table dropped, proceeding with load")
                                # Fall through to load logic
                        else:
                            print(f"   ✓ Table exists but is empty, proceeding with load...")
                            
            except Exception as e:
                # Table doesn't exist, proceed with load
                print(f"   ✓ Table doesn't exist yet, proceeding with initial load...")
        
        # ... rest of load logic ...
```

**Benefits:**
- Auto-recovery from partial loads (timeouts, crashes)
- Validates data completeness on every run
- Handles scale-up scenarios (user changes row counts)
- Prevents duplicate data issues

**Configuration:**
```python
# In notebook widgets:
dbutils.widgets.dropdown("checkpoint_mode", "smart", ["smart", "skip_all", "force_reload"])
dbutils.widgets.text("checkpoint_tolerance", "0.01", "Tolerance (0.01 = 1%)")

# "smart" = Check row counts, drop if incomplete
# "skip_all" = Always skip tables with data (current behavior)
# "force_reload" = Always drop and reload (no checkpoint)
```

---

## Testing Requirements

### Test 1: Long Table Names (MUST FAIL)
```python
# Test that names > 63 chars are rejected
table_name = "a" * 70  # 70 chars
# Expected: ValueError before any data generation
```

### Test 2: Name Collision Detection
```python
# Test that hash-based names don't collide
tables = ["table_" + str(i) for i in range(1000)]
stage_names = [get_stage_table_name(t) for t in tables]
# Expected: All unique, all <= 63 chars
```

### Test 3: Swap Verification
```python
# Test that swap failure is detected
# 1. Manually rename stage table to something else
# 2. Run swap
# Expected: RuntimeError raised, job fails
```

### Test 4: End-to-End Validation
```python
# Test full 30-table load with validation
# Expected: All 30 tables exist, have data, have indexes
```

---

## Prevention Checklist

Before deploying ANY fix:

- [ ] Table name length validation added
- [ ] Short stage table names implemented (hash-based or sequential)
- [ ] Post-swap verification added
- [ ] Short index names implemented
- [ ] Fail-fast error handling added
- [ ] Post-job validation added
- [ ] All 4 test cases passing
- [ ] Code review completed
- [ ] Single-table test validated with long name (> 63 chars)
- [ ] Documentation updated

---

## Lessons Learned

1. **PostgreSQL limits are real and enforced silently**
   - 63-char limit on identifiers is non-negotiable
   - No warnings, only silent truncation
   - Must validate at application level

2. **"Success" doesn't mean success**
   - Job status ≠ data integrity
   - Must validate outcomes, not just completion
   - Exception handling must fail-fast on critical errors

3. **Test with production-like names**
   - Single table test used short names
   - Production uses long, descriptive names
   - Must test with worst-case names

4. **Silent failures are the worst failures**
   - Caught exceptions without re-raising = silent data loss
   - Must log AND raise on unexpected errors
   - Must validate expected state after each operation

5. **Monitoring must validate outcomes**
   - Row count metrics
   - Index existence checks
   - Post-job validation step
   - Alert on any discrepancy

---

## Implementation Priority

1. **CRITICAL (Deploy immediately):**
   - [ ] Fix 1: Table name validation
   - [ ] Fix 3: Post-swap verification
   - [ ] Fix 5: Fail-fast error handling

2. **HIGH (Deploy before next load):**
   - [ ] Fix 2: Short stage table names
   - [ ] Fix 4: Short index names
   - [ ] Test 1-4: All tests passing

3. **MEDIUM (Deploy within 1 week):**
   - [ ] Fix 6: Post-job validation
   - [ ] Fix 7: Enhanced monitoring

---

## Sign-Off

**RCA Completed By:** AI Assistant  
**Reviewed By:** [User to sign off]  
**Approved By:** [User to sign off]  
**Date:** 2026-01-16

**Permanent fixes will be tracked in:** [GitHub issue / JIRA ticket]
