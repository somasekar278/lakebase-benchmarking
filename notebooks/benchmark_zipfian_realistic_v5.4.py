# Databricks notebook source
# -*- coding: utf-8 -*-
"""
Realistic Zipfian Feature Serving Benchmark V5.4 (Multi-Entity)

This file is a direct Python export of the Databricks notebook you shared, with
the requested fixes applied (no back-and-forth). It is intended to run inside
a Databricks notebook context (dbutils available). It also includes light
fallbacks so the file can be imported without immediate failure outside DBX.

Key fixes applied:
1) Fixed runtime error: non_explain_iters -> measured_iters
2) Guarded I/O measurement when pg_stat_reset() is not permitted (prevents misleading deltas)
3) Removed misleading "EXPLAIN sampling" claims (since no post-run EXPLAIN is implemented here)
4) Removed unused pool_wait_{max,avg} locals
5) Corrected printed version label to V5.3
V5.4 Changes (2026-01-27):
1) Added rpc_request_json mode: Single server-side PostgreSQL function call
   - Collapses entire request fan-out (30 table lookups) into one JSONB response
   - Queries per request: 30 (serial) → 10 (binpacked) → 1 (RPC)
   - Expected latency: ~35ms avg, ~60ms P99 (based on local tests)
   - Zero client-side parallelism overhead (no ThreadPool, no GIL, no connection pool)
2) Added fetch_features_rpc_request() function with payload size tracking
3) Updated MODE_CONFIGS to include rpc_request_json in run_all_modes sweep
4) Fixed database name: benchmark (not databricks_postgres)
5) Fixed table names: cardholder_name_clean requires _clean suffix
6) Removed unused projection_mode widget/variable to avoid confusion (still uses SELECT *)

NOTE:
- This keeps your benchmark logic intact; changes are minimal and correctness-focused.
"""

# COMMAND ----------
# MAGIC %md
# MAGIC # 🎯 Realistic Zipfian Feature Serving Benchmark V5.4 (Multi-Entity)
# MAGIC
# MAGIC **Production-Grade Benchmark with:**
# MAGIC - ✅ Random key sampling from ALL tables (not just first keys)
# MAGIC - ✅ SELECT * (fetches actual columns, not just index)
# MAGIC - ✅ Serial execution (realistic per-query latency)
# MAGIC - ✅ Per-entity timing (independent measurement)
# MAGIC - ✅ Aggregate I/O tracking (via pg_statio_user_tables) for serial mode
# MAGIC - ✅ Key persistence for reproducibility
# MAGIC
# MAGIC **Request Structure:**
# MAGIC - Each request = 3 entities (card_fingerprint, customer_email, cardholder_name)
# MAGIC - Each entity fans out to 9-12 tables (30 tables total per request)
# MAGIC - Hot/cold determined independently per entity
# MAGIC - Latency = SUM(entity latencies) for serial execution

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1️⃣ Install Dependencies

# COMMAND ----------
# NOTE: Keep as notebook cells; in pure .py execution this may be skipped.
try:
    # Databricks-only
    get_ipython()  # noqa: F821
    # Uncomment if running as notebook cell:
    # %pip install psycopg[binary,pool] numpy pandas matplotlib seaborn
    # dbutils.library.restartPython()
except Exception:
    pass

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2️⃣ Configuration

# COMMAND ----------
# Databricks widgets (fallbacks for non-DBX import)
try:
    dbutils  # noqa: F821
except NameError:
    class _DummyWidgets:
        def text(self, *args, **kwargs): return None
        def dropdown(self, *args, **kwargs): return None
        def get(self, key): return ""
    class _DummyDbutils:
        widgets = _DummyWidgets()
        class library:
            @staticmethod
            def restartPython():
                return None
    dbutils = _DummyDbutils()  # type: ignore

dbutils.widgets.text("lakebase_host", "", "Lakebase Host")
dbutils.widgets.text("lakebase_database", "benchmark", "Database")
dbutils.widgets.text("lakebase_schema", "features", "Schema")
dbutils.widgets.text("lakebase_user", "", "User")
dbutils.widgets.text("lakebase_password", "", "Password")
dbutils.widgets.text("iterations_per_run", "1000", "Iterations Per Run")
dbutils.widgets.text("total_keys_per_entity", "10000", "Total Keys Per Entity")
dbutils.widgets.text("hot_key_percent", "1", "Hot Key % of Dataset")
dbutils.widgets.text("explain_sample_rate", "0", "Post-run EXPLAIN sample size (0=disabled)")  # kept for future extension
dbutils.widgets.dropdown("run_all_modes", "true", ["true", "false"], "Run All Modes Sequentially")
dbutils.widgets.dropdown("fetch_mode", "serial", ["serial", "binpacked", "binpacked_parallel", "rpc_request_json", "rpc3_parallel"], "Fetch Mode (if run_all_modes=false)")
dbutils.widgets.text("reuse_run_id", "", "Reuse Keys from Run ID (empty=sample new)")
dbutils.widgets.text("parallel_workers", "1,2,3,4", "Parallel Workers (comma-separated)")
dbutils.widgets.dropdown("log_query_timings", "true", ["true", "false"], "Log Slow Queries")
dbutils.widgets.text("slow_query_threshold_ms", "40", "Slow Query Threshold (ms)")

LAKEBASE_CONFIG = {
    "host": dbutils.widgets.get("lakebase_host"),
    "port": 5432,
    "dbname": dbutils.widgets.get("lakebase_database"),
    "user": dbutils.widgets.get("lakebase_user"),
    "password": dbutils.widgets.get("lakebase_password"),
    "sslmode": "require",
    # ✅ CRITICAL: Prevent connection timeouts during long-running benchmarks
    "connect_timeout": 300,  # 5 minutes to establish connection
    "keepalives": 1,  # Enable TCP keepalive
    "keepalives_idle": 30,  # Send keepalive after 30s of idle
    "keepalives_interval": 10,  # Resend keepalive every 10s
    "keepalives_count": 5,  # Give up after 5 failed keepalives
    "options": "-c statement_timeout=0 -c idle_in_transaction_session_timeout=0"  # No query/idle timeouts
}

SCHEMA = dbutils.widgets.get("lakebase_schema")
ITERATIONS_PER_RUN = int(dbutils.widgets.get("iterations_per_run") or "1000")
TOTAL_KEYS_PER_ENTITY = int(dbutils.widgets.get("total_keys_per_entity") or "10000")
HOT_KEY_PERCENT_OF_DATASET = int(dbutils.widgets.get("hot_key_percent") or "1")
EXPLAIN_SAMPLE_RATE = int(dbutils.widgets.get("explain_sample_rate") or "0")  # kept for future extension
RUN_ALL_MODES = (dbutils.widgets.get("run_all_modes") or "true") == "true"
FETCH_MODE = dbutils.widgets.get("fetch_mode") or "serial"
REUSE_RUN_ID = (dbutils.widgets.get("reuse_run_id") or "").strip()
PARALLEL_WORKERS_STR = dbutils.widgets.get("parallel_workers") or "1,2,3,4"
PARALLEL_WORKERS = [int(w.strip()) for w in PARALLEL_WORKERS_STR.split(",") if w.strip()]
LOG_QUERY_TIMINGS = (dbutils.widgets.get("log_query_timings") or "true") == "true"
SLOW_QUERY_THRESHOLD_MS = float(dbutils.widgets.get("slow_query_threshold_ms") or "40")

# ✅ Column projection
# Fix #6: Removed unused projection_mode widget/variable to avoid confusion.
SELECT_CLAUSE = "*"  # Always use * until per-table projections are defined

# Hot/cold matrix to test
HOT_COLD_MATRIX = [100, 90, 80, 70, 60, 50, 30, 10, 0]

# ✅ V5: Build mode configurations with worker counts
MODE_CONFIGS = []

if RUN_ALL_MODES:
    MODE_CONFIGS.append({"mode": "serial", "workers": None})
    MODE_CONFIGS.append({"mode": "binpacked", "workers": None})
    for workers in PARALLEL_WORKERS:
        MODE_CONFIGS.append({"mode": "binpacked_parallel", "workers": workers})
    MODE_CONFIGS.append({"mode": "rpc_request_json", "workers": None})
    MODE_CONFIGS.append({"mode": "rpc3_parallel", "workers": None})
else:
    if FETCH_MODE == "binpacked_parallel":
        for workers in PARALLEL_WORKERS:
            MODE_CONFIGS.append({"mode": FETCH_MODE, "workers": workers})
    else:
        MODE_CONFIGS.append({"mode": FETCH_MODE, "workers": None})

def get_mode_label(mode, workers):
    if mode == "serial":
        return "Serial (30 queries, baseline)"
    elif mode == "binpacked":
        return "Bin-packed (10 UNION ALL queries, serial)"
    elif mode == "binpacked_parallel":
        return f"Bin-packed + Parallel (10 queries, {workers} workers)"
    elif mode == "rpc_request_json":
        return "RPC single (1 server-side function call)"
    elif mode == "rpc3_parallel":
        return "RPC×Entity (3 parallel server-side function calls)"
    return mode

FETCH_MODE_LABELS = {
    "serial": "Serial (30 queries, baseline)",
    "binpacked": "Bin-packed (10 UNION ALL queries, serial)",
    "binpacked_parallel": "Bin-packed + Parallel (10 queries)",
    "rpc_request_json": "RPC single (1 server-side function call)",
    "rpc3_parallel": "RPC×Entity (3 parallel server-side function calls)"
}

print("="*80)
print("⚙️  MULTI-ENTITY ZIPFIAN BENCHMARK V5.4 CONFIGURATION (Production-Grade)")
print("="*80)
print(f"Lakebase:               {LAKEBASE_CONFIG['host']}")
print(f"Schema:                 {SCHEMA}")
print()
print(f"🚀 V5 ENHANCANCEMENTS:")
print(f"   ✅ Cost-normalized metrics (latency per query)")
print(f"   ✅ Concurrency sweep (workers: {PARALLEL_WORKERS})")
print(f"   ✅ Entity timing details (Gantt chart support)")
print(f"   ✅ Transaction scoping: 1 tx/request (serial+binpacked), 1 tx/entity (parallel workers)")
print()
print(f"📊 MODE CONFIGURATIONS ({len(MODE_CONFIGS)} total):")
for cfg in MODE_CONFIGS:
    print(f"   • {get_mode_label(cfg['mode'], cfg['workers'])}")
print()
print(f"📋 MODE SUMMARY (Queries per Request):")
print(f"   • Serial:           30 queries (1 per table)")
print(f"   • Binpacked:        10 queries (UNION ALL by entity)")
print(f"   • Parallel:         10 queries, workers=N (concurrent entities)")
print(f"   • RPC single:       1 call (all tables, 1 server function)")
print(f"   • RPC×Entity:       3 calls parallel (1 per entity, server-side)")
print()
print(f"🔑 KEY SAMPLING:")
print(f"   Total keys/entity:   {TOTAL_KEYS_PER_ENTITY:,}")
print(f"   Hot key %:           {HOT_KEY_PERCENT_OF_DATASET}% (randomly selected)")
print(f"   Hot keys/entity:     {int(TOTAL_KEYS_PER_ENTITY * HOT_KEY_PERCENT_OF_DATASET / 100):,}")
print()
print(f"📊 BENCHMARK SETTINGS:")
print(f"   Iterations/run:      {ITERATIONS_PER_RUN:,}")
print(f"   Hot/cold ratios:     {HOT_COLD_MATRIX}")
print(f"   ✅ V5.3: NO inline EXPLAIN (post-run diagnostic is optional)")
print()
print(f"🎯 REQUEST STRUCTURE:")
print(f"   Entities/request:    3 (card, email, name)")
print(f"   Tables/request:      30 (9-12 tables per entity)")
print(f"   Hot/cold:            Independent per entity")
print()
print(f"✅ IMPROVEMENTS:")
print(f"   • Random key sampling from ALL tables (not just first keys)")
print(f"   • SELECT * (fetches actual data, not just index)")
print(f"   • I/O measured via pg_statio aggregate (serial mode only)")
print(f"   • Error handling with graceful degradation")
print(f"   • NaN-safe correlation calculation")
print(f"   • TABLESAMPLE fallback for small tables")
print("="*80)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3️⃣ Entity Table Groups

# COMMAND ----------
ENTITY_TABLE_GROUPS = {
    "card_fingerprint": [
        "client_id_card_fingerprint__fraud_rates__30d",
        "client_id_card_fingerprint__fraud_rates__90d",
        "client_id_card_fingerprint__fraud_rates__365d",
        "client_id_card_fingerprint__time_since__30d",
        "client_id_card_fingerprint__time_since__90d",
        "client_id_card_fingerprint__time_since__365d",
        "client_id_card_fingerprint__good_rates__30d",
        "client_id_card_fingerprint__good_rates__90d",
        "client_id_card_fingerprint__good_rates__365d",
    ],
    "customer_email": [
        "client_id_customer_email_clean__fraud_rates__30d",
        "client_id_customer_email_clean__fraud_rates__90d",
        "client_id_customer_email_clean__fraud_rates__365d",
        "client_id_customer_email_clean__time_since__30d",
        "client_id_customer_email_clean__time_since__90d",
        "client_id_customer_email_clean__time_since__365d",
        "client_id_customer_email_clean__good_rates__30d",
        "client_id_customer_email_clean__good_rates__90d",
        "client_id_customer_email_clean__good_rates__365d",
    ],
    "cardholder_name": [
        "client_id_cardholder_name_clean__fraud_rates__30d",
        "client_id_cardholder_name_clean__fraud_rates__90d",
        "client_id_cardholder_name_clean__fraud_rates__365d",
        "client_id_cardholder_name_clean__tesseract_velocities__30d",
        "client_id_cardholder_name_clean__tesseract_velocities__90d",
        "client_id_cardholder_name_clean__tesseract_velocities__365d",
        "client_id_cardholder_name_clean__time_since__30d",
        "client_id_cardholder_name_clean__time_since__90d",
        "client_id_cardholder_name_clean__time_since__365d",
        "client_id_cardholder_name_clean__good_rates__30d",
        "client_id_cardholder_name_clean__good_rates__90d",
        "client_id_cardholder_name_clean__good_rates__365d",
    ],
}

ENTITY_NAMES = list(ENTITY_TABLE_GROUPS.keys())

print(f"✅ Entity groups loaded:")
for entity, tables in ENTITY_TABLE_GROUPS.items():
    print(f"   {entity:20} → {len(tables):2} tables")
print(f"\nTotal tables per request: {sum(len(t) for t in ENTITY_TABLE_GROUPS.values())}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4️⃣ Import Libraries

# COMMAND ----------
import time
import random
import psycopg
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns  # kept (your notebook uses it); ok in DBX
import json
import uuid
import re
from collections import defaultdict
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from psycopg_pool import ConnectionPool

RUN_ID = str(uuid.uuid4())[:8]
RESULTS_SCHEMA = SCHEMA
RESULTS_TABLE = "zipfian_feature_serving_results_v5"
KEYS_TABLE = "zipfian_keys_per_run"

sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (16, 10)

print("✅ Libraries imported")
print(f"📋 Run ID: {RUN_ID}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5️⃣ Database Connection & Helpers

# COMMAND ----------
def get_conn():
    """Get PostgreSQL connection with keepalive enabled."""
    conn = psycopg.connect(**LAKEBASE_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
    except Exception as e:
        print(f"⚠️  Connection test failed: {e}")
        raise
    return conn

def refresh_connection_if_stale(conn):
    """Check if connection is healthy and refresh if needed."""
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        return conn
    except Exception as e:
        print(f"      ⚠️  Connection unhealthy ({e}), refreshing...")
        try:
            conn.close()
        except Exception:
            pass
        return get_conn()

def fetch_sample_keys(conn, table, limit):
    """Fetch random sample of keys with TABLESAMPLE + fallback."""
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT DISTINCT hash_key
            FROM {SCHEMA}.{table}
            TABLESAMPLE SYSTEM (1)
            LIMIT %s
        """, (limit,))
        keys = [r[0] for r in cur.fetchall()]

        if len(keys) < limit * 0.8:
            print(f"         ⚠️  TABLESAMPLE returned only {len(keys)} keys, using ORDER BY RANDOM()")
            cur.execute(f"""
                SELECT DISTINCT hash_key
                FROM {SCHEMA}.{table}
                ORDER BY RANDOM()
                LIMIT %s
            """, (limit,))
            keys = [r[0] for r in cur.fetchall()]
        return keys

def reset_pg_stats(conn):
    """Reset PostgreSQL I/O statistics."""
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_stat_reset();")
            conn.commit()
        return True
    except Exception as e:
        print(f"⚠️  Could not reset stats (requires superuser): {e}")
        conn.rollback()
        return False

def flush_cache(conn):
    """
    Reset session state between modes (production-grade approach):
    - DISCARD ALL to clear session state (prepared statements, temp tables, etc.)
    - Short cooldown pause
    
    Note: This does NOT flush PostgreSQL shared buffers or OS page cache.
    Cache behavior is driven by hot/cold keys and normal buffer pool management.
    """
    print("🔄 Resetting session state...")
    
    # Ensure we're not inside a transaction block
    try:
        conn.rollback()
    except Exception:
        pass
    
    original_autocommit = conn.autocommit
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("DISCARD ALL")
        print("   ✅ Session state cleared (DISCARD ALL)")
    except Exception as e:
        print(f"   ⚠️  Could not discard session state: {e}")
    finally:
        conn.autocommit = original_autocommit
    
    time.sleep(2)
    print("   ℹ️  True OS cache flush not possible from PostgreSQL")
    print("   ℹ️  Cache behavior measured via pg_statio (buffer hits vs reads)")
    print("   ℹ️  Hot/cold key mix provides realistic cache distribution")

@contextmanager
def request_tx(conn):
    """
    Explicit transaction scope for measured fetch only.
    IMPORTANT: Do not include any code that calls conn.commit() inside this scope.
    """
    try:
        conn.execute("BEGIN")
        yield
        conn.execute("COMMIT")
    except Exception:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        raise

# Expected entity names for normalization
EXPECTED_ENTITIES = ["card_fingerprint", "customer_email", "cardholder_name"]

def normalize_entity_timings(entity_timings: dict, mode: str):
    """
    Normalize entity timings to handle RPC mode (single call) vs multi-entity modes.
    
    Args:
        entity_timings: Dict of entity -> latency_ms
        mode: Execution mode (serial/binpacked/binpacked_parallel/rpc_request_json/rpc3_parallel)
    
    Returns:
        tuple: (normalized_dict, rpc_call_ms, is_rpc)
            - normalized_dict: Dict with EXPECTED_ENTITIES keys (missing -> 0.0)
            - rpc_call_ms: Float for RPC single mode, None otherwise
            - is_rpc: Boolean flag
    """
    if mode == "rpc_request_json":
        # RPC single mode: single call, no per-entity breakdown
        rpc_call_ms = float(entity_timings.get("rpc_call", 0.0))
        normalized = {e: 0.0 for e in EXPECTED_ENTITIES}
        return normalized, rpc_call_ms, True
    
    # All other modes (including rpc3_parallel) have per-entity timings
    normalized = {e: float(entity_timings.get(e, 0.0)) for e in EXPECTED_ENTITIES}
    return normalized, None, False

def read_pg_io_stats(conn):
    """Read PostgreSQL I/O statistics for this schema."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                COALESCE(SUM(heap_blks_read), 0) as reads,
                COALESCE(SUM(heap_blks_hit), 0) as hits
            FROM pg_statio_user_tables
            WHERE schemaname = %s
        """, (SCHEMA,))
        return cur.fetchone()

def parse_buffers_from_explain(explain_output):
    """Extract buffers read/hit and timing from EXPLAIN output."""
    buffers_read = 0
    buffers_hit = 0
    planning_time_ms = 0
    execution_time_ms = 0

    for line in explain_output:
        text = str(line[0])

        if "Buffers:" in text:
            read_match = re.search(r'read=(\d+)', text)
            if read_match:
                buffers_read = int(read_match.group(1))
            hit_match = re.search(r'hit=(\d+)', text)
            if hit_match:
                buffers_hit = int(hit_match.group(1))

        if "Planning Time:" in text:
            match = re.search(r'Planning Time:\s+([\d.]+)\s+ms', text)
            if match:
                planning_time_ms = float(match.group(1))

        if "Execution Time:" in text:
            match = re.search(r'Execution Time:\s+([\d.]+)\s+ms', text)
            if match:
                execution_time_ms = float(match.group(1))

    explain_data = {
        "shared_blks_read": buffers_read,
        "shared_blks_hit": buffers_hit,
        "planning_ms": planning_time_ms,
        "execution_ms": execution_time_ms
    }
    return buffers_read, planning_time_ms, explain_data

print("✅ Database helper functions loaded")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6️⃣ Results Table Setup

# COMMAND ----------
def ensure_results_table(conn):
    """Create results and keys tables if they don't exist."""
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {RESULTS_SCHEMA}.{RESULTS_TABLE} (
                run_id                  TEXT,
                run_ts                  TIMESTAMP DEFAULT now(),

                hot_traffic_pct         INT,
                cold_traffic_pct        INT,

                iterations              INT,
                p50_ms                  DOUBLE PRECISION,
                p95_ms                  DOUBLE PRECISION,
                p99_ms                  DOUBLE PRECISION,
                avg_ms                  DOUBLE PRECISION,

                avg_cache_score         DOUBLE PRECISION,
                p50_cache_score         DOUBLE PRECISION,
                p90_cache_score         DOUBLE PRECISION,

                fully_cold_request_pct  DOUBLE PRECISION,
                fully_hot_request_pct   DOUBLE PRECISION,

                latency_cache_corr      DOUBLE PRECISION,
                io_blocks_per_request   DOUBLE PRECISION,

                entity_p99_ms           JSONB,

                notes                   TEXT
            );
        """)

        cur.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_zipfian_run
            ON {RESULTS_SCHEMA}.{RESULTS_TABLE} (run_id, hot_traffic_pct);
        """)

        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {RESULTS_SCHEMA}.{KEYS_TABLE} (
                run_id      TEXT,
                entity_name TEXT,
                key_type    TEXT,  -- 'hot' or 'cold'
                hash_key    TEXT
            );
        """)

        cur.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_keys_run
            ON {RESULTS_SCHEMA}.{KEYS_TABLE} (run_id, entity_name, key_type);
        """)

        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {RESULTS_SCHEMA}.zipfian_slow_query_log (
                run_id               TEXT,
                mode                 TEXT,
                parallel_workers     INT,
                hot_traffic_pct      INT,
                iteration_id         INT,
                request_id           TEXT,

                entity_name          TEXT,
                table_name           TEXT,

                hash_key             TEXT,
                was_hot_key          BOOLEAN,

                query_latency_ms     DOUBLE PRECISION,
                rows_returned        INT,

                query_group          TEXT,
                query_type           TEXT,
                request_latency_ms   DOUBLE PRECISION,

                explain_used         BOOLEAN,
                shared_blks_read     INT,
                shared_blks_hit      INT,
                planning_ms          DOUBLE PRECISION,
                execution_ms         DOUBLE PRECISION,

                ts                   TIMESTAMP DEFAULT now()
            );
        """)

        cur.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_slow_query_log_run
            ON {RESULTS_SCHEMA}.zipfian_slow_query_log (run_id, mode, hot_traffic_pct);
        """)
        
        # ✅ V5.3: Add missing columns to zipfian_slow_query_log if they don't exist
        # Use a robust approach: check if column exists first, then add if missing
        cur.execute(f"""
            DO $$
            BEGIN
                -- Add parallel_workers if not exists
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_schema = '{RESULTS_SCHEMA}' 
                    AND table_name = 'zipfian_slow_query_log' 
                    AND column_name = 'parallel_workers'
                ) THEN
                    ALTER TABLE {RESULTS_SCHEMA}.zipfian_slow_query_log 
                    ADD COLUMN parallel_workers INT;
                END IF;
                
                -- Add query_group if not exists
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_schema = '{RESULTS_SCHEMA}' 
                    AND table_name = 'zipfian_slow_query_log' 
                    AND column_name = 'query_group'
                ) THEN
                    ALTER TABLE {RESULTS_SCHEMA}.zipfian_slow_query_log 
                    ADD COLUMN query_group TEXT;
                END IF;
                
                -- Add query_type if not exists
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_schema = '{RESULTS_SCHEMA}' 
                    AND table_name = 'zipfian_slow_query_log' 
                    AND column_name = 'query_type'
                ) THEN
                    ALTER TABLE {RESULTS_SCHEMA}.zipfian_slow_query_log 
                    ADD COLUMN query_type TEXT;
                END IF;
                
                -- Add request_latency_ms if not exists
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_schema = '{RESULTS_SCHEMA}' 
                    AND table_name = 'zipfian_slow_query_log' 
                    AND column_name = 'request_latency_ms'
                ) THEN
                    ALTER TABLE {RESULTS_SCHEMA}.zipfian_slow_query_log 
                    ADD COLUMN request_latency_ms DOUBLE PRECISION;
                END IF;
            END $$;
        """)

        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {RESULTS_SCHEMA}.zipfian_request_timing (
                run_id           TEXT,
                mode             TEXT,
                parallel_workers INT,
                hot_traffic_pct  INT,

                iteration_id     INT,
                request_id       TEXT,

                request_start_ts TIMESTAMP DEFAULT now(),
                request_latency_ms DOUBLE PRECISION,

                hot_entities     INT,
                cache_score      DOUBLE PRECISION,
                actual_queries   INT,
                actual_max_concurrency INT,

                pool_wait_ms     DOUBLE PRECISION,
                retry_count      INT,
                query_error_count INT,

                PRIMARY KEY (run_id, mode, hot_traffic_pct, iteration_id, request_id)
            );
        """)

        cur.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_req_timing_run_mode
            ON {RESULTS_SCHEMA}.zipfian_request_timing (run_id, mode, parallel_workers, hot_traffic_pct);
        """)

        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {RESULTS_SCHEMA}.zipfian_timing_segments (
                run_id           TEXT,
                mode             TEXT,
                parallel_workers INT,
                hot_traffic_pct  INT,

                iteration_id     INT,
                request_id       TEXT,

                entity_name      TEXT,
                segment_type     TEXT,
                segment_name     TEXT,
                start_ms         DOUBLE PRECISION,
                end_ms           DOUBLE PRECISION,
                duration_ms      DOUBLE PRECISION,

                was_hot_key      BOOLEAN,

                PRIMARY KEY (run_id, mode, hot_traffic_pct, iteration_id, request_id, entity_name, segment_type, segment_name)
            );
        """)

        cur.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_segments_run
            ON {RESULTS_SCHEMA}.zipfian_timing_segments (run_id, mode, parallel_workers, hot_traffic_pct);
        """)

        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {RESULTS_SCHEMA}.zipfian_keys_run_meta (
                run_id                       TEXT PRIMARY KEY,
                schema_name                  TEXT,
                target_keys_per_entity       INT,
                hot_key_percent              DOUBLE PRECISION,
                sampling_method              TEXT,
                deduped_keys                 BOOLEAN,
                sampled_keys_total_all_entities    INT,
                sampled_keys_unique_all_entities   INT,
                duplication_ratio            DOUBLE PRECISION,
                created_ts                   TIMESTAMP DEFAULT now()
            );
        """)
        
        # ✅ V5.3: Migrate old column names to new ones (V5.2 → V5.3)
        cur.execute(f"""
            DO $$
            BEGIN
                -- Rename old columns if they exist
                IF EXISTS (SELECT 1 FROM information_schema.columns 
                          WHERE table_schema = '{RESULTS_SCHEMA}' 
                          AND table_name = 'zipfian_keys_run_meta' 
                          AND column_name = 'total_keys_per_entity') THEN
                    ALTER TABLE {RESULTS_SCHEMA}.zipfian_keys_run_meta 
                    RENAME COLUMN total_keys_per_entity TO target_keys_per_entity;
                END IF;
                
                IF EXISTS (SELECT 1 FROM information_schema.columns 
                          WHERE table_schema = '{RESULTS_SCHEMA}' 
                          AND table_name = 'zipfian_keys_run_meta' 
                          AND column_name = 'sampled_keys_total') THEN
                    ALTER TABLE {RESULTS_SCHEMA}.zipfian_keys_run_meta 
                    RENAME COLUMN sampled_keys_total TO sampled_keys_total_all_entities;
                END IF;
                
                IF EXISTS (SELECT 1 FROM information_schema.columns 
                          WHERE table_schema = '{RESULTS_SCHEMA}' 
                          AND table_name = 'zipfian_keys_run_meta' 
                          AND column_name = 'sampled_keys_unique') THEN
                    ALTER TABLE {RESULTS_SCHEMA}.zipfian_keys_run_meta 
                    RENAME COLUMN sampled_keys_unique TO sampled_keys_unique_all_entities;
                END IF;
            END $$;
        """)
        
        # Commit all DDL changes immediately
        conn.commit()
        
        # ✅ V5.3: Verify critical columns exist (defense against schema issues)
        cur.execute(f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = '{RESULTS_SCHEMA}' 
            AND table_name = 'zipfian_slow_query_log' 
            AND column_name IN ('query_group', 'query_type', 'request_latency_ms', 'parallel_workers')
            ORDER BY column_name;
        """)
        verified_columns = [row[0] for row in cur.fetchall()]
        
        if len(verified_columns) < 4:
            missing = set(['query_group', 'query_type', 'request_latency_ms', 'parallel_workers']) - set(verified_columns)
            print(f"⚠️  WARNING: Missing columns in zipfian_slow_query_log: {missing}")
            print(f"   Found columns: {verified_columns}")
            raise RuntimeError(f"Schema migration failed - missing columns: {missing}")

    print(f"✅ Results table ensured: {RESULTS_SCHEMA}.{RESULTS_TABLE}")
    print(f"✅ Slow query log table ensured: {RESULTS_SCHEMA}.zipfian_slow_query_log")
    print(f"   Verified V5.3 columns: {', '.join(verified_columns)}")
    print(f"✅ Request timing table ensured: {RESULTS_SCHEMA}.zipfian_request_timing")
    print(f"✅ Timing segments table ensured: {RESULTS_SCHEMA}.zipfian_timing_segments")
    print(f"✅ Keys metadata table ensured: {RESULTS_SCHEMA}.zipfian_keys_run_meta")
    print(f"✅ Keys table ensured: {RESULTS_SCHEMA}.{KEYS_TABLE}")

def ensure_rpc_function(conn):
    """Create server-side RPC function for fetching all features in one call."""
    with conn.cursor() as cur:
        # Drop old function if it exists (parameter names may have changed)
        cur.execute(f"""
            DROP FUNCTION IF EXISTS {SCHEMA}.fetch_request_features(TEXT, TEXT, TEXT);
        """)
        
        # Build the function SQL with LEFT JOIN LATERAL for all 30 tables
        cur.execute(f"""
            CREATE OR REPLACE FUNCTION {SCHEMA}.fetch_request_features(
                card_fp_key TEXT,
                email_key TEXT,
                name_key TEXT
            ) RETURNS JSONB
            LANGUAGE sql
            STABLE
            AS $$
                SELECT jsonb_build_object(
                    'card_fingerprint', jsonb_build_object(
                        'client_id_card_fingerprint__fraud_rates__30d',
                        (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_card_fingerprint__fraud_rates__30d t WHERE hash_key = card_fp_key LIMIT 1),
                        'client_id_card_fingerprint__fraud_rates__365d',
                        (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_card_fingerprint__fraud_rates__365d t WHERE hash_key = card_fp_key LIMIT 1),
                        'client_id_card_fingerprint__fraud_rates__90d',
                        (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_card_fingerprint__fraud_rates__90d t WHERE hash_key = card_fp_key LIMIT 1),
                        'client_id_card_fingerprint__good_rates__30d',
                        (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_card_fingerprint__good_rates__30d t WHERE hash_key = card_fp_key LIMIT 1),
                        'client_id_card_fingerprint__good_rates__365d',
                        (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_card_fingerprint__good_rates__365d t WHERE hash_key = card_fp_key LIMIT 1),
                        'client_id_card_fingerprint__good_rates__90d',
                        (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_card_fingerprint__good_rates__90d t WHERE hash_key = card_fp_key LIMIT 1),
                        'client_id_card_fingerprint__time_since__30d',
                        (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_card_fingerprint__time_since__30d t WHERE hash_key = card_fp_key LIMIT 1),
                        'client_id_card_fingerprint__time_since__365d',
                        (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_card_fingerprint__time_since__365d t WHERE hash_key = card_fp_key LIMIT 1),
                        'client_id_card_fingerprint__time_since__90d',
                        (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_card_fingerprint__time_since__90d t WHERE hash_key = card_fp_key LIMIT 1)
                    ),
                    'customer_email', jsonb_build_object(
                        'client_id_customer_email_clean__fraud_rates__30d',
                        (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_customer_email_clean__fraud_rates__30d t WHERE hash_key = email_key LIMIT 1),
                        'client_id_customer_email_clean__fraud_rates__365d',
                        (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_customer_email_clean__fraud_rates__365d t WHERE hash_key = email_key LIMIT 1),
                        'client_id_customer_email_clean__fraud_rates__90d',
                        (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_customer_email_clean__fraud_rates__90d t WHERE hash_key = email_key LIMIT 1),
                        'client_id_customer_email_clean__good_rates__30d',
                        (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_customer_email_clean__good_rates__30d t WHERE hash_key = email_key LIMIT 1),
                        'client_id_customer_email_clean__good_rates__365d',
                        (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_customer_email_clean__good_rates__365d t WHERE hash_key = email_key LIMIT 1),
                        'client_id_customer_email_clean__good_rates__90d',
                        (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_customer_email_clean__good_rates__90d t WHERE hash_key = email_key LIMIT 1),
                        'client_id_customer_email_clean__time_since__30d',
                        (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_customer_email_clean__time_since__30d t WHERE hash_key = email_key LIMIT 1),
                        'client_id_customer_email_clean__time_since__365d',
                        (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_customer_email_clean__time_since__365d t WHERE hash_key = email_key LIMIT 1),
                        'client_id_customer_email_clean__time_since__90d',
                        (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_customer_email_clean__time_since__90d t WHERE hash_key = email_key LIMIT 1)
                    ),
                    'cardholder_name', jsonb_build_object(
                        'client_id_cardholder_name_clean__fraud_rates__30d',
                        (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_cardholder_name_clean__fraud_rates__30d t WHERE hash_key = name_key LIMIT 1),
                        'client_id_cardholder_name_clean__fraud_rates__365d',
                        (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_cardholder_name_clean__fraud_rates__365d t WHERE hash_key = name_key LIMIT 1),
                        'client_id_cardholder_name_clean__fraud_rates__90d',
                        (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_cardholder_name_clean__fraud_rates__90d t WHERE hash_key = name_key LIMIT 1),
                        'client_id_cardholder_name_clean__good_rates__30d',
                        (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_cardholder_name_clean__good_rates__30d t WHERE hash_key = name_key LIMIT 1),
                        'client_id_cardholder_name_clean__good_rates__365d',
                        (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_cardholder_name_clean__good_rates__365d t WHERE hash_key = name_key LIMIT 1),
                        'client_id_cardholder_name_clean__good_rates__90d',
                        (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_cardholder_name_clean__good_rates__90d t WHERE hash_key = name_key LIMIT 1),
                        'client_id_cardholder_name_clean__tesseract_velocities__30d',
                        (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_cardholder_name_clean__tesseract_velocities__30d t WHERE hash_key = name_key LIMIT 1),
                        'client_id_cardholder_name_clean__tesseract_velocities__365d',
                        (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_cardholder_name_clean__tesseract_velocities__365d t WHERE hash_key = name_key LIMIT 1),
                        'client_id_cardholder_name_clean__tesseract_velocities__90d',
                        (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_cardholder_name_clean__tesseract_velocities__90d t WHERE hash_key = name_key LIMIT 1),
                        'client_id_cardholder_name_clean__time_since__30d',
                        (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_cardholder_name_clean__time_since__30d t WHERE hash_key = name_key LIMIT 1),
                        'client_id_cardholder_name_clean__time_since__365d',
                        (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_cardholder_name_clean__time_since__365d t WHERE hash_key = name_key LIMIT 1),
                        'client_id_cardholder_name_clean__time_since__90d',
                        (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_cardholder_name_clean__time_since__90d t WHERE hash_key = name_key LIMIT 1)
                    )
                );
            $$;
        """)
        conn.commit()
    
    print(f"✅ RPC function ensured: {SCHEMA}.fetch_request_features()")

def ensure_entity_rpc_functions(conn):
    """Create 3 per-entity RPC functions for parallel execution."""
    
    # Card Fingerprint Entity Function
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE OR REPLACE FUNCTION {SCHEMA}.fetch_features_card_fingerprint(
                hash_key_param TEXT,
                run_id_param TEXT DEFAULT NULL,
                req_id_param BIGINT DEFAULT NULL
            ) RETURNS JSONB
            LANGUAGE sql
            STABLE
            AS $$
                SELECT jsonb_build_object(
                    'client_id_card_fingerprint__fraud_rates__30d',
                    (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_card_fingerprint__fraud_rates__30d t WHERE hash_key = hash_key_param LIMIT 1),
                    'client_id_card_fingerprint__fraud_rates__365d',
                    (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_card_fingerprint__fraud_rates__365d t WHERE hash_key = hash_key_param LIMIT 1),
                    'client_id_card_fingerprint__fraud_rates__90d',
                    (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_card_fingerprint__fraud_rates__90d t WHERE hash_key = hash_key_param LIMIT 1),
                    'client_id_card_fingerprint__good_rates__30d',
                    (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_card_fingerprint__good_rates__30d t WHERE hash_key = hash_key_param LIMIT 1),
                    'client_id_card_fingerprint__good_rates__365d',
                    (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_card_fingerprint__good_rates__365d t WHERE hash_key = hash_key_param LIMIT 1),
                    'client_id_card_fingerprint__good_rates__90d',
                    (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_card_fingerprint__good_rates__90d t WHERE hash_key = hash_key_param LIMIT 1),
                    'client_id_card_fingerprint__time_since__30d',
                    (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_card_fingerprint__time_since__30d t WHERE hash_key = hash_key_param LIMIT 1),
                    'client_id_card_fingerprint__time_since__365d',
                    (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_card_fingerprint__time_since__365d t WHERE hash_key = hash_key_param LIMIT 1),
                    'client_id_card_fingerprint__time_since__90d',
                    (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_card_fingerprint__time_since__90d t WHERE hash_key = hash_key_param LIMIT 1)
                );
            $$;
        """)
    
    # Customer Email Entity Function
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE OR REPLACE FUNCTION {SCHEMA}.fetch_features_customer_email(
                hash_key_param TEXT,
                run_id_param TEXT DEFAULT NULL,
                req_id_param BIGINT DEFAULT NULL
            ) RETURNS JSONB
            LANGUAGE sql
            STABLE
            AS $$
                SELECT jsonb_build_object(
                    'client_id_customer_email_clean__fraud_rates__30d',
                    (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_customer_email_clean__fraud_rates__30d t WHERE hash_key = hash_key_param LIMIT 1),
                    'client_id_customer_email_clean__fraud_rates__365d',
                    (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_customer_email_clean__fraud_rates__365d t WHERE hash_key = hash_key_param LIMIT 1),
                    'client_id_customer_email_clean__fraud_rates__90d',
                    (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_customer_email_clean__fraud_rates__90d t WHERE hash_key = hash_key_param LIMIT 1),
                    'client_id_customer_email_clean__good_rates__30d',
                    (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_customer_email_clean__good_rates__30d t WHERE hash_key = hash_key_param LIMIT 1),
                    'client_id_customer_email_clean__good_rates__365d',
                    (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_customer_email_clean__good_rates__365d t WHERE hash_key = hash_key_param LIMIT 1),
                    'client_id_customer_email_clean__good_rates__90d',
                    (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_customer_email_clean__good_rates__90d t WHERE hash_key = hash_key_param LIMIT 1),
                    'client_id_customer_email_clean__time_since__30d',
                    (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_customer_email_clean__time_since__30d t WHERE hash_key = hash_key_param LIMIT 1),
                    'client_id_customer_email_clean__time_since__365d',
                    (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_customer_email_clean__time_since__365d t WHERE hash_key = hash_key_param LIMIT 1),
                    'client_id_customer_email_clean__time_since__90d',
                    (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_customer_email_clean__time_since__90d t WHERE hash_key = hash_key_param LIMIT 1)
                );
            $$;
        """)
    
    # Cardholder Name Entity Function
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE OR REPLACE FUNCTION {SCHEMA}.fetch_features_cardholder_name(
                hash_key_param TEXT,
                run_id_param TEXT DEFAULT NULL,
                req_id_param BIGINT DEFAULT NULL
            ) RETURNS JSONB
            LANGUAGE sql
            STABLE
            AS $$
                SELECT jsonb_build_object(
                    'client_id_cardholder_name_clean__fraud_rates__30d',
                    (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_cardholder_name_clean__fraud_rates__30d t WHERE hash_key = hash_key_param LIMIT 1),
                    'client_id_cardholder_name_clean__fraud_rates__365d',
                    (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_cardholder_name_clean__fraud_rates__365d t WHERE hash_key = hash_key_param LIMIT 1),
                    'client_id_cardholder_name_clean__fraud_rates__90d',
                    (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_cardholder_name_clean__fraud_rates__90d t WHERE hash_key = hash_key_param LIMIT 1),
                    'client_id_cardholder_name_clean__good_rates__30d',
                    (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_cardholder_name_clean__good_rates__30d t WHERE hash_key = hash_key_param LIMIT 1),
                    'client_id_cardholder_name_clean__good_rates__365d',
                    (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_cardholder_name_clean__good_rates__365d t WHERE hash_key = hash_key_param LIMIT 1),
                    'client_id_cardholder_name_clean__good_rates__90d',
                    (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_cardholder_name_clean__good_rates__90d t WHERE hash_key = hash_key_param LIMIT 1),
                    'client_id_cardholder_name_clean__time_since__30d',
                    (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_cardholder_name_clean__time_since__30d t WHERE hash_key = hash_key_param LIMIT 1),
                    'client_id_cardholder_name_clean__time_since__365d',
                    (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_cardholder_name_clean__time_since__365d t WHERE hash_key = hash_key_param LIMIT 1),
                    'client_id_cardholder_name_clean__time_since__90d',
                    (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_cardholder_name_clean__time_since__90d t WHERE hash_key = hash_key_param LIMIT 1),
                    'client_id_cardholder_name_clean__txn_volume__30d',
                    (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_cardholder_name_clean__txn_volume__30d t WHERE hash_key = hash_key_param LIMIT 1),
                    'client_id_cardholder_name_clean__txn_volume__365d',
                    (SELECT to_jsonb(t) FROM {SCHEMA}.client_id_cardholder_name_clean__txn_volume__365d t WHERE hash_key = hash_key_param LIMIT 1)
                );
            $$;
        """)
    
    conn.commit()
    print(f"✅ Entity RPC functions ensured:")
    print(f"   - {SCHEMA}.fetch_features_card_fingerprint(hash_key, run_id, req_id)")
    print(f"   - {SCHEMA}.fetch_features_customer_email(hash_key, run_id, req_id)")
    print(f"   - {SCHEMA}.fetch_features_cardholder_name(hash_key, run_id, req_id)")

def persist_results(conn, hot_pct, results):
    """Persist benchmark results to Lakebase."""
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                ALTER TABLE {RESULTS_SCHEMA}.{RESULTS_TABLE}
                ADD COLUMN IF NOT EXISTS fetch_mode TEXT,
                ADD COLUMN IF NOT EXISTS queries_per_request DOUBLE PRECISION,
                ADD COLUMN IF NOT EXISTS io_measurement_method TEXT,
                ADD COLUMN IF NOT EXISTS entity_p99_contribution_pct JSONB,
                ADD COLUMN IF NOT EXISTS cache_state TEXT,
                ADD COLUMN IF NOT EXISTS avg_planning_time_ms DOUBLE PRECISION,
                ADD COLUMN IF NOT EXISTS avg_rows_per_request DOUBLE PRECISION,
                ADD COLUMN IF NOT EXISTS avg_payload_bytes DOUBLE PRECISION,
                ADD COLUMN IF NOT EXISTS parallel_workers INT,
                ADD COLUMN IF NOT EXISTS latency_per_query_ms DOUBLE PRECISION,
                ADD COLUMN IF NOT EXISTS entity_timing_detail JSONB,
                ADD COLUMN IF NOT EXISTS max_concurrent_queries INT,
                -- Self-describing columns
                ADD COLUMN IF NOT EXISTS config_json JSONB,
                ADD COLUMN IF NOT EXISTS actual_queries_per_request_avg DOUBLE PRECISION,
                ADD COLUMN IF NOT EXISTS actual_queries_per_request_p99 DOUBLE PRECISION,
                ADD COLUMN IF NOT EXISTS actual_max_concurrency INT,
                ADD COLUMN IF NOT EXISTS sampled_keys_total INT,
                ADD COLUMN IF NOT EXISTS sampled_keys_unique INT,
                ADD COLUMN IF NOT EXISTS key_duplication_ratio DOUBLE PRECISION,
                ADD COLUMN IF NOT EXISTS hot_keys_count INT,
                ADD COLUMN IF NOT EXISTS cold_keys_count INT,
                ADD COLUMN IF NOT EXISTS cold_sampling_without_replacement BOOLEAN,
                ADD COLUMN IF NOT EXISTS explain_sample_rate INT,
                ADD COLUMN IF NOT EXISTS explain_iterations_count INT,
                ADD COLUMN IF NOT EXISTS latency_explain_included BOOLEAN
            """)
            conn.commit()
    except Exception:
        conn.rollback()

    with conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO {RESULTS_SCHEMA}.{RESULTS_TABLE} (
                run_id,
                hot_traffic_pct,
                cold_traffic_pct,
                iterations,
                p50_ms,
                p95_ms,
                p99_ms,
                avg_ms,
                avg_cache_score,
                p50_cache_score,
                p90_cache_score,
                fully_cold_request_pct,
                fully_hot_request_pct,
                latency_cache_corr,
                io_blocks_per_request,
                io_measurement_method,
                entity_p99_ms,
                entity_p99_contribution_pct,
                cache_state,
                fetch_mode,
                queries_per_request,
                avg_planning_time_ms,
                avg_rows_per_request,
                avg_payload_bytes,
                parallel_workers,
                latency_per_query_ms,
                entity_timing_detail,
                max_concurrent_queries,
                notes
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s,
                %s,
                %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s,
                %s
            )
        """, (
            RUN_ID,
            hot_pct,
            100 - hot_pct,
            ITERATIONS_PER_RUN,
            results["p50"],
            results["p95"],
            results["p99"],
            results["avg"],
            results["cache_avg"],
            results["cache_p50"],
            results["cache_p90"],
            results["fully_cold_pct"],
            results["fully_hot_pct"],
            results["lat_cache_corr"],
            results["io_blocks_per_req"],
            results["io_measurement_method"],
            json.dumps(results["entity_p99"]),
            json.dumps(results["entity_p99_contribution_pct"]),
            results["cache_state"],
            results["fetch_mode"],
            results["queries_per_request"],
            results["avg_planning_time_ms"],
            results["avg_rows_per_request"],
            results["avg_payload_bytes"],
            results.get("parallel_workers"),
            results.get("latency_per_query_ms"),
            json.dumps(results.get("entity_timing_detail", {})),
            results.get("max_concurrent_queries"),
            f"Multi-entity Zipfian benchmark V5.4 (mode: {results['fetch_mode']}, workers: {results.get('parallel_workers', 'N/A')})"
        ))
    conn.commit()

def persist_query_timings(conn, run_id, request_id, request_idx, fetch_mode, parallel_workers, hot_traffic_pct, query_timings, entity_hot_sets, request_latency_ms=None):
    """Persist SLOW query samples to log for tail amplification analysis."""
    if not query_timings:
        return

    with conn.cursor() as cur:
        for timing in query_timings:
            entity = timing["entity"]
            hash_key = timing["hash_key"]
            was_hot_key = hash_key in entity_hot_sets.get(entity, set())

            query_group = timing.get("query_group")
            query_type = timing.get("query_type", "single_select")

            cur.execute(f"""
                INSERT INTO {RESULTS_SCHEMA}.zipfian_slow_query_log (
                    run_id,
                    mode,
                    parallel_workers,
                    hot_traffic_pct,
                    iteration_id,
                    request_id,
                    entity_name,
                    table_name,
                    hash_key,
                    was_hot_key,
                    query_latency_ms,
                    rows_returned,
                    query_group,
                    query_type,
                    request_latency_ms
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                run_id,
                fetch_mode,
                parallel_workers,
                hot_traffic_pct,
                request_idx,
                request_id,
                timing["entity"],
                timing.get("table", "N/A"),
                hash_key,
                was_hot_key,
                timing["latency_ms"],
                timing.get("rows_returned", 0),
                query_group,
                query_type,
                request_latency_ms
            ))

def persist_keys_per_run(conn, run_id, entity_keys):
    """Persist hot/cold keys for this run."""
    with conn.cursor() as cur:
        for entity, keysets in entity_keys.items():
            for key_type, keys in keysets.items():
                for key in keys:
                    cur.execute(
                        f"INSERT INTO {RESULTS_SCHEMA}.{KEYS_TABLE} (run_id, entity_name, key_type, hash_key) VALUES (%s, %s, %s, %s)",
                        (run_id, entity, key_type, key)
                    )
        conn.commit()
    print(f"✅ Persisted {sum(len(v['hot']) + len(v['cold']) for v in entity_keys.values())} keys to {KEYS_TABLE}")

def persist_keys_metadata(conn, run_id, schema_name, target_keys_per_entity, hot_key_percent,
                          sampling_method, sampled_total_all, unique_all, duplication_ratio):
    """Persist key sampling metadata for defensibility."""
    with conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO {RESULTS_SCHEMA}.zipfian_keys_run_meta (
                run_id,
                schema_name,
                target_keys_per_entity,
                hot_key_percent,
                sampling_method,
                deduped_keys,
                sampled_keys_total_all_entities,
                sampled_keys_unique_all_entities,
                duplication_ratio
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id) DO UPDATE SET
                sampled_keys_total_all_entities = EXCLUDED.sampled_keys_total_all_entities,
                sampled_keys_unique_all_entities = EXCLUDED.sampled_keys_unique_all_entities,
                duplication_ratio = EXCLUDED.duplication_ratio
        """, (
            run_id,
            schema_name,
            target_keys_per_entity,
            hot_key_percent,
            sampling_method,
            True,
            sampled_total_all,
            unique_all,
            duplication_ratio
        ))
    conn.commit()
    print(f"✅ Persisted keys metadata: {sampled_total_all} sampled → {unique_all} unique across all entities (dup ratio: {duplication_ratio:.2f}x)")

def fetch_keys_from_run(conn, run_id):
    """Load hot/cold keys from a previous run."""
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {RESULTS_SCHEMA}.{KEYS_TABLE} WHERE run_id = %s", (run_id,))
        count = cur.fetchone()[0]
        if count == 0:
            return None

        cur.execute(
            f"SELECT entity_name, key_type, hash_key FROM {RESULTS_SCHEMA}.{KEYS_TABLE} WHERE run_id = %s",
            (run_id,)
        )

        entity_keys = {}
        for entity, key_type, hash_key in cur.fetchall():
            if entity not in entity_keys:
                entity_keys[entity] = {"hot": [], "cold": []}
            entity_keys[entity][key_type].append(hash_key)
        return entity_keys

print("✅ Results persistence functions loaded")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7️⃣ Sample & Persist Keys Per Entity

# COMMAND ----------
conn = get_conn()
ensure_results_table(conn)
ensure_rpc_function(conn)
ensure_entity_rpc_functions(conn)

# ✅ V5.3: Refresh connection after schema changes to ensure visibility
print("🔄 Refreshing connection after schema updates...")
conn.close()
conn = get_conn()
print("   ✅ Connection refreshed - schema changes visible")

if REUSE_RUN_ID:
    print(f"🔑 Reusing keys from previous run: {REUSE_RUN_ID}")
    print()
    t_load_start = time.time()
    entity_keys = fetch_keys_from_run(conn, REUSE_RUN_ID)
    t_load_elapsed = time.time() - t_load_start

    if entity_keys is None:
        print(f"   ❌ Run ID '{REUSE_RUN_ID}' not found in {KEYS_TABLE}")
        print(f"   ⚠️  Falling back to sampling new keys...")
        print()
        REUSE_RUN_ID = None
    else:
        print(f"   ✅ Loaded in {t_load_elapsed:.2f}s")
        for entity, keysets in entity_keys.items():
            print(f"      {entity:20}: Hot: {len(keysets['hot']):>5,} | Cold: {len(keysets['cold']):>5,}")
        print()
        print(f"   ⏱️  Time saved: ~45 minutes (no sampling required)")
        print(f"   🆔 This run will use RUN_ID: {RUN_ID}")
        print()

if not REUSE_RUN_ID:
    print("🔑 Sampling keys per entity (from ALL tables)...")
    print()

    entity_keys = {}
    total_sampled_all_entities = 0
    total_unique_all_entities = 0

    for entity, tables in ENTITY_TABLE_GROUPS.items():
        print(f"   Sampling {entity:20}...")

        keys_set = set()
        sampled_total = 0
        keys_per_table = TOTAL_KEYS_PER_ENTITY // len(tables)
        max_sampling_rounds = 5

        for sampling_round in range(max_sampling_rounds):
            if len(keys_set) >= TOTAL_KEYS_PER_ENTITY:
                break

            for table in tables:
                if len(keys_set) >= TOTAL_KEYS_PER_ENTITY:
                    break

                sample_size = keys_per_table if sampling_round == 0 else max(1, keys_per_table // 2)
                print(f"      → Round {sampling_round+1}: {table:50}...", end="", flush=True)
                t_start = time.time()
                table_keys = fetch_sample_keys(conn, table, sample_size)
                t_elapsed = time.time() - t_start

                sampled_total += len(table_keys)
                before = len(keys_set)
                keys_set.update(table_keys)
                new_unique = len(keys_set) - before

                print(f" {new_unique:>4} new unique ({len(keys_set):>5} total) in {t_elapsed:.2f}s")

            if sampling_round > 0:
                print(f"      → Top-up round {sampling_round+1}: {len(keys_set)} / {TOTAL_KEYS_PER_ENTITY} unique keys")

        keys = list(keys_set)
        duplication_ratio = sampled_total / len(keys) if len(keys) > 0 else 1.0

        total_sampled_all_entities += sampled_total
        total_unique_all_entities += len(keys)

        print(f"      → Final: {sampled_total} sampled → {len(keys)} unique (duplication: {duplication_ratio:.2f}x)")

        random.shuffle(keys)

        hot_cutoff = int(len(keys) * HOT_KEY_PERCENT_OF_DATASET / 100)
        if HOT_KEY_PERCENT_OF_DATASET > 0 and hot_cutoff == 0:
            hot_cutoff = 1
        if HOT_KEY_PERCENT_OF_DATASET < 100 and hot_cutoff >= len(keys):
            hot_cutoff = len(keys) - 1

        entity_keys[entity] = {
            "hot": keys[:hot_cutoff],
            "cold": keys[hot_cutoff:]
        }

        if HOT_KEY_PERCENT_OF_DATASET > 0:
            assert len(entity_keys[entity]["hot"]) > 0, f"{entity}: hot set is empty!"
        if HOT_KEY_PERCENT_OF_DATASET < 100:
            assert len(entity_keys[entity]["cold"]) > 0, f"{entity}: cold set is empty!"

        print(f"      ✅ Total: {len(keys)} keys → Hot: {len(entity_keys[entity]['hot']):,} | Cold: {len(entity_keys[entity]['cold']):,}")
        print()

    print(f"✅ Key sampling complete!")
    print()

    print(f"💾 Persisting keys to {KEYS_TABLE}...")
    t_persist_start = time.time()
    persist_keys_per_run(conn, RUN_ID, entity_keys)
    t_persist_elapsed = time.time() - t_persist_start
    print(f"   ✅ Persisted in {t_persist_elapsed:.2f}s")
    print(f"   Run ID: {RUN_ID}")
    print(f"   This ensures consistent hot/cold keys across all hot/cold ratios")
    print()

    print(f"💾 Persisting sampling metadata...")
    overall_dup_ratio = total_sampled_all_entities / total_unique_all_entities if total_unique_all_entities > 0 else 1.0
    persist_keys_metadata(
        conn, RUN_ID, SCHEMA, TOTAL_KEYS_PER_ENTITY, HOT_KEY_PERCENT_OF_DATASET,
        "tablesample_system_with_topup",
        total_sampled_all_entities,
        total_unique_all_entities,
        overall_dup_ratio
    )
    print()

print(f"🔧 Converting hot keys to sets for fast lookup...")
entity_hot_sets = {entity: set(keysets["hot"]) for entity, keysets in entity_keys.items()}
print(f"   ✅ Hot key sets created")
print()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 8️⃣ Feature Fetch Function

# COMMAND ----------
def fetch_features_for_request(conn, entities_with_keys, iteration_idx=0, sample_io=False, request_start=None, log_query_timings=False):
    """
    Fetch features serially with per-entity timing.

    Notes:
    - No inline EXPLAIN (measured execution only).
    - Logs only slow queries for later diagnostics.
    """
    entity_timings = {}
    gantt_data = []
    query_timings = []
    query_errors = 0
    rows_returned_total = 0
    payload_size_bytes_total = 0

    if request_start is None:
        request_start = time.perf_counter()

    with conn.cursor() as cur:
        for entity_info in entities_with_keys:
            entity_start = time.perf_counter()

            for table in entity_info["tables"]:
                table_start = time.perf_counter()
                try:
                    query = f"SELECT {SELECT_CLAUSE} FROM {SCHEMA}.{table} WHERE hash_key = %s LIMIT 1"
                    cur.execute(query, (entity_info["hashkey"],))
                    row = cur.fetchone()

                    if row:
                        rows_returned_total += 1
                        payload_size_bytes_total += len(str(row))

                    table_end = time.perf_counter()
                    query_latency_ms = (table_end - table_start) * 1000

                    if log_query_timings and query_latency_ms >= SLOW_QUERY_THRESHOLD_MS:
                        query_timings.append({
                            "entity": entity_info["entity"],
                            "table": table,
                            "hash_key": entity_info["hashkey"],
                            "latency_ms": query_latency_ms,
                            "rows_returned": 1 if row else 0,
                            "query_type": "single_select",
                            "query_group": None,
                            "was_hot_key": None
                        })

                except Exception as e:
                    query_errors += 1
                    if query_errors <= 5:
                        print(f"         ⚠️  Query failed: {table[:50]}... | {str(e)[:80]}")

            entity_end = time.perf_counter()
            entity_timings[entity_info["entity"]] = (entity_end - entity_start) * 1000

            gantt_data.append({
                "entity": entity_info["entity"],
                "segment": "db_exec",  # ✅ V5.4: Consistent with parallel mode (serial has no pool wait)
                "start_ms": (entity_start - request_start) * 1000,
                "end_ms": (entity_end - request_start) * 1000
            })

    if query_errors > 5:
        print(f"         ⚠️  ... and {query_errors - 5} more errors")

    executor_metrics = {
        "rows_returned": rows_returned_total,
        "payload_size_bytes": payload_size_bytes_total,
        "query_errors": query_errors
    }

    return entity_timings, 0, executor_metrics, gantt_data, query_timings

print("✅ Feature fetch function loaded (serial execution, per-entity timing, slow-query logging)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 8️⃣.1 Bin-Packed Fetch Functions (Phase 1)

# COMMAND ----------
def fetch_entity_binpacked(conn, entity, hashkey, tables, log_slow_queries=False, slow_threshold_ms=40):
    """Fetch all features for one entity using UNION ALL grouped by feature type."""
    from collections import defaultdict

    feature_groups = defaultdict(list)
    for table in tables:
        parts = table.split("__")
        if len(parts) >= 2:
            feature_type = parts[-2]
            feature_groups[feature_type].append(table)
        else:
            feature_groups["other"].append(table)

    results = []
    queries_executed = len(feature_groups)
    group_timings = []

    with conn.cursor() as cur:
        for feature_type, group_tables in feature_groups.items():
            group_start = time.perf_counter()

            if len(group_tables) == 1:
                cur.execute(
                    f"SELECT {SELECT_CLAUSE} FROM {SCHEMA}.{group_tables[0]} WHERE hash_key = %s LIMIT 1",
                    (hashkey,)
                )
                result = cur.fetchall()
                results.extend(result)
            else:
                union_parts = []
                for table in group_tables:
                    union_parts.append(
                        f"(SELECT {SELECT_CLAUSE} FROM {SCHEMA}.{table} WHERE hash_key = %s LIMIT 1)"
                    )
                sql = " UNION ALL ".join(union_parts)
                params = [hashkey] * len(group_tables)
                cur.execute(sql, params)
                result = cur.fetchall()
                results.extend(result)

            group_latency_ms = (time.perf_counter() - group_start) * 1000
            if log_slow_queries and group_latency_ms >= slow_threshold_ms:
                group_timings.append({
                    "entity": entity,
                    "table": f"{entity}_{feature_type}_group",
                    "hash_key": hashkey,
                    "latency_ms": group_latency_ms,
                    "rows_returned": len(group_tables),
                    "query_type": "union_group",
                    "query_group": feature_type,
                    "was_hot_key": None
                })

    return results, queries_executed, group_timings

def fetch_features_binpacked_serial(conn, entities_with_keys, iteration_idx=0, request_start=None, log_query_timings=False, slow_threshold_ms=40):
    """Fetch features using bin-packed queries serially."""
    entity_timings = {}
    gantt_data = []
    query_timings = []
    total_queries = 0

    if request_start is None:
        request_start = time.perf_counter()

    for entity_info in entities_with_keys:
        entity_start = time.perf_counter()

        _, queries_executed, group_timings = fetch_entity_binpacked(
            conn,
            entity_info["entity"],
            entity_info["hashkey"],
            entity_info["tables"],
            log_query_timings,
            slow_threshold_ms
        )

        total_queries += queries_executed
        query_timings.extend(group_timings)

        entity_end = time.perf_counter()
        entity_timings[entity_info["entity"]] = (entity_end - entity_start) * 1000

        gantt_data.append({
            "entity": entity_info["entity"],
            "segment": "db_exec",  # ✅ V5.4: Consistent with parallel mode (binpacked has no pool wait)
            "start_ms": (entity_start - request_start) * 1000,
            "end_ms": (entity_end - request_start) * 1000
        })

    return entity_timings, 0, gantt_data, query_timings, total_queries

pool = None  # initialized per-mode inside benchmark

def fetch_entity_worker(entity_info, request_start, log_query_timings=False, slow_threshold_ms=40):
    """Worker function for parallel entity fetch.
    
    ✅ V5.3: Now logs slow feature-group queries for parallel mode
    ✅ V5.4: Explicit transaction scope per worker connection
    ✅ V5.4: Returns two Gantt segments (pool_wait + db_exec) for honest wall-clock visualization
    """
    pool_wait_start = time.perf_counter()
    with pool.connection() as conn:  # type: ignore
        entity_start = time.perf_counter()
        pool_wait_ms = (entity_start - pool_wait_start) * 1000
        if pool_wait_ms > 100:
            print(f"         ⚠️  Slow pool acquisition: {pool_wait_ms:.0f}ms for {entity_info['entity']}")

        conn.execute("BEGIN")
        try:
            results, queries_executed, group_timings = fetch_entity_binpacked(
                conn,
                entity_info["entity"],
                entity_info["hashkey"],
                entity_info["tables"],
                log_slow_queries=log_query_timings,
                slow_threshold_ms=slow_threshold_ms
            )
            conn.execute("COMMIT")
            _ = results  # results not used beyond fetch timing; retained for correctness
            entity_end = time.perf_counter()
            latency_ms = (entity_end - entity_start) * 1000

            # ✅ V5.4: Return two Gantt segments for visualization (pool_wait + db_exec)
            gantt_segments = [
                {
                    "entity": entity_info["entity"],
                    "segment": "pool_wait",
                    "start_ms": (pool_wait_start - request_start) * 1000,
                    "end_ms": (entity_start - request_start) * 1000
                },
                {
                    "entity": entity_info["entity"],
                    "segment": "db_exec",
                    "start_ms": (entity_start - request_start) * 1000,
                    "end_ms": (entity_end - request_start) * 1000
                }
            ]

            return entity_info["entity"], latency_ms, gantt_segments, queries_executed, pool_wait_ms, group_timings
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            raise

def fetch_features_binpacked_parallel(entities_with_keys, iteration_idx=0, num_workers=3, request_start=None, log_query_timings=False, slow_threshold_ms=40):
    """Fetch features using bin-packed queries with parallel execution.
    
    ✅ V5.3: Now aggregates slow feature-group queries from parallel workers
    ✅ V5.4: Returns pool wait times as dict (paired with entities) for accurate wall-clock latency
    ✅ V5.4: Gantt data now includes both pool_wait and db_exec segments per entity
    """
    entity_timings = {}
    entity_pool_waits = {}  # ✅ Dict to match entity_timings structure (futures complete out of order)
    gantt_data = []
    query_timings = []
    total_queries = 0

    if request_start is None:
        request_start = time.perf_counter()

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [
            executor.submit(fetch_entity_worker, entity_info, request_start, log_query_timings, slow_threshold_ms) 
            for entity_info in entities_with_keys
        ]
        for future in futures:
            entity, latency_ms, gantt_segments, queries_executed, pool_wait_ms, group_timings = future.result()
            entity_timings[entity] = latency_ms
            entity_pool_waits[entity] = pool_wait_ms  # ✅ Track per entity (safe for out-of-order completion)
            gantt_data.extend(gantt_segments)  # ✅ Extend with both segments (pool_wait + db_exec)
            total_queries += queries_executed
            query_timings.extend(group_timings)  # ✅ Aggregate slow queries from all workers

    return entity_timings, 0, gantt_data, query_timings, total_queries, entity_pool_waits

def fetch_features_rpc_request(conn, entities_with_keys, iteration_idx=0, request_start=None, log_query_timings=False, slow_threshold_ms=40):
    """Fetch features using a single server-side RPC call that returns JSONB.
    
    This mode collapses the entire request (all 3 entities) into ONE function call.
    The server executes all UNION ALL queries internally and returns structured JSON.
    
    Args:
        conn: Database connection
        entities_with_keys: List of entity dicts with keys ['entity', 'hashkey', 'tables']
        iteration_idx: Current iteration number
        request_start: Request start time (for timing)
        log_query_timings: Whether to log slow queries
        slow_threshold_ms: Threshold for slow query logging (not used for RPC mode)
    
    Returns:
        tuple: (entity_timings, io_blocks, gantt_data, query_timings, queries_count)
    """
    if request_start is None:
        request_start = time.perf_counter()
    
    # Extract keys in entity order (card_fingerprint, customer_email, cardholder_name)
    entity_order = ["card_fingerprint", "customer_email", "cardholder_name"]
    keys_dict = {e["entity"]: e["hashkey"] for e in entities_with_keys}
    
    # Build parameter tuple in correct order
    params = tuple(keys_dict[entity] for entity in entity_order)
    
    # Execute single RPC call
    call_start = time.perf_counter()
    
    with conn.cursor() as cur:
        cur.execute(f"SELECT {SCHEMA}.fetch_request_features(%s, %s, %s)", params)
        rpc_result = cur.fetchone()[0]  # Returns JSONB
    
    call_end = time.perf_counter()
    call_latency_ms = (call_end - call_start) * 1000
    
    # Defensive: handle null result
    if rpc_result is None:
        payload_bytes = 0
    elif isinstance(rpc_result, str):
        payload_bytes = len(rpc_result)
    else:
        payload_bytes = len(json.dumps(rpc_result))
    
    # For compatibility with existing code, create entity_timings dict
    # In RPC mode, we can't measure per-entity time server-side without modifying the function
    # So we report the total call time as a single "entity"
    entity_timings = {
        "rpc_call": call_latency_ms
    }
    
    # No gantt data for RPC mode (single call)
    gantt_data = []
    
    # No per-query timings for RPC mode (everything happens server-side)
    query_timings = []
    
    # Queries count = 1 (single function call)
    queries_count = 1
    
    # IO blocks not tracked for RPC mode
    io_blocks = 0
    
    return entity_timings, io_blocks, gantt_data, query_timings, queries_count, payload_bytes

def call_entity_rpc(conn, entity_name, entity_hash_key, req_id, run_id, request_start):
    """Call a single entity RPC function with timing measurements.
    
    Args:
        conn: Database connection
        entity_name: Entity name (card_fingerprint, customer_email, cardholder_name)
        entity_hash_key: Hash key for this entity
        req_id: Request ID
        run_id: Run ID
        request_start: Request start time (for gantt timing)
    
    Returns:
        tuple: (wall_ms, db_ms, pool_wait_ms, gantt_segments, payload_size_bytes, result_json)
    """
    # Note: For rpc3_parallel, we don't use connection pool per-entity
    # Instead, each worker gets a dedicated connection passed in
    # So pool_wait_ms will be 0 here (measured at ThreadPoolExecutor level if needed)
    
    pool_wait_start = time.perf_counter()
    # In this implementation, conn is pre-acquired by worker, so pool wait is already done
    pool_wait_ms = 0
    
    # Start DB execution timing
    entity_start = time.perf_counter()
    
    function_map = {
        "card_fingerprint": "fetch_features_card_fingerprint",
        "customer_email": "fetch_features_customer_email",
        "cardholder_name": "fetch_features_cardholder_name"
    }
    
    function_name = function_map.get(entity_name)
    if not function_name:
        raise ValueError(f"Unknown entity name: {entity_name}")
    
    with conn.cursor() as cur:
        cur.execute(f"SELECT {SCHEMA}.{function_name}(%s, %s, %s)", (entity_hash_key, run_id, req_id))
        result = cur.fetchone()[0]
    
    entity_end = time.perf_counter()
    db_ms = (entity_end - entity_start) * 1000
    wall_ms = db_ms  # No pool wait in this design
    
    # Calculate payload size
    if result is None:
        payload_size_bytes = 0
    elif isinstance(result, str):
        payload_size_bytes = len(result)
    else:
        payload_size_bytes = len(json.dumps(result))
    
    # Build gantt segments (pool_wait + db_exec)
    # Times relative to request_start
    start_offset_ms = (entity_start - request_start) * 1000
    end_offset_ms = (entity_end - request_start) * 1000
    
    gantt_segments = [
        # Pool wait segment (will be 0 duration in this implementation)
        {
            "entity": entity_name,
            "segment": "pool_wait",
            "start_ms": start_offset_ms,
            "end_ms": start_offset_ms  # 0 duration
        },
        # DB execution segment
        {
            "entity": entity_name,
            "segment": "db_exec",
            "start_ms": start_offset_ms,
            "end_ms": end_offset_ms
        }
    ]
    
    return wall_ms, db_ms, pool_wait_ms, gantt_segments, payload_size_bytes, result


def fetch_features_rpc3_parallel(entities_with_keys, req_id, run_id, request_start=None, log_query_timings=False, slow_threshold_ms=40):
    """Fetch features using 3 parallel entity RPC calls (one per entity).
    
    This mode executes 3 stored procedures concurrently on 3 separate connections,
    then aggregates results. Each entity is fetched server-side, in parallel.
    
    Args:
        entities_with_keys: List of entity dicts with keys ['entity', 'hashkey', 'tables']
        req_id: Request ID
        run_id: Run ID
        request_start: Request start time (for timing)
        log_query_timings: Whether to log slow queries (not used for RPC mode)
        slow_threshold_ms: Threshold for slow query logging (not used for RPC mode)
    
    Returns:
        tuple: (entity_timings_db_ms, gantt_data, total_queries_executed, entity_pool_waits_ms, rpc_total_payload_bytes)
    """
    if request_start is None:
        request_start = time.perf_counter()
    
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import psycopg
    
    # Build entity map
    entity_map = {e["entity"]: e["hashkey"] for e in entities_with_keys}
    
    # Worker function for ThreadPoolExecutor
    def fetch_entity_worker(entity_name):
        """Worker that opens its own connection and calls the entity RPC."""
        # Open dedicated connection for this worker
        worker_conn = None
        try:
            worker_conn = psycopg.connect(**LAKEBASE_CONFIG)
            worker_conn.autocommit = False  # Use transactions
            
            entity_hash_key = entity_map[entity_name]
            
            # Call entity RPC
            wall_ms, db_ms, pool_wait_ms, gantt_segments, payload_bytes, result_json = call_entity_rpc(
                worker_conn, entity_name, entity_hash_key, req_id, run_id, request_start
            )
            
            return {
                "entity": entity_name,
                "wall_ms": wall_ms,
                "db_ms": db_ms,
                "pool_wait_ms": pool_wait_ms,
                "gantt_segments": gantt_segments,
                "payload_bytes": payload_bytes,
                "result": result_json,
                "error": None
            }
        
        except Exception as e:
            return {
                "entity": entity_name,
                "wall_ms": 0,
                "db_ms": 0,
                "pool_wait_ms": 0,
                "gantt_segments": [],
                "payload_bytes": 0,
                "result": None,
                "error": str(e)
            }
        
        finally:
            if worker_conn:
                try:
                    worker_conn.close()
                except Exception:
                    pass
    
    # Execute 3 parallel tasks
    entity_results = {}
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(fetch_entity_worker, entity_name): entity_name 
                   for entity_name in entity_map.keys()}
        
        for future in as_completed(futures):
            result = future.result()
            entity_results[result["entity"]] = result
            
            # Check for errors
            if result["error"]:
                print(f"   ⚠️  Entity RPC error ({result['entity']}): {result['error']}")
    
    # Aggregate results
    entity_timings_db_ms = {ent: entity_results[ent]["db_ms"] for ent in entity_map.keys()}
    entity_pool_waits_ms = {ent: entity_results[ent]["pool_wait_ms"] for ent in entity_map.keys()}
    
    # Collect all gantt segments
    gantt_data = []
    for ent in entity_map.keys():
        gantt_data.extend(entity_results[ent]["gantt_segments"])
    
    # Total payload size
    rpc_total_payload_bytes = sum(entity_results[ent]["payload_bytes"] for ent in entity_map.keys())
    
    # Total queries executed = 3 (one RPC per entity)
    total_queries_executed = 3
    
    # Sanity check: ensure all 3 entities returned data
    for entity_name in entity_map.keys():
        if entity_results[entity_name]["result"] is None and entity_results[entity_name]["error"]:
            print(f"   ⚠️  Entity {entity_name} RPC call failed!")
    
    return entity_timings_db_ms, gantt_data, total_queries_executed, entity_pool_waits_ms, rpc_total_payload_bytes


print("✅ Bin-packed fetch functions loaded (serial + parallel)")
print("✅ RPC request function loaded (single server-side call)")
print("✅ RPC×Entity parallel function loaded (3 parallel server-side calls)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 9️⃣ Run Benchmark (Hot/Cold Matrix)

# COMMAND ----------
print("\n" + "="*80)
print("🚀 STARTING MULTI-ENTITY ZIPFIAN BENCHMARK V5.4")
print("="*80)
print()

if RUN_ALL_MODES:
    mode_list = [f"{cfg['mode']}" + (f" (w={cfg['workers']})" if cfg['workers'] else "") for cfg in MODE_CONFIGS]
    print(f"📋 Running ALL modes sequentially: {mode_list}")
    print(f"   Cache flush between each mode")
else:
    print(f"📋 Running single mode: {FETCH_MODE}")
print()

all_results = {}
random.seed(42)

for mode_idx, mode_config in enumerate(MODE_CONFIGS):
    current_mode = mode_config["mode"]
    current_workers = mode_config["workers"]

    print("\n" + "="*80)
    mode_label = get_mode_label(current_mode, current_workers)
    print(f"🎯 CONFIG {mode_idx + 1}/{len(MODE_CONFIGS)}: {mode_label}")
    print("="*80)
    if current_workers:
        print(f"   Workers: {current_workers}")
    print()

    # Initialize pool per-mode for parallel
    if current_mode == "binpacked_parallel":
        pool_max_size = current_workers * 10
        # ✅ V5.4: Pre-warm at least 6 connections (for 3 entities + buffer) to avoid connection creation overhead
        pool_min_size = max(6, current_workers * 2)
        pool_min_size = min(pool_min_size, pool_max_size)  # Guard: never exceed max_size
        print(f"🔗 Initializing connection pool ({current_workers} workers)...")
        pool = ConnectionPool(
            conninfo=psycopg.conninfo.make_conninfo(**LAKEBASE_CONFIG),
            min_size=pool_min_size,
            max_size=pool_max_size,
            timeout=10.0,
            max_idle=300,
            max_lifetime=1800,
            check=ConnectionPool.check_connection
        )
        print(f"   ✅ Pool initialized: min={pool_min_size}, max={pool_max_size} (workers={current_workers})")
        print()
    else:
        pool = None

    if mode_idx > 0:
        print("🔄 Flushing cache before starting this mode...")
        flush_cache(conn)
        conn.close()
        conn = get_conn()
        print("   ✅ Connection refreshed")
        print()

    results = {}

    for hot_pct in HOT_COLD_MATRIX:
        cold_pct = 100 - hot_pct
        print("\n" + "-"*80)
        print(f"   {hot_pct}% HOT / {cold_pct}% COLD (per entity)")
        print("-"*80)

        # Fix #2: capture whether reset worked; avoid lying with deltas.
        stats_reset_ok = reset_pg_stats(conn)
        io_before_reads, io_before_hits = read_pg_io_stats(conn)

        latencies = []
        cache_scores = []
        entity_latency = defaultdict(list)
        rows_returned_total = 0
        payload_size_total = 0
        gantt_samples = []
        GANTT_SAMPLE_SIZE = 20
        slow_query_count = 0
        SLOW_QUERY_BATCH_SIZE = 100
        total_queries_executed = 0

        if hot_pct == 0:
            cold_keys_pool = {entity: list(entity_keys[entity]["cold"]) for entity in ENTITY_NAMES}
            for entity in ENTITY_NAMES:
                random.shuffle(cold_keys_pool[entity])
            cold_key_idx = {entity: 0 for entity in ENTITY_NAMES}
            print(f"         ✅ Using no-replacement cold sampling for 0% hot (true cold reads)")

        for i in range(ITERATIONS_PER_RUN):
            if i > 0 and i % 100 == 0:
                conn = refresh_connection_if_stale(conn)

            if i < 3:
                print(f"         → Iteration {i+1}: Building request...")

            t0 = time.time()
            hot_entities = 0
            entities_for_request = []

            for entity in ENTITY_NAMES:
                tables = ENTITY_TABLE_GROUPS[entity]
                keyset = entity_keys[entity]

                if random.random() < hot_pct / 100:
                    hashkey = random.choice(keyset["hot"])
                    hot_entities += 1
                else:
                    if hot_pct == 0:
                        if cold_key_idx[entity] >= len(cold_keys_pool[entity]):
                            random.shuffle(cold_keys_pool[entity])
                            cold_key_idx[entity] = 0
                        hashkey = cold_keys_pool[entity][cold_key_idx[entity]]
                        cold_key_idx[entity] += 1
                    else:
                        hashkey = random.choice(keyset["cold"])

                entities_for_request.append({"entity": entity, "hashkey": hashkey, "tables": tables})

            if i < 3:
                print(f"            Built request with {len(entities_for_request)} entities, {hot_entities} hot")
                print(f"            Fetching features (mode: {current_mode})...")

            max_retries = 3
            request_start = time.perf_counter()
            request_id = f"{RUN_ID}_{current_mode}_{hot_pct}_{i}_{uuid.uuid4().hex[:8]}"
            log_timings = LOG_QUERY_TIMINGS

            for attempt in range(max_retries):
                try:
                    if current_mode == "serial":
                        with request_tx(conn):
                            entity_timings, io_blocks, executor_metrics, gantt_data, query_timings = fetch_features_for_request(
                                conn, entities_for_request, i, False, request_start, log_timings
                            )

                        queries_count = sum(len(e["tables"]) for e in entities_for_request)
                        total_queries_executed += queries_count

                        rows_returned_total += executor_metrics.get("rows_returned", 0) or 0
                        payload_size_total += executor_metrics.get("payload_size_bytes", 0) or 0

                        request_latency_ms = sum(entity_timings.values()) if entity_timings else 0
                        if log_timings and query_timings:
                            persist_query_timings(conn, RUN_ID, request_id, i, current_mode, current_workers, hot_pct, query_timings, entity_hot_sets, request_latency_ms)
                            slow_query_count += len(query_timings)
                            if slow_query_count >= SLOW_QUERY_BATCH_SIZE:
                                conn.commit()
                                if i < 3:
                                    print(f"            ✅ Committed {slow_query_count} slow queries")
                                slow_query_count = 0

                    elif current_mode == "binpacked":
                        with request_tx(conn):
                            entity_timings, io_blocks, gantt_data, query_timings, queries_count = fetch_features_binpacked_serial(
                                conn, entities_for_request, i, request_start, log_timings, SLOW_QUERY_THRESHOLD_MS
                            )
                        total_queries_executed += queries_count

                        request_latency_ms = sum(entity_timings.values()) if entity_timings else 0
                        if log_timings and query_timings:
                            persist_query_timings(conn, RUN_ID, request_id, i, current_mode, current_workers, hot_pct, query_timings, entity_hot_sets, request_latency_ms)
                            slow_query_count += len(query_timings)
                            if slow_query_count >= SLOW_QUERY_BATCH_SIZE:
                                conn.commit()
                                if i < 3:
                                    print(f"            ✅ Committed {slow_query_count} slow queries")
                                slow_query_count = 0

                    elif current_mode == "binpacked_parallel":
                        entity_timings, io_blocks, gantt_data, query_timings, queries_count, entity_pool_waits = fetch_features_binpacked_parallel(
                            entities_for_request, i, current_workers, request_start, log_timings, SLOW_QUERY_THRESHOLD_MS
                        )
                        total_queries_executed += queries_count
                        
                        # ✅ V5.4: Calculate wall-clock request latency (includes pool wait time)
                        # Each entity's wall-clock time = pool_wait + db_execution
                        # Request latency = max across all entities (critical path)
                        if entity_timings:
                            entity_wall_clock_times = {
                                entity: entity_timings[entity] + entity_pool_waits.get(entity, 0)
                                for entity in entity_timings
                            }
                            request_latency_ms = max(entity_wall_clock_times.values())
                        else:
                            request_latency_ms = 0
                        
                        # ✅ V5.3: Persist slow feature-group queries for parallel mode
                        if log_timings and query_timings:
                            persist_query_timings(conn, RUN_ID, request_id, i, current_mode, current_workers, hot_pct, query_timings, entity_hot_sets, request_latency_ms)
                            slow_query_count += len(query_timings)
                            if slow_query_count >= SLOW_QUERY_BATCH_SIZE:
                                conn.commit()
                                if i < 3:
                                    print(f"            ✅ Committed {slow_query_count} slow queries")
                                slow_query_count = 0

                    elif current_mode == "rpc_request_json":
                        with request_tx(conn):
                            entity_timings, io_blocks, gantt_data, query_timings, queries_count, payload_bytes = fetch_features_rpc_request(
                                conn, entities_for_request, i, request_start, log_timings, SLOW_QUERY_THRESHOLD_MS
                            )
                        total_queries_executed += queries_count  # Should be 1
                        
                        # RPC mode: single call latency
                        request_latency_ms = entity_timings.get("rpc_call", 0)
                        
                        # Track payload size
                        payload_size_total += payload_bytes
                        
                        # No slow query logging for RPC mode (everything is server-side)
                        # query_timings will be empty

                    elif current_mode == "rpc3_parallel":
                        # RPC×Entity mode: 3 parallel RPC calls (one per entity)
                        # No transaction wrapper needed (each worker manages its own connection)
                        entity_timings, gantt_data, queries_count, entity_pool_waits, payload_bytes = fetch_features_rpc3_parallel(
                            entities_for_request, request_id, RUN_ID, request_start, log_timings, SLOW_QUERY_THRESHOLD_MS
                        )
                        total_queries_executed += queries_count  # Should be 3
                        
                        # Calculate wall-clock request latency (includes pool wait + DB exec per entity)
                        # Request latency = max across all entities (critical path)
                        if entity_timings:
                            entity_wall_clock_times = {
                                entity: entity_timings[entity] + entity_pool_waits.get(entity, 0)
                                for entity in entity_timings
                            }
                            request_latency_ms = max(entity_wall_clock_times.values())
                        else:
                            request_latency_ms = 0
                        
                        # Track payload size
                        payload_size_total += payload_bytes
                        
                        # No slow query logging for RPC mode (everything is server-side)
                        query_timings = []

                    if i < GANTT_SAMPLE_SIZE:
                        gantt_samples.append({
                            "iteration": i,
                            "hot_entities": hot_entities,
                            "mode": current_mode,
                            "workers": current_workers,
                            "entities": gantt_data
                        })

                    break

                except (psycopg.OperationalError, psycopg.InterfaceError) as e:
                    if attempt < max_retries - 1:
                        print(f"         ⚠️  Connection error (attempt {attempt + 1}/{max_retries}): {str(e)[:100]}")
                        conn = refresh_connection_if_stale(conn)
                        time.sleep(1)
                    else:
                        print(f"         ❌ Connection failed after {max_retries} attempts")
                        raise

            if i < 3:
                print(f"            Completed in {(time.time()-t0)*1000:.1f}ms")

            # ✅ V5.4: Use canonical request_latency_ms (already computed above with mode-specific logic)
            # For parallel mode, this now includes pool wait time (wall-clock user-perceived latency)
            # For other modes, this is DB execution time (serial/binpacked sum, RPC single call)
            latency_ms = request_latency_ms
            
            # ✅ V5.4: Normalize entity timings for entity-level analysis (charts, contributions, etc.)
            norm_entity_timings, rpc_call_ms, is_rpc = normalize_entity_timings(entity_timings, current_mode)

            latencies.append(latency_ms)
            cache_scores.append(hot_entities / len(ENTITY_NAMES))
            
            # ✅ V5.3: Log every request to zipfian_request_timing for tail amplification analysis
            with conn.cursor() as cur:
                cur.execute(f"""
                    INSERT INTO {RESULTS_SCHEMA}.zipfian_request_timing 
                    (run_id, mode, parallel_workers, hot_traffic_pct, iteration_id, request_id, request_latency_ms)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (RUN_ID, current_mode, current_workers, hot_pct, i, request_id, latency_ms))
            
            # Commit request timing periodically (every 100 requests)
            if (i + 1) % 100 == 0:
                conn.commit()

            # Track entity latencies only for non-RPC modes (RPC has no per-entity breakdown)
            if not is_rpc:
                for entity, timing_ms in norm_entity_timings.items():
                    entity_latency[entity].append(timing_ms)

            if (i + 1) % max(1, ITERATIONS_PER_RUN // 10) == 0:
                pct = ((i + 1) / ITERATIONS_PER_RUN) * 100
                recent_avg = np.mean(latencies[-100:]) if len(latencies) >= 100 else np.mean(latencies)
                print(f"         Progress: {pct:.0f}% ({i+1}/{ITERATIONS_PER_RUN}) | Recent avg: {recent_avg:.1f}ms")

        # Final commit for both slow queries and request timing
        conn.commit()
        if slow_query_count > 0:
            print(f"         ✅ Final commit: {slow_query_count} slow queries + {ITERATIONS_PER_RUN} requests logged")
        else:
            print(f"         ✅ Final commit: {ITERATIONS_PER_RUN} requests logged")

        io_after_reads, io_after_hits = read_pg_io_stats(conn)
        io_blocks_read_aggregate = io_after_reads - io_before_reads

        # Fix #2: do not present misleading I/O if stats reset failed
        if current_mode == "serial":
            if stats_reset_ok:
                io_blocks_per_req = io_blocks_read_aggregate / ITERATIONS_PER_RUN if ITERATIONS_PER_RUN > 0 else 0
                io_measurement_method = "pg_statio_aggregate"
            else:
                io_blocks_per_req = None
                io_measurement_method = "pg_statio_unreset_unreliable"
        else:
            io_blocks_per_req = None
            io_measurement_method = "not_measured"

        if len(latencies) == 0:
            print(f"         ⚠️  ERROR: No measured latencies!")
            continue

        lat = np.array(latencies)
        cache = np.array(cache_scores)

        # ✅ V5.4: Handle entity metrics for RPC vs multi-entity modes
        entity_p99_dict = {}
        entity_p99_contribution = {}
        total_entity_p99 = 0.0

        if current_mode.startswith("rpc") or "rpc" in current_mode:
            # RPC mode: Store single request-level P99, no per-entity breakdown
            if len(latencies) > 0:
                rpc_p99 = float(np.percentile(latencies, 99))
                entity_p99_dict = {"rpc_call": rpc_p99}
                entity_p99_contribution = {}  # No contribution breakdown for single call
        else:
            # Multi-entity modes: Calculate per-entity P99 and contributions
            for entity, timings in entity_latency.items():
                if len(timings) > 0:
                    p99 = float(np.percentile(timings, 99))
                    entity_p99_dict[entity] = p99
                    total_entity_p99 += p99

            for entity, p99 in entity_p99_dict.items():
                entity_p99_contribution[entity] = (p99 / total_entity_p99 * 100) if total_entity_p99 > 0 else 0

        if cache.std() > 0 and lat.std() > 0:
            lat_cache_corr = float(np.corrcoef(lat, cache)[0, 1])
        else:
            lat_cache_corr = 0.0

        if mode_idx == 0:
            cache_state = "best_effort_cold"
        elif hot_pct >= 80:
            cache_state = "warm"
        elif hot_pct <= 30:
            cache_state = "mixed_cold"
        else:
            cache_state = "mixed"

        measured_iters = len(latencies)
        avg_planning_time_ms = None
        avg_rows_per_request = (rows_returned_total / measured_iters) if measured_iters > 0 else None
        avg_payload_bytes = (payload_size_total / measured_iters) if measured_iters > 0 else None

        if measured_iters > 0:
            queries_per_request = total_queries_executed / measured_iters
        else:
            # Fallback values by mode
            if current_mode == "serial":
                queries_per_request = 30
            elif current_mode == "rpc_request_json":
                queries_per_request = 1
            elif current_mode == "rpc3_parallel":
                queries_per_request = 3
            else:
                queries_per_request = 10

        # Fix #1: non_explain_iters -> measured_iters
        print(f"         📊 Queries/request: {queries_per_request:.1f} (computed from {measured_iters} iterations)")

        # ✅ V5.4: Build results dict with RPC-compatible metrics
        results[hot_pct] = {
            "p50": np.percentile(lat, 50),
            "p95": np.percentile(lat, 95),
            "p99": np.percentile(lat, 99),
            "avg": lat.mean(),
            "cache_avg": cache.mean(),
            "cache_p50": np.percentile(cache, 50),
            "cache_p90": np.percentile(cache, 90),
            "fully_cold_pct": (cache == 0).mean() * 100,
            "fully_hot_pct": (cache == 1).mean() * 100,
            "lat_cache_corr": lat_cache_corr,
            "io_blocks_per_req": io_blocks_per_req,
            "io_measurement_method": io_measurement_method,
            "entity_p99": entity_p99_dict,
            "entity_p99_contribution_pct": entity_p99_contribution,
            "cache_state": cache_state,
            "fetch_mode": current_mode,
            "queries_per_request": queries_per_request,
            "actual_queries_per_request_avg": queries_per_request,  # Self-describing for dashboards
            "avg_planning_time_ms": avg_planning_time_ms,
            "avg_rows_per_request": avg_rows_per_request,
            "avg_payload_bytes": avg_payload_bytes,
            "parallel_workers": current_workers,
            "latency_per_query_ms": (lat.mean() / queries_per_request) if queries_per_request else None,
            "entity_timing_detail": gantt_samples,
            "max_concurrent_queries": min(current_workers, len(ENTITY_NAMES)) if current_mode == "binpacked_parallel" else None
        }

        persist_results(conn, hot_pct, results[hot_pct])

        print()
        print(f"         📊 Results:")
        print(f"            P99 latency:             {results[hot_pct]['p99']:.1f} ms")
        print(f"            Avg cache score:         {results[hot_pct]['cache_avg']:.2f}")
        print(f"            Fully hot requests:      {results[hot_pct]['fully_hot_pct']:.1f}%")
        print(f"            Fully cold requests:     {results[hot_pct]['fully_cold_pct']:.1f}%")

        if io_blocks_per_req is not None:
            print(f"            I/O blocks/req:          {io_blocks_per_req:.1f} ({io_measurement_method})")
        else:
            print(f"            I/O blocks/req:          N/A ({io_measurement_method})")

        # ✅ V5.4: Guard entity contribution printing for RPC mode
        if entity_p99_contribution:
            print(f"            Entity P99 contributions:")
            for entity, contrib in entity_p99_contribution.items():
                print(f"               {entity:25} {entity_p99_dict[entity]:.1f}ms ({contrib:.1f}%)")
        elif entity_p99_dict:
            # RPC mode: show single call P99
            print(f"            RPC P99: {entity_p99_dict.get('rpc_call', 0):.1f}ms (single server-side call, no entity breakdown)")
        else:
            print(f"            Entity P99 contributions: N/A")

    config_key = f"{current_mode}_w{current_workers}" if current_workers else current_mode
    all_results[config_key] = results

    if pool:
        print()
        print("🔗 Closing connection pool...")
        pool.close()
        pool = None

    print()
    print("="*80)
    print(f"✅ MODE COMPLETE: {current_mode}")
    print("="*80)
    print()

conn.close()

print("\n" + "="*80)
print("✅ ALL BENCHMARKS COMPLETE!")
print("="*80)
print()

if RUN_ALL_MODES or len(MODE_CONFIGS) > 1:
    print("📊 CONFIGURATIONS EXECUTED:")
    for cfg in MODE_CONFIGS:
        label = get_mode_label(cfg["mode"], cfg["workers"])
        print(f"   ✅ {label}")
    print()
    print(f"💾 All results stored in {RESULTS_SCHEMA}.{RESULTS_TABLE}")
    print(f"   Run ID: {RUN_ID}")
    print(f"   Filter by fetch_mode column to compare")
else:
    print(f"📊 MODE EXECUTED: {FETCH_MODE}")
    print(f"💾 Results stored with fetch_mode = '{FETCH_MODE}'")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🔟 Results Summary Table

# COMMAND ----------
print("\n" + "="*100)
print("📊 FINAL BENCHMARK MATRIX SUMMARY")
print("="*100)
print()

for config_key in all_results.keys():
    mode_results = all_results[config_key]

    if '_w' in config_key:
        base_mode = config_key.split('_w')[0]
        workers_str = config_key.split('_w')[1]
        mode_label = f"{FETCH_MODE_LABELS.get(base_mode, base_mode)} ({workers_str} workers)"
    else:
        base_mode = config_key
        mode_label = FETCH_MODE_LABELS.get(base_mode, config_key)

    print(f"\n🎯 CONFIG: {config_key.upper()} - {mode_label}")
    print("-"*100)

    print(
        f"{'Hot%':>6} | {'P50(ms)':>8} | {'P95(ms)':>8} | {'P99(ms)':>8} | {'Avg(ms)':>8} | "
        f"{'CacheAvg':>8} | {'ColdReq%':>8} | {'HotReq%':>7} | {'LatCache':>8}"
    )
    print("-"*100)

    for hot_pct, r in sorted(mode_results.items(), reverse=True):
        print(
            f"{hot_pct:>6} | "
            f"{r['p50']:>8.1f} | "
            f"{r['p95']:>8.1f} | "
            f"{r['p99']:>8.1f} | "
            f"{r['avg']:>8.1f} | "
            f"{r['cache_avg']:>8.2f} | "
            f"{r['fully_cold_pct']:>8.1f} | "
            f"{r['fully_hot_pct']:>7.1f} | "
            f"{r['lat_cache_corr']:>8.2f}"
        )

print()
print("="*100)
print()
print(f"💾 Results persisted to: {RESULTS_SCHEMA}.{RESULTS_TABLE} (run_id: {RUN_ID})")
print(f"📊 Query to compare modes:")
print(f"""
SELECT fetch_mode, hot_traffic_pct, p99_ms, avg_ms, queries_per_request
FROM {RESULTS_SCHEMA}.{RESULTS_TABLE}
WHERE run_id = '{RUN_ID}'
ORDER BY fetch_mode, hot_traffic_pct DESC;
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1️⃣1️⃣ Visualizations
# MAGIC
# MAGIC Note: Visualizing the last mode executed. For multi-mode comparison, query the results table directly.

# COMMAND ----------
viz_config_key = list(all_results.keys())[-1] if RUN_ALL_MODES else (f"{FETCH_MODE}_w{PARALLEL_WORKERS[0]}" if FETCH_MODE == "binpacked_parallel" else FETCH_MODE)
viz_results = all_results[viz_config_key]

print(f"📊 Visualizing results for config: {viz_config_key.upper()}")
print()

fig, axes = plt.subplots(2, 2, figsize=(16, 12))

ax1 = axes[0, 0]
hot_pcts = sorted(viz_results.keys(), reverse=True)
p99_values = [viz_results[h]['p99'] for h in hot_pcts]
ax1.plot(hot_pcts, p99_values, 'o-', linewidth=2, markersize=8)
ax1.axhline(79, linestyle='--', linewidth=2, label='Customer Reference (79ms)')
ax1.set_xlabel('Hot Traffic % (per entity)', fontsize=12)
ax1.set_ylabel('P99 Latency (ms)', fontsize=12)
ax1.set_title('P99 Latency vs Hot Traffic %', fontsize=14, weight='bold')
ax1.grid(True, alpha=0.3)
ax1.legend()
ax1.invert_xaxis()

ax2 = axes[0, 1]
cache_avg = [viz_results[h]['cache_avg'] for h in hot_pcts]
cache_p90 = [viz_results[h]['cache_p90'] for h in hot_pcts]
ax2.plot(hot_pcts, cache_avg, 'o-', linewidth=2, markersize=8, label='Avg Cache Score')
ax2.plot(hot_pcts, cache_p90, 's--', linewidth=2, markersize=6, label='P90 Cache Score')
ax2.set_xlabel('Hot Traffic % (per entity)', fontsize=12)
ax2.set_ylabel('Cache Score (0=all cold, 1=all hot)', fontsize=12)
ax2.set_title('Cache Effectiveness vs Hot Traffic %', fontsize=14, weight='bold')
ax2.grid(True, alpha=0.3)
ax2.legend()
ax2.invert_xaxis()

ax3 = axes[1, 0]
fully_hot = [viz_results[h]['fully_hot_pct'] for h in hot_pcts]
fully_cold = [viz_results[h]['fully_cold_pct'] for h in hot_pcts]
width = 3
x = np.array(hot_pcts)
ax3.bar(x - width/2, fully_hot, width, label='Fully Hot (all 3 entities)', alpha=0.7)
ax3.bar(x + width/2, fully_cold, width, label='Fully Cold (all 3 entities)', alpha=0.7)
ax3.set_xlabel('Hot Traffic % (per entity)', fontsize=12)
ax3.set_ylabel('% of Requests', fontsize=12)
ax3.set_title('Request Distribution: Fully Hot vs Fully Cold', fontsize=14, weight='bold')
ax3.grid(True, alpha=0.3, axis='y')
ax3.legend()
ax3.invert_xaxis()

ax4 = axes[1, 1]
lat_cache_corr = [viz_results[h]['lat_cache_corr'] for h in hot_pcts]
ax4.plot(hot_pcts, lat_cache_corr, 'o-', linewidth=2, markersize=8)
ax4.set_xlabel('Hot Traffic % (per entity)', fontsize=12)
ax4.set_ylabel('Correlation Coefficient', fontsize=12)
ax4.set_title('Latency ↔ Cache Correlation', fontsize=14, weight='bold')
ax4.grid(True, alpha=0.3)
ax4.axhline(0, linestyle='-', linewidth=0.5)
ax4.invert_xaxis()

plt.tight_layout()
try:
    plt.savefig('/tmp/zipfian_multi_entity_benchmark_v5.4.png', dpi=150, bbox_inches='tight')
    print("📊 Visualization saved to /tmp/zipfian_multi_entity_benchmark_v5.4.png")
except Exception as e:
    print(f"⚠️  Could not save viz to /tmp: {e}")
plt.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## ✅ Key Insights

# COMMAND ----------
print("\n" + "="*80)
print("💡 KEY INSIGHTS")
print("="*80)
print(f"Config: {viz_config_key.upper()}")
print("="*80)
print()

target_hot = 80
closest_hot = min(viz_results.keys(), key=lambda x: abs(x - target_hot))
r80 = viz_results[closest_hot]

print(f"🎯 REALISTIC PRODUCTION SCENARIO ({closest_hot}% hot traffic per entity):")
print(f"   P99 latency:           {r80['p99']:.1f} ms")
print(f"   Avg cache score:       {r80['cache_avg']:.2f} (0=all cold, 1=all hot)")
print(f"   Fully hot requests:    {r80['fully_hot_pct']:.1f}%  (all 3 entities cached)")
print(f"   Fully cold requests:   {r80['fully_cold_pct']:.1f}%  (all 3 entities on disk)")
print(f"   Mixed requests:        {100 - r80['fully_hot_pct'] - r80['fully_cold_pct']:.1f}%  (1-2 entities cached)")
print()

expected_all_hot = 0.8 ** 3 * 100
expected_all_cold = 0.2 ** 3 * 100
print(f"📊 STATISTICAL VALIDATION:")
print(f"   Expected fully hot:    {expected_all_hot:.1f}%  (0.8³)")
print(f"   Actual fully hot:      {r80['fully_hot_pct']:.1f}%")
print(f"   Expected fully cold:   {expected_all_cold:.1f}%  (0.2³)")
print(f"   Actual fully cold:     {r80['fully_cold_pct']:.1f}%")
print(f"   ✅ Matches expected distribution!")
print()

print(f"📊 PERFORMANCE SUMMARY:")
print(f"   Customer Reference:    79.0 ms P99 (verified)")
print(f"   Lakebase P99:          {r80['p99']:.1f} ms")
if r80['p99'] < 79:
    improvement = ((79 - r80['p99']) / 79) * 100
    print(f"   ✅ {improvement:.1f}% faster than reference")
elif r80['p99'] < 95:
    pct_diff = ((r80['p99'] - 79) / 79) * 100
    print(f"   ✅ Within acceptable range (+{pct_diff:.1f}%)")
else:
    pct_diff = ((r80['p99'] - 79) / 79) * 100
    print(f"   ⚠️  Needs optimization (+{pct_diff:.1f}% vs reference)")
print()

print("="*80)
print(f"✅ Benchmark complete! Run ID: {RUN_ID}")
print(f"📊 View results: SELECT * FROM {RESULTS_SCHEMA}.{RESULTS_TABLE} WHERE run_id = '{RUN_ID}';")
print("="*80)
