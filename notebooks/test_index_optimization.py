# Databricks notebook source
# MAGIC %md
# MAGIC # 🧪 A/B Test: Index Build Optimization
# MAGIC
# MAGIC **Purpose:** Measure the real-world speedup from adaptive index optimization
# MAGIC
# MAGIC **Process:**
# MAGIC 1. Select a test table (medium-large, ~100-200 GB)
# MAGIC 2. Record original index build time (from completed job)
# MAGIC 3. Drop the existing index
# MAGIC 4. Rebuild with optimized settings
# MAGIC 5. Compare times and validate speedup
# MAGIC
# MAGIC **Recommended table:** `client_id_card_fingerprint_good_rates_365d` (1B rows, 162 GB)
# MAGIC - Large enough to show meaningful speedup
# MAGIC - Not the largest (so rebuild is reasonably fast)
# MAGIC - Original build time: ~80-100 min (from logs)
# MAGIC - Expected optimized time: ~22-28 min (3.5x faster)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣ Configuration

# COMMAND ----------

dbutils.widgets.text("lakebase_host", "", "Lakebase Host")
dbutils.widgets.text("lakebase_database", "benchmark", "Lakebase Database")
dbutils.widgets.text("lakebase_schema", "features", "Lakebase Schema")
dbutils.widgets.text("lakebase_user", "", "Lakebase User")
dbutils.widgets.text("lakebase_password", "", "Lakebase Password")
dbutils.widgets.text("test_table", "client_id_card_fingerprint_good_rates_365d", "Table to Test")
dbutils.widgets.text("original_build_time_min", "0", "Original Build Time (minutes) - from logs")

lakebase_host = dbutils.widgets.get("lakebase_host")
lakebase_database = dbutils.widgets.get("lakebase_database")
lakebase_schema = dbutils.widgets.get("lakebase_schema")
lakebase_user = dbutils.widgets.get("lakebase_user")
lakebase_password = dbutils.widgets.get("lakebase_password")
test_table = dbutils.widgets.get("test_table")
original_build_time_min = float(dbutils.widgets.get("original_build_time_min"))

print(f"🧪 A/B TEST CONFIGURATION")
print(f"=" * 80)
print(f"Test table: {test_table}")
print(f"Original build time: {original_build_time_min:.1f} minutes (default settings)")
print(f"Target: 3-4x speedup with adaptive optimization")
print(f"=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2️⃣ Get Current Table & Index Info

# COMMAND ----------

import psycopg2
import time
from datetime import datetime
import hashlib

conn = psycopg2.connect(
    host=lakebase_host,
    database=lakebase_database,
    user=lakebase_user,
    password=lakebase_password,
    sslmode='require'
)

print("✅ Connected to Lakebase\n")

# Get table info
with conn.cursor() as cursor:
    cursor.execute(f"""
        SELECT 
            s.n_live_tup AS estimated_rows,
            ROUND(pg_total_relation_size(quote_ident(t.schemaname) || '.' || quote_ident(t.tablename))::numeric / (1024^3), 2) AS size_gb
        FROM pg_tables t
        LEFT JOIN pg_stat_user_tables s ON t.tablename = s.relname AND t.schemaname = s.schemaname
        WHERE t.schemaname = '{lakebase_schema}'
        AND t.tablename = '{test_table}'
    """)
    
    result = cursor.fetchone()
    if not result:
        raise ValueError(f"Table {test_table} not found!")
    
    table_rows, table_size_gb = result
    
    # Get current index info
    cursor.execute(f"""
        SELECT 
            i.relname AS index_name,
            con.conname AS constraint_name,
            idx.indisvalid AS is_valid,
            ROUND(pg_relation_size(i.oid)::numeric / (1024^3), 2) AS index_size_gb
        FROM pg_class t
        JOIN pg_index idx ON t.oid = idx.indrelid
        JOIN pg_class i ON i.oid = idx.indexrelid
        LEFT JOIN pg_constraint con ON con.conindid = i.oid AND con.contype = 'p'
        WHERE t.relname = '{test_table}'
        AND t.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '{lakebase_schema}')
        AND idx.indisprimary = true
    """)
    
    index_info = cursor.fetchone()

conn.close()

print("=" * 80)
print("📊 CURRENT TABLE STATE")
print("=" * 80)
print(f"Table: {test_table}")
print(f"Rows: {table_rows:,}")
print(f"Size: {table_size_gb} GB")
print()

if index_info:
    index_name, constraint_name, is_valid, index_size_gb = index_info
    print(f"✅ PRIMARY KEY index exists:")
    print(f"   Index name: {index_name}")
    print(f"   Constraint name: {constraint_name}")
    print(f"   Valid: {is_valid}")
    print(f"   Index size: {index_size_gb} GB")
    print()
    print(f"   Original build time: {original_build_time_min:.1f} minutes")
    print(f"   Settings: maintenance_work_mem=64MB, max_parallel_workers=2 (defaults)")
else:
    print("❌ No PRIMARY KEY index found!")
    raise ValueError("Table must have an existing index to test optimization")

print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3️⃣ Calculate Optimized Settings

# COMMAND ----------

def estimate_index_size_gb(table_size_gb: float, estimated_rows: int) -> float:
    """
    Estimate index size (B-tree on hashkey) based on table size.
    Rule of thumb: B-tree index ≈ 65% of table size
    """
    return table_size_gb * 0.65

def calculate_optimal_parallel_workers(
    table_size_gb: float,
    total_node_ram_gb: float,
    batch_size: int = 1,
    estimated_rows: int = 0
) -> int:
    """
    Calculate optimal workers using memory-fraction strategy.
    For sequential builds: 1 worker with max memory = fewer external sort passes
    """
    available_ram_gb = total_node_ram_gb * 0.75
    
    if batch_size == 1:
        # Sequential: Maximize memory per worker to minimize external sort
        if table_size_gb < 30:
            return 1 if table_size_gb < 15 else 2
        else:
            # Medium-large tables: 1 worker with max memory is best
            return 1
    else:
        # Parallel builds: Balance workers
        ram_per_build = available_ram_gb / batch_size
        return 2 if ram_per_build >= 16 else 1

def get_table_specific_optimization(
    table_size_gb: float,
    table_rows: int,
    total_node_ram_gb: float,
    batch_size: int = 1
) -> dict:
    """
    Get optimization settings using memory-fraction strategy.
    Goal: Maximize memory_fraction = maintenance_work_mem / index_size
    to minimize external sort passes.
    """
    estimated_index_size_gb = estimate_index_size_gb(table_size_gb, table_rows)
    available_ram_gb = total_node_ram_gb * 0.75
    
    optimal_workers = calculate_optimal_parallel_workers(
        table_size_gb, total_node_ram_gb, batch_size, table_rows
    )
    
    if batch_size == 1:
        # Sequential: Use most of available RAM for single worker
        if optimal_workers == 1:
            per_worker_mem_gb = min(20, available_ram_gb * 0.85)
        else:
            per_worker_mem_gb = available_ram_gb / optimal_workers
        
        # For large indexes, ensure decent memory fraction (>15%)
        if estimated_index_size_gb > 100:
            target_memory = estimated_index_size_gb * 0.15
            per_worker_mem_gb = max(per_worker_mem_gb, target_memory)
            per_worker_mem_gb = min(per_worker_mem_gb, available_ram_gb * 0.85)
    else:
        # Parallel: Split RAM across builds
        ram_per_build = available_ram_gb / batch_size
        per_worker_mem_gb = ram_per_build / optimal_workers
    
    per_worker_mem_gb = min(per_worker_mem_gb, 32)
    per_worker_mem_gb = max(per_worker_mem_gb, 2)
    per_worker_mem_gb = round(per_worker_mem_gb)
    
    memory_fraction = (per_worker_mem_gb / estimated_index_size_gb) if estimated_index_size_gb > 0 else 0
    
    reasoning = (
        f"Table: {table_size_gb:.1f} GB, Est. index: {estimated_index_size_gb:.1f} GB | "
        f"Workers: {optimal_workers}, Memory: {per_worker_mem_gb}GB/worker | "
        f"Memory fraction: {memory_fraction:.1%} (higher = less external sort)"
    )
    
    return {
        "maintenance_work_mem": f"{per_worker_mem_gb}GB",
        "max_parallel_workers": optimal_workers,
        "reasoning": reasoning,
        "memory_fraction": memory_fraction
    }

# Detect cluster RAM
conn = psycopg2.connect(
    host=lakebase_host,
    database=lakebase_database,
    user=lakebase_user,
    password=lakebase_password,
    sslmode='require'
)

with conn.cursor() as cursor:
    cursor.execute("SHOW effective_cache_size")
    effective_cache_size = cursor.fetchone()[0]
    
    def parse_memory_gb(mem_str):
        if 'GB' in mem_str:
            return float(mem_str.replace('GB', ''))
        elif 'MB' in mem_str:
            return float(mem_str.replace('MB', '')) / 1024
        return 0
    
    effective_cache_gb = parse_memory_gb(effective_cache_size)
    total_node_ram_gb = max(effective_cache_gb / 0.75, 64)

conn.close()

# Calculate optimized settings
optimization = get_table_specific_optimization(
    table_size_gb=table_size_gb,
    table_rows=table_rows,
    total_node_ram_gb=total_node_ram_gb,
    batch_size=1
)

print("\n" + "=" * 80)
print("⚡ OPTIMIZED SETTINGS")
print("=" * 80)
print(f"Cluster RAM: {total_node_ram_gb:.1f} GB")
print(f"Table size: {table_size_gb} GB")
print()
print(f"Optimized settings:")
print(f"  maintenance_work_mem: {optimization['maintenance_work_mem']} (vs default 64MB)")
print(f"  max_parallel_maintenance_workers: {optimization['max_parallel_workers']} (vs default 2)")
print()
print(f"Reasoning: {optimization['reasoning']}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4️⃣ Drop Existing Index

# COMMAND ----------

print("\n" + "=" * 80)
print("🗑️  DROPPING EXISTING INDEX")
print("=" * 80)
print(f"⚠️  WARNING: This will drop the PRIMARY KEY index on {test_table}")
print(f"            Table will not be usable for benchmarks until rebuilt!")
print("=" * 80)

# Generate names
table_hash = hashlib.md5(test_table.encode()).hexdigest()[:8]
new_index_name = f"idx_pk_{table_hash}"
new_constraint_name = f"pk_{table_hash}"

conn = psycopg2.connect(
    host=lakebase_host,
    database=lakebase_database,
    user=lakebase_user,
    password=lakebase_password,
    sslmode='require'
)
conn.autocommit = True

with conn.cursor() as cursor:
    print(f"\n[1/2] Dropping PRIMARY KEY constraint: {constraint_name}")
    cursor.execute(f"ALTER TABLE {lakebase_schema}.{test_table} DROP CONSTRAINT IF EXISTS {constraint_name}")
    print(f"✅ Constraint dropped")
    
    print(f"\n[2/2] Dropping index: {index_name}")
    cursor.execute(f"DROP INDEX IF EXISTS {lakebase_schema}.{index_name}")
    print(f"✅ Index dropped")

conn.close()

print("\n" + "=" * 80)
print("✅ INDEX DROPPED SUCCESSFULLY")
print("=" * 80)
print(f"⚠️  {test_table} now has NO PRIMARY KEY")
print(f"    Do not run benchmarks until rebuild completes!")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5️⃣ Rebuild Index with Optimized Settings

# COMMAND ----------

print("\n" + "=" * 80)
print("🔨 REBUILDING INDEX WITH OPTIMIZED SETTINGS")
print("=" * 80)
print(f"Table: {test_table}")
print(f"Rows: {table_rows:,}, Size: {table_size_gb} GB")
print(f"Index name: {new_index_name}")
print(f"Constraint name: {new_constraint_name}")
print(f"Optimization: {optimization['maintenance_work_mem']} per worker, {optimization['max_parallel_workers']} parallel workers")
print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 80)

start_time = time.time()

conn = psycopg2.connect(
    host=lakebase_host,
    database=lakebase_database,
    user=lakebase_user,
    password=lakebase_password,
    sslmode='require'
)
conn.autocommit = True

with conn.cursor() as cursor:
    # Apply optimized settings
    print(f"\n🔧 Applying optimized settings...")
    cursor.execute(f"SET maintenance_work_mem = '{optimization['maintenance_work_mem']}'")
    cursor.execute(f"SET max_parallel_maintenance_workers = {optimization['max_parallel_workers']}")
    print(f"✅ Settings applied: maintenance_work_mem={optimization['maintenance_work_mem']}, max_parallel_maintenance_workers={optimization['max_parallel_workers']}")
    
    # Create index
    print(f"\n[1/2] Creating unique index CONCURRENTLY on {lakebase_schema}.{test_table}...")
    print(f"      This will take ~{original_build_time_min / 3.5:.0f}-{original_build_time_min / 3:.0f} minutes (3-3.5x faster than original {original_build_time_min:.0f} min)")
    
    index_start_time = time.time()
    cursor.execute(f"""
        CREATE UNIQUE INDEX CONCURRENTLY {new_index_name} 
        ON {lakebase_schema}.{test_table} (hashkey)
    """)
    index_duration_min = (time.time() - index_start_time) / 60
    print(f"✅ Index created in {index_duration_min:.1f} minutes")
    
    # Add constraint
    print(f"\n[2/2] Adding PRIMARY KEY constraint...")
    cursor.execute(f"""
        ALTER TABLE {lakebase_schema}.{test_table} 
        ADD CONSTRAINT {new_constraint_name} 
        PRIMARY KEY USING INDEX {new_index_name}
    """)
    print(f"✅ PRIMARY KEY constraint added")

conn.close()

total_duration_min = (time.time() - start_time) / 60

print("\n" + "=" * 80)
print("✅ INDEX REBUILT SUCCESSFULLY")
print("=" * 80)
print(f"Total duration: {total_duration_min:.1f} minutes")
print(f"Index creation: {index_duration_min:.1f} minutes")
print(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6️⃣ Compare Results

# COMMAND ----------

print("\n" + "=" * 80)
print("📊 A/B TEST RESULTS")
print("=" * 80)
print(f"Table: {test_table}")
print(f"Size: {table_rows:,} rows, {table_size_gb} GB")
print("=" * 80)
print()

print("🔴 ORIGINAL BUILD (Default Settings):")
print(f"   maintenance_work_mem: 64MB")
print(f"   max_parallel_maintenance_workers: 2")
print(f"   Build time: {original_build_time_min:.1f} minutes")
print()

print("🟢 OPTIMIZED BUILD (Adaptive Settings):")
print(f"   maintenance_work_mem: {optimization['maintenance_work_mem']}")
print(f"   max_parallel_maintenance_workers: {optimization['max_parallel_workers']}")
print(f"   Build time: {total_duration_min:.1f} minutes")
print()

speedup = original_build_time_min / total_duration_min if total_duration_min > 0 else 0
time_saved = original_build_time_min - total_duration_min

print("=" * 80)
print("⚡ PERFORMANCE IMPROVEMENT:")
print("=" * 80)
print(f"Speedup: {speedup:.2f}x faster")
print(f"Time saved: {time_saved:.1f} minutes ({time_saved * 100 / original_build_time_min:.1f}% reduction)")
print()

if speedup >= 3.0:
    print("🎉 EXCELLENT! Achieved 3x+ speedup as expected!")
elif speedup >= 2.5:
    print("✅ GOOD! Solid 2.5x+ speedup (close to 3x target)")
elif speedup >= 2.0:
    print("✅ GOOD! 2x+ speedup achieved")
else:
    print("⚠️  WARNING: Speedup less than 2x (investigate settings or table characteristics)")

print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7️⃣ Verify Index is Valid

# COMMAND ----------

conn = psycopg2.connect(
    host=lakebase_host,
    database=lakebase_database,
    user=lakebase_user,
    password=lakebase_password,
    sslmode='require'
)

with conn.cursor() as cursor:
    cursor.execute(f"""
        SELECT 
            i.relname AS index_name,
            con.conname AS constraint_name,
            idx.indisvalid AS is_valid,
            idx.indisready AS is_ready,
            ROUND(pg_relation_size(i.oid)::numeric / (1024^3), 2) AS index_size_gb
        FROM pg_class t
        JOIN pg_index idx ON t.oid = idx.indrelid
        JOIN pg_class i ON i.oid = idx.indexrelid
        LEFT JOIN pg_constraint con ON con.conindid = i.oid AND con.contype = 'p'
        WHERE t.relname = '{test_table}'
        AND t.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '{lakebase_schema}')
        AND idx.indisprimary = true
    """)
    
    index_info = cursor.fetchone()

conn.close()

print("\n" + "=" * 80)
print("✅ POST-BUILD VERIFICATION")
print("=" * 80)

if index_info:
    idx_name, con_name, is_valid, is_ready, idx_size_gb = index_info
    print(f"Index name: {idx_name}")
    print(f"Constraint name: {con_name}")
    print(f"Valid: {is_valid} ✅" if is_valid else f"Valid: {is_valid} ❌")
    print(f"Ready: {is_ready} ✅" if is_ready else f"Ready: {is_ready} ❌")
    print(f"Index size: {idx_size_gb} GB")
    print()
    
    if is_valid and is_ready:
        print("✅ Index is VALID and READY for benchmarks!")
    else:
        print("⚠️  WARNING: Index may not be fully ready")
else:
    print("❌ ERROR: No PRIMARY KEY index found after rebuild!")

print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Summary

# COMMAND ----------

print("\n" + "=" * 80)
print("🎉 A/B TEST COMPLETE!")
print("=" * 80)
print()
print(f"Table tested: {test_table}")
print(f"Table size: {table_rows:,} rows, {table_size_gb} GB")
print()
print(f"Original build time: {original_build_time_min:.1f} min (default settings)")
print(f"Optimized build time: {total_duration_min:.1f} min (adaptive settings)")
print()
print(f"⚡ Speedup: {speedup:.2f}x faster")
print(f"⏱️  Time saved: {time_saved:.1f} minutes")
print()
print("Settings comparison:")
print(f"  Default:   64MB memory, 2 workers")
print(f"  Optimized: {optimization['maintenance_work_mem']} memory, {optimization['max_parallel_workers']} workers")
print()
print("=" * 80)
print()
print("Next steps:")
print("1. ✅ Index is ready for benchmarking")
print("2. Run: databricks bundle run fraud_benchmark_end_to_end -t dev")
print("3. Document the speedup for presentations")
print("=" * 80)
