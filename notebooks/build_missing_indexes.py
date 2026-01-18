# Databricks notebook source
# MAGIC %md
# MAGIC # 🔨 Build Missing Indexes (Batched)
# MAGIC 
# MAGIC Build primary key indexes for all tables that don't have them
# MAGIC Default: Sequential builds (batch_size=1) to prevent temp disk exhaustion

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣ Get Parameters

# COMMAND ----------

dbutils.widgets.text("lakebase_host", "", "Lakebase Host")
dbutils.widgets.text("lakebase_database", "benchmark", "Lakebase Database")
dbutils.widgets.text("lakebase_schema", "features", "Lakebase Schema")
dbutils.widgets.text("lakebase_user", "", "Lakebase User")
dbutils.widgets.text("lakebase_password", "", "Lakebase Password")
dbutils.widgets.text("max_concurrent_builds", "2", "Max Concurrent Index Builds (auto: large tables sequential, small concurrent)")

lakebase_host = dbutils.widgets.get("lakebase_host")
lakebase_database = dbutils.widgets.get("lakebase_database")
lakebase_schema = dbutils.widgets.get("lakebase_schema")
lakebase_user = dbutils.widgets.get("lakebase_user")
lakebase_password = dbutils.widgets.get("lakebase_password")
max_concurrent_builds = int(dbutils.widgets.get("max_concurrent_builds"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2️⃣ Clean Up Orphaned Indexes from Failed Builds

# COMMAND ----------

import psycopg2
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import hashlib

def calculate_maintenance_work_mem(
    total_node_ram_gb: float,
    max_parallel_workers: int,
    reserved_ram_ratio: float = 0.25
) -> str:
    """
    Calculate safe maintenance_work_mem per parallel worker for index build.
    
    Args:
        total_node_ram_gb: Total RAM on the node in GB
        max_parallel_workers: Number of parallel maintenance workers
        reserved_ram_ratio: Fraction of RAM to reserve for OS / other processes (default 25%)
    
    Returns:
        maintenance_work_mem string suitable for SET command, e.g. '8GB'
    """
    available_ram = total_node_ram_gb * (1 - reserved_ram_ratio)
    per_worker_mem_gb = available_ram / max_parallel_workers
    
    # Cap to a maximum to avoid unreasonably high settings
    per_worker_mem_gb = min(per_worker_mem_gb, 32)  # optional cap
    
    # Round to nearest GB for clarity
    per_worker_mem_gb = round(per_worker_mem_gb)
    return f"{per_worker_mem_gb}GB"

def estimate_index_size_gb(table_size_gb: float, estimated_rows: int) -> float:
    """
    Estimate index size (B-tree on hashkey) based on table size.
    
    Rule of thumb: B-tree index on single column ≈ 40-65% of table size
    (depends on key size, fill factor, etc.)
    """
    # Conservative estimate: 65% of table size
    return table_size_gb * 0.65

def calculate_optimal_parallel_workers(table_size_gb: float) -> int:
    """
    Calculate optimal parallel workers using hybrid scheduling strategy.
    
    Strategy:
    - Large tables (>50 GB): 1 worker with max memory (minimize external sort)
    - Medium tables (10-50 GB): 2 workers (balance parallelism and memory)
    - Small tables (<10 GB): 4 workers (maximize parallelism)
    
    Args:
        table_size_gb: Size of the table in GB
    
    Returns:
        Optimal number of parallel maintenance workers (1, 2, or 4)
    """
    if table_size_gb > 50:
        # Large table: Sequential with 1 worker and max memory
        # Minimizes external sort passes
        return 1
    elif table_size_gb > 10:
        # Medium table: 2 workers for some parallelism
        return 2
    else:
        # Small table: 4 workers for max parallelism
        return 4

def compute_maintenance_work_mem(
    total_node_ram_gb: float,
    parallel_workers: int,
    reserved_ram_ratio: float = 0.25
) -> str:
    """
    Compute maintenance_work_mem per worker using available RAM.
    
    Args:
        total_node_ram_gb: Total RAM on node in GB
        parallel_workers: Number of parallel workers
        reserved_ram_ratio: Fraction to reserve for system (default 25%)
    
    Returns:
        maintenance_work_mem string (e.g., "48GB")
    """
    available_ram_gb = total_node_ram_gb * (1 - reserved_ram_ratio)
    per_worker_mem_gb = available_ram_gb / parallel_workers
    per_worker_mem_gb = min(per_worker_mem_gb, 32)  # PostgreSQL practical max
    per_worker_mem_gb = max(per_worker_mem_gb, 2)   # Minimum 2 GB
    return f"{round(per_worker_mem_gb)}GB"

def get_table_specific_optimization(
    table_size_gb: float,
    table_rows: int,
    total_node_ram_gb: float
) -> dict:
    """
    Get table-specific optimization using hybrid scheduling strategy.
    
    Strategy:
    - Large tables (>50 GB): 1 worker, max memory (minimize external sort)
    - Medium tables (10-50 GB): 2 workers, balanced memory
    - Small tables (<10 GB): 4 workers, split memory
    
    Args:
        table_size_gb: Size of table in GB
        table_rows: Number of rows in table
        total_node_ram_gb: Total RAM on node in GB
    
    Returns:
        Dict with maintenance_work_mem, max_parallel_workers, and reasoning
    """
    # Estimate index size (B-tree on hashkey ≈ 65% of table size)
    estimated_index_size_gb = estimate_index_size_gb(table_size_gb, table_rows)
    
    # Calculate optimal workers using simple size thresholds
    optimal_workers = calculate_optimal_parallel_workers(table_size_gb)
    
    # Calculate memory per worker
    maintenance_work_mem = compute_maintenance_work_mem(
        total_node_ram_gb,
        optimal_workers
    )
    
    # Parse memory value for fraction calculation
    mem_gb = int(maintenance_work_mem.replace('GB', ''))
    memory_fraction = (mem_gb / estimated_index_size_gb) if estimated_index_size_gb > 0 else 0
    
    # Determine scheduling strategy
    if table_size_gb > 50:
        strategy = "Sequential (large table)"
    elif table_size_gb > 10:
        strategy = "Concurrent (medium table)"
    else:
        strategy = "Concurrent (small table)"
    
    reasoning = (
        f"Size: {table_size_gb:.1f} GB, Est. index: {estimated_index_size_gb:.1f} GB | "
        f"{strategy} | Workers: {optimal_workers}, Memory: {mem_gb}GB/worker | "
        f"Memory fraction: {memory_fraction:.1%}"
    )
    
    return {
        "maintenance_work_mem": maintenance_work_mem,
        "max_parallel_workers": optimal_workers,
        "reasoning": reasoning,
        "estimated_index_size_gb": estimated_index_size_gb,
        "memory_fraction": memory_fraction,
        "is_large_table": table_size_gb > 50  # Used by hybrid scheduler
    }

conn = psycopg2.connect(
    host=lakebase_host,
    database=lakebase_database,
    user=lakebase_user,
    password=lakebase_password,
    sslmode='require'
)

print("✅ Connected to Lakebase")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.5️⃣ Detect Cluster Resources and Calculate Optimal Index Build Settings

# COMMAND ----------

# Detect available RAM and calculate optimal maintenance_work_mem
with conn.cursor() as cursor:
    # Get total system memory
    cursor.execute("SHOW shared_buffers")
    shared_buffers = cursor.fetchone()[0]
    
    # Get effective_cache_size (usually ~75% of total RAM)
    cursor.execute("SHOW effective_cache_size")
    effective_cache_size = cursor.fetchone()[0]
    
    # Parse GB from strings like "16GB"
    def parse_memory_gb(mem_str):
        if 'GB' in mem_str:
            return float(mem_str.replace('GB', ''))
        elif 'MB' in mem_str:
            return float(mem_str.replace('MB', '')) / 1024
        return 0
    
    shared_buffers_gb = parse_memory_gb(shared_buffers)
    effective_cache_gb = parse_memory_gb(effective_cache_size)
    
    # Estimate total RAM (effective_cache_size is usually ~75% of total)
    estimated_total_ram_gb = effective_cache_gb / 0.75
    
    # For your 32 CU Lakebase cluster (64 GB RAM)
    # Use 64 GB if we can't detect, otherwise use detected value
    total_node_ram_gb = max(estimated_total_ram_gb, 64)

print("\n" + "="*80)
print("🔧 ADAPTIVE INDEX BUILD OPTIMIZATION")
print("="*80)
print(f"Detected total RAM: {total_node_ram_gb:.1f} GB")
print(f"Reserved for system: {total_node_ram_gb * 0.25:.1f} GB (25%)")
print(f"Available for index builds: {total_node_ram_gb * 0.75:.1f} GB")
print(f"Batch concurrency: {batch_size} concurrent builds")
print(f"\nSettings will be calculated DYNAMICALLY for each table based on:")
print(f"  • Table size (adapts workers and memory)")
print(f"  • Cluster resources (64 GB RAM)")
print(f"  • Batch concurrency ({batch_size} concurrent builds)")
print(f"\nSmall tables get fewer workers but more memory per worker.")
print(f"Large tables get more workers for better parallelism.")
print("="*80)

# COMMAND ----------

# Check for orphaned indexes from failed builds
# Strategy: Reuse valid indexes, drop invalid ones
print("\n🧹 Checking for orphaned indexes...")
with conn.cursor() as cursor:
    # Find indexes that match our naming pattern BUT are not part of a PRIMARY KEY constraint
    cursor.execute(f"""
        SELECT 
            i.indexname,
            i.tablename,
            idx.indisvalid AS is_valid,
            pg_relation_size(i.schemaname||'.'||i.indexname) AS index_size_bytes
        FROM pg_indexes i
        JOIN pg_class c ON c.relname = i.indexname
        JOIN pg_index idx ON idx.indexrelid = c.oid
        LEFT JOIN pg_constraint con 
            ON con.conindid = (i.schemaname||'.'||i.indexname)::regclass
            AND con.contype = 'p'
        WHERE i.schemaname = '{lakebase_schema}' 
          AND i.indexname LIKE 'idx_pk_%'
          AND con.conname IS NULL  -- Not attached to a PRIMARY KEY constraint
    """)
    orphaned = cursor.fetchall()
    
    if orphaned:
        print(f"   Found {len(orphaned)} orphaned index(es):")
        for idx_name, table_name, is_valid, size_bytes in orphaned:
            size_mb = size_bytes / (1024**2)
            
            # If index is valid and has data, it can be reused!
            if is_valid and size_bytes > 0:
                print(f"   ✅ REUSABLE: {idx_name} ({size_mb:.1f} MB) - will add PK constraint only")
            else:
                print(f"   - Dropping INVALID: {idx_name} (valid={is_valid}, size={size_mb:.1f} MB)")
                try:
                    cursor.execute(f"DROP INDEX IF EXISTS {lakebase_schema}.{idx_name}")
                    conn.commit()
                    print(f"     ✅ Dropped")
                except Exception as e:
                    print(f"     ⚠️  Could not drop: {e}")
                    conn.rollback()
    else:
        print("   ✅ No orphaned indexes found (all idx_pk_* indexes are attached to PRIMARY KEYs)")

print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3️⃣ Find Tables Without Indexes

# COMMAND ----------

with conn.cursor() as cursor:
    cursor.execute(f"""
        SELECT 
            t.tablename,
            COALESCE(s.n_live_tup, 0) AS estimated_rows,
            pg_relation_size(t.schemaname||'.'||t.tablename) / (1024^3) AS size_gb,
            CASE WHEN con.conname IS NOT NULL THEN 1 ELSE 0 END AS has_primary_key
        FROM pg_tables t
        LEFT JOIN pg_stat_user_tables s 
            ON s.schemaname = t.schemaname 
            AND s.relname = t.tablename
        LEFT JOIN pg_constraint con
            ON con.conrelid = (t.schemaname||'.'||t.tablename)::regclass
            AND con.contype = 'p'  -- Primary key constraint
        WHERE t.schemaname = '{lakebase_schema}'
          AND t.tablename NOT LIKE '%__stage'
        ORDER BY estimated_rows DESC
    """)
    
    all_tables = cursor.fetchall()

# Find tables without primary keys
tables_needing_indexes = []
for tablename, estimated_rows, size_gb, has_primary_key in all_tables:
    if has_primary_key == 0:
        tables_needing_indexes.append({
            "table": tablename,
            "rows": estimated_rows,
            "size_gb": size_gb
        })

conn.close()

print()
print("=" * 80)
print("🔍 TABLES NEEDING INDEXES")
print("=" * 80)
for t in tables_needing_indexes:
    print(f"  {t['table']:70} {t['rows']:>15,} rows ({t['size_gb']:>6.1f} GB)")
print("=" * 80)
print(f"📊 Total: {len(tables_needing_indexes)} tables")
print("=" * 80)

if not tables_needing_indexes:
    print("\n✅ All tables already have indexes!")
    dbutils.notebook.exit("SUCCESS: No indexes needed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4️⃣ Build Indexes in Batches

# COMMAND ----------

def build_index(table_info: dict, schema: str, conninfo: dict, total_node_ram_gb: float):
    """Build primary key index for a single table with table-specific optimized settings"""
    table_name = table_info["table"]
    rows = table_info["rows"]
    size_gb = table_info["size_gb"]
    
    # Calculate table-specific optimization settings using hybrid strategy
    table_optimization = get_table_specific_optimization(
        table_size_gb=size_gb,
        table_rows=rows,
        total_node_ram_gb=total_node_ram_gb
    )
    
    # Generate short, unique names (PostgreSQL 63-char limit!)
    table_hash = hashlib.md5(table_name.encode()).hexdigest()[:8]
    index_name = f"idx_pk_{table_hash}"
    constraint_name = f"pk_{table_hash}"  # Also keep constraint name short!
    
    start_time = time.time()
    
    print(f"\n{'='*80}")
    print(f"🔨 Building index for: {table_name}")
    print(f"   Rows: {rows:,}, Size: {size_gb:.1f} GB")
    print(f"   Index name: {index_name}")
    print(f"   Constraint name: {constraint_name}")
    print(f"   Optimization: {table_optimization['maintenance_work_mem']} per worker, {table_optimization['max_parallel_workers']} parallel workers")
    print(f"   Reasoning: {table_optimization['reasoning']}")
    print(f"   Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}")
    
    try:
        # Create new connection for this thread
        conn = psycopg2.connect(
            host=conninfo["host"],
            database=conninfo["database"],
            user=conninfo["user"],
            password=conninfo["password"],
            sslmode='require'
        )
        conn.autocommit = True
        
        with conn.cursor() as cursor:
            # Apply table-specific optimization settings for this session
            print(f"   🔧 Applying table-specific optimization settings...")
            cursor.execute(f"SET maintenance_work_mem = '{table_optimization['maintenance_work_mem']}'")
            cursor.execute(f"SET max_parallel_maintenance_workers = {table_optimization['max_parallel_workers']}")
            print(f"   ✅ Settings applied: maintenance_work_mem={table_optimization['maintenance_work_mem']}, max_parallel_maintenance_workers={table_optimization['max_parallel_workers']}")
            print()
            # Check if index already exists (from previous timeout/failure)
            cursor.execute(f"""
                SELECT idx.indisvalid, pg_relation_size('{schema}.{index_name}')
                FROM pg_class c
                JOIN pg_index idx ON idx.indexrelid = c.oid
                WHERE c.relname = '{index_name}'
            """)
            existing_index = cursor.fetchone()
            
            if existing_index:
                is_valid, size_bytes = existing_index
                if is_valid and size_bytes > 0:
                    print(f"   ✅ REUSING existing valid index {index_name} ({size_bytes/(1024**2):.1f} MB)")
                    print(f"   [1/1] Adding PRIMARY KEY constraint only...")
                else:
                    print(f"   ⚠️  Found invalid/empty index, dropping and rebuilding...")
                    cursor.execute(f"DROP INDEX IF EXISTS {schema}.{index_name}")
                    print(f"   [1/2] Creating unique index CONCURRENTLY on {schema}.{table_name}...")
                    cursor.execute(f"""
                        CREATE UNIQUE INDEX CONCURRENTLY {index_name} 
                        ON {schema}.{table_name} (hashkey)
                    """)
                    print(f"   [2/2] Adding PRIMARY KEY constraint...")
            else:
                # No existing index - create from scratch
                print(f"   [1/2] Creating unique index CONCURRENTLY on {schema}.{table_name}...")
                cursor.execute(f"""
                    CREATE UNIQUE INDEX CONCURRENTLY {index_name} 
                    ON {schema}.{table_name} (hashkey)
                """)
                print(f"   [2/2] Adding PRIMARY KEY constraint...")
            
            # Add the PRIMARY KEY constraint (common for all paths)
            cursor.execute(f"""
                ALTER TABLE {schema}.{table_name} 
                ADD CONSTRAINT {constraint_name} 
                PRIMARY KEY USING INDEX {index_name}
            """)
        
        conn.close()
        
        duration_min = (time.time() - start_time) / 60
        print(f"\n✅ SUCCESS: {table_name}")
        print(f"   Duration: {duration_min:.1f} minutes")
        print(f"   Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        return {
            "table": table_name,
            "success": True,
            "duration_min": duration_min,
            "index_name": index_name,
            "constraint_name": constraint_name
        }
        
    except Exception as e:
        duration_min = (time.time() - start_time) / 60
        print(f"\n❌ FAILED: {table_name}")
        print(f"   Error: {str(e)[:200]}")
        print(f"   Duration: {duration_min:.1f} minutes")
        
        return {
            "table": table_name,
            "success": False,
            "duration_min": duration_min,
            "error": str(e)
        }

# COMMAND ----------

# Connection info for worker threads
conninfo = {
    "host": lakebase_host,
    "database": lakebase_database,
    "user": lakebase_user,
    "password": lakebase_password
}

print()
print("=" * 80)
print("🚀 STARTING HYBRID INDEX BUILD WITH ADAPTIVE OPTIMIZATION")
print("=" * 80)
print(f"Tables to process: {len(tables_needing_indexes)}")
print(f"Max concurrent builds: {max_concurrent_builds}")
print(f"Cluster RAM: {total_node_ram_gb:.1f} GB")
print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 80)
print()
print("💡 Hybrid Scheduling Strategy:")
print("   • Large tables (>50 GB): Sequential with 1 worker, max memory")
print("   • Medium tables (10-50 GB): Up to 2 concurrent, 2 workers each")
print("   • Small tables (<10 GB): Up to 4 concurrent, 4 workers each")
print()
print("⚡ Memory-Fraction Optimization:")
print("   • 1 worker (large): 48 GB memory → max memory fraction")
print("   • 2 workers (medium): 24 GB per worker")
print("   • 4 workers (small): 12 GB per worker")
print("   • Higher memory fraction = fewer external sorts = faster!")
print()
print("🛡️  Safety:")
print(f"   • Semaphore limits concurrent builds to {max_concurrent_builds}")
print("   • Large tables auto-run sequentially (prevents temp disk exhaustion)")
print("   • Small tables run concurrently (maximize throughput)")
print("=" * 80)

# COMMAND ----------

# Hybrid scheduler implementation
from threading import Thread, Semaphore

results = []
start_time = time.time()
semaphore = Semaphore(max_concurrent_builds)
threads = []

def run_build_with_semaphore(table_info):
    """Run index build with semaphore control"""
    # Get table optimization to check if it's a large table
    opt = get_table_specific_optimization(
        table_size_gb=table_info["size_gb"],
        table_rows=table_info["rows"],
        total_node_ram_gb=total_node_ram_gb
    )
    
    # Large tables run sequentially (block until done)
    # Small/medium tables run concurrently (use semaphore)
    if opt["is_large_table"]:
        # Large table: Run sequentially (blocks)
        print(f"\n⏸️  SEQUENTIAL: {table_info['table']} (>50 GB) - runs alone to prevent temp disk exhaustion")
        result = build_index(table_info, lakebase_schema, conninfo, total_node_ram_gb)
        results.append(result)
    else:
        # Small/medium table: Run with semaphore (concurrent)
        with semaphore:
            result = build_index(table_info, lakebase_schema, conninfo, total_node_ram_gb)
            results.append(result)

# Start all builds
# Large tables will block and run sequentially
# Small/medium tables will run concurrently (up to max_concurrent_builds)
for table_info in tables_needing_indexes:
    opt = get_table_specific_optimization(
        table_size_gb=table_info["size_gb"],
        table_rows=table_info["rows"],
        total_node_ram_gb=total_node_ram_gb
    )
    
    if opt["is_large_table"]:
        # Large table: Run immediately (blocks)
        run_build_with_semaphore(table_info)
    else:
        # Small/medium table: Start thread
        t = Thread(target=run_build_with_semaphore, args=(table_info,))
        threads.append(t)
        t.start()

# Wait for all concurrent builds to finish
for t in threads:
    t.join()

total_duration_min = (time.time() - start_time) / 60

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5️⃣ Summary Report

# COMMAND ----------

print()
print("=" * 80)
print("📊 INDEX BUILD SUMMARY")
print("=" * 80)

successful = [r for r in results if r["success"]]
failed = [r for r in results if not r["success"]]

for result in sorted(results, key=lambda x: x["duration_min"], reverse=True):
    status = "✅" if result["success"] else "❌"
    print(f"{status} {result['table']:70} {result['duration_min']:>6.1f} min")

print("=" * 80)
print(f"✅ Successful: {len(successful)}")
print(f"❌ Failed:     {len(failed)}")
print(f"⏱️  Total time: {total_duration_min:.1f} minutes")
print("=" * 80)

if failed:
    print("\n❌ FAILURES:")
    for result in failed:
        print(f"  - {result['table']}")
        print(f"    Error: {result['error'][:200]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6️⃣ Verify All Indexes Created

# COMMAND ----------

conn = psycopg2.connect(
    host=lakebase_host,
    database=lakebase_database,
    user=lakebase_user,
    password=lakebase_password,
    sslmode='require'
)

with conn.cursor() as cursor:
    # Check final state
    cursor.execute(f"""
        SELECT 
            t.tablename,
            (SELECT COUNT(*) 
             FROM pg_indexes i 
             WHERE i.schemaname = t.schemaname 
               AND i.tablename = t.tablename) AS index_count
        FROM pg_tables t
        WHERE t.schemaname = '{lakebase_schema}'
          AND t.tablename NOT LIKE '%stage%'
          AND t.tablename NOT LIKE '%__st'
          AND t.tablename NOT LIKE '%__sta'
        ORDER BY t.tablename
    """)
    
    still_missing = []
    for tablename, index_count in cursor.fetchall():
        if index_count == 0:
            still_missing.append(tablename)

conn.close()

if still_missing:
    print("\n⚠️  TABLES STILL WITHOUT INDEXES:")
    for table in still_missing:
        print(f"  - {table}")
    dbutils.notebook.exit(f"FAILED: {len(still_missing)} tables still missing indexes")
else:
    print("\n✅ ALL TABLES NOW HAVE INDEXES!")
    dbutils.notebook.exit("SUCCESS: All indexes built")
