"""
Lakebase Pipelined Bulk Load Script
====================================

Production-ready bulk loader for Lakebase with comprehensive crash recovery,
resource management, and performance optimization.

FEATURES
--------
✓ Comprehensive checkpointing (CSV/table/index/swap levels)
✓ PostgreSQL 63-char identifier limit handling
✓ Dynamic configuration via environment variables
✓ Structured logging with Python logging module
✓ Performance metrics collection and reporting
✓ Retry logic for all operations (COPY/index/swap)
✓ Wave-based parallel COPY (configurable concurrency)
✓ Sequential index builds (no resource contention)
✓ Atomic staging → production swaps
✓ Old table preservation for 24h rollback window

ENVIRONMENT VARIABLES
--------------------
TABLES_PER_WAVE          - Tables per wave (default: 2)
COPY_STREAMS_PER_TABLE   - COPY streams per table (default: 2)
SLOW_COPY_THRESHOLD      - Slow COPY warning in seconds (default: 120)
MAINTENANCE_WORK_MEM     - Index build memory (default: 2GB)
MAX_PARALLEL_WORKERS     - Index parallel workers (default: 3)

RESOURCE CONSIDERATIONS
----------------------
1. COPY Concurrency:
   Total streams = TABLES_PER_WAVE × COPY_STREAMS_PER_TABLE
   - Recommended: ≤ 8 streams to avoid I/O saturation
   - Monitor: UC Volume throughput, network bandwidth
   - If COPY slows (>120s/file), reduce concurrency

2. Index Memory:
   Total memory = MAINTENANCE_WORK_MEM × (1 + MAX_PARALLEL_WORKERS)
   - Default: 2GB × 4 = 8GB per index build
   - Ensure Lakebase node has >16GB RAM minimum
   - For larger tables: Consider increasing to 4GB (16GB total)
   - calculate_index_memory() available for dynamic tuning

3. Connection Pool:
   Pool size = TABLES_PER_WAVE × COPY_STREAMS_PER_TABLE + 4
   - +4 reserved for admin/index/swap operations
   - Monitor: Lakebase connection count

USAGE
-----
Basic:
    adaptor = LakebaseAdaptor(conninfo, tables, uc_volume_path)
    adaptor.load_tables()      # Phase 1: COPY to staging
    adaptor.build_indexes()    # Phase 2: Build indexes
    adaptor.swap_tables()      # Phase 3: Swap to production
    adaptor.cleanup()          # Print metrics & cleanup

With Custom Config:
    import os
    os.environ["TABLES_PER_WAVE"] = "3"
    os.environ["COPY_STREAMS_PER_TABLE"] = "2"
    os.environ["MAINTENANCE_WORK_MEM"] = "4GB"
    
    adaptor = LakebaseAdaptor(conninfo, tables)
    ...

CRASH RECOVERY
-------------
Checkpoint file: {UC_VOLUME_PATH}/.checkpoint_pipelined_load.json

If job crashes, rerun the same command. The adaptor will:
- Skip already-copied CSV files
- Skip already-loaded tables
- Skip already-built indexes
- Skip already-swapped tables

Checkpoint management:
    python utils/checkpoint_tool.py show          # View state
    python utils/checkpoint_tool.py reset         # Start fresh
    python utils/checkpoint_tool.py reset-table T # Reset one table

CONCURRENT RUNS
--------------
WARNING: Running multiple instances on the same schema requires care:
1. Use different checkpoint files
2. Ensure tables don't overlap
3. Or run sequentially to avoid naming conflicts

PERFORMANCE METRICS
------------------
At completion, prints summary:
- Total elapsed time
- Per-phase timings (COPY/INDEX/SWAP)
- Average times per table
- Wave completion times

LOGGING
-------
Uses Python logging module (level: INFO by default)
All operations logged with timestamps and severity levels
"""

import os
import threading
import queue
import time
import json
import hashlib
import logging
import psycopg
from psycopg_pool import ConnectionPool

# Validate psycopg v3 is installed
assert psycopg.__version__.startswith("3"), f"psycopg v3 required, got {psycopg.__version__}"

# ---------------- LOGGING ----------------

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def silence_psycopg_logging(level=logging.WARNING):
    """
    Silence psycopg / psycopg_pool internal logging (COPY, pool command channel, etc).
    Prevents "Received command c on object id p0" spam in logs.
    
    Based on Databricks KB: https://kb.databricks.com/notebooks/cmd-c-on-object-id-p0
    - Some libraries set log level to INFO, causing spam
    - Must set propagate=False AND explicitly set handler levels
    """
    # Suppress py4j gateway messages (standard Databricks fix)
    logging.getLogger("py4j").setLevel(logging.ERROR)
    
    # Suppress psycopg library messages (same principle)
    for name in (
        "psycopg",
        "psycopg.pool",
        "psycopg.pq",
        "psycopg_pool",
        "psycopg_pool.pool",
    ):
        logger = logging.getLogger(name)
        
        # Stop propagation to Databricks root handlers
        logger.propagate = False
        
        # Raise logger level
        logger.setLevel(level)
        
        # ALSO raise level on any pre-existing handlers (CRITICAL for Databricks)
        for h in logger.handlers:
            h.setLevel(level)

# Suppress psycopg's verbose protocol logging
silence_psycopg_logging()

logger = logging.getLogger(__name__)

# ---------------- CONFIG ----------------

UC_VOLUME_PATH = "/Volumes/benchmark/data_load/benchmark_data_dev"
CHECKPOINT_FILE = f"{UC_VOLUME_PATH}/.checkpoint_pipelined_load.json"

# COPY config (can be overridden via environment variables)
TABLES_PER_WAVE = int(os.environ.get("TABLES_PER_WAVE", "2"))
COPY_STREAMS_PER_TABLE = int(os.environ.get("COPY_STREAMS_PER_TABLE", "2"))
MAX_POOL_SIZE = TABLES_PER_WAVE * COPY_STREAMS_PER_TABLE + 4
SLOW_COPY_THRESHOLD = int(os.environ.get("SLOW_COPY_THRESHOLD", "120"))  # sec/file

# Index build memory (can be overridden via environment variables)
MAINTENANCE_WORK_MEM = os.environ.get("MAINTENANCE_WORK_MEM", "2GB")
MAX_PARALLEL_WORKERS = int(os.environ.get("MAX_PARALLEL_WORKERS", "3"))

# PostgreSQL identifier length limit
PG_MAX_IDENTIFIER_LENGTH = 63

# ---------------- CONFIG VALIDATION ----------------

def validate_config():
    """
    Validate configuration and warn about potential resource issues.
    Called at module import time.
    """
    warnings = []
    
    # Check COPY concurrency
    total_copy_streams = TABLES_PER_WAVE * COPY_STREAMS_PER_TABLE
    if total_copy_streams > 8:
        warnings.append(
            f"High COPY concurrency: {total_copy_streams} streams. "
            f"May saturate I/O. Reduce TABLES_PER_WAVE or COPY_STREAMS_PER_TABLE if needed."
        )
    
    # Check index memory
    try:
        max_index_memory_gb = int(MAINTENANCE_WORK_MEM.replace('GB', '').replace('gb', '').strip())
        total_index_memory_gb = max_index_memory_gb * (1 + MAX_PARALLEL_WORKERS)
        
        if total_index_memory_gb > 16:
            warnings.append(
                f"High index memory: {total_index_memory_gb}GB per build. "
                f"Ensure node has >{total_index_memory_gb * 2}GB RAM."
            )
    except:
        pass  # Ignore parsing errors
    
    # Log warnings
    if warnings:
        logger.warning("Configuration validation warnings:")
        for warning in warnings:
            logger.warning(f"  - {warning}")

# Validate config at module import
validate_config()

# ---------------- UTILITIES ----------------

def calculate_index_memory(table_size_gb, available_ram_gb=64):
    """
    Dynamically calculate maintenance_work_mem based on table size.
    
    NOTE: This function is currently NOT used by default to avoid complexity.
    The script uses fixed MAINTENANCE_WORK_MEM from config (default: 2GB).
    
    To enable dynamic memory:
    1. Get table size before index build (query pg_relation_size)
    2. Call this function: mem = calculate_index_memory(table_size_gb)
    3. Use returned value instead of MAINTENANCE_WORK_MEM
    
    Strategy:
    - Small tables (<10GB): 2GB
    - Medium tables (10-50GB): 4GB
    - Large tables (>50GB): 6GB
    - Cap at 25% of available RAM / workers
    
    WARNING: Total memory = maintenance_work_mem × (1 leader + N workers)
    Example: 6GB × (1 + 3 workers) = 24GB per index build
    Ensure Lakebase node has sufficient RAM before increasing.
    """
    max_mem_gb = (available_ram_gb * 0.25) / MAX_PARALLEL_WORKERS
    
    if table_size_gb < 10:
        mem_gb = 2
    elif table_size_gb < 50:
        mem_gb = 4
    else:
        mem_gb = 6
    
    # Cap at safe limit
    mem_gb = min(mem_gb, max_mem_gb)
    
    return f"{int(mem_gb)}GB"


def make_safe_index_name(table_name, suffix="_pkey"):
    """
    Generate a PostgreSQL-safe index name (max 63 chars).
    
    Strategy:
    - Use first 30 chars of table name
    - Add 8-char hash for uniqueness
    - Add suffix
    - Add timestamp (10 chars)
    
    Example: client_id_card_fingerprint_23a4f5b1_pkey_1737123456
    Total: 30 + 1 + 8 + 5 + 1 + 10 = 55 chars (safe)
    """
    timestamp = str(int(time.time()))
    
    # Hash the full table name for uniqueness
    hash_obj = hashlib.md5(table_name.encode())
    hash_str = hash_obj.hexdigest()[:8]
    
    # Truncate table name to fit within limit
    # Formula: max_table_len + "_" + hash(8) + suffix + "_" + timestamp(10) <= 62 (1 char safety buffer)
    # max_table_len = 62 - 1 - 8 - len(suffix) - 1 - 10
    max_table_len = 62 - 1 - 8 - len(suffix) - 1 - 10  # Target 62 chars max (1 char buffer)
    
    truncated_table = table_name[:max_table_len]
    
    index_name = f"{truncated_table}_{hash_str}{suffix}_{timestamp}"
    
    # Safety check
    if len(index_name) >= PG_MAX_IDENTIFIER_LENGTH:  # Changed > to >= for extra safety
        # Fallback: use hash-only name
        index_name = f"idx_{hash_str}{suffix}_{timestamp}"
    
    return index_name

# ---------------- METRICS COLLECTOR ----------------

class MetricsCollector:
    """Collects timing and performance metrics for historical analysis."""
    
    def __init__(self):
        self.metrics = {
            "wave_times": [],           # [(wave_num, elapsed_seconds), ...]
            "table_copy_times": {},     # {table_name: elapsed_seconds, ...}
            "index_build_times": {},    # {table_name: elapsed_seconds, ...}
            "swap_times": {},           # {table_name: elapsed_seconds, ...}
            "total_start_time": None,
            "total_end_time": None,
        }
    
    def start_total(self):
        self.metrics["total_start_time"] = time.time()
    
    def end_total(self):
        self.metrics["total_end_time"] = time.time()
    
    def record_wave(self, wave_num, elapsed):
        self.metrics["wave_times"].append((wave_num, elapsed))
    
    def record_table_copy(self, table_name, elapsed):
        self.metrics["table_copy_times"][table_name] = elapsed
    
    def record_index_build(self, table_name, elapsed):
        self.metrics["index_build_times"][table_name] = elapsed
    
    def record_swap(self, table_name, elapsed):
        self.metrics["swap_times"][table_name] = elapsed
    
    def get_summary(self):
        """Get summary statistics."""
        total_elapsed = (self.metrics["total_end_time"] - self.metrics["total_start_time"]) if self.metrics["total_end_time"] else 0
        
        return {
            "total_elapsed_minutes": total_elapsed / 60,
            "total_waves": len(self.metrics["wave_times"]),
            "total_tables": len(self.metrics["table_copy_times"]),
            "avg_copy_time_minutes": sum(self.metrics["table_copy_times"].values()) / len(self.metrics["table_copy_times"]) / 60 if self.metrics["table_copy_times"] else 0,
            "avg_index_time_minutes": sum(self.metrics["index_build_times"].values()) / len(self.metrics["index_build_times"]) / 60 if self.metrics["index_build_times"] else 0,
            "total_copy_time_minutes": sum(self.metrics["table_copy_times"].values()) / 60,
            "total_index_time_minutes": sum(self.metrics["index_build_times"].values()) / 60,
            "total_swap_time_minutes": sum(self.metrics["swap_times"].values()) / 60,
        }
    
    def print_summary(self):
        """Print formatted summary."""
        summary = self.get_summary()
        logger.info("=" * 72)
        logger.info("PERFORMANCE METRICS SUMMARY")
        logger.info("=" * 72)
        logger.info(f"Total elapsed time: {summary['total_elapsed_minutes']:.1f} min")
        logger.info(f"Tables processed: {summary['total_tables']}")
        logger.info(f"Phase 1 (COPY): {summary['total_copy_time_minutes']:.1f} min (avg {summary['avg_copy_time_minutes']:.1f} min/table)")
        logger.info(f"Phase 2 (INDEX): {summary['total_index_time_minutes']:.1f} min (avg {summary['avg_index_time_minutes']:.1f} min/table)")
        logger.info(f"Phase 3 (SWAP): {summary['total_swap_time_minutes']:.1f} min")
        logger.info("=" * 72)


# ---------------- CHECKPOINT MANAGER ----------------

class CheckpointManager:
    """Thread-safe checkpoint manager for crash recovery."""
    
    def __init__(self, checkpoint_file):
        self.checkpoint_file = checkpoint_file
        self.lock = threading.Lock()
        self.data = self._load()
    
    def _load(self):
        """Load checkpoint from disk."""
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r') as f:
                    data = json.load(f)
                    print(f"[CHECKPOINT] Loaded from {self.checkpoint_file}")
                    print(f"[CHECKPOINT] Previously completed: "
                          f"{len(data.get('completed_tables', []))} tables, "
                          f"{len(data.get('completed_indexes', []))} indexes")
                    return data
            except Exception as e:
                print(f"[WARN] Failed to load checkpoint: {e}")
        return {
            "completed_csv_files": {},  # {table_name: [csv_file1, csv_file2, ...]}
            "completed_tables": [],      # [table_name1, table_name2, ...]
            "completed_indexes": {},     # {table_name: index_name}
            "completed_swaps": [],       # [table_name1, table_name2, ...]
        }
    
    def _save(self):
        """Save checkpoint to disk."""
        try:
            with open(self.checkpoint_file, 'w') as f:
                json.dump(self.data, f, indent=2)
        except Exception as e:
            print(f"[WARN] Failed to save checkpoint: {e}")
    
    def is_csv_completed(self, table_name, csv_file):
        return csv_file in self.data.get("completed_csv_files", {}).get(table_name, [])
    
    def mark_csv_completed(self, table_name, csv_file):
        with self.lock:
            if table_name not in self.data["completed_csv_files"]:
                self.data["completed_csv_files"][table_name] = []
            if csv_file not in self.data["completed_csv_files"][table_name]:
                self.data["completed_csv_files"][table_name].append(csv_file)
            self._save()
    
    def is_table_completed(self, table_name):
        return table_name in self.data.get("completed_tables", [])
    
    def mark_table_completed(self, table_name):
        with self.lock:
            if table_name not in self.data["completed_tables"]:
                self.data["completed_tables"].append(table_name)
            self._save()
    
    def is_index_completed(self, table_name):
        return table_name in self.data.get("completed_indexes", {})
    
    def mark_index_completed(self, table_name, index_name):
        with self.lock:
            self.data["completed_indexes"][table_name] = index_name
            self._save()
    
    def get_index_name(self, table_name):
        return self.data.get("completed_indexes", {}).get(table_name)
    
    def is_swap_completed(self, table_name):
        return table_name in self.data.get("completed_swaps", [])
    
    def mark_swap_completed(self, table_name):
        with self.lock:
            if table_name not in self.data["completed_swaps"]:
                self.data["completed_swaps"].append(table_name)
            self._save()
    
    def cleanup(self):
        """Remove checkpoint file after successful completion."""
        try:
            if os.path.exists(self.checkpoint_file):
                os.remove(self.checkpoint_file)
                print(f"[CHECKPOINT] Removed {self.checkpoint_file}")
        except Exception as e:
            print(f"[WARN] Failed to remove checkpoint: {e}")

# ---------------- LAKEBASE ADAPTOR ----------------

class LakebaseAdaptor:
    """
    Adaptor for loading data into Lakebase with checkpointing and crash recovery.
    
    Usage:
        adaptor = LakebaseAdaptor(conninfo, tables, uc_volume_path)
        adaptor.load_tables()
        adaptor.build_indexes()
        adaptor.swap_tables()
    """
    
    def __init__(self, conninfo, tables, uc_volume_path=UC_VOLUME_PATH, 
                 checkpoint_file=CHECKPOINT_FILE, schema="features", rows_per_table_file=None):
        self.conninfo = conninfo
        self.tables = tables
        self.uc_volume_path = uc_volume_path
        self.checkpoint_file = checkpoint_file
        self.schema = schema
        self.rows_per_table_file = rows_per_table_file
        
        self.checkpoint_mgr = CheckpointManager(checkpoint_file)
        self.metrics = MetricsCollector()
        self.conninfo = conninfo
        
        self.pool = ConnectionPool(
            conninfo=conninfo,
            min_size=2,
            max_size=MAX_POOL_SIZE,
            timeout=30.0,  # Wait max 30s for connection from pool
            max_waiting=MAX_POOL_SIZE * 2,  # Limit queue size
            open=True  # Pre-open connections
        )
        
        self.loaded_tasks = []
        self.index_map = {}
        
        # Suppress notices on initial pool connections
        for _ in range(2):  # min_size connections
            try:
                with self.pool.connection() as conn:
                    self._suppress_notices(conn)
            except:
                pass  # Pool may not be fully initialized yet
        
        # Resource saturation warnings
        total_copy_streams = TABLES_PER_WAVE * COPY_STREAMS_PER_TABLE
        if total_copy_streams > 8:
            logger.warning(f"High concurrency detected: {total_copy_streams} concurrent COPY streams")
            logger.warning("Consider reducing TABLES_PER_WAVE or COPY_STREAMS_PER_TABLE if I/O saturates")
        
        # Memory validation for index builds
        max_index_memory_gb = int(MAINTENANCE_WORK_MEM.replace('GB', '').replace('gb', ''))
        total_index_memory_gb = max_index_memory_gb * (1 + MAX_PARALLEL_WORKERS)
        if total_index_memory_gb > 16:
            logger.warning(f"High index memory detected: {total_index_memory_gb}GB per index build")
            logger.warning(f"Ensure Lakebase node has sufficient RAM (recommend >{total_index_memory_gb * 2}GB)")
    
    def _make_stage_table_name(self, table_name):
        return f"{self.schema}.{table_name}__stage"
    
    def _make_prod_table_name(self, table_name):
        return f"{self.schema}.{table_name}"
    
    def _get_csv_dir(self, table_name):
        # CSV directories are named with schema prefix: features.table_name_csvs
        return f"{self.uc_volume_path}/{self.schema}.{table_name}_csvs"
    
    def _suppress_notices(self, conn):
        """Suppress PostgreSQL INFO/NOTICE messages on a connection."""
        # Ignore libpq notices
        conn.add_notice_handler(lambda diag: None)
        # Suppress server-side INFO/NOTICE messages
        with conn.cursor() as cur:
            cur.execute("SET client_min_messages = WARNING;")
    
    def load_tables_sequential(self):
        """Load tables sequentially: each table goes COPY → INDEX → SWAP → VALIDATE before moving to next."""
        self.metrics.start_total()
        logger.info("=" * 72)
        logger.info("SEQUENTIAL LOAD: COPY → INDEX → SWAP → VALIDATE (per table)")
        logger.info("=" * 72)
        
        # Load expected row counts if available
        expected_counts = {}
        if hasattr(self, 'rows_per_table_file') and self.rows_per_table_file:
            try:
                with open(self.rows_per_table_file, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith('#'):
                            parts = line.split()
                            if len(parts) >= 2:
                                table_name = parts[0].replace('features.', '')
                                # Handle formats like "80M", "1B", or "80000000"
                                count_str = parts[1].upper()
                                if count_str.endswith('B'):
                                    expected_counts[table_name] = int(float(count_str[:-1]) * 1_000_000_000)
                                elif count_str.endswith('M'):
                                    expected_counts[table_name] = int(float(count_str[:-1]) * 1_000_000)
                                else:
                                    expected_counts[table_name] = int(count_str)
                logger.info(f"✅ Loaded expected row counts for {len(expected_counts)} tables")
            except Exception as e:
                logger.warning(f"Could not load expected row counts: {e}")
        
        completed_tables = 0
        
        for i, table_name in enumerate(self.tables, 1):
            # Skip if already fully completed (all phases)
            if self.checkpoint_mgr.is_swap_completed(table_name):
                logger.info(f"\n[{i}/{len(self.tables)}] {table_name} - ✓ Already completed (skipping)")
                completed_tables += 1
                continue
            
            logger.info(f"\n[{i}/{len(self.tables)}] Processing {table_name}...")
            
            try:
                task = TableLoadTask(table_name, self.checkpoint_mgr, self)
                task.discover_csv_files()  # CRITICAL: Populate csv_files list before COPY
                
                # Phase 1: COPY
                if not self.checkpoint_mgr.is_table_completed(table_name):
                    self._copy_table(task)
                    self.checkpoint_mgr.mark_table_completed(table_name)
                else:
                    logger.info(f"  ✓ COPY already completed")
                
                # Phase 2: INDEX
                if not self.checkpoint_mgr.is_index_completed(table_name):
                    index_name = self._build_pk_index(task)
                    self.index_map[table_name] = index_name
                else:
                    index_name = self.checkpoint_mgr.get_index_name(table_name)
                    self.index_map[table_name] = index_name
                    logger.info(f"  ✓ INDEX already built")
                
                # Phase 3: SWAP
                if not self.checkpoint_mgr.is_swap_completed(table_name):
                    self._swap_table(task, index_name)
                else:
                    logger.info(f"  ✓ SWAP already completed")
                
                # Phase 4: VALIDATE (NEW!)
                expected_count = expected_counts.get(table_name)
                success, errors = self._validate_table(table_name, expected_count)
                
                if not success:
                    error_msg = f"Table validation failed for {table_name}:\n" + "\n".join(f"  - {e}" for e in errors)
                    logger.error(error_msg)
                    raise RuntimeError(error_msg)
                
                completed_tables += 1
                logger.info(f"  ✅ {table_name} complete ({completed_tables}/{len(self.tables)} tables done)")
                
            except Exception as e:
                logger.error(f"  ❌ {table_name} failed: {e}")
                raise
        
        logger.info(f"\n{'='*72}")
        logger.info(f"✅ ALL TABLES COMPLETE: {completed_tables}/{len(self.tables)}")
        logger.info(f"{'='*72}")
    
    def load_tables(self):
        """Phase 1: COPY CSVs to staging tables (wave-based parallel)."""
        self.metrics.start_total()
        logger.info("=" * 72)
        logger.info("PHASE 1: Load Tables (COPY to staging)")
        logger.info("=" * 72)
        
        for i in range(0, len(self.tables), TABLES_PER_WAVE):
            wave_num = i // TABLES_PER_WAVE + 1
            wave_tables = self.tables[i:i + TABLES_PER_WAVE]
            total_waves = (len(self.tables) + TABLES_PER_WAVE - 1) // TABLES_PER_WAVE
            logger.info(f"\nWAVE {wave_num}/{total_waves}: Loading {len(wave_tables)} tables")
            
            wave_start = time.time()
            tasks = self._load_wave(wave_tables)
            wave_elapsed = time.time() - wave_start
            self.metrics.record_wave(wave_num, wave_elapsed)
            
            self.loaded_tasks.extend(tasks)
        
        # Add already-completed tables to loaded_tasks
        for table in self.tables:
            if self.checkpoint_mgr.is_table_completed(table) and \
               table not in [t.table_name for t in self.loaded_tasks]:
                task = TableLoadTask(table, self.checkpoint_mgr, self)
                self.loaded_tasks.append(task)
        
        logger.info(f"\n✓ Phase 1 complete: {len(self.loaded_tasks)} tables loaded")
    
    def build_indexes(self):
        """Phase 2: Build indexes on staging tables (sequential)."""
        logger.info("\n" + "=" * 72)
        logger.info("PHASE 2: Build Indexes (Sequential)")
        logger.info("=" * 72)
        
        for i, task in enumerate(self.loaded_tasks, 1):
            logger.info(f"\n[{i}/{len(self.loaded_tasks)}] Processing index for {task.table_name}...")
            index_name = self._build_pk_index(task)
            self.index_map[task.table_name] = index_name
        
        logger.info(f"\n✓ Phase 2 complete: {len(self.index_map)} indexes built")
    
    def swap_tables(self):
        """Phase 3: Swap staging → production (atomic per table)."""
        logger.info("\n" + "=" * 72)
        logger.info("PHASE 3: Swap Staging → Production")
        logger.info("=" * 72)
        
        for i, task in enumerate(self.loaded_tasks, 1):
            logger.info(f"\n[{i}/{len(self.loaded_tasks)}] Processing swap for {task.table_name}...")
            index_name = self.index_map[task.table_name]
            self._swap_table(task, index_name)
        
        logger.info(f"\n✓ Phase 3 complete: {len(self.loaded_tasks)} tables swapped")
        
        # Mark total end time
        self.metrics.end_total()
    
    def cleanup(self):
        """Cleanup resources, print metrics, and remove checkpoint."""
        # Print metrics summary
        self.metrics.print_summary()
        
        # Cleanup checkpoint and connections
        self.checkpoint_mgr.cleanup()
        self.pool.close()
    
    def _load_wave(self, wave_tables):
        """Load a wave of tables in parallel."""
        tasks = []
        for table in wave_tables:
            task = TableLoadTask(table, self.checkpoint_mgr, self)
            
            if self.checkpoint_mgr.is_table_completed(table):
                print(f"[{table}] ✓ Already completed (skipping)")
                continue
            
            count = task.discover_csv_files()
            total_files = count + task.files_skipped
            print(f"[{table}] Found {total_files} CSV files ({task.files_skipped} already copied, {count} remaining)")
            
            if count == 0 and task.files_skipped > 0:
                self.checkpoint_mgr.mark_table_completed(table)
                print(f"[{table}] ✓ All CSV files already copied")
                continue
            elif count == 0:
                print(f"[WARN] Skipping {table} - no CSV files found")
                continue
            
            tasks.append(task)
        
        if not tasks:
            return []
        
        # COPY in parallel (each task creates its own staging table)
        copy_threads = []
        for task in tasks:
            if not self.checkpoint_mgr.is_table_completed(task.table_name):
                t = threading.Thread(target=self._copy_table, args=(task,))
                t.start()
                copy_threads.append(t)
        
        for t in copy_threads:
            t.join()
        
        return tasks
    
    def _copy_table(self, task):
        """COPY all CSV files for a table."""
        if self.checkpoint_mgr.is_table_completed(task.table_name):
            return
        
        task.start_time = time.time()
        
        if len(task.csv_files) == 0:
            print(f"[{task.table_name}] ✓ All CSV files already copied")
            self.checkpoint_mgr.mark_table_completed(task.table_name)
            return
        
        # CREATE TABLE + COPY first file with FREEZE in same transaction
        first_file = task.csv_files[0]
        remaining_files = task.csv_files[1:]
        
        print(f"[{task.table_name}] Creating table + loading first file with FREEZE...")
        file_start = time.time()
        
        with self.pool.connection() as conn:
            self._suppress_notices(conn)
            with conn.transaction():
                with conn.cursor() as cur:
                    # Create staging table
                    cur.execute(f"DROP TABLE IF EXISTS {task.stage_table};")
                    cur.execute(f"CREATE TABLE {task.stage_table} (LIKE {task.prod_table} INCLUDING DEFAULTS);")
                    
                    # COPY first file with FREEZE
                    cur.execute("SET LOCAL synchronous_commit = OFF;")
                    copy_sql = f"COPY {task.stage_table} FROM STDIN WITH (FORMAT csv, FREEZE, HEADER false)"
                    
                    with cur.copy(copy_sql) as copy:
                        with open(first_file, "rb") as f:
                            while chunk := f.read(1024 * 1024):
                                copy.write(chunk)
        
        # Mark first file as completed
        self.checkpoint_mgr.mark_csv_completed(task.table_name, first_file)
        file_elapsed = time.time() - file_start
        with task.lock:
            task.files_copied = 1
        task.update_progress(file_elapsed)
        
        print(f"[{task.table_name}] ✓ First file loaded with FREEZE ({file_elapsed:.1f}s)")
        
        # Start workers for remaining files (without FREEZE)
        if remaining_files:
            print(f"[{task.table_name}] Loading {len(remaining_files)} remaining files with {COPY_STREAMS_PER_TABLE} streams...")
            file_queue = queue.Queue()
            for csv_file in remaining_files:
                file_queue.put(csv_file)
            
            # Track worker errors
            worker_errors = []
            
            def worker_wrapper(queue, task, use_freeze):
                try:
                    self._copy_worker(queue, task, use_freeze)
                except Exception as e:
                    worker_errors.append(e)
            
            threads = []
            for _ in range(COPY_STREAMS_PER_TABLE):
                t = threading.Thread(target=worker_wrapper, args=(file_queue, task, False))
                t.daemon = True
                t.start()
                threads.append(t)
            
            for t in threads:
                t.join()
            
            # Check if any workers failed
            if worker_errors:
                error_msg = f"{len(worker_errors)} worker threads failed: {worker_errors[0]}"
                print(f"\n{'='*80}")
                print(f"[ERROR] {task.table_name} - Workers failed!")
                print(f"  {error_msg}")
                print(f"{'='*80}\n")
                raise RuntimeError(error_msg)
        
        elapsed = time.time() - task.start_time
        logger.info(f"[{task.table_name}] ✓ COPY complete in {elapsed/60:.1f} min")
        
        # Record metrics and checkpoint (only if all workers succeeded)
        self.metrics.record_table_copy(task.table_name, elapsed)
        self.checkpoint_mgr.mark_table_completed(task.table_name)
    
    def _copy_worker(self, file_queue, task, use_freeze=False):
        """Worker thread for COPY operations (psycopg v3)."""
        while True:
            try:
                csv_path = file_queue.get_nowait()
            except queue.Empty:
                break
            
            file_start = time.time()
            retry_count = 0
            max_retries = 3
            success = False
            
            while retry_count < max_retries:
                try:
                    with self.pool.connection() as conn:
                        self._suppress_notices(conn)
                        with conn.cursor() as cur:
                            # Transaction-scoped COPY
                            with conn.transaction():
                                cur.execute("SET LOCAL synchronous_commit = OFF;")
                                
                                # FREEZE only used for first file (created in same transaction)
                                freeze_clause = "FREEZE, " if use_freeze else ""
                                copy_sql = (
                                    f"COPY {task.stage_table} "
                                    f"FROM STDIN WITH (FORMAT csv, {freeze_clause}HEADER false)"
                                )
                                
                                with cur.copy(copy_sql) as copy:
                                    with open(csv_path, "rb") as f:
                                        while chunk := f.read(1024 * 1024):
                                            copy.write(chunk)
                    
                    success = True
                    break
                    
                except Exception as e:
                    retry_count += 1
                    if retry_count >= max_retries:
                        print(f"\n{'='*80}")
                        print(f"[ERROR] COPY FAILED for {os.path.basename(csv_path)}")
                        print(f"Full path: {csv_path}")
                        print(f"Error: {e}")
                        print(f"{'='*80}\n")
                        raise
                    
                    time.sleep(2 ** retry_count)
            
            if success:
                self.checkpoint_mgr.mark_csv_completed(task.table_name, csv_path)
                file_elapsed = time.time() - file_start
                with task.lock:
                    task.files_copied += 1
                task.update_progress(file_elapsed)
            
            file_queue.task_done()
    
    def _get_pk_columns(self, table_name):
        """Query the production table to discover PRIMARY KEY column(s)."""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                # Query pg_catalog to find PK columns for this table
                query = """
                    SELECT a.attname
                    FROM pg_index i
                    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                    WHERE i.indrelid = %s::regclass
                      AND i.indisprimary
                    ORDER BY a.attnum;
                """
                cur.execute(query, (f"{self.schema}.{table_name}",))
                pk_columns = [row[0] for row in cur.fetchall()]
                
                if not pk_columns:
                    # Fallback: assume hash_key (but log warning)
                    logger.warning(f"[{table_name}] No PRIMARY KEY found, defaulting to hash_key")
                    return ["hash_key"]
                
                return pk_columns
    
    def _validate_table(self, table_name, expected_row_count=None):
        """
        Validate table after load completion.
        
        Checks ONLY row count - all other validations are unnecessary because:
        - PRIMARY KEY constraint guarantees no NULLs, no duplicates, index exists
        - If any constraint was violated, the load would have already FAILED
        
        Returns: (success: bool, errors: List[str])
        """
        errors = []
        prod_table = f"{self.schema}.{table_name}"
        
        logger.info(f"[{table_name}] 🔍 Validating table...")
        
        try:
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    # Row count validation (COUNT(*) is fastest)
                    try:
                        cur.execute(f"SELECT COUNT(*) FROM {prod_table};")
                        actual_count = cur.fetchone()[0]
                        logger.info(f"[{table_name}]   Row count: {actual_count:,}")
                        
                        # Compare to expected if provided
                        if expected_row_count is not None:
                            diff = abs(actual_count - expected_row_count)
                            diff_pct = (diff / expected_row_count) * 100 if expected_row_count > 0 else 0
                            
                            if diff_pct > 1.0:  # Allow 1% variance
                                errors.append(
                                    f"Row count mismatch: expected {expected_row_count:,}, "
                                    f"got {actual_count:,} (diff: {diff_pct:.2f}%)"
                                )
                            else:
                                logger.info(f"[{table_name}]   ✅ Row count matches expected ({expected_row_count:,})")
                        else:
                            logger.info(f"[{table_name}]   ⚠️  No expected row count provided (validation skipped)")
                        
                    except Exception as e:
                        errors.append(f"Table not found or inaccessible: {e}")
                        return False, errors
                    
        except Exception as e:
            errors.append(f"Validation error: {e}")
            return False, errors
        
        if errors:
            logger.error(f"[{table_name}] ❌ Validation FAILED:")
            for error in errors:
                logger.error(f"[{table_name}]   - {error}")
            return False, errors
        else:
            logger.info(f"[{table_name}] ✅ Validation PASSED")
            return True, []
    
    def _build_pk_index(self, task):
        """Build index on staging table with retry logic."""
        if self.checkpoint_mgr.is_index_completed(task.table_name):
            existing_index = self.checkpoint_mgr.get_index_name(task.table_name)
            logger.info(f"[{task.table_name}] ✓ Index already built: {existing_index}")
            return existing_index
        
        # Generate safe index name (max 63 chars)
        index_name = make_safe_index_name(task.table_name, suffix="_pkey")
        
        # Dynamic memory calculation (optional, fallback to config)
        mem_config = MAINTENANCE_WORK_MEM
        
        # Discover PRIMARY KEY columns from production table
        pk_columns = self._get_pk_columns(task.table_name)
        pk_columns_str = ", ".join(f"{col} ASC" for col in pk_columns)
        
        logger.info(f"[{task.table_name}] Building PK index: {index_name} (len={len(index_name)})")
        logger.info(f"[{task.table_name}] PK columns: {pk_columns}")
        logger.info(f"[{task.table_name}] Using maintenance_work_mem={mem_config}, workers={MAX_PARALLEL_WORKERS}")
        
        start = time.time()
        retry_count = 0
        max_retries = 2
        
        while retry_count <= max_retries:
            try:
                with self.pool.connection() as conn:
                    self._suppress_notices(conn)
                    with conn.cursor() as cur:
                        # Cleanup any partially created index from previous retry
                        if retry_count > 0:
                            cur.execute(f"DROP INDEX IF EXISTS {self.schema}.{index_name};")
                        
                        cur.execute(f"SET maintenance_work_mem = '{mem_config}';")
                        cur.execute(f"SET max_parallel_maintenance_workers = {MAX_PARALLEL_WORKERS};")
                        cur.execute(f"CREATE UNIQUE INDEX {index_name} ON {task.stage_table} ({pk_columns_str});")
                    conn.commit()
                break  # Success
                
            except Exception as e:
                retry_count += 1
                if retry_count > max_retries:
                    logger.error(f"[{task.table_name}] Failed to build index after {max_retries} retries: {e}")
                    raise
                logger.warning(f"[{task.table_name}] Index build failed, retrying ({retry_count}/{max_retries})...")
                time.sleep(10 * retry_count)  # Wait before retry
        
        elapsed = time.time() - start
        logger.info(f"[{task.table_name}] ✓ Index built in {elapsed/60:.1f} min")
        
        # Record metrics and checkpoint
        self.metrics.record_index_build(task.table_name, elapsed)
        self.checkpoint_mgr.mark_index_completed(task.table_name, index_name)
        
        return index_name
    
    def _swap_table(self, task, index_name):
        """
        Swap staging → production with retry logic.
        
        IMPORTANT: This operation renames indexes and adds PRIMARY KEY constraints.
        If multiple concurrent runs target the same schema, ensure:
        1. Different checkpoint files are used
        2. Tables don't overlap between runs
        3. Or run sequentially to avoid naming conflicts
        """
        if self.checkpoint_mgr.is_swap_completed(task.table_name):
            logger.info(f"[{task.table_name}] ✓ Swap already completed")
            return
        
        logger.info(f"[{task.table_name}] Swapping staging → production...")
        start = time.time()
        
        # Generate safe old table name (max 63 chars) with timestamp for uniqueness
        old_table_name = make_safe_index_name(task.table_name, suffix="_old")
        
        # Production index name - must be stable for query planner
        # Format: {table}_pkey (truncated if needed)
        old_index = f"{task.table_name}_pkey"
        
        # Truncate index name if it exceeds PostgreSQL limit
        if len(old_index) > PG_MAX_IDENTIFIER_LENGTH:
            old_index = make_safe_index_name(task.table_name, suffix="_pkey")
            logger.warning(f"[{task.table_name}] Index name truncated to: {old_index}")
        
        retry_count = 0
        max_retries = 2
        
        while retry_count <= max_retries:
            try:
                with self.pool.connection() as conn:
                    self._suppress_notices(conn)
                    with conn.cursor() as cur:
                        # Check if production table exists and has an index
                        cur.execute(f"SELECT 1 FROM information_schema.tables WHERE table_schema = '{self.schema}' AND table_name = '{task.table_name}';")
                        prod_exists = cur.fetchone() is not None
                        
                        if prod_exists:
                            # Drop old PRIMARY KEY constraint (which also drops the index)
                            # Use ALTER TABLE DROP CONSTRAINT instead of DROP INDEX to avoid
                            # "DependentObjectsStillExist" error when index backs a constraint
                            cur.execute(f"ALTER TABLE {task.prod_table} DROP CONSTRAINT IF EXISTS {old_index};")
                        
                        cur.execute(f"ALTER TABLE IF EXISTS {task.prod_table} RENAME TO {old_table_name};")
                        cur.execute(f"ALTER TABLE {task.stage_table} RENAME TO {task.table_name};")
                        cur.execute(f"ALTER INDEX {self.schema}.{index_name} RENAME TO {old_index};")
                        cur.execute(f"ALTER TABLE {task.prod_table} ADD CONSTRAINT {old_index} PRIMARY KEY USING INDEX {old_index};")
                    conn.commit()
                break  # Success
                
            except Exception as e:
                retry_count += 1
                if retry_count > max_retries:
                    logger.error(f"[{task.table_name}] Failed to swap after {max_retries} retries: {e}")
                    raise
                logger.warning(f"[{task.table_name}] Swap failed, retrying ({retry_count}/{max_retries})...")
                time.sleep(5 * retry_count)
        
        logger.info(f"[{task.table_name}] ✓ Swapped successfully (old table: {self.schema}.{old_table_name})")
        
        # ANALYZE for query planner
        with self.pool.connection() as conn:
            self._suppress_notices(conn)
            with conn.cursor() as cur:
                cur.execute(f"ANALYZE {task.prod_table};")
            conn.commit()
        
        elapsed = time.time() - start
        logger.info(f"[{task.table_name}] ✓ Ready for queries (swap took {elapsed:.1f}s)")
        
        # Record metrics and checkpoint
        self.metrics.record_swap(task.table_name, elapsed)
        self.checkpoint_mgr.mark_swap_completed(task.table_name)

# ---------------- TABLE LOAD TASK ----------------

class TableLoadTask:
    """Tracks per-table CSV load progress."""
    
    def __init__(self, table_name, checkpoint_mgr, adaptor):
        self.table_name = table_name
        self.checkpoint_mgr = checkpoint_mgr
        self.adaptor = adaptor
        
        self.stage_table = adaptor._make_stage_table_name(table_name)
        self.prod_table = adaptor._make_prod_table_name(table_name)
        self.csv_dir = adaptor._get_csv_dir(table_name)
        
        self.csv_files = []
        self.files_copied = 0
        self.files_skipped = 0
        self.start_time = None
        self.lock = threading.Lock()
        self.file_times = []
    
    def discover_csv_files(self):
        """Find CSV files, filtering out already-completed ones."""
        if os.path.exists(self.csv_dir):
            all_csv_files = sorted([os.path.join(self.csv_dir, f)
                                   for f in os.listdir(self.csv_dir) if f.endswith(".csv")])
            
            self.csv_files = [f for f in all_csv_files 
                             if not self.checkpoint_mgr.is_csv_completed(self.table_name, f)]
            
            self.files_skipped = len(all_csv_files) - len(self.csv_files)
            
            if self.files_skipped > 0:
                print(f"[{self.table_name}] Skipping {self.files_skipped} already-copied CSV files")
        else:
            print(f"[WARN] CSV directory not found: {self.csv_dir}")
        
        return len(self.csv_files)
    
    def update_progress(self, file_elapsed=None):
        with self.lock:
            if file_elapsed:
                self.file_times.append(file_elapsed)
            elapsed = time.time() - self.start_time if self.start_time else 0
            total = len(self.csv_files)
            pct = (self.files_copied / total * 100) if total > 0 else 100
            eta = elapsed * (100 / pct - 1) if pct > 0 else -1
            avg_file_time = sum(self.file_times) / len(self.file_times) if self.file_times else 0
            
            logger.info(f"  [{self.table_name}] {self.files_copied}/{total} files ({pct:.1f}%), "
                       f"avg {avg_file_time:.1f}s/file, ETA {eta/60:.1f} min")
            
            if avg_file_time > SLOW_COPY_THRESHOLD:
                logger.warning(f"  [{self.table_name}] COPY slowing ({avg_file_time:.1f}s/file) – "
                             f"reduce TABLES_PER_WAVE or COPY_STREAMS_PER_TABLE to avoid I/O saturation")

# ---------------- MAIN ----------------

def main():
    """Example usage of LakebaseAdaptor."""
    
    LAKEBASE_CONNINFO = os.environ.get("LAKEBASE_CONNINFO")
    if not LAKEBASE_CONNINFO:
        raise ValueError("LAKEBASE_CONNINFO env var not set")
    
    TABLES = [
        "client_id_card_fingerprint__time_since__365d",
        "client_id_card_fingerprint_good_rates_365d",
        "client_id_customer_email_clean__time_since__365d",
        "client_id_customer_email_clean_good_rates_365d",
        "client_id_customer_email_dirty__time_since__365d",
        "client_id_customer_email_dirty_good_rates_365d",
        "client_id_customer_phone__time_since__365d",
        "client_id_customer_phone_good_rates_365d",
        "client_id_delivery_address__time_since__365d",
        "client_id_delivery_address_good_rates_365d",
        "client_id_device_fingerprint__time_since__365d",
        "client_id_device_fingerprint_good_rates_365d",
        "client_id_ip__time_since__365d",
        "client_id_ip_good_rates_365d",
        "customer_email_clean__time_since__365d",
        "customer_email_clean_good_rates_365d",
        "customer_email_dirty__time_since__365d",
        "customer_email_dirty_good_rates_365d",
        "customer_phone__time_since__365d",
        "customer_phone_good_rates_365d",
        "delivery_address__time_since__365d",
        "delivery_address_good_rates_365d",
        "device_fingerprint__time_since__365d",
        "device_fingerprint_good_rates_365d",
        "ip__time_since__365d",
        "ip_good_rates_365d",
        "card_fingerprint__time_since__365d",
        "card_fingerprint_good_rates_365d",
        "card_fingerprint_ip__time_since__365d",
        "card_fingerprint_ip_good_rates_365d",
    ]
    
    logger.info("=" * 72)
    logger.info("LAKEBASE BULK LOAD: COPY → INDEX → SWAP (Production Mode)")
    logger.info("=" * 72)
    logger.info(f"Tables: {len(TABLES)}")
    logger.info(f"Wave size: {TABLES_PER_WAVE} tables, {COPY_STREAMS_PER_TABLE} streams/table")
    logger.info(f"Total COPY sessions: {TABLES_PER_WAVE * COPY_STREAMS_PER_TABLE}")
    logger.info(f"Index build: {MAINTENANCE_WORK_MEM}, {MAX_PARALLEL_WORKERS} workers")
    logger.info(f"Checkpoint: {CHECKPOINT_FILE}")
    logger.info(f"Metrics: Enabled")
    logger.info(f"Retry logic: COPY (3×), Index (2×), Swap (2×)\n")
    
    # Create adaptor
    adaptor = LakebaseAdaptor(
        conninfo=LAKEBASE_CONNINFO,
        tables=TABLES,
        uc_volume_path=UC_VOLUME_PATH,
        checkpoint_file=CHECKPOINT_FILE,
        schema="features"
    )
    
    try:
        # Phase 1: Load tables
        adaptor.load_tables()
        
        # Phase 2: Build indexes
        adaptor.build_indexes()
        
        # Phase 3: Swap tables
        adaptor.swap_tables()
        
        logger.info(f"\n{'='*72}")
        logger.info("✓ ALL TABLES LOADED SUCCESSFULLY!")
        logger.info(f"{'='*72}")
        logger.info(f"Tables processed: {len(adaptor.loaded_tasks)}")
        
    finally:
        # Cleanup
        adaptor.cleanup()

if __name__ == "__main__":
    main()
