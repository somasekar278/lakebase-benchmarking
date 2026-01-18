"""
Instrumented Feature Server for Detailed Latency Analysis

Captures component-level timing:
- Network RTT
- Query execution time
- Result transfer time
- Per-table breakdown
- Cache statistics
- I/O metrics
"""

from typing import List, Dict, Optional, Tuple
import time
import psycopg
from psycopg_pool import ConnectionPool
from dataclasses import dataclass, field


@dataclass
class TimingBreakdown:
    """Detailed timing breakdown for a single request"""
    total_ms: float
    connection_acquire_ms: float
    network_rtt_ms: float
    query_execution_ms: float
    result_transfer_ms: float
    per_table_ms: Dict[str, float] = field(default_factory=dict)
    
    def to_dict(self) -> Dict:
        return {
            'total_ms': self.total_ms,
            'connection_acquire_ms': self.connection_acquire_ms,
            'network_rtt_ms': self.network_rtt_ms,
            'query_execution_ms': self.query_execution_ms,
            'result_transfer_ms': self.result_transfer_ms,
            'per_table_ms': self.per_table_ms
        }


@dataclass
class DatabaseMetrics:
    """PostgreSQL performance metrics"""
    cache_hit_ratio: float
    buffer_hits: int
    buffer_reads: int
    index_scans: Dict[str, int]
    sequential_scans: Dict[str, int]
    rows_fetched: Dict[str, int]
    
    def to_dict(self) -> Dict:
        return {
            'cache_hit_ratio': self.cache_hit_ratio,
            'buffer_hits': self.buffer_hits,
            'buffer_reads': self.buffer_reads,
            'total_index_scans': sum(self.index_scans.values()),
            'total_seq_scans': sum(self.sequential_scans.values()),
            'total_rows_fetched': sum(self.rows_fetched.values())
        }


class InstrumentedFeatureServer:
    """
    Feature server with detailed instrumentation
    
    Captures:
    - Component-level timing (network, execution, transfer)
    - Per-table latency breakdown
    - Cache hit ratios
    - I/O statistics
    - Index usage
    """
    
    def __init__(
        self,
        lakebase_config: Dict,
        table_schemas: Dict,
        pool_size: int = 10,
        max_pool_size: Optional[int] = None
    ):
        self.table_schemas = table_schemas
        
        # Build connection string
        conninfo = (
            f"host={lakebase_config['host']} "
            f"port={lakebase_config.get('port', 5432)} "
            f"dbname={lakebase_config['database']} "
            f"user={lakebase_config['user']} "
            f"password={lakebase_config['password']} "
            f"sslmode={lakebase_config.get('sslmode', 'require')}"
        )
        
        self.pool = ConnectionPool(
            conninfo=conninfo,
            min_size=pool_size,
            max_size=max_pool_size or (pool_size * 2),
            timeout=5.0
        )
        
        self.schema = lakebase_config.get('schema', 'public')
        
        print(f"✅ Instrumented feature server initialized")
        print(f"   Tables: {len(table_schemas)}")
        print(f"   Pool size: {pool_size}")
    
    def get_features_with_timing(
        self,
        hashkey: str,
        table_names: List[str]
    ) -> Tuple[Dict, TimingBreakdown]:
        """
        Retrieve features with detailed timing breakdown
        
        Returns:
            (results, timing_breakdown)
        """
        overall_start = time.perf_counter()
        
        # Component timings
        conn_acquire_start = time.perf_counter()
        
        results = {}
        per_table_times = {}
        
        with self.pool.connection() as conn:
            conn_acquire_ms = (time.perf_counter() - conn_acquire_start) * 1000
            
            # Measure network RTT with simple ping query
            rtt_start = time.perf_counter()
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            network_rtt_ms = (time.perf_counter() - rtt_start) * 1000
            
            # Execute all queries with per-table timing
            query_exec_start = time.perf_counter()
            
            with conn.cursor() as cursor:
                # OPTIMAL PATTERN: Separate execute and fetch phases
                with conn.pipeline():
                    # Phase 1: Queue all executes (single flush)
                    for table_name in table_names:
                        schema = self.table_schemas[table_name]
                        
                        # Track start time for this table
                        table_start = time.perf_counter()
                        cursor.execute(schema.query, (hashkey,))
                        per_table_times[table_name] = (time.perf_counter() - table_start) * 1000
                
                query_exec_ms = (time.perf_counter() - query_exec_start) * 1000
                
                # Phase 2: Fetch all results (after pipeline flush)
                transfer_start = time.perf_counter()
                
                for table_name in table_names:
                    schema = self.table_schemas[table_name]
                    result = cursor.fetchone()
                    
                    if result:
                        results[table_name] = dict(zip(schema.columns, result))
                    else:
                        results[table_name] = None
                
                result_transfer_ms = (time.perf_counter() - transfer_start) * 1000
        
        total_ms = (time.perf_counter() - overall_start) * 1000
        
        timing = TimingBreakdown(
            total_ms=total_ms,
            connection_acquire_ms=conn_acquire_ms,
            network_rtt_ms=network_rtt_ms,
            query_execution_ms=query_exec_ms,
            result_transfer_ms=result_transfer_ms,
            per_table_ms=per_table_times
        )
        
        return results, timing
    
    def get_database_metrics(self) -> DatabaseMetrics:
        """
        Capture PostgreSQL performance metrics
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cursor:
                # Cache hit ratio
                cursor.execute(f"""
                    SELECT 
                        sum(heap_blks_read) as heap_read,
                        sum(heap_blks_hit) as heap_hit,
                        COALESCE(
                            sum(heap_blks_hit)::float / 
                            NULLIF(sum(heap_blks_hit) + sum(heap_blks_read), 0) * 100,
                            0
                        ) as hit_ratio
                    FROM pg_statio_user_tables
                    WHERE schemaname = '{self.schema}'
                """)
                
                row = cursor.fetchone()
                buffer_reads = row[0] or 0
                buffer_hits = row[1] or 0
                cache_hit_ratio = row[2] or 0.0
                
                # Index usage per table
                cursor.execute(f"""
                    SELECT 
                        tablename,
                        idx_scan,
                        seq_scan,
                        idx_tup_fetch
                    FROM pg_stat_user_tables
                    WHERE schemaname = '{self.schema}'
                """)
                
                index_scans = {}
                seq_scans = {}
                rows_fetched = {}
                
                for row in cursor.fetchall():
                    table, idx_scan, seq_scan, idx_tup_fetch = row
                    index_scans[table] = idx_scan or 0
                    seq_scans[table] = seq_scan or 0
                    rows_fetched[table] = idx_tup_fetch or 0
        
        return DatabaseMetrics(
            cache_hit_ratio=cache_hit_ratio,
            buffer_hits=buffer_hits,
            buffer_reads=buffer_reads,
            index_scans=index_scans,
            sequential_scans=seq_scans,
            rows_fetched=rows_fetched
        )
    
    def reset_database_stats(self):
        """Reset PostgreSQL statistics for clean measurement"""
        with self.pool.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT pg_stat_reset_single_table_counters(oid) FROM pg_class WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '{self.schema}')")
                conn.commit()
        
        print(f"✅ Database statistics reset for schema '{self.schema}'")
    
    def get_connection_pool_stats(self) -> Dict:
        """Get connection pool statistics"""
        return {
            'pool_size': self.pool.min_size,
            'max_size': self.pool.max_size,
            'available': self.pool._pool.qsize() if hasattr(self.pool, '_pool') else 0
        }
    
    def close(self):
        """Close connection pool"""
        self.pool.close()


def calculate_component_percentiles(timings: List[TimingBreakdown]) -> Dict:
    """
    Calculate percentiles for each timing component
    
    Args:
        timings: List of TimingBreakdown objects
        
    Returns:
        Dict with p50/p95/p99 for each component
    """
    import numpy as np
    
    components = {
        'total': [t.total_ms for t in timings],
        'connection_acquire': [t.connection_acquire_ms for t in timings],
        'network_rtt': [t.network_rtt_ms for t in timings],
        'query_execution': [t.query_execution_ms for t in timings],
        'result_transfer': [t.result_transfer_ms for t in timings]
    }
    
    percentiles = {}
    
    for component, values in components.items():
        percentiles[component] = {
            'p50': np.percentile(values, 50),
            'p95': np.percentile(values, 95),
            'p99': np.percentile(values, 99),
            'avg': np.mean(values),
            'min': min(values),
            'max': max(values)
        }
    
    return percentiles
