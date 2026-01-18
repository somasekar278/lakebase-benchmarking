"""
Production-ready Feature Server for Lakebase
Optimized for low-latency multi-table feature retrieval

Key optimizations:
1. psycopg3 explicit pipelining (single flush)
2. Column-specific queries (no SELECT *)
3. Pre-computed column maps (fast deserialization)
4. Connection pooling (pin to workers)
5. Prepared statements (reuse query plans)
"""

from typing import List, Dict, Optional, Tuple
import time
from psycopg_pool import ConnectionPool
import psycopg


class FeatureTableSchema:
    """
    Pre-computed schema for a feature table
    Avoids runtime metadata parsing
    """
    def __init__(self, table_name: str, columns: List[str]):
        self.table_name = table_name
        self.columns = columns
        self.column_list = ', '.join(columns)
        
        # Pre-build query for performance
        self.query = f"""
            SELECT {self.column_list} 
            FROM {table_name} 
            WHERE hashkey = %s
        """


class LakebaseFeatureServer:
    """
    Production feature serving layer for Lakebase
    
    Optimized for:
    - Low latency (p50 < 40ms, p99 < 70ms)
    - High throughput (100+ QPS)
    - Efficient multi-table retrieval
    
    Usage:
        server = LakebaseFeatureServer(config, table_schemas)
        features = server.get_features(hashkey, table_names)
    """
    
    def __init__(
        self, 
        lakebase_config: Dict,
        table_schemas: Dict[str, FeatureTableSchema],
        pool_size: int = 10,
        max_pool_size: Optional[int] = None,
        enable_warmup: bool = True
    ):
        """
        Initialize feature server with connection pool
        
        Args:
            lakebase_config: Dict with host, database, user, password
            table_schemas: Dict of {table_name: FeatureTableSchema}
            pool_size: Minimum pool size (should >= expected concurrency)
            max_pool_size: Maximum pool size (RECOMMENDED: same as pool_size for stability)
            enable_warmup: If True, pre-warm indexes on initialization
        """
        self.table_schemas = table_schemas
        self.schema = lakebase_config.get('schema', 'public')
        
        # Build connection string
        conninfo = (
            f"host={lakebase_config['host']} "
            f"port={lakebase_config.get('port', 5432)} "
            f"dbname={lakebase_config['database']} "
            f"user={lakebase_config['user']} "
            f"password={lakebase_config['password']} "
            f"sslmode={lakebase_config.get('sslmode', 'require')}"
        )
        
        self.conninfo = conninfo
        
        # CRITICAL: Fixed-size pool for stability
        # Prevents cache eviction from connection churn
        # Preserves prepared statements
        if max_pool_size is None:
            max_pool_size = pool_size  # FIXED SIZE (recommended)
        
        self.pool = ConnectionPool(
            conninfo=conninfo,
            min_size=pool_size,
            max_size=max_pool_size,
            timeout=30.0  # Increased from 5s to handle high concurrency (100 clients competing for pool)
        )
        
        pool_type = "FIXED" if pool_size == max_pool_size else "ELASTIC"
        
        print(f"✅ Feature server initialized")
        print(f"   Tables: {len(table_schemas)}")
        print(f"   Pool: {pool_size}-{max_pool_size} connections ({pool_type})")
        
        if pool_size != max_pool_size:
            print(f"   ⚠️  RECOMMENDATION: Use fixed pool (min_size = max_size)")
            print(f"      Elastic pools can cause cache eviction issues")
        
        # Pre-warm indexes if enabled
        if enable_warmup:
            self._prewarm_indexes()
    
    def get_features(
        self, 
        hashkey: str, 
        table_names: List[str],
        return_timing: bool = False
    ) -> Dict[str, Dict]:
        """
        Retrieve features from multiple tables in ONE round-trip
        
        Uses psycopg3 explicit pipelining with connection-local prepared statements.
        
        Args:
            hashkey: SHA256 hash for lookup (64-char hex string)
            table_names: List of table names to query
            return_timing: If True, return (results, timing_ms)
            
        Returns:
            Dict of {table_name: {column: value, ...}}
            
        Performance:
            - Single round-trip (explicit pipeline)
            - Connection-local prepared statements (query plan reuse)
            - Sequential execution per connection
            - Expected: 0.3-0.8ms per table (warm cache)
            - Total: ~10-25ms for 30 tables + network RTT
        """
        start_time = time.perf_counter()
        
        results = {}
        
        # Get connection from pool (pinned to this request)
        with self.pool.connection() as conn:
            with conn.cursor() as cursor:
                
                # CRITICAL: Separate execute and fetch phases for optimal pipelining
                with conn.pipeline():
                    # Phase 1: Queue all executes (single flush)
                    for table_name in table_names:
                        schema = self.table_schemas.get(table_name)
                        
                        if not schema:
                            raise ValueError(f"Unknown table: {table_name}")
                        
                        # Execute with column-specific query (no SELECT *)
                        # Connection-local prepared statements are auto-cached by psycopg
                        cursor.execute(schema.query, (hashkey,))
                
                # Phase 2: Fetch all results (after pipeline flush)
                # This ensures minimal round-trips and server-side parallelism
                for table_name in table_names:
                    schema = self.table_schemas[table_name]
                    
                    # Fetch result for this query
                    result = cursor.fetchone()
                    
                    if result:
                        # Fast deserialization using pre-computed column map
                        # Avoids: dict(zip(cursor.description, row))
                        results[table_name] = dict(zip(schema.columns, result))
                    else:
                        # Hashkey not found in this table
                        results[table_name] = None
        
        latency_ms = (time.perf_counter() - start_time) * 1000
        
        if return_timing:
            return results, latency_ms
        return results
    
    def get_features_batch(
        self,
        hashkeys: List[str],
        table_names: List[str]
    ) -> Dict[str, Dict[str, Dict]]:
        """
        Batch retrieval for multiple hashkeys
        
        Uses WHERE hashkey IN (...) for efficiency.
        
        Args:
            hashkeys: List of hashkeys to look up
            table_names: List of tables to query
            
        Returns:
            Dict of {hashkey: {table_name: {column: value}}}
        """
        if not hashkeys:
            return {}
        
        results = {hk: {} for hk in hashkeys}
        
        with self.pool.connection() as conn:
            with conn.pipeline():
                
                for table_name in table_names:
                    schema = self.table_schemas[table_name]
                    
                    # Batch query with IN clause
                    placeholders = ','.join(['%s'] * len(hashkeys))
                    query = f"""
                        SELECT hashkey, {schema.column_list}
                        FROM {table_name}
                        WHERE hashkey IN ({placeholders})
                    """
                    
                    conn.execute(query, hashkeys)
            
            # Fetch results for each table
            for table_name in table_names:
                schema = self.table_schemas[table_name]
                rows = conn.fetchall()
                
                for row in rows:
                    hashkey = row[0]
                    feature_values = row[1:]
                    results[hashkey][table_name] = dict(zip(schema.columns, feature_values))
        
        return results
    
    def _prewarm_indexes(self):
        """
        Pre-warm B-tree index roots (internal, called at init)
        
        This is CHEAP (microseconds per table) but EFFECTIVE:
        - Touches index root
        - Warms upper B-tree levels
        - Prevents first-query disk I/O penalty
        """
        print(f"   🔥 Pre-warming {len(self.table_schemas)} indexes...")
        
        with self.pool.connection() as conn:
            with conn.cursor() as cursor:
                for table_name in self.table_schemas.keys():
                    # Touch index root (returns instantly, no actual data)
                    cursor.execute(f"""
                        SELECT 1 FROM {self.schema}.{table_name} 
                        WHERE hashkey = 'warmup_dummy_key' 
                        LIMIT 1
                    """)
                    cursor.fetchone()
        
        print(f"   ✅ Index pre-warm complete")
    
    def warmup(self, sample_hashkeys: List[str], table_names: List[str]):
        """
        Warm up cache with sample queries
        
        Critical for consistent benchmark results.
        Ensures index + heap pages are in buffer cache.
        
        This should be run AFTER index pre-warming (done at init).
        """
        print(f"🔥 Warming up cache with real queries...")
        print(f"   Tables: {len(table_names)}")
        print(f"   Sample keys: {len(sample_hashkeys)}")
        
        for i, hashkey in enumerate(sample_hashkeys):
            self.get_features(hashkey, table_names)
            
            if (i + 1) % 10 == 0:
                print(f"   Progress: {i+1}/{len(sample_hashkeys)}")
        
        print(f"✅ Cache warmup complete")
    
    def get_cache_stats(self) -> Dict:
        """
        Get cache hit ratio statistics
        
        Target: > 99% for optimal performance
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT
                        SUM(heap_blks_hit) AS hits,
                        SUM(heap_blks_read) AS reads,
                        CASE 
                            WHEN SUM(heap_blks_hit + heap_blks_read) = 0 THEN 0
                            ELSE (SUM(heap_blks_hit)::float / SUM(heap_blks_hit + heap_blks_read)) * 100
                        END AS hit_ratio
                    FROM pg_statio_user_tables
                    WHERE schemaname = %s
                """, (self.schema,))
                
                row = cursor.fetchone()
                hits, reads, hit_ratio = row
                
                return {
                    'heap_blks_hit': hits or 0,
                    'heap_blks_read': reads or 0,
                    'hit_ratio_percent': hit_ratio or 0.0,
                    'status': '✅ Excellent' if (hit_ratio or 0) > 99 else '⚠️ Needs Warmup'
                }
    
    def close(self):
        """Close connection pool"""
        self.pool.close()


def create_feature_schemas_from_ddl(
    conn,
    schema: str = "features",
    exclude_columns: List[str] = None
) -> Dict[str, FeatureTableSchema]:
    """
    Auto-discover tables and create FeatureTableSchema objects
    
    Args:
        conn: psycopg connection
        schema: PostgreSQL schema name
        exclude_columns: Columns to exclude from SELECT (e.g., ['updated_at'])
        
    Returns:
        Dict of {table_name: FeatureTableSchema}
    """
    exclude_columns = exclude_columns or []
    
    print(f"🔍 Discovering tables in schema '{schema}'...")
    
    with conn.cursor() as cursor:
        # Get all tables
        cursor.execute("""
            SELECT tablename 
            FROM pg_tables 
            WHERE schemaname = %s
              AND tablename NOT LIKE '%%stage%%'
              AND tablename NOT LIKE '%%__st'
              AND tablename NOT LIKE '%%__sta'
            ORDER BY tablename
        """, (schema,))
        
        tables = [row[0] for row in cursor.fetchall()]
        
        schemas = {}
        
        for table_name in tables:
            # Get columns for this table
            cursor.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = %s
                  AND table_name = %s
                ORDER BY ordinal_position
            """, (schema, table_name))
            
            columns = [
                row[0] for row in cursor.fetchall()
                if row[0] not in exclude_columns
            ]
            
            # Create schema with explicit column list
            full_table_name = f"{schema}.{table_name}"
            schemas[table_name] = FeatureTableSchema(full_table_name, columns)
            
            print(f"   ✓ {table_name}: {len(columns)} columns")
    
    print(f"✅ Discovered {len(schemas)} tables")
    
    return schemas


def get_sample_hashkeys(
    conn,
    schema: str,
    table_name: str,
    limit: int = 100
) -> List[str]:
    """
    Get sample hashkeys from a table for warmup/testing
    
    Uses TABLESAMPLE for fast sampling on large tables (avoids ORDER BY RANDOM which is O(n log n)).
    
    Args:
        conn: psycopg connection
        schema: PostgreSQL schema
        table_name: Table to sample from
        limit: Number of samples
        
    Returns:
        List of hashkey strings
    """
    with conn.cursor() as cursor:
        # Use TABLESAMPLE SYSTEM for fast page-level sampling (milliseconds vs minutes!)
        # SYSTEM samples at page level (very fast, but not perfectly random)
        # For benchmarking, we don't need perfect randomness - just diverse keys
        cursor.execute(f"""
            SELECT hashkey 
            FROM {schema}.{table_name}
            TABLESAMPLE SYSTEM (1)
            LIMIT %s
        """, (limit,))
        
        hashkeys = [row[0] for row in cursor.fetchall()]
        
        # If TABLESAMPLE didn't return enough rows (small table), fall back to simple LIMIT
        if len(hashkeys) < limit:
            cursor.execute(f"""
                SELECT hashkey 
                FROM {schema}.{table_name}
                LIMIT %s
            """, (limit,))
            hashkeys = [row[0] for row in cursor.fetchall()]
        
        return hashkeys
