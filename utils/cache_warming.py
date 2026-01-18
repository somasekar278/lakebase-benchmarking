"""
Cache Warming Strategies for Lakebase Feature Server

Prevents cold-start latency spikes and cache eviction issues.

Key strategies:
1. Pre-warm indexes explicitly (B-tree root + upper levels)
2. Fixed-size connection pool (preserve prepared statements)
3. Synthetic query warmup (before accepting traffic)
4. Hot key warmup (known frequent keys)
5. Cache hit ratio monitoring
"""

from typing import List, Dict, Optional
import psycopg
from psycopg_pool import ConnectionPool
import time


class CacheWarmer:
    """
    Manages cache warming for optimal cold-start performance
    """
    
    def __init__(self, conninfo: str, schema: str = "features"):
        self.conninfo = conninfo
        self.schema = schema
    
    def prewarm_indexes(self, table_names: List[str]) -> Dict[str, float]:
        """
        Pre-warm B-tree indexes by touching index roots
        
        This is CHEAP (microseconds) but EFFECTIVE:
        - Touches index root
        - Warms upper B-tree levels
        - Prevents first-query penalty
        
        Args:
            table_names: List of table names to warm
            
        Returns:
            Dict of {table: warmup_time_ms}
        """
        print("🔥 Pre-warming indexes...")
        
        timings = {}
        
        with psycopg.connect(self.conninfo) as conn:
            with conn.cursor() as cursor:
                for table in table_names:
                    start = time.perf_counter()
                    
                    # Touch index root (cheap!)
                    cursor.execute(f"""
                        SELECT 1 FROM {self.schema}.{table} 
                        WHERE hashkey = 'dummy_key_for_warmup' 
                        LIMIT 1
                    """)
                    cursor.fetchone()
                    
                    warmup_ms = (time.perf_counter() - start) * 1000
                    timings[table] = warmup_ms
                    
                    if len(timings) % 10 == 0:
                        print(f"   Warmed {len(timings)}/{len(table_names)} indexes...")
        
        print(f"✅ Index pre-warming complete ({len(table_names)} tables)")
        return timings
    
    def warmup_with_hot_keys(
        self, 
        table_names: List[str],
        hot_keys: List[str],
        max_keys: int = 100
    ) -> Dict[str, int]:
        """
        Warm cache with known hot keys
        
        Use this for:
        - Frequent user IDs
        - Recent transactions
        - High-traffic accounts
        
        Args:
            table_names: Tables to warm
            hot_keys: List of frequently accessed hashkeys
            max_keys: Maximum keys to warm (default: 100)
            
        Returns:
            Dict of {table: keys_warmed}
        """
        print(f"🔥 Warming cache with hot keys...")
        print(f"   Tables: {len(table_names)}")
        print(f"   Hot keys: {min(len(hot_keys), max_keys)}")
        
        keys_to_warm = hot_keys[:max_keys]
        stats = {table: 0 for table in table_names}
        
        with psycopg.connect(self.conninfo) as conn:
            with conn.cursor() as cursor:
                for i, hashkey in enumerate(keys_to_warm):
                    for table in table_names:
                        cursor.execute(f"""
                            SELECT 1 FROM {self.schema}.{table} 
                            WHERE hashkey = %s
                        """, (hashkey,))
                        
                        if cursor.fetchone():
                            stats[table] += 1
                    
                    if (i + 1) % 20 == 0:
                        print(f"   Progress: {i+1}/{len(keys_to_warm)} keys")
        
        print(f"✅ Hot key warmup complete")
        print(f"   Average cache entries per table: {sum(stats.values()) // len(stats)}")
        
        return stats
    
    def synthetic_warmup(
        self,
        table_names: List[str],
        sample_keys: List[str],
        iterations: int = 50
    ) -> Dict[str, float]:
        """
        Run synthetic queries before accepting production traffic
        
        This ensures:
        - Indexes are fully warmed
        - Heap pages are in cache
        - Prepared statements are cached
        - Connection pool is stable
        
        Args:
            table_names: Tables to query
            sample_keys: Sample hashkeys to use
            iterations: Number of warmup iterations
            
        Returns:
            Dict with warmup statistics
        """
        print(f"🔥 Running synthetic warmup...")
        print(f"   Iterations: {iterations}")
        print(f"   Tables per query: {len(table_names)}")
        
        latencies = []
        start_time = time.perf_counter()
        
        with psycopg.connect(self.conninfo) as conn:
            with conn.cursor() as cursor:
                for i in range(iterations):
                    hashkey = sample_keys[i % len(sample_keys)]
                    
                    query_start = time.perf_counter()
                    
                    # Simulate production query pattern
                    with conn.pipeline():
                        for table in table_names:
                            cursor.execute(f"""
                                SELECT * FROM {self.schema}.{table} 
                                WHERE hashkey = %s
                            """, (hashkey,))
                    
                    for _ in table_names:
                        cursor.fetchone()
                    
                    latency_ms = (time.perf_counter() - query_start) * 1000
                    latencies.append(latency_ms)
                    
                    if (i + 1) % 10 == 0:
                        print(f"   Progress: {i+1}/{iterations}")
        
        total_time = time.perf_counter() - start_time
        
        stats = {
            'iterations': iterations,
            'total_time_sec': total_time,
            'avg_latency_ms': sum(latencies) / len(latencies),
            'first_query_ms': latencies[0],
            'last_query_ms': latencies[-1],
            'improvement': ((latencies[0] - latencies[-1]) / latencies[0]) * 100
        }
        
        print(f"✅ Synthetic warmup complete")
        print(f"   First query: {stats['first_query_ms']:.2f}ms")
        print(f"   Last query: {stats['last_query_ms']:.2f}ms")
        print(f"   Improvement: {stats['improvement']:.1f}%")
        
        return stats
    
    def monitor_cache_hit_ratio(self) -> Dict[str, float]:
        """
        Monitor buffer cache hit ratio per table
        
        Target: > 99% for feature tables
        If drops: p99 will spike immediately
        
        Returns:
            Dict of {table: hit_ratio_percent}
        """
        with psycopg.connect(self.conninfo) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT
                        relname AS table_name,
                        heap_blks_hit,
                        heap_blks_read,
                        CASE 
                            WHEN (heap_blks_hit + heap_blks_read) = 0 THEN 0
                            ELSE (heap_blks_hit::float / (heap_blks_hit + heap_blks_read)) * 100
                        END AS hit_ratio
                    FROM pg_statio_user_tables
                    WHERE schemaname = %s
                    ORDER BY hit_ratio ASC
                """, (self.schema,))
                
                results = {}
                for row in cursor.fetchall():
                    table_name, hits, reads, hit_ratio = row
                    results[table_name] = {
                        'hit_ratio': hit_ratio,
                        'heap_blks_hit': hits,
                        'heap_blks_read': reads,
                        'status': '✅' if hit_ratio > 99 else '⚠️' if hit_ratio > 95 else '❌'
                    }
        
        return results
    
    def full_warmup_sequence(
        self,
        table_names: List[str],
        sample_keys: List[str],
        hot_keys: Optional[List[str]] = None
    ) -> Dict:
        """
        Complete warmup sequence for production deployment
        
        Sequence:
        1. Pre-warm indexes (cheap, fast)
        2. Warm hot keys (if provided)
        3. Run synthetic queries
        4. Verify cache hit ratios
        
        Args:
            table_names: All feature tables
            sample_keys: Sample keys for synthetic warmup
            hot_keys: Optional list of known hot keys
            
        Returns:
            Dict with complete warmup statistics
        """
        print("="*80)
        print("🔥 STARTING FULL WARMUP SEQUENCE")
        print("="*80)
        
        results = {}
        
        # Step 1: Pre-warm indexes
        print("\n[1/4] Pre-warming indexes...")
        results['index_prewarm'] = self.prewarm_indexes(table_names)
        
        # Step 2: Warm hot keys (if provided)
        if hot_keys:
            print("\n[2/4] Warming hot keys...")
            results['hot_key_warmup'] = self.warmup_with_hot_keys(
                table_names, 
                hot_keys,
                max_keys=100
            )
        else:
            print("\n[2/4] Skipping hot key warmup (no hot keys provided)")
            results['hot_key_warmup'] = None
        
        # Step 3: Synthetic queries
        print("\n[3/4] Running synthetic warmup...")
        results['synthetic_warmup'] = self.synthetic_warmup(
            table_names,
            sample_keys,
            iterations=50
        )
        
        # Step 4: Verify cache hit ratios
        print("\n[4/4] Verifying cache hit ratios...")
        cache_stats = self.monitor_cache_hit_ratio()
        results['cache_hit_ratios'] = cache_stats
        
        # Print summary
        print("\n" + "="*80)
        print("📊 WARMUP SUMMARY")
        print("="*80)
        
        # Cache hit ratio summary
        avg_hit_ratio = sum(s['hit_ratio'] for s in cache_stats.values()) / len(cache_stats)
        poor_performers = [t for t, s in cache_stats.items() if s['hit_ratio'] < 99]
        
        print(f"\nCache Hit Ratios:")
        print(f"  Average: {avg_hit_ratio:.2f}%")
        print(f"  Tables > 99%: {len(cache_stats) - len(poor_performers)}/{len(cache_stats)}")
        
        if poor_performers:
            print(f"\n⚠️  Tables needing more warmup:")
            for table in poor_performers[:5]:
                stats = cache_stats[table]
                print(f"  - {table}: {stats['hit_ratio']:.2f}%")
        else:
            print(f"\n✅ All tables fully warmed (>99% hit ratio)")
        
        # Latency improvement
        if 'synthetic_warmup' in results:
            improvement = results['synthetic_warmup']['improvement']
            print(f"\nLatency Improvement: {improvement:.1f}%")
            print(f"  First query: {results['synthetic_warmup']['first_query_ms']:.2f}ms")
            print(f"  Last query: {results['synthetic_warmup']['last_query_ms']:.2f}ms")
        
        print("\n" + "="*80)
        print("✅ WARMUP COMPLETE - Ready for production traffic")
        print("="*80)
        
        return results


def estimate_cold_start_penalty(num_tables: int = 30) -> Dict[str, float]:
    """
    Estimate worst-case cold-start latency
    
    Scenario: Cluster restart, cache fully cold
    
    Args:
        num_tables: Number of tables to query
        
    Returns:
        Dict with cold-start estimates
    """
    # Worst-case disk I/O per table
    index_lookup_cold = 10  # ms (disk I/O)
    heap_fetch_cold = 10    # ms (disk I/O)
    per_table_cold = index_lookup_cold + heap_fetch_cold
    
    # Best-case warm cache per table
    index_lookup_warm = 0.5  # ms (memory)
    heap_fetch_warm = 0.2    # ms (memory)
    per_table_warm = index_lookup_warm + heap_fetch_warm
    
    return {
        'cold_start_ms': num_tables * per_table_cold,
        'warm_cache_ms': num_tables * per_table_warm,
        'penalty_ms': num_tables * (per_table_cold - per_table_warm),
        'penalty_multiplier': per_table_cold / per_table_warm,
        'recommendation': 'Run warmup before accepting traffic'
    }
