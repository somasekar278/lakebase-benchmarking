"""
Lakebase connection management with pooling for optimal performance.
"""
import os
import sys
import psycopg2
from psycopg2 import pool

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# Import configuration
from config import LAKEBASE_CONFIG, BENCHMARK_CONFIG


class LakebaseConnectionPool:
    """Connection pool manager for Lakebase (PostgreSQL)."""
    
    def __init__(self, min_connections=2, max_connections=10):
        """
        Initialize connection pool.
        
        Args:
            min_connections: Minimum number of connections in pool
            max_connections: Maximum number of connections in pool
        """
        self.pool = psycopg2.pool.ThreadedConnectionPool(
            min_connections,
            max_connections,
            host=LAKEBASE_CONFIG['host'],
            port=LAKEBASE_CONFIG['port'],
            database=LAKEBASE_CONFIG['database'],
            user=LAKEBASE_CONFIG['user'],
            password=LAKEBASE_CONFIG['password'],
            # Performance optimizations
            options=f'-c statement_timeout={BENCHMARK_CONFIG["statement_timeout"]}'
        )
    
    def get_connection(self):
        """Get a connection from the pool."""
        return self.pool.getconn()
    
    def return_connection(self, conn):
        """Return a connection to the pool."""
        self.pool.putconn(conn)
    
    def close_all(self):
        """Close all connections in the pool."""
        self.pool.closeall()


def get_single_connection():
    """
    Get a single connection (not from pool).
    Useful for one-off operations like setup/teardown.
    """
    return psycopg2.connect(
        host=LAKEBASE_CONFIG['host'],
        port=LAKEBASE_CONFIG['port'],
        database=LAKEBASE_CONFIG['database'],
        user=LAKEBASE_CONFIG['user'],
        password=LAKEBASE_CONFIG['password'],
        sslmode=LAKEBASE_CONFIG.get('sslmode', 'require')
    )


def test_connection():
    """Test the Lakebase connection."""
    try:
        conn = get_single_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        cursor.close()
        conn.close()
        print(f"✅ Successfully connected to Lakebase")
        print(f"   PostgreSQL version: {version[0]}")
        return True
    except Exception as e:
        print(f"❌ Failed to connect to Lakebase: {e}")
        return False


if __name__ == "__main__":
    test_connection()

