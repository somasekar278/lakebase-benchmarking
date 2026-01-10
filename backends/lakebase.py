"""
Lakebase (PostgreSQL) backend implementation.
"""

import time
import psycopg2
from typing import Dict, List, Any
from core.backend import Backend, BackendType, QueryResult


class LakebaseBackend(Backend):
    """
    Lakebase backend for benchmarking.
    
    Lakebase is PostgreSQL-compatible, so we use psycopg2.
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.backend_type = BackendType.LAKEBASE
        self.connection = None
        self.schema = config.get('schema', 'features')
        
        # Initialize cost tracking
        self.initialize_cost_tracking()
    
    def connect(self) -> bool:
        """Connect to Lakebase"""
        try:
            self.connection = psycopg2.connect(
                host=self.config['host'],
                port=self.config.get('port', 5432),
                database=self.config['database'],
                user=self.config['user'],
                password=self.config['password'],
                options=f"-c statement_timeout={self.config.get('statement_timeout', 30000)}"
            )
            return True
        except Exception as e:
            print(f"❌ Lakebase connection failed: {e}")
            return False
    
    def disconnect(self) -> bool:
        """Disconnect from Lakebase"""
        if self.connection:
            self.connection.close()
            self.connection = None
        return True
    
    def create_tables(self, workload) -> bool:
        """
        Create tables based on workload definition.
        
        Args:
            workload: Workload object with get_tables() method
        
        Returns:
            True if successful
        """
        if not self.connection:
            return False
        
        try:
            cursor = self.connection.cursor()
            
            # Create schema if needed
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
            
            # Create tables
            for table_def in workload.get_tables():
                # Build CREATE TABLE SQL
                columns = []
                for col_name, col_type in table_def.columns.items():
                    columns.append(f"{col_name} {col_type}")
                
                sql = f"""
                    CREATE TABLE IF NOT EXISTS {self.schema}.{table_def.name} (
                        {', '.join(columns)}
                    )
                """
                cursor.execute(sql)
                
                # Create indexes
                for index_col in table_def.indexes:
                    index_name = f"idx_{table_def.name}_{index_col}"
                    cursor.execute(f"""
                        CREATE INDEX IF NOT EXISTS {index_name}
                        ON {self.schema}.{table_def.name}({index_col})
                    """)
            
            self.connection.commit()
            cursor.close()
            return True
        
        except Exception as e:
            print(f"❌ Table creation failed: {e}")
            if self.connection:
                self.connection.rollback()
            return False
    
    def load_data(self, table_name: str, data: List[Dict[str, Any]]) -> bool:
        """Load data into table"""
        if not self.connection or not data:
            return False
        
        try:
            cursor = self.connection.cursor()
            
            # Get columns from first row
            columns = list(data[0].keys())
            placeholders = ','.join(['%s'] * len(columns))
            
            sql = f"""
                INSERT INTO {self.schema}.{table_name} ({','.join(columns)})
                VALUES ({placeholders})
            """
            
            # Batch insert
            values = [tuple(row[col] for col in columns) for row in data]
            cursor.executemany(sql, values)
            
            self.connection.commit()
            cursor.close()
            return True
        
        except Exception as e:
            print(f"❌ Data load failed: {e}")
            if self.connection:
                self.connection.rollback()
            return False
    
    def query(self, query_pattern, keys: List[str]) -> QueryResult:
        """
        Execute a single query.
        
        Args:
            query_pattern: QueryPattern object
            keys: List of keys to query
        
        Returns:
            QueryResult with timing and data
        """
        if not self.connection:
            return QueryResult(
                success=False,
                latency_ms=0,
                rows_returned=0,
                error="Not connected"
            )
        
        try:
            start = time.time()
            
            cursor = self.connection.cursor()
            
            # Execute query
            if query_pattern.query:
                # Custom SQL query
                cursor.execute(query_pattern.query, (keys,))
            else:
                # Default key lookup
                table = query_pattern.tables[0]
                cursor.execute(
                    f"SELECT * FROM {self.schema}.{table} WHERE primary_key = ANY(%s::CHAR(64)[])",
                    (keys,)
                )
            
            # Fetch results
            rows = cursor.fetchall()
            latency_ms = (time.time() - start) * 1000
            
            cursor.close()
            
            return QueryResult(
                success=True,
                latency_ms=latency_ms,
                rows_returned=len(rows),
                data=None,  # Don't return data for benchmarking
                metadata={'backend': 'lakebase'}
            )
        
        except Exception as e:
            latency_ms = (time.time() - start) * 1000 if 'start' in locals() else 0
            return QueryResult(
                success=False,
                latency_ms=latency_ms,
                rows_returned=0,
                error=str(e)
            )
    
    def batch_query(self, query_patterns: List, keys_dict: Dict[str, List[str]]) -> QueryResult:
        """
        Execute batch query (stored procedure for binpacking).
        
        Args:
            query_patterns: List of QueryPattern objects
            keys_dict: Dict mapping table names to key lists
        
        Returns:
            QueryResult with combined timing
        """
        if not self.connection:
            return QueryResult(
                success=False,
                latency_ms=0,
                rows_returned=0,
                error="Not connected"
            )
        
        try:
            start = time.time()
            
            cursor = self.connection.cursor()
            
            # Assume stored procedure exists (e.g., fraud_batch_lookup)
            # Build parameters
            sp_name = query_patterns[0].stored_procedure if query_patterns else None
            
            if sp_name:
                # Call stored procedure
                params = [keys_dict.get(t, []) for t in keys_dict.keys()]
                cursor.execute(f"SELECT * FROM {self.schema}.{sp_name}({','.join(['%s'] * len(params))})", params)
            else:
                # Fallback: execute queries sequentially
                total_rows = 0
                for pattern in query_patterns:
                    table = pattern.tables[0]
                    keys = keys_dict.get(table, [])
                    if keys:
                        cursor.execute(
                            f"SELECT * FROM {self.schema}.{table} WHERE primary_key = ANY(%s::CHAR(64)[])",
                            (keys,)
                        )
                        total_rows += len(cursor.fetchall())
            
            rows = cursor.fetchall()
            latency_ms = (time.time() - start) * 1000
            
            cursor.close()
            
            return QueryResult(
                success=True,
                latency_ms=latency_ms,
                rows_returned=len(rows),
                metadata={'backend': 'lakebase', 'method': 'binpacked'}
            )
        
        except Exception as e:
            latency_ms = (time.time() - start) * 1000 if 'start' in locals() else 0
            return QueryResult(
                success=False,
                latency_ms=latency_ms,
                rows_returned=0,
                error=str(e)
            )
    
    def cleanup(self) -> bool:
        """Drop all tables"""
        if not self.connection:
            return False
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"DROP SCHEMA IF EXISTS {self.schema} CASCADE")
            self.connection.commit()
            cursor.close()
            return True
        except Exception as e:
            print(f"❌ Cleanup failed: {e}")
            return False
    
    def supports_feature(self, feature: str) -> bool:
        """Check feature support"""
        supported = {
            'transactions', 'indexes', 'stored_procedures',
            'joins', 'aggregations', 'full_text_search'
        }
        return feature.lower() in supported

