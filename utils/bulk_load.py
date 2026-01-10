"""
Bulk load utilities for Lakebase using PostgreSQL COPY command.

For large tables (>100M rows), COPY is orders of magnitude faster than JDBC writes.

UNLOGGED Tables Option:
- Can be 2-3x faster for bulk loads (no WAL overhead)
- ⚠️ WARNING: Data is lost if database crashes during load
- Recommended only for reproducible benchmark data
"""

import os
import psycopg2
from typing import Optional, Dict, Any
import time


class BulkLoader:
    """
    Handles bulk loading from Unity Catalog volumes to Lakebase using COPY.
    
    Args:
        host: Lakebase host
        port: Lakebase port
        database: Database name
        user: Username
        password: Password
        use_unlogged: If True, creates UNLOGGED tables for faster loads
                      ⚠️ WARNING: Data lost if database crashes!
    """
    
    def __init__(
        self, 
        host: str, 
        port: str, 
        database: str, 
        user: str, 
        password: str,
        use_unlogged: bool = False
    ):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.use_unlogged = use_unlogged
        
        if use_unlogged:
            self._print_unlogged_warning()
    
    def copy_from_csv(
        self,
        table_name: str,
        schema: str,
        csv_path: str,
        columns: Optional[list] = None,
        delimiter: str = ',',
        header: bool = True,
        quote_char: str = '"',
        escape_char: Optional[str] = None,
        null_string: str = '',
        encoding: str = 'UTF8'
    ) -> Dict[str, Any]:
        """
        Bulk load data from CSV file using PostgreSQL COPY command.
        
        Args:
            table_name: Name of the target table
            schema: Schema name
            csv_path: Path to CSV file (local or volume-mounted)
            columns: List of column names (None = all columns in order)
            delimiter: CSV delimiter
            header: Whether CSV has header row
            quote_char: Quote character
            escape_char: Escape character
            null_string: String to interpret as NULL
            encoding: File encoding
        
        Returns:
            Dict with load statistics (rows loaded, duration, etc.)
        """
        conn = None
        cursor = None
        
        try:
            # Connect to Lakebase
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            cursor = conn.cursor()
            
            # Build COPY command
            full_table = f"{schema}.{table_name}"
            columns_str = f"({', '.join(columns)})" if columns else ""
            
            copy_sql = f"""
                COPY {full_table}{columns_str}
                FROM STDIN
                WITH (
                    FORMAT csv,
                    HEADER {str(header).lower()},
                    DELIMITER '{delimiter}',
                    QUOTE '{quote_char}',
                    NULL '{null_string}',
                    ENCODING '{encoding}'
                )
            """
            
            if escape_char:
                copy_sql = copy_sql.replace(
                    f"ENCODING '{encoding}'",
                    f"ESCAPE '{escape_char}', ENCODING '{encoding}'"
                )
            
            print(f"🚀 Starting bulk load: {csv_path} → {full_table}")
            print(f"   Method: PostgreSQL COPY")
            
            start_time = time.time()
            
            # Open CSV file and copy
            with open(csv_path, 'r', encoding=encoding) as f:
                cursor.copy_expert(copy_sql, f)
            
            # Commit transaction
            conn.commit()
            
            duration = time.time() - start_time
            rows_loaded = cursor.rowcount
            
            print(f"✅ Bulk load complete!")
            print(f"   Rows loaded: {rows_loaded:,}")
            print(f"   Duration: {duration:.2f}s")
            print(f"   Throughput: {rows_loaded/duration:,.0f} rows/s")
            
            return {
                'success': True,
                'rows_loaded': rows_loaded,
                'duration_seconds': duration,
                'throughput_rows_per_sec': rows_loaded / duration,
                'method': 'copy',
                'file': csv_path
            }
        
        except Exception as e:
            if conn:
                conn.rollback()
            print(f"❌ Bulk load failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'method': 'copy',
                'file': csv_path
            }
        
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    def copy_from_parquet_via_csv(
        self,
        table_name: str,
        schema: str,
        parquet_path: str,
        csv_temp_path: str,
        columns: Optional[list] = None
    ) -> Dict[str, Any]:
        """
        Load from Parquet by converting to CSV first, then using COPY.
        
        Note: This is a helper that assumes conversion happens outside this function
        (e.g., in Spark/Databricks). This function just handles the COPY part.
        
        Args:
            table_name: Name of the target table
            schema: Schema name
            parquet_path: Original Parquet file path (for logging)
            csv_temp_path: Path to converted CSV file
            columns: List of column names
        
        Returns:
            Dict with load statistics
        """
        print(f"📦 Loading from Parquet (via CSV conversion)")
        print(f"   Source: {parquet_path}")
        print(f"   Temp CSV: {csv_temp_path}")
        
        return self.copy_from_csv(
            table_name=table_name,
            schema=schema,
            csv_path=csv_temp_path,
            columns=columns,
            header=True
        )
    
    def set_table_unlogged(
        self,
        table_name: str,
        schema: str = "features"
    ) -> bool:
        """
        Convert a table to UNLOGGED mode for faster bulk loading.
        
        ⚠️ WARNING: UNLOGGED tables are NOT crash-safe!
        - Data is lost if database crashes during load
        - Use only for reproducible benchmark data
        - Remember to convert back to LOGGED after load
        
        Args:
            table_name: Name of the table
            schema: Schema name
        
        Returns:
            True if successful, False otherwise
        """
        conn = None
        cursor = None
        
        try:
            print(f"\n⚠️  CONVERTING TABLE TO UNLOGGED MODE")
            print(f"   Table: {schema}.{table_name}")
            print(f"   ⚠️  WARNING: Data will be lost if database crashes!")
            print(f"   This is safe for reproducible benchmark data.\n")
            
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            cursor = conn.cursor()
            
            # Convert to UNLOGGED
            cursor.execute(f"ALTER TABLE {schema}.{table_name} SET UNLOGGED")
            conn.commit()
            
            print(f"✅ Table is now UNLOGGED (faster loads, but not crash-safe)")
            return True
        
        except Exception as e:
            print(f"❌ Failed to set UNLOGGED: {e}")
            if conn:
                conn.rollback()
            return False
        
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    def set_table_logged(
        self,
        table_name: str,
        schema: str = "features"
    ) -> bool:
        """
        Convert a table back to LOGGED mode (crash-safe).
        
        This should be done after bulk loading is complete to ensure
        data durability for production use.
        
        Args:
            table_name: Name of the table
            schema: Schema name
        
        Returns:
            True if successful, False otherwise
        """
        conn = None
        cursor = None
        
        try:
            print(f"\n🔒 Converting table back to LOGGED mode...")
            print(f"   Table: {schema}.{table_name}")
            
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            cursor = conn.cursor()
            
            # Convert to LOGGED
            cursor.execute(f"ALTER TABLE {schema}.{table_name} SET LOGGED")
            conn.commit()
            
            print(f"✅ Table is now LOGGED (crash-safe, production-ready)")
            return True
        
        except Exception as e:
            print(f"❌ Failed to set LOGGED: {e}")
            if conn:
                conn.rollback()
            return False
        
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    def _print_unlogged_warning(self):
        """Print warning about UNLOGGED tables"""
        print("\n" + "=" * 70)
        print("⚠️  WARNING: UNLOGGED TABLES ENABLED")
        print("=" * 70)
        print("UNLOGGED tables provide 2-3x faster bulk loading by skipping")
        print("write-ahead logging (WAL).")
        print()
        print("❌ TRADE-OFF: Data is NOT crash-safe!")
        print("   - If the database crashes during load, ALL DATA IS LOST")
        print("   - You will need to REPEAT THE ENTIRE LOAD PROCESS")
        print()
        print("✅ Safe for:")
        print("   - Reproducible benchmark data (can be regenerated)")
        print("   - Test environments")
        print("   - Data that can be reloaded from source")
        print()
        print("❌ NOT recommended for:")
        print("   - Production data")
        print("   - Irreplaceable data")
        print("   - Data that cannot be easily regenerated")
        print()
        print("💡 Best Practice:")
        print("   1. Load data with UNLOGGED tables (fast)")
        print("   2. Convert back to LOGGED after load (safe)")
        print("   3. Take backups of completed data")
        print("=" * 70 + "\n")


def estimate_load_method(num_rows: int, threshold: int = 100_000_000) -> str:
    """
    Determine the best load method based on row count.
    
    Args:
        num_rows: Number of rows to load
        threshold: Threshold above which to use bulk load (default 100M)
    
    Returns:
        'bulk' or 'jdbc'
    """
    if num_rows >= threshold:
        return 'bulk'
    else:
        return 'jdbc'


def get_volume_path(volume_name: str = "benchmark_data") -> str:
    """
    Get the path to Unity Catalog volume.
    
    Args:
        volume_name: Name of the volume
    
    Returns:
        Path to volume in DBFS
    """
    # Unity Catalog volumes are mounted at /Volumes/<catalog>/<schema>/<volume>
    # For now, we'll use a default path - can be customized via config
    return f"/Volumes/main/default/{volume_name}"


if __name__ == "__main__":
    # Example usage
    print("Bulk Load Utility")
    print("=" * 60)
    
    # Example 1: Determine load method
    for rows in [1_000_000, 10_000_000, 100_000_000, 1_000_000_000]:
        method = estimate_load_method(rows)
        print(f"Rows: {rows:>15,} → Method: {method}")
    
    print("\n" + "=" * 60)
    print("Load Methods:")
    print("  • JDBC:  Good for < 100M rows, uses Spark parallelism")
    print("  • COPY:  Optimal for > 100M rows, 10-100x faster")
    print("=" * 60)

