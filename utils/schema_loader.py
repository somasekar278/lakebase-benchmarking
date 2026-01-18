"""
Schema Loader - Accept DDL and automatically execute + generate data

This module makes it easy for users to:
1. Provide DDL statements (SQL, dict, or YAML)
2. Framework executes DDL on Lakebase
3. Framework generates matching synthetic data
4. Framework loads data automatically

Usage:
    from utils.schema_loader import SchemaLoader
    
    # Option 1: From SQL file
    loader = SchemaLoader.from_sql_file('my_schema.sql', LAKEBASE_CONFIG)
    loader.execute_ddl()
    loader.generate_and_load_data(rows_per_table=1_000_000)
    
    # Option 2: From dict
    schema = {
        'users': {
            'columns': {
                'user_id': 'VARCHAR(64) PRIMARY KEY',
                'email': 'VARCHAR(255)',
                'score': 'NUMERIC(5,2)'
            },
            'indexes': ['email']
        }
    }
    loader = SchemaLoader.from_dict(schema, LAKEBASE_CONFIG)
    loader.execute_and_load(rows_per_table=1_000_000)
    
    # Option 3: From YAML
    loader = SchemaLoader.from_yaml('schema.yaml', LAKEBASE_CONFIG)
    loader.execute_and_load(rows_per_table=1_000_000)
"""

import re
import yaml
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, expr, md5, sha2, concat_ws, rand, when, monotonically_increasing_id
from pyspark.sql.types import *


class SchemaLoader:
    """
    Unified schema loader that accepts DDL in multiple formats
    and handles execution + data generation + loading
    """
    
    def __init__(self, lakebase_config: Dict[str, Any]):
        """
        Initialize schema loader
        
        Args:
            lakebase_config: Lakebase connection configuration
        """
        self.lakebase_config = lakebase_config
        self.tables = {}  # table_name -> schema info
        self.ddl_statements = []
        
    @classmethod
    def from_sql_file(cls, sql_file: str, lakebase_config: Dict[str, Any]) -> 'SchemaLoader':
        """
        Create SchemaLoader from a SQL file containing DDL statements
        
        Args:
            sql_file: Path to SQL file with CREATE TABLE statements
            lakebase_config: Lakebase connection configuration
            
        Returns:
            SchemaLoader instance
        """
        loader = cls(lakebase_config)
        loader.parse_sql_file(sql_file)
        return loader
    
    @classmethod
    def from_dict(cls, schema_dict: Dict[str, Any], lakebase_config: Dict[str, Any]) -> 'SchemaLoader':
        """
        Create SchemaLoader from a Python dictionary
        
        Args:
            schema_dict: Dictionary defining tables and columns
            lakebase_config: Lakebase connection configuration
            
        Example schema_dict:
            {
                'users': {
                    'columns': {
                        'user_id': 'VARCHAR(64) PRIMARY KEY',
                        'email': 'VARCHAR(255)',
                        'score': 'NUMERIC(5,2)'
                    },
                    'indexes': ['email']
                },
                'orders': {
                    'columns': {
                        'order_id': 'VARCHAR(64) PRIMARY KEY',
                        'user_id': 'VARCHAR(64)',
                        'amount': 'NUMERIC(18,2)'
                    }
                }
            }
        """
        loader = cls(lakebase_config)
        loader.parse_dict(schema_dict)
        return loader
    
    @classmethod
    def from_yaml(cls, yaml_file: str, lakebase_config: Dict[str, Any]) -> 'SchemaLoader':
        """
        Create SchemaLoader from a YAML file
        
        Args:
            yaml_file: Path to YAML file defining schema
            lakebase_config: Lakebase connection configuration
        """
        with open(yaml_file, 'r') as f:
            schema_dict = yaml.safe_load(f)
        return cls.from_dict(schema_dict, lakebase_config)
    
    def parse_sql_file(self, sql_file: str):
        """
        Parse SQL file and extract table definitions
        
        Args:
            sql_file: Path to SQL file
        """
        with open(sql_file, 'r') as f:
            sql_content = f.read()
        
        # Split into individual statements
        statements = [s.strip() for s in sql_content.split(';') if s.strip()]
        
        # Get schema name for prefixing
        schema_name = self.lakebase_config.get('schema', 'public')
        
        for stmt in statements:
            stmt_upper = stmt.upper()
            
            if 'DROP TABLE' in stmt_upper:
                # Add schema prefix to DROP TABLE if not already present
                if f'{schema_name}.' not in stmt.lower():
                    # Check which pattern to replace (avoid double replacement)
                    if 'IF EXISTS' in stmt_upper:
                        stmt = stmt.replace('DROP TABLE IF EXISTS', f'DROP TABLE IF EXISTS {schema_name}.', 1)
                    else:
                        stmt = stmt.replace('DROP TABLE', f'DROP TABLE {schema_name}.', 1)
                self.ddl_statements.append(stmt)
            
            elif 'CREATE TABLE' in stmt_upper:
                table_name, columns = self._parse_create_table(stmt)
                self.tables[table_name] = {'columns': columns, 'indexes': []}
                
                # PHASE 1 PATTERN: Create production table directly (no PK/indexes)
                # Remove PRIMARY KEY and INDEX constraints for Phase 1
                production_columns = {}
                for col_name, col_type in columns.items():
                    # Strip PRIMARY KEY constraint (will be added in Phase 2)
                    clean_col_type = col_type.replace(' PRIMARY KEY', '').replace('PRIMARY KEY', '')
                    production_columns[col_name] = clean_col_type
                
                # Generate production table DDL (no constraints)
                production_ddl = self._generate_create_table_ddl(
                    f"{schema_name}.{table_name}", 
                    production_columns
                )
                self.ddl_statements.append(production_ddl)
            
            elif 'CREATE INDEX' in stmt_upper:
                # STAGE+SWAP PATTERN: Skip CREATE INDEX during initial DDL
                # Indexes will be built AFTER COPY on staging tables
                # Extract index info and save for later
                index_info = self._parse_create_index(stmt)
                if index_info:
                    table_name, index_col = index_info
                    if table_name in self.tables:
                        self.tables[table_name]['indexes'].append(index_col)
                # Don't append to DDL statements - will build later
        
        print(f"✅ Parsed {len(self.tables)} tables from {sql_file}")
        for table_name in self.tables:
            print(f"   - {table_name}: {len(self.tables[table_name]['columns'])} columns")
    
    def _parse_create_table(self, stmt: str) -> Tuple[str, Dict[str, str]]:
        """
        Parse CREATE TABLE statement
        
        Returns:
            (table_name, {column_name: column_definition})
        """
        # Extract table name
        match = re.search(r'CREATE TABLE\s+(\w+)\s*\(', stmt, re.IGNORECASE)
        if not match:
            raise ValueError(f"Could not parse table name from: {stmt}")
        
        table_name = match.group(1)
        
        # Extract column definitions (simplified parser)
        columns = {}
        col_section = re.search(r'\((.*)\)', stmt, re.DOTALL)
        if col_section:
            col_text = col_section.group(1)
            # Split by comma, but be careful with nested parentheses
            col_lines = []
            current = ''
            paren_depth = 0
            for char in col_text:
                if char == '(':
                    paren_depth += 1
                elif char == ')':
                    paren_depth -= 1
                elif char == ',' and paren_depth == 0:
                    col_lines.append(current.strip())
                    current = ''
                    continue
                current += char
            if current.strip():
                col_lines.append(current.strip())
            
            for line in col_lines:
                line = line.strip()
                if not line or line.upper().startswith('CONSTRAINT') or line.upper().startswith('FOREIGN KEY'):
                    continue
                
                # Extract column name (first word)
                parts = line.split()
                if parts:
                    col_name = parts[0]
                    col_def = ' '.join(parts[1:])
                    columns[col_name] = col_def
        
        return table_name, columns
    
    def _generate_create_table_ddl(self, full_table_name: str, columns: Dict[str, str]) -> str:
        """
        Generate CREATE TABLE DDL from column definitions
        
        Args:
            full_table_name: Fully qualified table name (schema.table)
            columns: Dict of {column_name: column_type}
        
        Returns:
            CREATE TABLE DDL statement
        """
        col_defs = []
        for col_name, col_type in columns.items():
            col_defs.append(f"    {col_name} {col_type}")
        
        ddl = f"CREATE TABLE {full_table_name} (\n"
        ddl += ",\n".join(col_defs)
        ddl += "\n)"
        
        return ddl
    
    def _parse_create_index(self, stmt: str) -> Optional[Tuple[str, str]]:
        """
        Parse CREATE INDEX statement
        
        Returns:
            (table_name, column_name) or None
        """
        # Pattern: CREATE INDEX idx_name ON table_name(column_name)
        match = re.search(r'ON\s+(\w+)\s*\((\w+)\)', stmt, re.IGNORECASE)
        if match:
            return match.group(1), match.group(2)
        return None
    
    def parse_dict(self, schema_dict: Dict[str, Any]):
        """
        Parse schema from dictionary
        
        Args:
            schema_dict: Dictionary defining tables
        """
        self.tables = schema_dict
        
        # Generate DDL statements from dict
        for table_name, table_def in schema_dict.items():
            columns = table_def.get('columns', {})
            indexes = table_def.get('indexes', [])
            
            # Generate CREATE TABLE
            col_defs = [f"{col_name} {col_type}" for col_name, col_type in columns.items()]
            create_table = f"CREATE TABLE {table_name} (\n    " + ",\n    ".join(col_defs) + "\n)"
            self.ddl_statements.append(create_table)
            
            # Generate CREATE INDEX
            for idx_col in indexes:
                create_index = f"CREATE INDEX idx_{table_name}_{idx_col} ON {table_name}({idx_col})"
                self.ddl_statements.append(create_index)
        
        print(f"✅ Parsed {len(self.tables)} tables from dictionary")
        for table_name in self.tables:
            print(f"   - {table_name}: {len(self.tables[table_name]['columns'])} columns")
    
    def execute_ddl(self, skip_tables_with_data=True):
        """
        Execute DDL statements on Lakebase
        
        Args:
            skip_tables_with_data: If True, skip DROP/CREATE for tables that already have data
        """
        print("\n" + "=" * 80)
        print("EXECUTING DDL ON LAKEBASE")
        print("=" * 80)
        
        conn = psycopg2.connect(
            host=self.lakebase_config['host'],
            port=self.lakebase_config['port'],
            database=self.lakebase_config['database'],
            user=self.lakebase_config['user'],
            password=self.lakebase_config['password'],
            sslmode=self.lakebase_config.get('sslmode', 'require')
        )
        
        # ✅ CHECKPOINT: Get list of tables that already have data
        tables_with_data = set()
        if skip_tables_with_data:
            try:
                cur = conn.cursor()
                schema_name = self.lakebase_config.get('schema', 'public')
                cur.execute(f"SET search_path TO {schema_name}")
                
                print(f"\n🔍 Checking for existing tables with data...")
                for table_name in self.tables.keys():
                    try:
                        cur.execute(f"SELECT COUNT(*) FROM {schema_name}.{table_name}")
                        count = cur.fetchone()[0]
                        if count > 0:
                            tables_with_data.add(table_name)
                            print(f"   ✓ {table_name}: {count:,} rows (will SKIP)")
                    except psycopg2.Error:
                        # Table doesn't exist yet - rollback to clear failed transaction
                        conn.rollback()
                
                # Commit or rollback to close any open transaction
                conn.commit()
                cur.close()
                
                if tables_with_data:
                    print(f"\n💡 Found {len(tables_with_data)} tables with data - will skip DDL for these")
                else:
                    print(f"\n💡 No existing tables with data found")
            except Exception as e:
                print(f"\n⚠️  Checkpoint check failed ({str(e)[:50]}), proceeding with full DDL")
                conn.rollback()  # Clear any failed transaction
                tables_with_data = set()
        
        try:
            cur = conn.cursor()
            
            # Set schema if provided
            if 'schema' in self.lakebase_config:
                schema_name = self.lakebase_config['schema']
                cur.execute(f"SET search_path TO {schema_name}")
                print(f"\n✅ Using schema: {schema_name}\n")
            
            skipped_count = 0
            executed_count = 0
            
            for i, stmt in enumerate(self.ddl_statements, 1):
                # PHASE 1 RULE:
                # - Skip CREATE TABLE if table has data (checkpoint protection)
                
                should_skip = False
                if skip_tables_with_data:
                    # Skip tables that already have data
                    for table_name in tables_with_data:
                        # Exact match to avoid false positives
                        if f" {table_name}" in stmt.lower() or f".{table_name}" in stmt.lower():
                            should_skip = True
                            break
                
                if should_skip:
                    print(f"\n[{i}/{len(self.ddl_statements)}] SKIPPING (table has data - checkpoint protected):")
                    print(f"   {stmt[:80]}..." if len(stmt) > 80 else f"   {stmt}")
                    skipped_count += 1
                    continue
                
                print(f"\n[{i}/{len(self.ddl_statements)}] Executing:")
                print(f"   {stmt[:80]}..." if len(stmt) > 80 else f"   {stmt}")
                
                try:
                    cur.execute(stmt)
                    conn.commit()
                    print("   ✅ Success")
                    executed_count += 1
                except psycopg2.Error as e:
                    # If table already exists, that's okay
                    # If table doesn't exist for DROP, that's also okay
                    if 'already exists' in str(e).lower() or 'does not exist' in str(e).lower():
                        print(f"   ⚠️  {e} (skipping)")
                        conn.rollback()
                        executed_count += 1
                    else:
                        print(f"   ❌ Error: {e}")
                        conn.rollback()
                        raise
            
            cur.close()
            print("\n" + "=" * 80)
            print(f"✅ DDL EXECUTION COMPLETE")
            print(f"   Executed: {executed_count} statements")
            print(f"   Skipped: {skipped_count} statements (tables with data)")
            print(f"   Total tables: {len(self.tables)}")
            print("=" * 80 + "\n")
        
        finally:
            conn.close()
    
    def generate_and_load_data(
        self,
        rows_per_table: int = 1_000_000,
        rows_per_table_dict: Optional[Dict[str, int]] = None,
        uc_volume_path: Optional[str] = None
    ):
        """
        Generate synthetic data for all tables and load to Lakebase - PHASE 1: COPY ONLY
        
        PHASE 1 Pattern (No Indexes):
        1. Sort tables by row count (largest first)
        2. COPY directly to production table (no indexes)
        3. Checkpoint protects existing data
        4. NO INDEX builds (Phase 2 will handle)
        
        Benefits:
        - 35-40% faster (no INDEX contention)
        - Simpler failure recovery
        - Industry-standard approach
        
        Args:
            rows_per_table: Default number of rows to generate per table
            rows_per_table_dict: Optional dict mapping table_name -> row_count
                                 Example: {'users': 10_000_000, 'orders': 50_000_000}
            uc_volume_path: Unity Catalog volume path (for bulk loading)
        """
        from utils.bulk_load import load_dataframe_to_lakebase
        import psycopg2
        
        spark = SparkSession.builder.getOrCreate()
        schema = self.lakebase_config.get('schema', 'public')
        
        # 🔢 STEP 1: Sort tables by row count (largest first)
        table_row_counts = []
        for table_name, table_def in self.tables.items():
            if rows_per_table_dict and table_name in rows_per_table_dict:
                num_rows = rows_per_table_dict[table_name]
            else:
                num_rows = rows_per_table
            table_row_counts.append((table_name, table_def, num_rows))
        
        # Sort by row count descending (largest first)
        table_row_counts.sort(key=lambda x: x[2], reverse=True)
        
        print("\n" + "=" * 80)
        print(f"🚀 PHASE 1: PURE COPY (NO INDEXES)")
        print("=" * 80)
        print(f"📊 Load order (largest → smallest):")
        for i, (tbl, _, rows) in enumerate(table_row_counts[:5], 1):
            print(f"   {i}. {tbl}: {rows:,} rows")
        if len(table_row_counts) > 5:
            print(f"   ... and {len(table_row_counts) - 5} more tables")
        print(f"\n⚡ Strategy: COPY only (35-40% faster - no INDEX contention)")
        print(f"📋 Indexes will be built in Phase 2 (separate job)")
        print("=" * 80 + "\n")
        
        tables_loaded = 0
        tables_skipped = 0
        
        # 📦 STEP 2: Process each table (COPY ONLY - no indexes)
        for i, (table_name, table_def, num_rows) in enumerate(table_row_counts):
            print(f"\n📊 Table {i+1}/{len(table_row_counts)}: {table_name} ({num_rows:,} rows)")
            
            # ✅ CHECKPOINT: Check if table already has data
            try:
                conn = psycopg2.connect(
                    host=self.lakebase_config['host'],
                    port=self.lakebase_config.get('port', 5432),
                    database=self.lakebase_config['database'],
                    user=self.lakebase_config['user'],
                    password=self.lakebase_config['password']
                )
                cursor = conn.cursor()
                
                # Set PostgreSQL performance settings for faster writes
                cursor.execute("SET synchronous_commit = off")
                cursor.execute("SET maintenance_work_mem = '1GB'")
                conn.commit()
                
                cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table_name}")
                existing_rows = cursor.fetchone()[0]
                cursor.close()
                conn.close()
                
                if existing_rows > 0:
                    print(f"   ⏭️  SKIPPING - Table already has {existing_rows:,} rows ✅")
                    print(f"   💡 Protected by checkpoint (Phase 1 will not reload)")
                    tables_skipped += 1
                    continue
                else:
                    print(f"   ✓ Table is empty, proceeding with COPY...")
            except Exception:
                # If check fails (table doesn't exist, etc.), proceed with load
                print(f"   ✓ Table doesn't exist yet, proceeding with initial load...")
            
            # 📝 STEP 1: Check if data already exists in UC volume
            csv_path = f"{uc_volume_path}/{table_name}_csvs" if uc_volume_path else None
            csv_exists = False
            
            if csv_path:
                try:
                    # Check if CSV files exist in UC volume (simple dbutils check)
                    from pyspark.dbutils import DBUtils
                    dbutils = DBUtils(spark)
                    files = dbutils.fs.ls(csv_path)
                    
                    # Check if there are actual CSV/data files (not just _SUCCESS)
                    csv_files = [f for f in files if f.name.endswith('.csv') or f.name.startswith('part-')]
                    
                    if csv_files:
                        csv_exists = True
                        print(f"   [1/2] ✅ Data already exists in UC volume: {csv_path}")
                        print(f"         Found {len(csv_files)} existing files - SKIPPING data generation")
                except Exception:
                    # Path doesn't exist or error checking, proceed with generation
                    csv_exists = False
                    print(f"   [1/2] 📝 No existing data in UC volume (will generate {num_rows:,} rows)")
            
            # Generate data only if it doesn't exist in UC volume
            if not csv_exists:
                if not csv_path:
                    print(f"   [1/2] Generating {num_rows:,} rows...")
                else:
                    print(f"   [1/2] Generating {num_rows:,} rows (will write to UC volume)...")
                    
                df = self._generate_table_data(spark, table_name, table_def, num_rows)
            else:
                # Data exists in volume, skip generation
                df = None
            
            # 📦 STEP 2: COPY directly to PRODUCTION table (no indexes, no swap)
            print(f"   [2/2] COPY to {table_name} (PHASE 1: no indexes)...")
            result = load_dataframe_to_lakebase(
                spark_df=df,  # None if CSV exists, DataFrame if newly generated
                table_name=table_name,  # ← Load DIRECTLY to production
                lakebase_config=self.lakebase_config,
                uc_volume_path=uc_volume_path,
                schema=schema,
                num_rows=num_rows
            )
            
            print(f"   ✅ COPY complete: {result['rows']:,} rows in {result['duration_minutes']:.2f} min")
            print(f"   Method: {result['method']}")
            print(f"   📋 Table ready (indexes will be built in Phase 2)")
            tables_loaded += 1
        
        print("\n" + "=" * 80)
        print(f"✅ PHASE 1 COMPLETE: DATA LOADED (NO INDEXES)")
        print("=" * 80)
        print(f"   Tables loaded: {tables_loaded}")
        print(f"   Tables skipped (checkpoint protected): {tables_skipped}")
        print(f"   Total tables: {len(self.tables)}")
        print(f"   ⚡ Performance: 35-40% faster (no INDEX contention)")
        print(f"   📋 Next step: Run Phase 2 to build indexes (separate job)")
        print("=" * 80 + "\n")
    
    def _generate_table_data(
        self,
        spark: SparkSession,
        table_name: str,
        table_def: Dict[str, Any],
        num_rows: int
    ):
        """
        Generate synthetic data for a table based on column types
        
        Args:
            spark: SparkSession
            table_name: Name of table
            table_def: Table definition with columns
            num_rows: Number of rows to generate
            
        Returns:
            Spark DataFrame with synthetic data
        """
        columns = table_def['columns']
        
        # Start with row numbers
        df = spark.range(num_rows)
        
        for col_name, col_type in columns.items():
            col_type_upper = col_type.upper()
            
            # Generate data based on type
            if 'PRIMARY KEY' in col_type_upper or col_name.endswith('_id') or 'hashkey' in col_name.lower() or 'hash_key' in col_name.lower():
                # Primary key / hash key: use SHA256 hex strings (64 chars)
                df = df.withColumn(col_name, 
                    sha2(concat_ws('-', col('id').cast('string'), lit(table_name)), 256))
            
            elif 'VARCHAR' in col_type_upper and 'email' in col_name.lower():
                # Email addresses
                df = df.withColumn(col_name,
                    concat_ws('', lit('user'), col('id').cast('string'), lit('@example.com')))
            
            elif 'VARCHAR' in col_type_upper:
                # Generic strings
                df = df.withColumn(col_name,
                    concat_ws('_', lit(col_name), col('id').cast('string')))
            
            elif 'NUMERIC' in col_type_upper or 'DECIMAL' in col_type_upper:
                # Numeric values: random between 0 and 1000
                df = df.withColumn(col_name, (rand() * 1000).cast('decimal(18,2)'))
            
            elif 'DOUBLE' in col_type_upper or 'FLOAT' in col_type_upper or 'REAL' in col_type_upper:
                # Double precision / float values: random between 0 and 1000
                df = df.withColumn(col_name, rand() * 1000)
            
            elif 'TIMESTAMP' in col_type_upper:
                # Timestamp values: use proper timestamp type (not unix epoch integers)
                # Generate timestamps in the past year
                from pyspark.sql.functions import from_unixtime
                df = df.withColumn(col_name,
                    from_unixtime(lit(1700000000) + (rand() * 31536000)).cast('timestamp'))
            
            elif 'INTEGER' in col_type_upper or 'INT' in col_type_upper:
                # Integers: random between 0 and 1000
                df = df.withColumn(col_name, (rand() * 1000).cast('int'))
            
            elif 'BIGINT' in col_type_upper:
                # Bigint values: large random integers
                df = df.withColumn(col_name, (rand() * 1000000000).cast('bigint'))
            
            elif 'BOOLEAN' in col_type_upper or 'BOOL' in col_type_upper:
                # Boolean: random true/false
                df = df.withColumn(col_name, (rand() > 0.5).cast('boolean'))
            
            else:
                # Default: string representation
                df = df.withColumn(col_name,
                    concat_ws('_', lit(col_name), col('id').cast('string')))
        
        # Drop the temporary id column
        df = df.drop('id')
        
        return df
    
    def _build_primary_key_index(
        self,
        staging_table: str,
        table_def: Dict[str, Any],
        schema: str
    ):
        """
        Build PRIMARY KEY index CONCURRENTLY on staging table
        
        Args:
            staging_table: Name of staging table (without schema)
            table_def: Table definition with columns
            schema: Schema name
        """
        import psycopg2
        import time
        
        # Find PRIMARY KEY column (usually hashkey)
        pk_column = None
        for col_name, col_type in table_def['columns'].items():
            if 'PRIMARY KEY' in col_type.upper() or 'hashkey' in col_name.lower():
                pk_column = col_name
                break
        
        if not pk_column:
            print(f"      ⚠️  No PRIMARY KEY column found, skipping index...")
            return
        
        # Build index CONCURRENTLY (non-blocking)
        index_name = f"{staging_table}_pk"
        sql = f"""
            CREATE UNIQUE INDEX CONCURRENTLY {index_name} 
            ON {schema}.{staging_table} ({pk_column})
        """
        
        start_time = time.time()
        
        try:
            # Need a separate connection with autocommit for CONCURRENTLY
            conn = psycopg2.connect(
                host=self.lakebase_config['host'],
                port=self.lakebase_config.get('port', 5432),
                database=self.lakebase_config['database'],
                user=self.lakebase_config['user'],
                password=self.lakebase_config['password']
            )
            conn.autocommit = True
            cursor = conn.cursor()
            
            print(f"      🔨 CREATE UNIQUE INDEX CONCURRENTLY on {pk_column}...")
            cursor.execute(sql)
            cursor.close()
            conn.close()
            
            duration_min = (time.time() - start_time) / 60
            print(f"      ✅ Index built in {duration_min:.1f} min")
            
        except Exception as e:
            print(f"      ❌ Index build failed: {e}")
            raise
    
    def _atomic_swap(
        self,
        staging_table: str,
        production_table: str,
        schema: str
    ):
        """
        Atomically swap staging table to production table
        
        Pattern:
        1. ALTER TABLE production RENAME TO production__old
        2. ALTER TABLE staging RENAME TO production
        3. DROP TABLE production__old (if exists)
        
        Args:
            staging_table: Name of staging table (without schema)
            production_table: Name of production table (without schema)
            schema: Schema name
        """
        import psycopg2
        
        try:
            conn = psycopg2.connect(
                host=self.lakebase_config['host'],
                port=self.lakebase_config.get('port', 5432),
                database=self.lakebase_config['database'],
                user=self.lakebase_config['user'],
                password=self.lakebase_config['password']
            )
            conn.autocommit = False
            cursor = conn.cursor()
            
            # Step 1: Rename production → production__old (if it exists)
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {schema}.{production_table}")
                count = cursor.fetchone()[0]
                if count > 0:
                    print(f"      ⚠️  Production table has {count:,} rows, preserving as {production_table}__old")
                else:
                    print(f"      💡 Production table exists but is empty, will drop and replace")
                
                # Rename production table (whether empty or not)
                cursor.execute(f"ALTER TABLE {schema}.{production_table} RENAME TO {production_table}__old")
            except psycopg2.Error as e:
                # Production table doesn't exist, that's fine
                print(f"      💡 Production table doesn't exist yet (will be created from staging)")
                conn.rollback()
            
            # Step 2: Rename staging → production (atomic)
            cursor.execute(f"ALTER TABLE {schema}.{staging_table} RENAME TO {production_table}")
            
            # Step 3: Rename index to match production table
            staging_index = f"{staging_table}_pk"
            production_index = f"{production_table}_pk"
            try:
                cursor.execute(f"ALTER INDEX {schema}.{staging_index} RENAME TO {production_index}")
            except psycopg2.Error as e:
                # Index might not exist or have different name, not critical
                print(f"      ⚠️  Index rename: {e}")
                conn.rollback()
            
            conn.commit()
            cursor.close()
            conn.close()
            
            print(f"      ✅ Swap complete: {staging_table} → {production_table}")
            
            # Step 4: Cleanup old table (separate transaction, non-critical)
            try:
                conn = psycopg2.connect(
                    host=self.lakebase_config['host'],
                    port=self.lakebase_config.get('port', 5432),
                    database=self.lakebase_config['database'],
                    user=self.lakebase_config['user'],
                    password=self.lakebase_config['password']
                )
                conn.autocommit = True
                cursor = conn.cursor()
                cursor.execute(f"DROP TABLE IF EXISTS {schema}.{production_table}__old CASCADE")
                cursor.close()
                conn.close()
                print(f"      🗑️  Cleaned up old table")
            except Exception as e:
                print(f"      ⚠️  Cleanup warning: {e}")
        
        except Exception as e:
            print(f"      ❌ Swap failed: {e}")
            raise
    
    def print_load_summary(
        self,
        rows_per_table: int = 1_000_000,
        rows_per_table_dict: Optional[Dict[str, int]] = None
    ):
        """
        Print a summary of what will be created and loaded
        
        Args:
            rows_per_table: Default number of rows per table
            rows_per_table_dict: Optional dict mapping table_name -> row_count
        """
        print("\n" + "=" * 80)
        print("📋 LOAD SUMMARY")
        print("=" * 80)
        
        total_rows = 0
        total_tables = len(self.tables)
        
        for i, (table_name, table_def) in enumerate(self.tables.items(), 1):
            columns = table_def['columns']
            indexes = table_def.get('indexes', [])
            
            # Determine row count for this table
            if rows_per_table_dict and table_name in rows_per_table_dict:
                num_rows = rows_per_table_dict[table_name]
            else:
                num_rows = rows_per_table
            
            total_rows += num_rows
            
            print(f"\n[{i}/{total_tables}] Table: {table_name}")
            print(f"    Rows to load: {num_rows:,}")
            print(f"    Columns ({len(columns)}):")
            
            for col_name, col_type in list(columns.items())[:10]:  # Show first 10 columns
                print(f"       - {col_name:30s} {col_type}")
            
            if len(columns) > 10:
                print(f"       ... and {len(columns) - 10} more columns")
            
            if indexes:
                print(f"    Indexes ({len(indexes)}): {', '.join(indexes)}")
        
        print("\n" + "=" * 80)
        print(f"📊 TOTAL SUMMARY")
        print("=" * 80)
        print(f"   Total tables: {total_tables}")
        print(f"   Total rows: {total_rows:,}")
        print(f"   Average rows per table: {total_rows // total_tables:,}")
        
        # Estimate time based on row counts
        estimated_time_min = 0
        for table_name, table_def in self.tables.items():
            if rows_per_table_dict and table_name in rows_per_table_dict:
                num_rows = rows_per_table_dict[table_name]
            else:
                num_rows = rows_per_table
            
            # Rough time estimates
            if num_rows < 1_000_000:
                estimated_time_min += 0.5  # JDBC: ~30 seconds
            elif num_rows < 10_000_000:
                estimated_time_min += num_rows / 2_000_000  # Serial: ~2M rows/min
            else:
                estimated_time_min += num_rows / 6_000_000  # Parallel: ~6M rows/min
        
        print(f"   Estimated load time: ~{estimated_time_min:.0f} minutes")
        print("=" * 80)
        
        # Prompt user to confirm
        print("\n💡 Loading method will be automatically selected per table:")
        print("   - < 1M rows: JDBC (fast, simple)")
        print("   - 1-10M rows: Serial bulk load (faster)")
        print("   - > 10M rows: Parallel bulk load (fastest)")
        print("\n")
    
    def execute_and_load(
        self,
        rows_per_table: int = 1_000_000,
        rows_per_table_dict: Optional[Dict[str, int]] = None,
        uc_volume_path: Optional[str] = None,
        skip_tables_with_data: bool = True
    ):
        """
        One-step: execute DDL and load data (with checkpoint support)
        
        Args:
            rows_per_table: Default number of rows per table
            rows_per_table_dict: Optional dict mapping table_name -> row_count
            uc_volume_path: Unity Catalog volume path (for bulk loading)
            skip_tables_with_data: If True, skip tables that already have data (resumable)
        """
        self.execute_ddl(skip_tables_with_data=skip_tables_with_data)
        self.print_load_summary(rows_per_table, rows_per_table_dict)
        self.generate_and_load_data(rows_per_table, rows_per_table_dict, uc_volume_path)
        
        print("\n🎉 SCHEMA LOADED AND DATA POPULATED!")
        print(f"✅ {len(self.tables)} tables ready for benchmarking\n")


# Convenience function
def load_schema_and_data(
    source: str,
    lakebase_config: Dict[str, Any],
    rows_per_table: int = 1_000_000,
    rows_per_table_dict: Optional[Dict[str, int]] = None,
    uc_volume_path: Optional[str] = None,
    skip_tables_with_data: bool = True
):
    """
    One-line function to load schema and data from any source (with checkpoint support)
    
    Args:
        source: Path to SQL file, YAML file, or dict
        lakebase_config: Lakebase connection config
        rows_per_table: Default rows to generate per table
        rows_per_table_dict: Optional dict for per-table row counts
                             Example: {'users': 10_000_000, 'orders': 50_000_000}
        uc_volume_path: UC volume path for bulk loading
        skip_tables_with_data: If True (default), skip tables that already have data (resumable)
        
    Usage:
        # Same rows for all tables
        load_schema_and_data('my_schema.sql', LAKEBASE_CONFIG, rows_per_table=10_000_000)
        
        # Different rows per table
        load_schema_and_data('my_schema.sql', LAKEBASE_CONFIG, 
                            rows_per_table_dict={'users': 5_000_000, 'orders': 50_000_000})
        
        # Mix: specify some tables, others use default
        load_schema_and_data('my_schema.sql', LAKEBASE_CONFIG, 
                            rows_per_table=1_000_000,
                            rows_per_table_dict={'big_table': 100_000_000})
        
        # Force reload all tables (skip checkpoint)
        load_schema_and_data('my_schema.sql', LAKEBASE_CONFIG, 
                            rows_per_table=1_000_000,
                            skip_tables_with_data=False)
    """
    if isinstance(source, dict):
        loader = SchemaLoader.from_dict(source, lakebase_config)
    elif source.endswith('.yaml') or source.endswith('.yml'):
        loader = SchemaLoader.from_yaml(source, lakebase_config)
    else:
        loader = SchemaLoader.from_sql_file(source, lakebase_config)
    
    loader.execute_and_load(rows_per_table, rows_per_table_dict, uc_volume_path, skip_tables_with_data)
    return loader
