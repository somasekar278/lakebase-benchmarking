# Databricks notebook source
# MAGIC %md
# MAGIC # Load Schema from DDL - Complete Workflow
# MAGIC 
# MAGIC **This notebook does EVERYTHING:**
# MAGIC 1. Reads your DDL file (CREATE TABLE statements)
# MAGIC 2. Creates tables on Lakebase
# MAGIC 3. Generates synthetic data matching your schema
# MAGIC 4. Loads data (auto-optimizes: JDBC/serial/parallel bulk)
# MAGIC 
# MAGIC **Usage:**
# MAGIC - Upload your DDL file (.txt or .sql) to Databricks
# MAGIC - Update the config below
# MAGIC - Run all cells
# MAGIC - Done! ✅

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Install Dependencies

# COMMAND ----------

%pip install psycopg2-binary pyyaml
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Configuration

# COMMAND ----------

# Get parameters from job (or use defaults for interactive runs)
dbutils.widgets.text("lakebase_host", "ep-autumn-fire-d318blbk.database.eu-west-1.cloud.databricks.com", "Lakebase Host")
dbutils.widgets.text("lakebase_database", "benchmark", "Database")
dbutils.widgets.text("lakebase_schema", "features", "Schema")
dbutils.widgets.text("lakebase_user", "fraud_benchmark_user", "User")
dbutils.widgets.text("lakebase_password", "", "Password")
dbutils.widgets.text("ddl_file_path", "", "DDL File Path")
dbutils.widgets.text("rows_per_table", "1000000", "Rows Per Table (default)")
dbutils.widgets.text("rows_per_table_file", "", "Rows Per Table File (CSV: table_name,row_count)")
dbutils.widgets.text("uc_volume_path", "/Volumes/benchmark/data_load/benchmark_data", "UC Volume Path")

# Lakebase connection from parameters
LAKEBASE_CONFIG = {
    'host': dbutils.widgets.get("lakebase_host"),
    'port': 5432,
    'database': dbutils.widgets.get("lakebase_database"),
    'user': dbutils.widgets.get("lakebase_user"),
    'password': dbutils.widgets.get("lakebase_password"),
    'schema': dbutils.widgets.get("lakebase_schema"),
    'sslmode': 'require',
}

DDL_FILE_PATH = dbutils.widgets.get("ddl_file_path")
ROWS_PER_TABLE = int(dbutils.widgets.get("rows_per_table"))
ROWS_PER_TABLE_FILE = dbutils.widgets.get("rows_per_table_file")
UC_VOLUME_PATH = dbutils.widgets.get("uc_volume_path")

# Load per-table row counts from file if provided
ROWS_PER_TABLE_DICT = {}
if ROWS_PER_TABLE_FILE and ROWS_PER_TABLE_FILE.strip():
    print(f"📊 Loading per-table row counts from: {ROWS_PER_TABLE_FILE}")
    try:
        with open(ROWS_PER_TABLE_FILE, 'r') as f:
            for line in f:
                line = line.strip()
                if line and ',' in line:
                    table_name, row_count = line.split(',', 1)
                    ROWS_PER_TABLE_DICT[table_name.strip()] = int(row_count.strip())
        print(f"✅ Loaded {len(ROWS_PER_TABLE_DICT)} table-specific row counts")
    except Exception as e:
        print(f"⚠️ Warning: Could not load rows_per_table_file: {e}")
        print(f"   Falling back to default: {ROWS_PER_TABLE:,} rows per table")

# Determine if bulk loading is needed
max_rows = max(ROWS_PER_TABLE_DICT.values()) if ROWS_PER_TABLE_DICT else ROWS_PER_TABLE
USE_BULK_LOADING = max_rows >= 1_000_000  # Auto-enable for large datasets

print("✅ Configuration loaded from parameters")
print(f"   Lakebase: {LAKEBASE_CONFIG['host']}")
print(f"   Database: {LAKEBASE_CONFIG['database']}")
print(f"   Schema: {LAKEBASE_CONFIG['schema']}")
print(f"   Default rows per table: {ROWS_PER_TABLE:,}")
if ROWS_PER_TABLE_DICT:
    print(f"   Custom row counts:")
    for table, rows in ROWS_PER_TABLE_DICT.items():
        print(f"      - {table}: {rows:,}")
print(f"   Bulk loading: {USE_BULK_LOADING}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Get DDL File Path
# MAGIC 
# MAGIC Read DDL from the file path provided in parameters

# COMMAND ----------

# Get DDL file path from widget parameter
ddl_file_param = dbutils.widgets.get("ddl_file_path")

if ddl_file_param and ddl_file_param.strip():
    DDL_FILE_PATH = ddl_file_param
    print(f"✅ Using DDL file: {DDL_FILE_PATH}")
    
    # Verify file exists
    try:
        with open(DDL_FILE_PATH, 'r') as f:
            ddl_content = f.read()
        print(f"✅ DDL file loaded successfully ({len(ddl_content)} bytes)")
    except Exception as e:
        raise ValueError(f"❌ Cannot read DDL file at {DDL_FILE_PATH}: {e}")
else:
    raise ValueError("❌ ddl_file_path parameter is required but was not provided")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Preview DDL (Optional)

# COMMAND ----------

# Preview what will be executed
print("📋 DDL to be executed:")
print("=" * 80)
with open(DDL_FILE_PATH, 'r') as f:
    print(f.read())
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Checkpoint Support (Resumable Loads)
# MAGIC 
# MAGIC **The schema_loader now has built-in checkpoint support:**
# MAGIC - Checks which tables already have data BEFORE dropping/recreating
# MAGIC - Skips tables that already have data
# MAGIC - Allows resumable loads if a job fails partway through
# MAGIC 
# MAGIC **To force a clean reload:** Manually drop tables first

# COMMAND ----------

print("✅ Checkpoint enabled - existing tables with data will be preserved")
print("   (Use DROP TABLE manually to force reload)\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Load Schema and Generate Data
# MAGIC 
# MAGIC **This is the main step - it does everything!**
# MAGIC 
# MAGIC **What happens:**
# MAGIC 1. Parse your DDL
# MAGIC 2. Create tables on Lakebase
# MAGIC 3. **Show summary** (review tables, columns, row counts before loading)
# MAGIC 4. Generate synthetic data matching your schema
# MAGIC 5. Load data (auto-optimizes: JDBC/serial/parallel bulk)

# COMMAND ----------

import sys

# Add DABs synced files to path
workspace_files_path = "/Workspace/{}/.bundle/lakebase-benchmarking/dev/files".format(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
)
sys.path.insert(0, workspace_files_path)

from utils.schema_loader import load_schema_and_data
import time

start_time = time.time()

print("\n" + "=" * 80)
print("STARTING FULL WORKFLOW")
print("=" * 80)
print(f"DDL file: {DDL_FILE_PATH}")
print(f"Default rows per table: {ROWS_PER_TABLE:,}")
if ROWS_PER_TABLE_DICT:
    print(f"Custom row counts: {len(ROWS_PER_TABLE_DICT)} tables")
print(f"Bulk loading: {USE_BULK_LOADING}")
print("=" * 80 + "\n")

try:
    # This does EVERYTHING:
    # 1. Parse DDL
    # 2. Execute DDL on Lakebase (creates tables)
    # 3. Show summary (tables, columns, row counts) ← YOU REVIEW THIS
    # 4. Generate data (with per-table row counts if specified)
    # 5. Load data (auto-optimized: JDBC/serial/parallel bulk)
    
    loader = load_schema_and_data(
        source=DDL_FILE_PATH,
        lakebase_config=LAKEBASE_CONFIG,
        rows_per_table=ROWS_PER_TABLE,
        rows_per_table_dict=ROWS_PER_TABLE_DICT if ROWS_PER_TABLE_DICT else None,
        uc_volume_path=UC_VOLUME_PATH if USE_BULK_LOADING else None
    )
    
    duration = (time.time() - start_time) / 60
    
    print("\n" + "=" * 80)
    print("🎉 SUCCESS!")
    print("=" * 80)
    print(f"✅ Created {len(loader.tables)} tables:")
    for table_name in loader.tables:
        if ROWS_PER_TABLE_DICT and table_name in ROWS_PER_TABLE_DICT:
            rows = ROWS_PER_TABLE_DICT[table_name]
        else:
            rows = ROWS_PER_TABLE
        print(f"   - {table_name}: {rows:,} rows")
    print(f"\n✅ Total time: {duration:.1f} minutes")
    print("=" * 80 + "\n")
    
    print("💡 Next steps:")
    print("   1. Verify data:")
    for table_name in list(loader.tables.keys())[:3]:  # Show first 3
        print(f"      SELECT COUNT(*) FROM {table_name};")
    print("   2. Run benchmarks: notebooks/benchmarks/benchmark_lakebase.py")
    
except Exception as e:
    print(f"\n❌ Error: {e}")
    import traceback
    traceback.print_exc()
    
    # Fail the notebook (and the job)
    dbutils.notebook.exit(f"FAILED: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Verify Results

# COMMAND ----------

# Verify tables were created and have data
import psycopg2

# Create connection without 'schema'
conn_config = {k: v for k, v in LAKEBASE_CONFIG.items() if k != 'schema'}
schema_name = LAKEBASE_CONFIG['schema']

conn = psycopg2.connect(**conn_config)

try:
    cur = conn.cursor()
    
    # Set schema
    cur.execute(f"SET search_path TO {schema_name}, public")
    
    # List tables
    cur.execute("""
        SELECT tablename 
        FROM pg_tables 
        WHERE schemaname = %s 
        ORDER BY tablename
    """, (schema_name,))
    
    tables = cur.fetchall()
    
    print("📊 Tables in Lakebase:")
    print("=" * 80)
    for (table_name,) in tables:
        # Get row count
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cur.fetchone()[0]
        print(f"   {table_name:30s} {count:>15,} rows")
    print("=" * 80)
    
    cur.close()
    
finally:
    conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Sample Data

# COMMAND ----------

# Show sample data from first table
import psycopg2

# Create connection without 'schema'
conn_config = {k: v for k, v in LAKEBASE_CONFIG.items() if k != 'schema'}
schema_name = LAKEBASE_CONFIG['schema']

conn = psycopg2.connect(**conn_config)

try:
    cur = conn.cursor()
    cur.execute(f"SET search_path TO {schema_name}, public")
    
    # Get first table name
    cur.execute("""
        SELECT tablename 
        FROM pg_tables 
        WHERE schemaname = %s 
        ORDER BY tablename 
        LIMIT 1
    """, (schema_name,))
    
    result = cur.fetchone()
    if not result:
        print("⚠️  No tables found - data loading may have failed")
        dbutils.notebook.exit("No tables created")
    
    table_name = result[0]
    
    print(f"📋 Sample data from {table_name}:")
    print("=" * 80)
    
    # Get sample rows
    cur.execute(f"SELECT * FROM {table_name} LIMIT 5")
    
    # Get column names
    col_names = [desc[0] for desc in cur.description]
    print("   " + " | ".join(col_names))
    print("   " + "-" * 70)
    
    for row in cur.fetchall():
        print("   " + " | ".join(str(val)[:20] for val in row))
    
    print("=" * 80)
    
    cur.close()
    
finally:
    conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Done!
# MAGIC 
# MAGIC Your tables are created and loaded with data.
# MAGIC 
# MAGIC **What happened:**
# MAGIC 1. ✅ Parsed your DDL
# MAGIC 2. ✅ Created tables on Lakebase
# MAGIC 3. ✅ Generated synthetic data
# MAGIC 4. ✅ Loaded data (automatically optimized)
# MAGIC 
# MAGIC **Next steps:**
# MAGIC - Run benchmarks: `notebooks/benchmarks/benchmark_lakebase.py`
# MAGIC - Load your own data: Use `utils/smart_loader.py`
# MAGIC - Compare backends: `notebooks/benchmarks/benchmark_comparison.py`
