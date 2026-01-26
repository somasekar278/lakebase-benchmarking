# Databricks notebook source
# MAGIC %md
# MAGIC # Generate CSVs for All 30 Fraud Feature Tables
# MAGIC 
# MAGIC Creates tables + generates CSV files (no loading to Lakebase).

# COMMAND ----------

import os
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, col, concat_ws, lit, sha2, from_unixtime, date_format

# Add UC Volume to path for importing validators
sys.path.insert(0, '/Volumes/benchmark/data_load/benchmark_data_dev/python_modules')

# Import validator
from csv_timestamp_validator import validate_csv_timestamps, validate_csv_columns_match_ddl

# COMMAND ----------

# Get parameters
try:
    uc_volume_path = dbutils.widgets.get("uc_volume_path")
except:
    uc_volume_path = "/Volumes/benchmark/data_load/benchmark_data_dev"

try:
    ddl_file_path = dbutils.widgets.get("ddl_file_path")
except:
    ddl_file_path = "/Volumes/benchmark/data_load/benchmark_data_dev/fraud_feature_tables.sql"

try:
    rows_per_table_file = dbutils.widgets.get("rows_per_table_file")
except:
    rows_per_table_file = "/Volumes/benchmark/data_load/benchmark_data_dev/fraud_tables_row_counts.txt"

# Read row counts from file
print(f"📖 Reading row counts from: {rows_per_table_file}")
table_row_counts = {}
with open(rows_per_table_file, 'r') as f:
    for line in f:
        line = line.strip()
        if line and ',' in line:
            table, count = line.split(',')
            # Store without features. prefix for matching
            table_row_counts[table.replace('features.', '')] = int(count)

print(f"✓ Loaded row counts for {len(table_row_counts)} tables")

print(f"\n✓ Configuration loaded")
print(f"  UC Volume: {uc_volume_path}")
print(f"  DDL File: {ddl_file_path}")
print(f"  Row counts file: {rows_per_table_file}")

# COMMAND ----------

# Constants
TARGET_ROWS_PER_PARTITION = 25_000_000
MAX_PARTITIONS = 128

# COMMAND ----------

# Parse DDL and extract table names + schemas
print(f"📖 Reading DDL from: {ddl_file_path}")

with open(ddl_file_path, 'r') as f:
    ddl_content = f.read()

tables = []
table_schemas = {}  # table_name -> {columns: {col_name: col_type}}

# Split by CREATE TABLE to parse each table
statements = ddl_content.split('CREATE TABLE')
for stmt in statements[1:]:  # Skip first empty part
    if not stmt.strip():
        continue
    
    # Extract table name
    table_name_part = stmt.split('(')[0].strip()
    table_name = table_name_part.strip()
    
    # Extract columns (between first ( and last ))
    if '(' in stmt and ')' in stmt:
        col_section = stmt[stmt.index('(')+1:stmt.rindex(')')]
        columns = {}
        
        for line in col_section.split(','):
            line = line.strip()
            if line and not line.upper().startswith('CONSTRAINT'):
                parts = line.split(None, 1)  # Split on first whitespace
                if len(parts) >= 2:
                    col_name = parts[0].strip()
                    col_type = parts[1].strip()
                    columns[col_name] = col_type
        
        table_schemas[table_name.replace('features.', '')] = {'columns': columns}
    
    tables.append(table_name)

print(f"✅ Found {len(tables)} tables in DDL")
print(f"✅ Parsed schemas for {len(table_schemas)} tables")

# COMMAND ----------

# Note: Table creation happens in pipelined_load, not here
# This notebook only generates CSVs

# COMMAND ----------

def generate_synthetic_data(table_name, num_rows, table_schema):
    """Generate synthetic data matching the exact DDL schema."""
    # Calculate partitions
    num_partitions = max(1, min(MAX_PARTITIONS, 
                                (num_rows + TARGET_ROWS_PER_PARTITION - 1) // TARGET_ROWS_PER_PARTITION))
    
    # Generate base dataframe
    df = spark.range(0, num_rows, numPartitions=num_partitions)
    
    # Get columns from schema
    columns = table_schema.get('columns', {})
    
    if not columns:
        raise ValueError(f"No schema found for table {table_name}")
    
    # Generate columns based on DDL schema
    for col_name, col_type in columns.items():
        col_type_upper = col_type.upper()
        
        if 'PRIMARY KEY' in col_type_upper or col_name == 'hash_key':
            # Primary key: use SHA256 hash
            df = df.withColumn(col_name, sha2(col("id").cast("string"), 256))
        
        elif 'TIMESTAMP' in col_type_upper:
            # TIMESTAMP: MUST be string formatted for CSV ingestion
            df = df.withColumn(col_name, 
                date_format(
                    from_unixtime(lit(1700000000) + (rand() * 31536000)),
                    'yyyy-MM-dd HH:mm:ss'
                )
            )
        
        elif 'BIGINT' in col_type_upper or 'INT' in col_type_upper:
            # Integer columns
            if 'count' in col_name.lower():
                df = df.withColumn(col_name, (rand() * 1000).cast("bigint"))
            else:
                df = df.withColumn(col_name, (rand() * 86400 * 365).cast("bigint"))
        
        elif 'DOUBLE' in col_type_upper or 'FLOAT' in col_type_upper or 'NUMERIC' in col_type_upper:
            # Float/double columns (rates, averages, sums)
            if 'rate' in col_name.lower():
                df = df.withColumn(col_name, rand())  # 0.0 to 1.0
            elif 'average' in col_name.lower():
                df = df.withColumn(col_name, rand() * 10000)
            elif 'sum' in col_name.lower():
                df = df.withColumn(col_name, rand() * 100000)
            else:
                df = df.withColumn(col_name, rand() * 1000)
        
        elif 'TEXT' in col_type_upper or 'VARCHAR' in col_type_upper or 'CHAR' in col_type_upper:
            # Text columns (non-primary key)
            df = df.withColumn(col_name, concat_ws("_", lit("value"), col("id").cast("string")))
        
        else:
            # Default: text
            df = df.withColumn(col_name, concat_ws("_", lit("value"), col("id").cast("string")))
    
    # Drop the temporary id column
    df = df.drop("id")
    
    return df

# COMMAND ----------

# Generate CSVs for all tables
print(f"\n" + "=" * 80)
print(f"GENERATING CSVs FOR {len(tables)} TABLES")
print("=" * 80)

overall_start = time.time()

for i, table_name in enumerate(tables, 1):
    print(f"\n[{i}/{len(tables)}] {table_name}")
    
    # CSV directory includes schema prefix (e.g. features.table_name_csvs)
    csv_path = f"{uc_volume_path}/{table_name}_csvs"
    
    # For schema lookups, strip the features. prefix
    table_name_clean = table_name.replace('features.', '')
    
    # Check if CSVs already exist
    try:
        files = dbutils.fs.ls(csv_path)
        csv_files = [f for f in files if f.name.endswith('.csv') or f.name.startswith('part-')]
        
        if csv_files:
            print(f"   ⏭️  SKIPPING - {len(csv_files)} CSV files already exist")
            continue
    except:
        pass  # Directory doesn't exist, proceed with generation
    
    start_time = time.time()
    
    # Get row count for this specific table (table_name_clean already defined above)
    num_rows = table_row_counts.get(table_name_clean, 400000000)
    
    print(f"📊 Generating data for: {table_name} ({num_rows:,} rows)")
    num_partitions = max(1, num_rows // TARGET_ROWS_PER_PARTITION)
    print(f"   Using {num_partitions} partitions (~{num_rows//num_partitions:,} rows each)")
    
    # Get schema for this table (table_name_clean already defined above)
    table_def = table_schemas.get(table_name_clean, {'columns': {}})
    
    # Generate data
    df = generate_synthetic_data(table_name, num_rows, table_def)
    
    # Write to UC Volume
    print(f"   💾 Writing CSVs to: {csv_path}")
    df.write.mode("overwrite") \
        .option("header", "false") \
        .option("maxRecordsPerFile", TARGET_ROWS_PER_PARTITION) \
        .csv(csv_path)
    
    # Count generated files
    files = dbutils.fs.ls(csv_path)
    csv_files = [f for f in files if f.name.endswith('.csv') or f.name.startswith('part-')]
    csv_count = len(csv_files)
    
    elapsed = time.time() - start_time
    print(f"   ✅ Generated {csv_count} CSV files in {elapsed/60:.1f} min")
    
    # Validate: Check timestamp formatting (CRITICAL - must be strings, not integers)
    print(f"   🔍 Validating CSV format...")
    
    # Get table schema (table_name_clean already defined above)
    table_def = table_schemas.get(table_name_clean, {'columns': {}})
    
    # Validate timestamps are strings, not integers
    validation_errors = validate_csv_timestamps(spark, table_name, table_def, csv_path)
    
    # Also validate column count matches DDL
    validation_errors.extend(validate_csv_columns_match_ddl(spark, table_name, table_def, csv_path))
    
    if validation_errors:
        print(f"   ❌ VALIDATION FAILED:")
        for err in validation_errors:
            print(f"      {err}")
        raise RuntimeError(f"CSV validation failed for {table_name}. Fix issues and restart.")
    else:
        print(f"   ✅ Validation passed: Format correct, timestamps are strings")

overall_elapsed = time.time() - overall_start

print(f"\n" + "=" * 80)
print(f"✅ CSV GENERATION COMPLETE!")
total_rows = sum(table_row_counts.get(t.replace('features.', ''), 0) for t in tables)
print("=" * 80)
print(f"Total time: {overall_elapsed/3600:.1f} hours")
print(f"Tables: {len(tables)}")
print(f"Total rows: {total_rows:,} ({total_rows/1_000_000_000:.2f}B)")
print(f"CSV location: {uc_volume_path}/{{table_name}}_csvs/")
print()
print("Next step:")
print("  → Run pipelined_load.py to load CSVs → Lakebase")
print("=" * 80)
