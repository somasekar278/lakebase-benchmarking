"""
CSV Timestamp Validator - Schema-aware validation

Validates that TIMESTAMPTZ columns in CSVs are formatted correctly,
by checking the actual DDL schema instead of hardcoded positions.
"""

from typing import List, Dict, Any
from pyspark.sql import SparkSession


def validate_csv_timestamps(
    spark: SparkSession,
    table_name: str,
    table_def: Dict[str, Any],
    csv_path: str
) -> List[str]:
    """
    Validate that TIMESTAMPTZ columns in CSV are formatted as strings, not integers.
    
    This validation is SCHEMA-AWARE:
    - Identifies TIMESTAMPTZ columns from table definition
    - Maps them to CSV column positions (_c0, _c1, _c2, ...)
    - Checks only those columns for proper timestamp format
    
    Args:
        spark: SparkSession
        table_name: Table name
        table_def: Table definition with 'columns' dict
        csv_path: Path to CSV directory
        
    Returns:
        List of error messages (empty if validation passes)
        
    Example:
        table_def = {
            'columns': {
                'hash_key': 'TEXT PRIMARY KEY',
                'entity__time_of_first_request': 'TIMESTAMPTZ',
                'entity__time_of_last_request': 'TIMESTAMPTZ',
                'updated_at': 'TIMESTAMPTZ'
            }
        }
        
        # Will check _c1, _c2, _c3 (positions 1, 2, 3) for timestamp format
    """
    errors = []
    
    try:
        columns = table_def.get('columns', {})
        
        # Step 1: Find TIMESTAMPTZ columns from DDL
        timestamp_columns = []
        for col_name, col_type in columns.items():
            if 'TIMESTAMP' in col_type.upper():
                timestamp_columns.append(col_name)
        
        if not timestamp_columns:
            # No timestamp columns in this table - skip validation
            return errors
        
        # Step 2: Map column names to CSV positions (_c0, _c1, _c2, ...)
        column_names = list(columns.keys())
        timestamp_positions = []
        for ts_col in timestamp_columns:
            if ts_col in column_names:
                idx = column_names.index(ts_col)
                csv_col = f"_c{idx}"
                timestamp_positions.append((ts_col, csv_col, idx))
        
        # Step 3: Read sample CSV (1 row is enough)
        df_sample = spark.read.csv(
            csv_path,
            header=False,
            inferSchema=True  # Let Spark infer types - integers will be IntegerType
        ).limit(1)
        
        if df_sample.count() == 0:
            errors.append("CSV is empty")
            return errors
        
        # Step 4: Check each timestamp column
        for col_name, csv_col, idx in timestamp_positions:
            # Get the column type Spark inferred
            csv_cols = df_sample.columns
            
            if idx >= len(csv_cols):
                errors.append(
                    f"❌ COLUMN MISMATCH: Expected {col_name} at position {idx}, "
                    f"but CSV only has {len(csv_cols)} columns"
                )
                continue
            
            actual_csv_col = csv_cols[idx]
            dtype = dict(df_sample.dtypes)[actual_csv_col]
            
            # Get sample value
            sample_val = df_sample.select(actual_csv_col).first()[0]
            
            # FAIL if it's an integer type
            if 'int' in dtype.lower():
                errors.append(
                    f"❌ TIMESTAMP BUG: Column '{col_name}' (position {idx}) "
                    f"= {sample_val} (type: {dtype}, expected: string timestamp)"
                )
            # FAIL if value is numeric
            elif isinstance(sample_val, (int, float)):
                errors.append(
                    f"❌ TIMESTAMP BUG: Column '{col_name}' (position {idx}) "
                    f"= {sample_val} (integer/float, expected: string like '2024-01-20 12:34:56')"
                )
            # SUCCESS if it's a string with proper format
            elif isinstance(sample_val, str):
                # Quick check: should contain space and colons (date and time)
                if ' ' not in sample_val or ':' not in sample_val:
                    errors.append(
                        f"⚠️  TIMESTAMP FORMAT: Column '{col_name}' (position {idx}) "
                        f"= '{sample_val}' (string, but unexpected format - expected 'YYYY-MM-DD HH:MM:SS')"
                    )
                # else: Valid timestamp string - no error
            
    except Exception as e:
        # Only fail on critical errors
        if 'Path does not exist' in str(e) or 'File not found' in str(e):
            errors.append(f"CSV not found: {str(e)[:100]}")
        else:
            # Log but don't fail on validation infrastructure errors
            errors.append(f"Validation check failed: {str(e)[:100]}")
    
    return errors


def validate_csv_columns_match_ddl(
    spark: SparkSession,
    table_name: str,
    table_def: Dict[str, Any],
    csv_path: str
) -> List[str]:
    """
    Validate that CSV has the correct number of columns matching DDL.
    
    Args:
        spark: SparkSession
        table_name: Table name
        table_def: Table definition with 'columns' dict
        csv_path: Path to CSV directory
        
    Returns:
        List of error messages (empty if validation passes)
    """
    errors = []
    
    try:
        expected_columns = list(table_def.get('columns', {}).keys())
        expected_count = len(expected_columns)
        
        # Read sample to check column count
        df_sample = spark.read.csv(csv_path, header=False).limit(1)
        actual_count = len(df_sample.columns)
        
        if actual_count != expected_count:
            errors.append(
                f"❌ COLUMN COUNT MISMATCH: DDL has {expected_count} columns, "
                f"CSV has {actual_count} columns"
            )
            errors.append(
                f"   Expected columns: {', '.join(expected_columns[:5])}..."
                if len(expected_columns) > 5
                else f"   Expected columns: {', '.join(expected_columns)}"
            )
    
    except Exception as e:
        if 'Path does not exist' in str(e) or 'File not found' in str(e):
            errors.append(f"CSV not found: {str(e)[:100]}")
    
    return errors
