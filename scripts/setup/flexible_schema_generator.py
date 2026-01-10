"""
Flexible Schema Generator for Lakebase Benchmarking

Generate N tables with M features each to simulate real-world feature store workloads.
Repository: lakebase-benchmarking

Usage:
    python flexible_schema_generator.py --num-tables 30 --features-per-table 5 --rows-per-table 100000000
    
Examples:
    # Customer's workload: 30-50 tables with ~150 features total
    python flexible_schema_generator.py --num-tables 30 --features-per-table 5 --rows-per-table 100000000
    python flexible_schema_generator.py --num-tables 50 --features-per-table 3 --rows-per-table 100000000
    
    # Quick test: 10 tables with 3 features each
    python flexible_schema_generator.py --num-tables 10 --features-per-table 3 --rows-per-table 1000000
"""

import argparse
import os
from typing import List, Dict

class FlexibleSchemaGenerator:
    """Generate flexible table schemas for fraud detection benchmarking"""
    
    # Feature templates by category (realistic fraud detection features)
    FEATURE_TEMPLATES = {
        'count': [
            'txn_count_{}d',
            'approved_count_{}d',
            'declined_count_{}d',
            'chargeback_count_{}d',
            'refund_count_{}d',
            'dispute_count_{}d',
        ],
        'amount': [
            'avg_amount_{}d',
            'max_amount_{}d',
            'min_amount_{}d',
            'sum_amount_{}d',
            'median_amount_{}d',
        ],
        'rate': [
            'approval_rate_{}d',
            'decline_rate_{}d',
            'chargeback_rate_{}d',
            'refund_rate_{}d',
        ],
        'distinct': [
            'distinct_merchants_{}d',
            'distinct_cards_{}d',
            'distinct_devices_{}d',
            'distinct_ips_{}d',
            'distinct_countries_{}d',
            'distinct_emails_{}d',
        ],
        'velocity': [
            'velocity_{}h',
            'peak_velocity_{}h',
        ],
        'risk': [
            'risk_score_device',
            'risk_score_ip',
            'risk_score_email',
            'risk_score_card',
            'risk_score_merchant',
            'risk_score_geo',
            'risk_score_behavior',
            'risk_score_network',
        ],
        'time': [
            'time_since_first_txn_days',
            'time_since_last_txn_hours',
            'hour_of_day',
            'day_of_week',
            'is_weekend',
            'is_business_hours',
        ],
        'binary': [
            'is_cross_border',
            'is_high_value',
            'is_recurring',
            'is_mobile',
            'is_new_customer',
            'has_prior_fraud',
        ]
    }
    
    TIME_WINDOWS = [1, 7, 14, 30, 60, 90, 180, 365]  # days
    HOUR_WINDOWS = [1, 3, 6, 12, 24]  # hours
    
    def __init__(self, num_tables: int, features_per_table: int, rows_per_table: int):
        self.num_tables = num_tables
        self.features_per_table = features_per_table
        self.rows_per_table = rows_per_table
        
    def generate_feature_names(self, table_idx: int) -> List[str]:
        """Generate realistic feature names for a table"""
        features = []
        category_cycle = ['count', 'amount', 'rate', 'distinct', 'velocity', 'risk', 'time', 'binary']
        
        for i in range(self.features_per_table):
            category = category_cycle[i % len(category_cycle)]
            templates = self.FEATURE_TEMPLATES[category]
            template = templates[(i // len(category_cycle)) % len(templates)]
            
            # Apply time windows for temporal features
            if '{}d' in template:
                time_window = self.TIME_WINDOWS[i % len(self.TIME_WINDOWS)]
                feature_name = template.format(time_window)
            elif '{}h' in template:
                hour_window = self.HOUR_WINDOWS[i % len(self.HOUR_WINDOWS)]
                feature_name = template.format(hour_window)
            else:
                feature_name = template
            
            # Make feature name unique per table
            features.append(f"{feature_name}_t{table_idx:02d}")
        
        return features
    
    def generate_table_name(self, table_idx: int) -> str:
        """Generate table name"""
        return f"feature_table_{table_idx:02d}"
    
    def generate_create_table_sql(self, table_idx: int) -> str:
        """Generate CREATE TABLE SQL for a single table"""
        table_name = self.generate_table_name(table_idx)
        features = self.generate_feature_names(table_idx)
        
        sql = f"CREATE TABLE IF NOT EXISTS features.{table_name} (\n"
        sql += "    primary_key CHAR(64) PRIMARY KEY,\n"
        sql += "    raw_fingerprint TEXT,\n"
        
        for feature in features:
            sql += f"    {feature} NUMERIC,\n"
        
        sql += "    updated_at BIGINT\n"
        sql += ");\n"
        
        return sql
    
    def generate_setup_sql(self, output_file: str = None) -> str:
        """Generate complete setup SQL for all tables"""
        sql = "-- " + "=" * 70 + "\n"
        sql += f"-- Flexible Schema: {self.num_tables} tables × {self.features_per_table} features\n"
        sql += f"-- Total features: {self.num_tables * self.features_per_table}\n"
        sql += f"-- Rows per table: {self.rows_per_table:,}\n"
        sql += "-- " + "=" * 70 + "\n\n"
        
        sql += "-- 1. Create schema (if not exists)\n"
        sql += "CREATE SCHEMA IF NOT EXISTS features;\n\n"
        
        sql += "-- 2. Create user (if not exists)\n"
        sql += "CREATE ROLE IF NOT EXISTS fraud_benchmark_user WITH LOGIN PASSWORD 'fraud_benchmark_user_123!';\n\n"
        
        sql += "-- 3. Create tables\n\n"
        
        for i in range(self.num_tables):
            sql += f"-- Table {i+1}/{self.num_tables}: {self.generate_table_name(i)}\n"
            sql += self.generate_create_table_sql(i)
            sql += "\n"
        
        sql += "-- 4. Grant permissions\n\n"
        sql += "GRANT USAGE ON SCHEMA features TO fraud_benchmark_user;\n\n"
        
        # Grant table permissions
        table_names = [f"features.{self.generate_table_name(i)}" for i in range(self.num_tables)]
        
        # Split into chunks of 10 tables per GRANT statement (to avoid line length issues)
        for chunk_start in range(0, len(table_names), 10):
            chunk_end = min(chunk_start + 10, len(table_names))
            chunk = table_names[chunk_start:chunk_end]
            
            sql += "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON\n"
            sql += "    " + ",\n    ".join(chunk) + "\n"
            sql += "TO fraud_benchmark_user;\n\n"
        
        sql += "-- 5. Grant CREATE permission for stored procedures\n"
        sql += "GRANT CREATE ON SCHEMA features TO fraud_benchmark_user;\n\n"
        
        sql += "-- " + "=" * 70 + "\n"
        sql += "-- Verification queries\n"
        sql += "-- " + "=" * 70 + "\n\n"
        
        sql += "-- Check tables exist\n"
        sql += "SELECT table_name FROM information_schema.tables \n"
        sql += "WHERE table_schema = 'features' AND table_name LIKE 'feature_table_%'\n"
        sql += "ORDER BY table_name;\n\n"
        
        sql += f"-- Expected: {self.num_tables} tables\n"
        
        if output_file:
            with open(output_file, 'w') as f:
                f.write(sql)
            print(f"✅ Generated setup SQL: {output_file}")
        
        return sql
    
    def generate_stored_procedure(self, output_file: str = None) -> str:
        """Generate stored procedure for batch lookup across all tables"""
        
        sql = "-- " + "=" * 70 + "\n"
        sql += f"-- Stored Procedure: Binpacked lookup for {self.num_tables} tables\n"
        sql += "-- " + "=" * 70 + "\n\n"
        
        sql += "CREATE OR REPLACE FUNCTION features.fraud_batch_lookup_flexible(\n"
        
        # Generate parameters (one array per table)
        params = [f"    keys_{i:02d} TEXT[]" for i in range(self.num_tables)]
        sql += ",\n".join(params)
        sql += "\n)\n"
        
        sql += "RETURNS TABLE(\n"
        sql += "    table_name TEXT,\n"
        sql += "    data JSONB\n"
        sql += ")\n"
        sql += "LANGUAGE plpgsql\n"
        sql += "STABLE PARALLEL SAFE\n"
        sql += "AS $$\n"
        sql += "BEGIN\n"
        sql += "    -- CRITICAL: Cast TEXT[] to CHAR(64)[] to enable index usage\n"
        sql += "    -- This optimization makes queries 203x faster\n\n"
        
        # Generate RETURN QUERY for each table
        for i in range(self.num_tables):
            table_name = self.generate_table_name(i)
            sql += "    RETURN QUERY\n"
            sql += "    SELECT\n"
            sql += f"        '{table_name}'::TEXT,\n"
            sql += "        to_jsonb(t.*) AS data\n"
            sql += f"    FROM features.{table_name} t\n"
            sql += f"    WHERE primary_key = ANY(keys_{i:02d}::CHAR(64)[]);\n\n"
        
        sql += "    RETURN;\n"
        sql += "END;\n"
        sql += "$$;\n"
        
        if output_file:
            with open(output_file, 'w') as f:
                f.write(sql)
            print(f"✅ Generated stored procedure: {output_file}")
        
        return sql
    
    def generate_python_constants(self, output_file: str = None) -> str:
        """Generate Python constants for use in benchmarks and data loading"""
        
        py = "# " + "=" * 70 + "\n"
        py += f"# Auto-generated: {self.num_tables} tables × {self.features_per_table} features\n"
        py += "# " + "=" * 70 + "\n\n"
        
        py += f"NUM_TABLES = {self.num_tables}\n"
        py += f"FEATURES_PER_TABLE = {self.features_per_table}\n"
        py += f"ROWS_PER_TABLE = {self.rows_per_table}\n"
        py += f"TOTAL_FEATURES = {self.num_tables * self.features_per_table}\n\n"
        
        py += "TABLE_NAMES = [\n"
        for i in range(self.num_tables):
            py += f"    '{self.generate_table_name(i)}',\n"
        py += "]\n\n"
        
        py += "TABLE_CONFIGS = {\n"
        for i in range(self.num_tables):
            table_name = self.generate_table_name(i)
            features = self.generate_feature_names(i)
            py += f"    '{table_name}': {{\n"
            py += f"        'rows': {self.rows_per_table},\n"
            py += f"        'features': {features},\n"
            py += f"        'key_offset': {i * self.rows_per_table}\n"
            py += "    },\n"
        py += "}\n"
        
        if output_file:
            with open(output_file, 'w') as f:
                f.write(py)
            print(f"✅ Generated Python constants: {output_file}")
        
        return py
    
    def generate_python_config(self, output_file: str = None) -> str:
        """
        Generate Python configuration file for use with the benchmarking framework.
        
        This is similar to generate_python_constants but formatted for framework integration.
        """
        py = '"""\\nAuto-generated schema configuration\\n'
        py += f"Generated for: {self.num_tables} tables × {self.features_per_table} features\\n"
        py += '"""\\n\\n'
        
        py += f"# Schema configuration\\n"
        py += f"NUM_TABLES = {self.num_tables}\\n"
        py += f"FEATURES_PER_TABLE = {self.features_per_table}\\n"
        py += f"ROWS_PER_TABLE = {self.rows_per_table}\\n"
        py += f"TOTAL_FEATURES = {self.num_tables * self.features_per_table}\\n\\n"
        
        py += "# Table definitions\\n"
        py += "TABLES = {\\n"
        for i in range(self.num_tables):
            table_name = self.generate_table_name(i)
            features = self.generate_feature_names(i)
            py += f"    '{table_name}': {{\\n"
            py += f"        'rows': {self.rows_per_table},\\n"
            py += f"        'features': {features},\\n"
            py += f"        'num_features': {len(features)},\\n"
            py += "    },\\n"
        py += "}\\n\\n"
        
        py += "# Table names (for convenience)\\n"
        py += "TABLE_NAMES = list(TABLES.keys())\\n\\n"
        
        py += "# Stored procedure name\\n"
        py += "STORED_PROC_NAME = 'fraud_batch_lookup_flexible'\\n"
        
        if output_file:
            with open(output_file, 'w') as f:
                f.write(py)
            print(f"✅ Generated Python config: {output_file}")
        
        return py
    
    def generate_all(self, output_dir: str = "generated"):
        """Generate all files (SQL, Python constants, docs)"""
        os.makedirs(output_dir, exist_ok=True)
        
        print(f"\n{'='*70}")
        print(f"Generating Flexible Schema")
        print(f"{'='*70}")
        print(f"Tables: {self.num_tables}")
        print(f"Features per table: {self.features_per_table}")
        print(f"Total features: {self.num_tables * self.features_per_table}")
        print(f"Rows per table: {self.rows_per_table:,}")
        print(f"Output directory: {output_dir}")
        print(f"{'='*70}\n")
        
        # Generate setup SQL
        setup_sql_file = os.path.join(output_dir, "setup_flexible_schema.sql")
        self.generate_setup_sql(setup_sql_file)
        
        # Generate stored procedure
        stored_proc_file = os.path.join(output_dir, "setup_flexible_stored_proc.sql")
        self.generate_stored_procedure(stored_proc_file)
        
        # Generate Python constants
        py_constants_file = os.path.join(output_dir, "flexible_schema_config.py")
        self.generate_python_constants(py_constants_file)
        
        # Generate README
        readme_file = os.path.join(output_dir, "README.md")
        self._generate_readme(readme_file)
        
        print(f"\n✅ All files generated in: {output_dir}/")
        print(f"\n📋 Next steps:")
        print(f"   1. Create tables:  psql -f {setup_sql_file}")
        print(f"   2. Load data:      python load_flexible_data.py")
        print(f"   3. Create stored proc: psql -f {stored_proc_file}")
        print(f"   4. Run benchmark:  python benchmark_flexible.py")
        
    def _generate_readme(self, output_file: str):
        """Generate README for the generated schema"""
        readme = f"""# Flexible Schema Configuration

## Configuration

- **Tables:** {self.num_tables}
- **Features per table:** {self.features_per_table}
- **Total features:** {self.num_tables * self.features_per_table}
- **Rows per table:** {self.rows_per_table:,}
- **Total rows:** {self.num_tables * self.rows_per_table:,}

## Table Structure

Each table follows this pattern:
```sql
CREATE TABLE features.feature_table_XX (
    primary_key CHAR(64) PRIMARY KEY,  -- SHA256 hash
    raw_fingerprint TEXT,
    feature_1_tXX NUMERIC,
    feature_2_tXX NUMERIC,
    ...
    feature_{self.features_per_table}_tXX NUMERIC,
    updated_at BIGINT
);
```

## Usage

### 1. Create Tables
```bash
psql -h <host> -d benchmark -U admin -f setup_flexible_schema.sql
```

### 2. Load Data
```bash
# Use the generated config
python load_flexible_data.py
```

### 3. Create Stored Procedure
```bash
psql -h <host> -d benchmark -U admin -f setup_flexible_stored_proc.sql
```

### 4. Run Benchmark
```bash
python benchmark_flexible.py
```

## Stored Procedure

The stored procedure `fraud_batch_lookup_flexible()` accepts {self.num_tables} TEXT[] parameters:

```sql
SELECT * FROM features.fraud_batch_lookup_flexible(
    keys_00, keys_01, ..., keys_{self.num_tables-1:02d}
);
```

This enables single-call binpacked lookups across all {self.num_tables} tables.

## Performance Expectations

**Hypothesis:**
- Customer's DynamoDB: P99 = 79ms for 30-50 tables, ~150 features
- Lakebase Target: P99 < 80ms for {self.num_tables} tables, {self.num_tables * self.features_per_table} features

**Key optimizations:**
- CHAR(64)[] casting for index usage
- Binpacking via stored procedure
- Connection pooling
- Warm cache
"""
        with open(output_file, 'w') as f:
            f.write(readme)
        print(f"✅ Generated README: {output_file}")

def main():
    parser = argparse.ArgumentParser(
        description="Generate flexible schema for N tables with M features each"
    )
    parser.add_argument(
        '--num-tables', '-n',
        type=int,
        default=30,
        help='Number of tables to generate (default: 30)'
    )
    parser.add_argument(
        '--features-per-table', '-f',
        type=int,
        default=5,
        help='Number of features per table (default: 5)'
    )
    parser.add_argument(
        '--rows-per-table', '-r',
        type=int,
        default=100_000_000,
        help='Number of rows per table (default: 100M)'
    )
    parser.add_argument(
        '--output-dir', '-o',
        type=str,
        default='generated',
        help='Output directory for generated files (default: generated/)'
    )
    
    args = parser.parse_args()
    
    generator = FlexibleSchemaGenerator(
        num_tables=args.num_tables,
        features_per_table=args.features_per_table,
        rows_per_table=args.rows_per_table
    )
    
    generator.generate_all(output_dir=args.output_dir)

if __name__ == '__main__':
    main()

