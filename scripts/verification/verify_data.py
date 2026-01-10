#!/usr/bin/env python3
"""
Verify Data Load Script
Checks that all tables have the correct row counts and data after load.
"""

import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Connection details
PGHOST = os.getenv('PGHOST', 'ep-autumn-fire-d318blbk.database.eu-west-1.cloud.databricks.com')
PGDATABASE = os.getenv('PGDATABASE', 'benchmark')
PGUSER = os.getenv('PGUSER', 'fraud_benchmark_user')
PGPASSWORD = os.getenv('PGPASSWORD', 'fraud_benchmark_user_123!')

# Expected row counts
EXPECTED_COUNTS = {
    'fraud_reports_365d': 1_000_000,
    'good_rate_90d_lag_730d': 10_000_000,
    'request_capture_times': 100_000_000
}

def verify_data():
    """Verify data load completeness and correctness"""
    
    print("=" * 70)
    print("🔍 Verifying Data Load")
    print("=" * 70)
    
    try:
        # Connect to database
        conn = psycopg2.connect(
            host=PGHOST,
            database=PGDATABASE,
            user=PGUSER,
            password=PGPASSWORD
        )
        cursor = conn.cursor()
        
        # Check 1: Row counts
        print("\n1️⃣  Checking row counts...")
        print("-" * 70)
        
        all_passed = True
        for table_name, expected_count in EXPECTED_COUNTS.items():
            cursor.execute(f"SELECT COUNT(*) FROM features.{table_name}")
            actual_count = cursor.fetchone()[0]
            
            status = "✅ PASS" if actual_count == expected_count else "❌ FAIL"
            if actual_count != expected_count:
                all_passed = False
            
            print(f"{table_name:30s} | Expected: {expected_count:>12,} | Actual: {actual_count:>12,} | {status}")
        
        # Check 2: Table sizes
        print("\n2️⃣  Checking table sizes...")
        print("-" * 70)
        
        cursor.execute("""
            SELECT 
                tablename,
                pg_size_pretty(pg_total_relation_size('features.'||tablename)) AS size
            FROM pg_tables 
            WHERE schemaname = 'features'
              AND tablename IN ('fraud_reports_365d', 'good_rate_90d_lag_730d', 'request_capture_times')
            ORDER BY pg_total_relation_size('features.'||tablename) DESC
        """)
        
        for row in cursor.fetchall():
            table_name, size = row
            print(f"{table_name:30s} | Size: {size}")
        
        # Check 3: Sample data
        print("\n3️⃣  Checking sample data exists...")
        print("-" * 70)
        
        for table_name in EXPECTED_COUNTS.keys():
            cursor.execute(f"SELECT primary_key FROM features.{table_name} LIMIT 1")
            result = cursor.fetchone()
            
            if result:
                print(f"{table_name:30s} | ✅ Data present (sample key: {result[0][:16]}...)")
            else:
                print(f"{table_name:30s} | ❌ No data found!")
                all_passed = False
        
        # Check 4: Table persistence
        print("\n4️⃣  Checking table persistence...")
        print("-" * 70)
        
        cursor.execute("""
            SELECT 
                c.relname as table_name,
                CASE c.relpersistence
                    WHEN 'p' THEN 'LOGGED'
                    WHEN 'u' THEN 'UNLOGGED'
                    WHEN 't' THEN 'TEMPORARY'
                END as persistence
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'features' 
              AND c.relkind = 'r'
              AND c.relname IN ('fraud_reports_365d', 'good_rate_90d_lag_730d', 'request_capture_times')
            ORDER BY c.relname
        """)
        
        for row in cursor.fetchall():
            table_name, persistence = row
            status = "✅" if persistence == "LOGGED" else "⚠️"
            print(f"{table_name:30s} | {persistence} {status}")
        
        # Summary
        print("\n" + "=" * 70)
        if all_passed:
            print("✅ VERIFICATION PASSED - All tables loaded correctly!")
            print("=" * 70)
            print("\n🚀 Ready to run benchmarks!")
            print("\nNext steps:")
            print("  1. python setup_stored_proc.py")
            print("  2. python test_lakebase_detailed.py")
            return 0
        else:
            print("❌ VERIFICATION FAILED - Some tables have issues")
            print("=" * 70)
            print("\n⚠️  Please rerun data load job")
            return 1
        
    except Exception as e:
        print(f"\n❌ Error during verification: {e}")
        return 1
    
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    exit(verify_data())

