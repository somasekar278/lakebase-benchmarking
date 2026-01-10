#!/usr/bin/env python3
"""
Pre-Flight Checklist: Verify all setup before benchmarking
Checks schema, tables, permissions, data, etc.
"""

import os
import sys
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Connection details
PGHOST = os.getenv('PGHOST', 'ep-autumn-fire-d318blbk.database.eu-west-1.cloud.databricks.com')
PGDATABASE = os.getenv('PGDATABASE', 'benchmark')
PGUSER = os.getenv('PGUSER', 'fraud_benchmark_user')
PGPASSWORD = os.getenv('PGPASSWORD', 'fraud_benchmark_user_123!')

def check(condition, message_pass, message_fail):
    """Helper to print check results"""
    if condition:
        print(f"✅ {message_pass}")
        return True
    else:
        print(f"❌ {message_fail}")
        return False

def verify_setup():
    """Run all pre-flight checks"""
    
    print("=" * 70)
    print("PRE-FLIGHT CHECKLIST FOR LAKEBASE BENCHMARK")
    print("=" * 70)
    
    all_checks_passed = True
    
    try:
        # Connect to database
        conn = psycopg2.connect(
            host=PGHOST,
            database=PGDATABASE,
            user=PGUSER,
            password=PGPASSWORD
        )
        cursor = conn.cursor()
        
        # Check 1: Schema exists
        print("\n1️⃣  Checking schema...")
        cursor.execute("SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = 'features'")
        schema_exists = cursor.fetchone()[0] == 1
        all_checks_passed &= check(schema_exists, 
            "features schema exists", 
            "features schema missing - run setup_lakebase.sql")
        
        # Check 2: User exists
        print("\n2️⃣  Checking user...")
        cursor.execute("SELECT COUNT(*) FROM pg_user WHERE usename = 'fraud_benchmark_user'")
        user_exists = cursor.fetchone()[0] == 1
        all_checks_passed &= check(user_exists,
            "fraud_benchmark_user exists",
            "fraud_benchmark_user missing - run setup_lakebase.sql")
        
        # Check 3: Tables exist
        print("\n3️⃣  Checking tables...")
        expected_tables = ['fraud_reports_365d', 'good_rate_90d_lag_730d', 'request_capture_times']
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'features' 
              AND table_name IN ('fraud_reports_365d', 'good_rate_90d_lag_730d', 'request_capture_times')
            ORDER BY table_name
        """)
        actual_tables = [row[0] for row in cursor.fetchall()]
        
        for table in expected_tables:
            table_exists = table in actual_tables
            all_checks_passed &= check(table_exists,
                f"{table} exists",
                f"{table} missing - run setup_lakebase.sql")
        
        # Check 4: Table permissions
        print("\n4️⃣  Checking table permissions...")
        required_perms = {'SELECT', 'INSERT', 'DELETE', 'TRUNCATE'}
        
        for table in expected_tables:
            cursor.execute("""
                SELECT privilege_type
                FROM information_schema.table_privileges
                WHERE grantee = 'fraud_benchmark_user' 
                  AND table_schema = 'features'
                  AND table_name = %s
            """, (table,))
            actual_perms = {row[0] for row in cursor.fetchall()}
            
            has_all_perms = required_perms.issubset(actual_perms)
            all_checks_passed &= check(has_all_perms,
                f"{table}: {', '.join(sorted(actual_perms))}",
                f"{table}: missing permissions - run setup_lakebase.sql")
        
        # Check 5: Schema CREATE permission
        print("\n5️⃣  Checking schema CREATE permission...")
        cursor.execute("""
            SELECT has_schema_privilege('fraud_benchmark_user', 'features', 'CREATE')
        """)
        has_create = cursor.fetchone()[0]
        all_checks_passed &= check(has_create,
            "Has CREATE permission on features schema",
            "Missing CREATE permission - run: GRANT CREATE ON SCHEMA features TO fraud_benchmark_user;")
        
        # Check 6: Data loaded
        print("\n6️⃣  Checking data loaded...")
        expected_counts = {
            'fraud_reports_365d': 1_000_000,
            'good_rate_90d_lag_730d': 10_000_000,
            'request_capture_times': 100_000_000
        }
        
        for table, expected_count in expected_counts.items():
            cursor.execute(f"SELECT COUNT(*) FROM features.{table}")
            actual_count = cursor.fetchone()[0]
            
            if actual_count == expected_count:
                print(f"✅ {table}: {actual_count:,} rows (correct)")
            elif actual_count > 0:
                print(f"⚠️  {table}: {actual_count:,} rows (expected {expected_count:,})")
                all_checks_passed = False
            else:
                print(f"❌ {table}: empty - run data load job")
                all_checks_passed = False
        
        # Check 7: Tables are LOGGED
        print("\n7️⃣  Checking table persistence...")
        cursor.execute("""
            SELECT 
                c.relname,
                c.relpersistence
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'features' 
              AND c.relkind = 'r'
              AND c.relname IN ('fraud_reports_365d', 'good_rate_90d_lag_730d', 'request_capture_times')
            ORDER BY c.relname
        """)
        
        for table_name, persistence in cursor.fetchall():
            is_logged = persistence == 'p'
            persistence_name = 'LOGGED' if is_logged else 'UNLOGGED'
            all_checks_passed &= check(is_logged,
                f"{table_name}: {persistence_name}",
                f"{table_name}: UNLOGGED - data may be lost on restart!")
        
        # Summary
        print("\n" + "=" * 70)
        if all_checks_passed:
            print("✅ ALL CHECKS PASSED - Ready to benchmark!")
            print("=" * 70)
            print("\n🚀 Next steps:")
            print("  1. python setup_stored_proc.py")
            print("  2. python test_lakebase_detailed.py")
            print()
            return 0
        else:
            print("❌ SOME CHECKS FAILED - Fix issues before proceeding")
            print("=" * 70)
            print("\n⚠️  Review failures above and:")
            print("  - Run setup_lakebase.sql for missing schema/tables/permissions")
            print("  - Run data load job if tables are empty")
            print("  - Set tables to LOGGED if they are UNLOGGED")
            print()
            return 1
        
    except Exception as e:
        print(f"\n❌ Error during verification: {e}")
        return 1
    
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    sys.exit(verify_setup())

