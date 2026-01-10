"""
Automated setup script for Lakebase Data API.

Configures all required infrastructure for the fraud_benchmark_user to access
the Data API, including:
- authenticator role
- pgrst schema and configuration
- User role creation and permissions

Usage:
    python setup_data_api.py --user your.email@company.com
"""

import os
import sys
import argparse
import psycopg2

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

# Import configuration
from config import LAKEBASE_CONFIG, DATA_API_CONFIG

def get_single_connection():
    """Create database connection using config"""
    return psycopg2.connect(
        host=LAKEBASE_CONFIG['host'],
        port=LAKEBASE_CONFIG['port'],
        database=LAKEBASE_CONFIG['database'],
        user=LAKEBASE_CONFIG['user'],
        password=LAKEBASE_CONFIG['password'],
        sslmode=LAKEBASE_CONFIG.get('sslmode', 'require')
    )

def setup_authenticator_role(cursor):
    """Create the authenticator role required by Data API"""
    print("\n1️⃣  Creating authenticator role...")
    
    # Check if role exists
    cursor.execute("""
        SELECT COUNT(*) FROM pg_roles WHERE rolname = 'authenticator'
    """)
    exists = cursor.fetchone()[0] > 0
    
    if exists:
        print("   ✅ authenticator role already exists")
        return
    
    # Create authenticator role
    cursor.execute("""
        CREATE ROLE authenticator 
        LOGIN 
        NOINHERIT 
        NOCREATEDB 
        NOCREATEROLE 
        NOSUPERUSER
    """)
    print("   ✅ authenticator role created")

def setup_pgrst_schema(cursor):
    """Set up pgrst schema and configuration for Data API"""
    print("\n2️⃣  Setting up pgrst schema and configuration...")
    
    # Create pgrst schema
    cursor.execute("CREATE SCHEMA IF NOT EXISTS pgrst")
    print("   ✅ pgrst schema created")
    
    # Create pre_config function with features schema exposed
    cursor.execute("""
        CREATE OR REPLACE FUNCTION pgrst.pre_config()
        RETURNS VOID AS $$
        SELECT
          set_config('pgrst.db_schemas', 'public,features', true),
          set_config('pgrst.db_aggregates_enabled', 'true', true)
        $$ LANGUAGE SQL
    """)
    print("   ✅ pgrst.pre_config() function created")
    print("   📋 Exposed schemas: public, features")
    
    # Grant permissions to authenticator
    cursor.execute("GRANT USAGE ON SCHEMA pgrst TO authenticator")
    cursor.execute("GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA pgrst TO authenticator")
    print("   ✅ Granted permissions to authenticator")

def setup_user_role(cursor, user_email):
    """Create Postgres role for Databricks user via databricks_auth extension"""
    print(f"\n3️⃣  Setting up user role for {user_email}...")
    
    # Enable databricks_auth extension
    cursor.execute("CREATE EXTENSION IF NOT EXISTS databricks_auth")
    print("   ✅ databricks_auth extension enabled")
    
    # Check if user role exists
    cursor.execute(f"""
        SELECT COUNT(*) FROM pg_roles WHERE rolname = '{user_email}'
    """)
    exists = cursor.fetchone()[0] > 0
    
    if exists:
        print(f"   ✅ Role for {user_email} already exists")
    else:
        # Create user role
        cursor.execute(f"SELECT databricks_create_role('{user_email}', 'USER')")
        print(f"   ✅ Created role for {user_email}")

def grant_schema_permissions(cursor, user_email):
    """Grant schema and table permissions to user"""
    print(f"\n4️⃣  Granting schema permissions to {user_email}...")
    
    # Grant usage on features schema
    cursor.execute(f'GRANT USAGE ON SCHEMA features TO "{user_email}"')
    print("   ✅ Granted USAGE on features schema")
    
    # Grant SELECT on all tables in features schema
    cursor.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA features TO "{user_email}"')
    print("   ✅ Granted SELECT on all tables in features schema")
    
    # Grant EXECUTE on all functions in features schema
    cursor.execute(f'GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA features TO "{user_email}"')
    print("   ✅ Granted EXECUTE on all functions (stored procedures)")

def grant_table_permissions(cursor, user_email):
    """Grant permissions on flexible schema tables"""
    print(f"\n5️⃣  Granting permissions on flexible tables...")
    
    # Get all feature_table_* tables
    cursor.execute("""
        SELECT tablename 
        FROM pg_tables 
        WHERE schemaname = 'features' 
        AND tablename LIKE 'feature_table_%'
        ORDER BY tablename
    """)
    tables = cursor.fetchall()
    
    if not tables:
        print("   ℹ️  No feature_table_* tables found (will grant when tables are created)")
        return
    
    print(f"   📋 Found {len(tables)} flexible tables")
    
    # Grant SELECT on each table
    for (table_name,) in tables:
        cursor.execute(f'GRANT SELECT ON features.{table_name} TO "{user_email}"')
    
    print(f"   ✅ Granted SELECT on {len(tables)} tables")

def verify_setup(cursor, user_email):
    """Verify Data API setup is complete"""
    print("\n6️⃣  Verifying setup...")
    
    checks = []
    
    # Check authenticator role
    cursor.execute("SELECT COUNT(*) FROM pg_roles WHERE rolname = 'authenticator'")
    checks.append(("authenticator role", cursor.fetchone()[0] > 0))
    
    # Check pgrst schema
    cursor.execute("SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = 'pgrst'")
    checks.append(("pgrst schema", cursor.fetchone()[0] > 0))
    
    # Check pgrst.pre_config function
    cursor.execute("SELECT COUNT(*) FROM pg_proc WHERE proname = 'pre_config' AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'pgrst')")
    checks.append(("pgrst.pre_config()", cursor.fetchone()[0] > 0))
    
    # Check user role
    cursor.execute(f"SELECT COUNT(*) FROM pg_roles WHERE rolname = '{user_email}'")
    checks.append((f"user role ({user_email})", cursor.fetchone()[0] > 0))
    
    # Check user permissions on features schema
    cursor.execute(f"""
        SELECT COUNT(*) FROM information_schema.schema_privileges 
        WHERE grantee = '{user_email}' 
        AND table_schema = 'features' 
        AND privilege_type = 'USAGE'
    """)
    checks.append(("features schema USAGE", cursor.fetchone()[0] > 0))
    
    # Print verification results
    all_passed = True
    for check_name, passed in checks:
        if passed:
            print(f"   ✅ {check_name}")
        else:
            print(f"   ❌ {check_name}")
            all_passed = False
    
    return all_passed

def main():
    parser = argparse.ArgumentParser(
        description="Set up Lakebase Data API for a Databricks user"
    )
    parser.add_argument(
        '--user', '-u',
        type=str,
        default=DATA_API_CONFIG.get('user_email'),
        help=f'Databricks user email (default: {DATA_API_CONFIG.get("user_email", "not configured")})'
    )
    parser.add_argument(
        '--verify-only',
        action='store_true',
        help='Only verify setup, do not make changes'
    )
    
    args = parser.parse_args()
    
    print("=" * 80)
    print("LAKEBASE DATA API SETUP")
    print("=" * 80)
    print(f"\nUser: {args.user}")
    print(f"Mode: {'Verify only' if args.verify_only else 'Setup and verify'}")
    
    # Connect to database
    try:
        conn = get_single_connection()
        cursor = conn.cursor()
        
        # Check current user has sufficient privileges
        cursor.execute("SELECT current_user, session_user")
        current_user, session_user = cursor.fetchone()
        print(f"Connected as: {current_user}")
        
        if args.verify_only:
            # Only verify
            success = verify_setup(cursor, args.user)
        else:
            # Run full setup
            setup_authenticator_role(cursor)
            conn.commit()
            
            setup_pgrst_schema(cursor)
            conn.commit()
            
            setup_user_role(cursor, args.user)
            conn.commit()
            
            grant_schema_permissions(cursor, args.user)
            conn.commit()
            
            grant_table_permissions(cursor, args.user)
            conn.commit()
            
            # Verify
            success = verify_setup(cursor, args.user)
        
        cursor.close()
        conn.close()
        
        if success:
            print("\n" + "=" * 80)
            print("✅ DATA API SETUP COMPLETE")
            print("=" * 80)
            print("\n📋 Next steps:")
            print("   1. Get your Data API URL:")
            print("      Format: https://<project-id>.<region>.cloud.databricks.com/api/v1")
            print("\n   2. Generate a Databricks OAuth token:")
            print("      Databricks UI → User Settings → Developer → Access tokens")
            print("\n   3. Update benchmark configuration:")
            print("      DATA_API_BASE_URL = 'https://...'")
            print("      DATABRICKS_TOKEN = 'dapi...'")
            print("\n   4. Run benchmark:")
            print("      notebooks/benchmarks/benchmark_flexible_with_data_api.py")
            print("\n" + "=" * 80)
        else:
            print("\n" + "=" * 80)
            print("⚠️  SETUP INCOMPLETE")
            print("=" * 80)
            print("\nSome checks failed. Review errors above.")
            sys.exit(1)
    
    except psycopg2.Error as e:
        print(f"\n❌ Database error: {e}")
        print("\nMake sure you're running this script with admin privileges.")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()

