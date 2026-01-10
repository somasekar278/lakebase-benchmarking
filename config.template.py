"""
Configuration template for Lakebase Benchmarking Framework.

Repository: lakebase-benchmarking

INSTRUCTIONS:
1. Copy this file to config.py: cp config.template.py config.py
2. Edit config.py with your actual values
3. DO NOT commit config.py (it contains secrets)
"""

# =============================================================================
# Lakebase Database Configuration
# =============================================================================

LAKEBASE_CONFIG = {
    # Lakebase connection endpoint
    'host': 'your-lakebase-endpoint.database.REGION.cloud.databricks.com',
    'port': 5432,
    'database': 'your_database_name',
    
    # User for running benchmarks (NOT admin user)
    'user': 'your_benchmark_user',
    'password': 'your_secure_password',
    
    # Schema where feature tables are stored
    'schema': 'features',
    
    # SSL mode (required for Lakebase)
    'sslmode': 'require',
}

# =============================================================================
# Data API Configuration
# =============================================================================

DATA_API_CONFIG = {
    # Data API endpoint
    # Format: https://<project-id>.<region>.cloud.databricks.com/api/v1
    # Find project ID in your Lakebase project URL
    'base_url': 'https://proj-XXXXX.REGION.cloud.databricks.com/api/v1',
    
    # Databricks OAuth token
    # Generate: Databricks UI → User Settings → Developer → Access tokens
    'token': 'dapiXXXXXXXXXXXXXXXXXXXXXXXXXX',
    
    # Databricks user email (for creating Postgres role)
    'user_email': 'your.email@company.com',
}

# =============================================================================
# Benchmark Configuration
# =============================================================================

BENCHMARK_CONFIG = {
    # Number of warm-up iterations before benchmark
    'num_warmup': 5,
    
    # Number of benchmark iterations
    'num_iterations': 100,
    
    # Number of keys to lookup per table
    'keys_per_table': 25,
    
    # PostgreSQL work_mem setting (for query optimization)
    'work_mem': '256MB',
    
    # Statement timeout (milliseconds)
    'statement_timeout': 300000,  # 5 minutes
}

# =============================================================================
# Comparison Baselines
# =============================================================================

BASELINE_CONFIG = {
    # Your current system's P99 latency (ms) - for comparison
    'dynamodb_p99': 79,
    
    # Your SLA target (ms)
    'sla_target': 120,
}

# =============================================================================
# Flexible Schema Configuration
# =============================================================================

FLEXIBLE_SCHEMA_CONFIG = {
    # Default number of tables to generate
    'default_num_tables': 30,
    
    # Default features per table
    'default_features_per_table': 5,
    
    # Default rows per table
    'default_rows_per_table': 100_000_000,
    
    # Output directory for generated schemas
    'default_output_dir': 'generated',
}

# =============================================================================
# Helper Functions (DO NOT MODIFY)
# =============================================================================

def get_lakebase_connection_string():
    """Get PostgreSQL connection string"""
    cfg = LAKEBASE_CONFIG
    return (
        f"host={cfg['host']} "
        f"port={cfg['port']} "
        f"dbname={cfg['database']} "
        f"user={cfg['user']} "
        f"password={cfg['password']} "
        f"sslmode={cfg['sslmode']}"
    )

def get_lakebase_jdbc_url():
    """Get JDBC connection URL for Spark"""
    cfg = LAKEBASE_CONFIG
    return f"jdbc:postgresql://{cfg['host']}:{cfg['port']}/{cfg['database']}"

def get_data_api_headers():
    """Get HTTP headers for Data API requests"""
    return {
        "Authorization": f"Bearer {DATA_API_CONFIG['token']}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

def validate_config():
    """Validate that required config values are set"""
    issues = []
    
    # Check Lakebase config
    if 'your-lakebase-endpoint' in LAKEBASE_CONFIG['host']:
        issues.append("Lakebase host not configured")
    
    if LAKEBASE_CONFIG['user'] == 'your_benchmark_user':
        issues.append("Lakebase user not configured")
    
    # Check Data API config
    if 'proj-XXXXX' in DATA_API_CONFIG['base_url']:
        issues.append("Data API base URL not configured")
    
    if DATA_API_CONFIG['token'].startswith('dapiXXXXX'):
        issues.append("Data API token not configured")
    
    if DATA_API_CONFIG['user_email'] == 'your.email@company.com':
        issues.append("Data API user email not configured")
    
    if issues:
        print("⚠️  Configuration issues found:")
        for issue in issues:
            print(f"   - {issue}")
        print("\n💡 Edit config.py to fix these issues")
        return False
    
    return True

def print_config():
    """Print current configuration (masks sensitive values)"""
    print("=" * 80)
    print("CURRENT CONFIGURATION")
    print("=" * 80)
    
    print("\n🔧 Lakebase:")
    print(f"   Host: {LAKEBASE_CONFIG['host']}")
    print(f"   Database: {LAKEBASE_CONFIG['database']}")
    print(f"   Schema: {LAKEBASE_CONFIG['schema']}")
    print(f"   User: {LAKEBASE_CONFIG['user']}")
    print(f"   Password: {'*' * len(LAKEBASE_CONFIG['password'])}")
    
    print("\n🌐 Data API:")
    print(f"   Base URL: {DATA_API_CONFIG['base_url']}")
    print(f"   Token: {DATA_API_CONFIG['token'][:10]}...{DATA_API_CONFIG['token'][-4:] if len(DATA_API_CONFIG['token']) > 14 else '****'}")
    print(f"   User: {DATA_API_CONFIG['user_email']}")
    
    print("\n📊 Benchmarks:")
    print(f"   Warm-up iterations: {BENCHMARK_CONFIG['num_warmup']}")
    print(f"   Benchmark iterations: {BENCHMARK_CONFIG['num_iterations']}")
    print(f"   Keys per table: {BENCHMARK_CONFIG['keys_per_table']}")
    
    print("\n🎯 Baselines:")
    print(f"   DynamoDB P99: {BASELINE_CONFIG['dynamodb_p99']}ms")
    print(f"   SLA Target: {BASELINE_CONFIG['sla_target']}ms")
    
    print("\n" + "=" * 80)

if __name__ == '__main__':
    # Run validation when config.py is executed directly
    print_config()
    print()
    if validate_config():
        print("✅ Configuration is valid!")
    else:
        print("❌ Please fix configuration issues above")

