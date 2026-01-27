#!/bin/bash
# Deploy RPC Request Function to Lakebase
# Usage: ./deploy_rpc_function.sh [host] [user] [database]

set -e

# Parse arguments or use defaults
LAKEBASE_HOST="${1:-}"
LAKEBASE_USER="${2:-}"
LAKEBASE_DB="${3:-benchmark}"

if [ -z "$LAKEBASE_HOST" ] || [ -z "$LAKEBASE_USER" ]; then
    echo "Usage: $0 <host> <user> [database]"
    echo ""
    echo "Example:"
    echo "  $0 my-lakebase-cluster.region.provider.com myuser benchmark"
    echo ""
    echo "Or set environment variables:"
    echo "  export LAKEBASE_HOST=..."
    echo "  export LAKEBASE_USER=..."
    echo "  export LAKEBASE_DB=benchmark"
    exit 1
fi

echo "============================================================"
echo "🚀 Deploying RPC Request Function"
echo "============================================================"
echo "Host:     $LAKEBASE_HOST"
echo "User:     $LAKEBASE_USER"
echo "Database: $LAKEBASE_DB"
echo ""

SQL_FILE="sql/create_rpc_request_function.sql"

if [ ! -f "$SQL_FILE" ]; then
    echo "❌ Error: $SQL_FILE not found!"
    echo "   Make sure you're in the lakebase-benchmarking repo root."
    exit 1
fi

echo "📄 Found SQL file: $SQL_FILE"
echo ""
echo "⏳ Deploying function..."
echo ""

if psql -h "$LAKEBASE_HOST" -U "$LAKEBASE_USER" -d "$LAKEBASE_DB" -f "$SQL_FILE"; then
    echo ""
    echo "✅ Function deployed successfully!"
    echo ""
    echo "Verifying..."
    psql -h "$LAKEBASE_HOST" -U "$LAKEBASE_USER" -d "$LAKEBASE_DB" -c "
        SELECT 
            proname AS function_name,
            pg_get_function_arguments(oid) AS arguments,
            pg_get_function_result(oid) AS returns,
            prosrc IS NOT NULL AS has_source
        FROM pg_proc 
        WHERE proname = 'fetch_request_features' 
          AND pronamespace = 'features'::regnamespace;
    "
    
    echo ""
    echo "============================================================"
    echo "✅ RPC Function Ready!"
    echo "============================================================"
    echo ""
    echo "Next steps:"
    echo "  1. Run benchmark with: fetch_mode = 'rpc_request_json'"
    echo "  2. Or enable run_all_modes = true to include RPC in full sweep"
    echo ""
    echo "Expected performance:"
    echo "  • Queries/request: 1 (vs 10 binpacked, 30 serial)"
    echo "  • 10-30ms latency improvement vs binpacked_parallel"
    echo "  • Zero client-side overhead"
    echo ""
else
    echo ""
    echo "❌ Deployment failed!"
    echo ""
    echo "Troubleshooting:"
    echo "  1. Check connection: psql -h $LAKEBASE_HOST -U $LAKEBASE_USER -d $LAKEBASE_DB -c 'SELECT 1'"
    echo "  2. Check permissions: User needs CREATE FUNCTION privilege"
    echo "  3. Check schema: features schema must exist"
    exit 1
fi
