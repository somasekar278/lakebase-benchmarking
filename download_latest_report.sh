#!/bin/bash
# Download Latest Zipfian Benchmark Report
# Usage: ./download_latest_report.sh [run_id]

set -e

PROFILE="fe-sandbox-one-env-som-workspace-v4"
LOCAL_DIR="./generated_reports"
RUN_ID="${1:-latest}"

echo "=================================================="
echo "📥 Downloading Zipfian Benchmark Report"
echo "=================================================="
echo ""

# If run_id is "latest", find the most recent one
if [ "$RUN_ID" = "latest" ]; then
    echo "🔍 Finding latest run ID..."
    
    # List directories in benchmark_reports (simple format: just names)
    DIRS=$(databricks fs ls --profile "$PROFILE" dbfs:/FileStore/benchmark_reports/ 2>&1 | grep -v "^latest$" | grep -v "^$" | sort -r | head -1)
    
    if [ -z "$DIRS" ]; then
        echo "❌ No reports found in dbfs:/FileStore/benchmark_reports/"
        echo ""
        echo "Available directories:"
        databricks fs ls --profile "$PROFILE" dbfs:/FileStore/benchmark_reports/ 2>&1 || true
        exit 1
    fi
    
    RUN_ID="$DIRS"
    echo "✅ Found latest run: $RUN_ID"
    echo ""
fi

REMOTE_PATH="dbfs:/FileStore/benchmark_reports/$RUN_ID/report.html"
LOCAL_FILE="$LOCAL_DIR/zipfian_report_${RUN_ID}.html"

echo "📊 Report Details:"
echo "   Run ID: $RUN_ID"
echo "   Remote: $REMOTE_PATH"
echo "   Local:  $LOCAL_FILE"
echo ""

# Create local directory
mkdir -p "$LOCAL_DIR"

# Download
echo "⬇️  Downloading..."
if databricks fs cp --profile "$PROFILE" "$REMOTE_PATH" "$LOCAL_FILE" 2>&1; then
    echo ""
    echo "✅ Download complete!"
    echo ""
    echo "📁 Saved to: $LOCAL_FILE"
    echo ""
    
    # Try to open in browser
    if command -v open &> /dev/null; then
        echo "🌐 Opening in browser..."
        open "$LOCAL_FILE"
    elif command -v xdg-open &> /dev/null; then
        echo "🌐 Opening in browser..."
        xdg-open "$LOCAL_FILE"
    else
        echo "💡 Open manually: $LOCAL_FILE"
    fi
else
    echo ""
    echo "❌ Download failed!"
    echo ""
    echo "Check if the report exists:"
    echo "  databricks fs ls --profile $PROFILE dbfs:/FileStore/benchmark_reports/$RUN_ID/"
    exit 1
fi

echo ""
echo "=================================================="
echo "✅ Done!"
echo "=================================================="
