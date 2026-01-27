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
    
    # List all valid run_ids (8-char hex) and check their report.html timestamps
    TEMP_FILE=$(mktemp)
    
    for dir in $(databricks fs ls --profile "$PROFILE" dbfs:/FileStore/benchmark_reports/ 2>&1 | grep -E "^[a-f0-9]{8}$"); do
        # Get timestamp of report.html in this directory (column 3 from --long output)
        TIMESTAMP=$(databricks fs ls --profile "$PROFILE" --long "dbfs:/FileStore/benchmark_reports/$dir/" 2>/dev/null | grep "report.html" | awk '{print $3}')
        if [ -n "$TIMESTAMP" ]; then
            echo "$TIMESTAMP $dir" >> "$TEMP_FILE"
        fi
    done
    
    # Sort by timestamp (descending) and get the most recent run_id
    if [ -s "$TEMP_FILE" ]; then
        RUN_ID=$(sort -r "$TEMP_FILE" | head -1 | awk '{print $2}')
    fi
    rm -f "$TEMP_FILE"
    
    if [ -z "$RUN_ID" ]; then
        echo "❌ No valid run IDs found in dbfs:/FileStore/benchmark_reports/"
        echo ""
        echo "Available directories:"
        databricks fs ls --profile "$PROFILE" dbfs:/FileStore/benchmark_reports/ 2>&1 || true
        echo ""
        echo "💡 Looking for 8-character hex run IDs (e.g., 4a0a662f)"
        exit 1
    fi
    
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
