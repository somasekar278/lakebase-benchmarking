#!/bin/bash
# Quick script to deploy and run CSV backup job
# Usage: ./run_csv_backup.sh

set -e

PROFILE="fe-sandbox-one-env-som-workspace-v4"

echo "=========================================="
echo "📦 CSV BACKUP JOB - QUICK START"
echo "=========================================="
echo ""

# Step 1: Deploy the job
echo "📤 Step 1: Deploying CSV backup job..."
databricks bundle deploy -t dev --profile "$PROFILE"

if [ $? -eq 0 ]; then
    echo "✅ Job deployed successfully"
else
    echo "❌ Job deployment failed"
    exit 1
fi
echo ""

# Step 2: Find the job ID
echo "🔍 Step 2: Finding job ID..."
JOB_ID=$(databricks jobs list --profile "$PROFILE" --output json | \
         jq -r '.jobs[] | select(.settings.name | contains("CSV Backup")) | .job_id' | head -1)

if [ -z "$JOB_ID" ]; then
    echo "❌ Could not find CSV Backup job"
    echo ""
    echo "Available jobs:"
    databricks jobs list --profile "$PROFILE" | grep -i backup || echo "No backup jobs found"
    exit 1
fi

echo "✅ Found job ID: $JOB_ID"
echo ""

# Step 3: Run the job
echo "🚀 Step 3: Starting CSV backup job..."
echo "  Job ID: $JOB_ID"
echo "  This will compress CSVs from UC Volume to DBFS"
echo ""

read -p "Start backup job now? (y/n): " START_JOB

if [ "$START_JOB" != "y" ]; then
    echo ""
    echo "Job not started. To run manually:"
    echo "  databricks jobs run-now --job-id $JOB_ID --profile $PROFILE"
    exit 0
fi

echo ""
echo "⏳ Starting job (this may take 30-60 minutes)..."
RUN_ID=$(databricks jobs run-now --job-id "$JOB_ID" --profile "$PROFILE" --output json | jq -r '.run_id')

if [ -z "$RUN_ID" ]; then
    echo "❌ Failed to start job"
    exit 1
fi

echo "✅ Job started!"
echo "  Run ID: $RUN_ID"
echo ""
echo "📊 Monitor progress:"
echo "  databricks jobs run-get --run-id $RUN_ID --profile $PROFILE"
echo ""
echo "🌐 Or view in Databricks UI:"
echo "  https://fe-sandbox-one-env-som-workspace-v4.cloud.databricks.com/jobs/$JOB_ID/runs/$RUN_ID"
echo ""
echo "⏳ Waiting for job to complete..."
echo "  (Press Ctrl+C to stop monitoring, job will continue running)"
echo ""

# Monitor job status
while true; do
    STATUS=$(databricks jobs run-get --run-id "$RUN_ID" --profile "$PROFILE" --output json | jq -r '.state.life_cycle_state')
    RESULT=$(databricks jobs run-get --run-id "$RUN_ID" --profile "$PROFILE" --output json | jq -r '.state.result_state // "RUNNING"')
    
    echo "  Status: $STATUS - $RESULT"
    
    if [ "$STATUS" == "TERMINATED" ]; then
        break
    fi
    
    sleep 30
done

echo ""
if [ "$RESULT" == "SUCCESS" ]; then
    echo "=========================================="
    echo "✅ CSV BACKUP COMPLETE!"
    echo "=========================================="
    echo ""
    echo "📦 Next step: Download CSVs to your laptop"
    echo "  ./download_backup_to_volume.sh"
    echo ""
else
    echo "=========================================="
    echo "❌ JOB FAILED: $RESULT"
    echo "=========================================="
    echo ""
    echo "Check logs in Databricks UI"
    exit 1
fi
