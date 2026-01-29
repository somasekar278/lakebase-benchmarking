#!/bin/bash
# Complete Backup Script - Run before workspace wipeout
# Usage: ./backup_all.sh

set -e

echo "=========================================="
echo "🚨 WORKSPACE BACKUP - PRE-WIPEOUT"
echo "=========================================="
echo ""

# Configuration
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR="./workspace_backup_${TIMESTAMP}"
LOCAL_VOLUME="/Volumes/Official"
PROFILE="fe-sandbox-one-env-som-workspace-v4"

# Create backup structure
mkdir -p "$BACKUP_DIR"/{postgres,csvs,jobs,notebooks,reports,config}

echo "📦 Backup strategy:"
echo "  1. Local staging: $BACKUP_DIR"
echo "  2. Final destination: $LOCAL_VOLUME"

echo "📁 Backup directory: $BACKUP_DIR"
echo ""

# ============================================================
# 1. POSTGRES TABLES
# ============================================================
echo "📊 Step 1: Backing up Postgres tables..."
echo "📖 Reading credentials from databricks.yml..."
echo ""

# Extract credentials from databricks.yml (dev target)
LAKEBASE_HOST=$(grep -A 20 "targets:" databricks.yml | grep "lakebase_host:" | head -1 | awk '{print $2}')
LAKEBASE_DB=$(grep -A 20 "targets:" databricks.yml | grep "lakebase_database:" | head -1 | awk '{print $2}')
LAKEBASE_USER=$(grep -A 2 "lakebase_user:" databricks.yml | grep "default:" | awk '{print $2}')
LAKEBASE_PASSWORD=$(grep -A 2 "lakebase_password:" databricks.yml | grep "default:" | awk '{print $2}')

echo "✅ Found credentials:"
echo "  Host: $LAKEBASE_HOST"
echo "  Database: $LAKEBASE_DB"
echo "  User: $LAKEBASE_USER"
echo ""
echo "Dumping tables..."

# Main results table (V5)
PGPASSWORD="$LAKEBASE_PASSWORD" pg_dump -h "$LAKEBASE_HOST" -U "$LAKEBASE_USER" -d "$LAKEBASE_DB" \
  -t features.zipfian_feature_serving_results_v5 \
  --data-only --column-inserts \
  > "$BACKUP_DIR/postgres/zipfian_results_v5.sql" 2>/dev/null || echo "⚠️  V5 results table not found or empty"

# Request timing
PGPASSWORD="$LAKEBASE_PASSWORD" pg_dump -h "$LAKEBASE_HOST" -U "$LAKEBASE_USER" -d "$LAKEBASE_DB" \
  -t features.zipfian_request_timing \
  --data-only --column-inserts \
  > "$BACKUP_DIR/postgres/zipfian_request_timing.sql" 2>/dev/null || echo "⚠️  Request timing table not found or empty"

# Slow query log
PGPASSWORD="$LAKEBASE_PASSWORD" pg_dump -h "$LAKEBASE_HOST" -U "$LAKEBASE_USER" -d "$LAKEBASE_DB" \
  -t features.zipfian_slow_query_log \
  --data-only --column-inserts \
  > "$BACKUP_DIR/postgres/zipfian_slow_query_log.sql" 2>/dev/null || echo "⚠️  Slow query log table not found or empty"

# All zipfian tables (comprehensive)
PGPASSWORD="$LAKEBASE_PASSWORD" pg_dump -h "$LAKEBASE_HOST" -U "$LAKEBASE_USER" -d "$LAKEBASE_DB" \
  -n features --table='zipfian*' \
  --data-only --column-inserts \
  > "$BACKUP_DIR/postgres/all_zipfian_tables.sql" 2>/dev/null || echo "⚠️  Some tables may not exist"

echo "✅ Postgres dumps complete"
echo ""

# ============================================================
# 2. JOBS - BACKUP ALL JOBS AUTOMATICALLY
# ============================================================
echo "📋 Step 2: Backing up ALL job definitions..."
databricks jobs list --profile "$PROFILE" > "$BACKUP_DIR/jobs/jobs_list.txt"

echo "Found jobs:"
cat "$BACKUP_DIR/jobs/jobs_list.txt"
echo ""

# Extract all job IDs and back them up
JOB_COUNT=0
while IFS= read -r line; do
  # Extract job ID (first column)
  JOB_ID=$(echo "$line" | awk '{print $1}')
  
  # Skip header lines and empty lines
  if [[ "$JOB_ID" =~ ^[0-9]+$ ]]; then
    echo "  Backing up job $JOB_ID..."
    databricks jobs get --job-id "$JOB_ID" --profile "$PROFILE" > "$BACKUP_DIR/jobs/job_${JOB_ID}.json" 2>/dev/null
    if [ $? -eq 0 ]; then
      ((JOB_COUNT++))
    else
      echo "    ⚠️  Failed to backup job $JOB_ID"
    fi
  fi
done < "$BACKUP_DIR/jobs/jobs_list.txt"

echo "✅ Backed up $JOB_COUNT jobs"
echo ""

# ============================================================
# 3. NOTEBOOKS
# ============================================================
echo "📓 Step 3: Backing up notebooks..."
cp -r notebooks/* "$BACKUP_DIR/notebooks/"
echo "✅ Notebooks backed up"
echo ""

# ============================================================
# 4. CONFIG FILES
# ============================================================
echo "⚙️  Step 4: Backing up config files..."
cp databricks.yml "$BACKUP_DIR/config/" 2>/dev/null || echo "⚠️  databricks.yml not found"
cp -r resources "$BACKUP_DIR/config/" 2>/dev/null || echo "⚠️  resources/ not found"
cp requirements.txt "$BACKUP_DIR/config/" 2>/dev/null || echo "⚠️  requirements.txt not found"
cp report_template.html "$BACKUP_DIR/config/"
cp *.sql "$BACKUP_DIR/config/" 2>/dev/null || true
cp *.sh "$BACKUP_DIR/config/" 2>/dev/null || true
cp *.md "$BACKUP_DIR/config/" 2>/dev/null || true
echo "✅ Config files backed up"
echo ""

# ============================================================
# 5. GENERATED REPORTS - AUTO DOWNLOAD
# ============================================================
echo "📄 Step 5: Downloading generated reports..."
databricks fs cp --profile "$PROFILE" -r dbfs:/FileStore/benchmark_reports "$BACKUP_DIR/reports/" 2>/dev/null && echo "✅ Reports downloaded" || echo "⚠️  No reports found (this is OK)"
echo ""

# ============================================================
# 6. COMPRESS BACKUP
# ============================================================
echo "🗜️  Step 6: Compressing backup..."
tar -czf "${BACKUP_DIR}.tar.gz" "$BACKUP_DIR"
BACKUP_SIZE=$(du -h "${BACKUP_DIR}.tar.gz" | cut -f1)
echo "✅ Compressed to: ${BACKUP_DIR}.tar.gz ($BACKUP_SIZE)"
echo ""

# ============================================================
# 7. COPY TO LOCAL VOLUME
# ============================================================
echo "📦 Step 7: Copying to $LOCAL_VOLUME..."

if [ -d "$LOCAL_VOLUME" ]; then
    # Create backup directory in local volume
    mkdir -p "$LOCAL_VOLUME/lakebase_backups"
    
    # Copy compressed backup
    cp "${BACKUP_DIR}.tar.gz" "$LOCAL_VOLUME/lakebase_backups/"
    
    # Also keep uncompressed copy for easy access
    cp -r "$BACKUP_DIR" "$LOCAL_VOLUME/lakebase_backups/"
    
    echo "✅ Copied to $LOCAL_VOLUME/lakebase_backups/"
    echo ""
    
    # Show space used
    VOLUME_USAGE=$(du -sh "$LOCAL_VOLUME/lakebase_backups" | cut -f1)
    VOLUME_AVAILABLE=$(df -h "$LOCAL_VOLUME" | awk 'NR==2 {print $4}')
    echo "💾 Volume status:"
    echo "  Used: $VOLUME_USAGE"
    echo "  Available: $VOLUME_AVAILABLE"
else
    echo "⚠️  $LOCAL_VOLUME not found - backup saved to current directory only"
    echo "  You can manually copy later: cp ${BACKUP_DIR}.tar.gz /Volumes/Official/"
fi
echo ""

# ============================================================
# SUMMARY
# ============================================================
echo "=========================================="
echo "✅ BACKUP COMPLETE"
echo "=========================================="
echo ""
echo "📦 Local staging: ${BACKUP_DIR}.tar.gz ($BACKUP_SIZE)"
if [ -d "$LOCAL_VOLUME" ]; then
    echo "📦 Volume backup: $LOCAL_VOLUME/lakebase_backups/"
fi
echo ""
echo "Contents:"
echo "  ✓ Postgres tables (all zipfian_* tables)"
echo "  ✓ Job definitions"
echo "  ✓ Notebooks (all .py files)"
echo "  ✓ Config files (databricks.yml, requirements.txt, etc.)"
echo "  ✓ SQL scripts and shell scripts"
echo "  ✓ Documentation (.md files)"
if [ "$DOWNLOAD_REPORTS" = "y" ]; then
  echo "  ✓ Generated reports"
fi
echo ""
echo "⚠️  STILL NEEDED:"
echo "  - CSVs from UC Volume:"
echo "    1. Upload backup_csvs_from_volume.py to Databricks"
echo "    2. Run the notebook"
echo "    3. Run ./download_backup_to_volume.sh"
echo ""
echo "💾 Final location: $LOCAL_VOLUME/lakebase_backups/"
echo "🔐 Keep this backup safe! Workspace wipes Saturday."
echo "=========================================="
