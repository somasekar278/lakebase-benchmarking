#!/bin/bash
# Download CSV backup from Databricks to local /Volumes/Official
# Run this AFTER executing backup_csvs_from_volume.py in Databricks

set -e

PROFILE="fe-sandbox-one-env-som-workspace-v4"
DBFS_BACKUP_PATH="dbfs:/FileStore/backups/"
LOCAL_DESTINATION="/Volumes/Official"

echo "=========================================="
echo "📥 DOWNLOAD BACKUP TO /Volumes/Official"
echo "=========================================="
echo ""

# Check if destination exists
if [ ! -d "$LOCAL_DESTINATION" ]; then
    echo "❌ Error: $LOCAL_DESTINATION does not exist!"
    echo "   Please check the path and try again."
    exit 1
fi

# Check available space
AVAILABLE_GB=$(df -Hg "$LOCAL_DESTINATION" | awk 'NR==2 {print $4}' | sed 's/G//')
echo "💾 Available space on /Volumes/Official: ${AVAILABLE_GB}GB"
echo ""

# List available backups
echo "📦 Finding latest backup in DBFS..."
LATEST_BACKUP=$(databricks fs ls --profile "$PROFILE" "$DBFS_BACKUP_PATH" 2>/dev/null | \
                grep "benchmark_csvs_.*\.tar\.gz" | \
                sort -r | head -1 | awk '{print $NF}')

if [ -z "$LATEST_BACKUP" ]; then
    echo "❌ No backup found in $DBFS_BACKUP_PATH"
    echo ""
    echo "Please run backup_csvs_from_volume.py in Databricks first!"
    exit 1
fi

echo "✅ Found backup: $LATEST_BACKUP"
echo ""

# Get file size
FILE_SIZE=$(databricks fs ls --profile "$PROFILE" "$LATEST_BACKUP" 2>/dev/null | awk '{print $1}')
echo "📏 Backup size: $FILE_SIZE"
echo ""

# Confirm download
read -p "Download to $LOCAL_DESTINATION? (y/n): " CONFIRM
if [ "$CONFIRM" != "y" ]; then
    echo "❌ Download cancelled"
    exit 0
fi

# Download
echo ""
echo "⬇️  Downloading..."
echo "  From: $LATEST_BACKUP"
echo "  To: $LOCAL_DESTINATION/"
echo ""
echo "⏳ This may take several minutes for large files..."
echo ""

FILENAME=$(basename "$LATEST_BACKUP")
databricks fs cp --profile "$PROFILE" "$LATEST_BACKUP" "$LOCAL_DESTINATION/$FILENAME"

if [ $? -eq 0 ]; then
    echo ""
    echo "=========================================="
    echo "✅ DOWNLOAD COMPLETE"
    echo "=========================================="
    echo ""
    echo "📦 Backup location: $LOCAL_DESTINATION/$FILENAME"
    echo ""
    
    # Check if user wants to extract
    read -p "Extract backup now? (y/n): " EXTRACT
    if [ "$EXTRACT" = "y" ]; then
        echo ""
        echo "📦 Extracting..."
        cd "$LOCAL_DESTINATION"
        tar -xzf "$FILENAME"
        
        if [ $? -eq 0 ]; then
            echo "✅ Extraction complete!"
            echo ""
            echo "📁 Files extracted to: $LOCAL_DESTINATION/"
            echo ""
            
            # Show extracted contents
            echo "📂 Extracted files:"
            ls -lh "$LOCAL_DESTINATION" | grep -v ".tar.gz" | tail -n +2
        else
            echo "❌ Extraction failed"
        fi
    else
        echo ""
        echo "🔓 To extract later, run:"
        echo "  cd $LOCAL_DESTINATION"
        echo "  tar -xzf $FILENAME"
    fi
    
    echo ""
    echo "=========================================="
    echo "✅ ALL DONE!"
    echo "=========================================="
else
    echo ""
    echo "❌ Download failed!"
    echo ""
    echo "Troubleshooting:"
    echo "  1. Check Databricks profile: $PROFILE"
    echo "  2. Verify backup exists: databricks fs ls --profile $PROFILE $DBFS_BACKUP_PATH"
    echo "  3. Check network connection"
    exit 1
fi
