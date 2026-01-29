# Databricks notebook source
# MAGIC %md
# MAGIC # Backup CSVs from UC Volume
# MAGIC 
# MAGIC Compresses and prepares CSVs for download before workspace wipeout.

# COMMAND ----------

import os
import tarfile
from datetime import datetime

# COMMAND ----------

# Configuration
UC_VOLUME = "/Volumes/benchmark/data_load/benchmark_data_dev"  # Source: CSVs to backup
OUTPUT_TAR = f"/tmp/benchmark_csvs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.tar.gz"
DBFS_BACKUP_PATH = "dbfs:/FileStore/backups/"

print(f"📊 Backup Configuration:")
print(f"  Source (CSVs): {UC_VOLUME}")
print(f"  Temp location: {OUTPUT_TAR}")
print(f"  DBFS target: {DBFS_BACKUP_PATH}")
print(f"  Final destination: /Volumes/Official (your laptop)")
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Check CSV Sizes

# COMMAND ----------

print("📏 Analyzing CSV sizes...")
print()

total_size_bytes = 0
file_info = []

for root, dirs, files in os.walk(UC_VOLUME):
    for file in files:
        if file.endswith(('.csv', '.sql', '.txt')):
            filepath = os.path.join(root, file)
            try:
                size = os.path.getsize(filepath)
                total_size_bytes += size
                file_info.append({
                    'name': file,
                    'path': filepath,
                    'size_mb': size / (1024**2),
                    'size_gb': size / (1024**3)
                })
            except Exception as e:
                print(f"⚠️  Could not stat {file}: {e}")

print(f"📊 Summary:")
print(f"  Total files: {len(file_info)}")
print(f"  Total size: {total_size_bytes / (1024**3):.2f} GB ({total_size_bytes / (1024**2):.1f} MB)")
print()

if len(file_info) > 0:
    print("📁 Largest files:")
    for item in sorted(file_info, key=lambda x: x['size_mb'], reverse=True)[:20]:
        if item['size_mb'] >= 1:
            print(f"  {item['name']:<50} {item['size_mb']:>10.1f} MB")
        else:
            print(f"  {item['name']:<50} {item['size_mb']*1024:>10.1f} KB")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Compress CSVs

# COMMAND ----------

print("🗜️  Compressing files...")
print(f"  This may take several minutes for large datasets...")
print()

files_added = 0
try:
    with tarfile.open(OUTPUT_TAR, "w:gz") as tar:
        for item in file_info:
            try:
                # Add with relative path to keep structure
                arcname = item['path'].replace(UC_VOLUME + '/', '')
                tar.add(item['path'], arcname=arcname)
                files_added += 1
                if files_added % 10 == 0:
                    print(f"  Added {files_added}/{len(file_info)} files...")
            except Exception as e:
                print(f"⚠️  Could not add {item['name']}: {e}")
    
    compressed_size = os.path.getsize(OUTPUT_TAR)
    compression_ratio = (1 - compressed_size / total_size_bytes) * 100 if total_size_bytes > 0 else 0
    
    print()
    print(f"✅ Compression complete!")
    print(f"  Files added: {files_added}")
    print(f"  Original size: {total_size_bytes / (1024**3):.2f} GB")
    print(f"  Compressed size: {compressed_size / (1024**3):.2f} GB")
    print(f"  Compression ratio: {compression_ratio:.1f}%")
    print()
    
except Exception as e:
    print(f"❌ Compression failed: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Copy to DBFS for Download

# COMMAND ----------

print("📤 Copying to DBFS...")

# Ensure backup directory exists
dbutils.fs.mkdirs(DBFS_BACKUP_PATH)

# Copy tar.gz to DBFS
dbfs_target = DBFS_BACKUP_PATH.rstrip('/') + '/' + os.path.basename(OUTPUT_TAR)
dbutils.fs.cp(f"file:{OUTPUT_TAR}", dbfs_target)

print(f"✅ Copied to: {dbfs_target}")
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Download Instructions

# COMMAND ----------

print("========================================")
print("✅ BACKUP READY FOR DOWNLOAD")
print("========================================")
print()
print(f"📦 DBFS location: {dbfs_target}")
print(f"📏 Size: {compressed_size / (1024**3):.2f} GB")
print()
print("📥 Download to your laptop (100GB /Volumes/Official):")
print(f"  databricks fs cp --profile fe-sandbox-one-env-som-workspace-v4 {dbfs_target} /Volumes/Official/")
print()
print("🔓 Extract command (if needed):")
print(f"  cd /Volumes/Official")
print(f"  tar -xzf {os.path.basename(OUTPUT_TAR)}")
print()
print("💾 Your 100GB laptop volume has plenty of space for this backup!")
print("⚠️  Run this download BEFORE Saturday workspace wipeout!")
print("========================================")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (optional)

# COMMAND ----------

# Uncomment to remove temp file after confirming download
# os.remove(OUTPUT_TAR)
# print(f"🗑️  Cleaned up temp file: {OUTPUT_TAR}")
