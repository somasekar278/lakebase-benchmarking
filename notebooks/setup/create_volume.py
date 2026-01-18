# Databricks notebook source
# MAGIC %md
# MAGIC # Create Unity Catalog Volume
# MAGIC 
# MAGIC Creates Unity Catalog volume for bulk loading if it doesn't exist.

# COMMAND ----------

# Get parameters
dbutils.widgets.text("catalog", "benchmark", "Catalog Name")
dbutils.widgets.text("schema", "data_load", "Schema Name")
dbutils.widgets.text("volume", "benchmark_data", "Volume Name")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")

print(f"Creating volume: {catalog}.{schema}.{volume}")

# COMMAND ----------

# Ensure catalog exists (must be created manually by admin if not)
try:
    spark.sql(f"USE CATALOG {catalog}")
    print(f"✅ Catalog exists: {catalog}")
except Exception as e:
    print(f"❌ Catalog '{catalog}' does not exist or you don't have access")
    print(f"   Please create it manually: CREATE CATALOG IF NOT EXISTS {catalog}")
    raise

# COMMAND ----------

# Create schema if not exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
print(f"✅ Schema ready: {catalog}.{schema}")

# COMMAND ----------

# Create volume if not exists
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}
    COMMENT 'Volume for Lakebase bulk loading'
""")
print(f"✅ Volume ready: {catalog}.{schema}.{volume}")

# COMMAND ----------

# Verify volume access
volume_path = f"/Volumes/{catalog}/{schema}/{volume}"
try:
    dbutils.fs.ls(volume_path)
    print(f"✅ Volume accessible: {volume_path}")
except Exception as e:
    print(f"❌ Cannot access volume: {e}")
    raise

# COMMAND ----------

dbutils.notebook.exit(f"Volume created: {volume_path}")
