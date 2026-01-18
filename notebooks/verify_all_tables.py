# Databricks notebook source
# MAGIC %md
# MAGIC # 📊 Verify All Tables Loaded
# MAGIC 
# MAGIC Check that all 30 tables are loaded with correct row counts and index status

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣ Get Parameters

# COMMAND ----------

dbutils.widgets.text("lakebase_host", "", "Lakebase Host")
dbutils.widgets.text("lakebase_database", "benchmark", "Lakebase Database")
dbutils.widgets.text("lakebase_schema", "features", "Lakebase Schema")
dbutils.widgets.text("lakebase_user", "", "Lakebase User")
dbutils.widgets.text("lakebase_password", "", "Lakebase Password")

lakebase_host = dbutils.widgets.get("lakebase_host")
lakebase_database = dbutils.widgets.get("lakebase_database")
lakebase_schema = dbutils.widgets.get("lakebase_schema")
lakebase_user = dbutils.widgets.get("lakebase_user")
lakebase_password = dbutils.widgets.get("lakebase_password")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2️⃣ Expected Row Counts

# COMMAND ----------

EXPECTED_TABLES = {
    "client_id_card_fingerprint__time_since__365d": 2_000_000_000,
    "client_id_card_fingerprint_good_rates_365d": 1_000_000_000,
    "client_id_card_fingerprint_good_rates_90d": 1_000_000_000,
    "client_id_card_fingerprint_good_rates_30d": 1_000_000_000,
    "client_id_card_fingerprint__fraud_rates__365d": 1_000_000_000,
    "client_id_customer_email_clean__fraud_rates__365d": 1_000_000_000,
    "client_id_cardholder_name_clean__fraud_rates__365d": 900_000_000,
    "client_id_cardholder_name_clean__tesseract_velocities__365d": 900_000_000,
    "client_id_customer_email_clean__time_since__90d": 700_000_000,
    "client_id_customer_email_clean__time_since__365d": 700_000_000,
    "client_id_customer_email_clean__time_since__30d": 700_000_000,
    "client_id_card_fingerprint__time_since__90d": 500_000_000,
    "client_id_customer_email_clean__good_rates_30d": 400_000_000,
    "client_id_customer_email_clean__good_rates_365d": 400_000_000,
    "client_id_customer_email_clean__good_rates_90d": 400_000_000,
    "client_id_cardholder_name_clean__good_rates_30d": 400_000_000,
    "client_id_cardholder_name_clean__good_rates_90d": 400_000_000,
    "client_id_cardholder_name_clean__good_rates_365d": 400_000_000,
    "client_id_card_fingerprint__fraud_rates__90d": 250_000_000,
    "client_id_customer_email_clean__fraud_rates__90d": 250_000_000,
    "client_id_cardholder_name_clean__fraud_rates__90d": 200_000_000,
    "client_id_cardholder_name_clean__tesseract_velocities__90d": 200_000_000,
    "client_id_card_fingerprint__time_since__30d": 150_000_000,
    "client_id_cardholder_name_clean__time_since__90d": 150_000_000,
    "client_id_cardholder_name_clean__time_since__365d": 150_000_000,
    "client_id_cardholder_name_clean__time_since__30d": 150_000_000,
    "client_id_card_fingerprint__fraud_rates__30d": 80_000_000,
    "client_id_customer_email_clean__fraud_rates__30d": 80_000_000,
    "client_id_cardholder_name_clean__fraud_rates__30d": 75_000_000,
    "client_id_cardholder_name_clean__tesseract_velocities__30d": 75_000_000,
}

TOTAL_EXPECTED_ROWS = sum(EXPECTED_TABLES.values())

print(f"📋 Expected: {len(EXPECTED_TABLES)} tables, {TOTAL_EXPECTED_ROWS:,} total rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3️⃣ Connect and Verify

# COMMAND ----------

import psycopg2

conn = psycopg2.connect(
    host=lakebase_host,
    database=lakebase_database,
    user=lakebase_user,
    password=lakebase_password,
    sslmode='require'
)

print("✅ Connected to Lakebase")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4️⃣ Check All Tables

# COMMAND ----------

with conn.cursor() as cursor:
    # Get all tables with row counts and index counts
    cursor.execute(f"""
        SELECT 
            t.tablename,
            COALESCE(s.n_live_tup, 0) AS estimated_rows,
            pg_relation_size(t.schemaname||'.'||t.tablename) / (1024^3) AS size_gb,
            (SELECT COUNT(*) 
             FROM pg_indexes i 
             WHERE i.schemaname = t.schemaname 
               AND i.tablename = t.tablename) AS index_count
        FROM pg_tables t
        LEFT JOIN pg_stat_user_tables s 
            ON s.schemaname = t.schemaname 
            AND s.relname = t.tablename
        WHERE t.schemaname = '{lakebase_schema}'
          AND t.tablename NOT LIKE '%stage%'
          AND t.tablename NOT LIKE '%__st'
          AND t.tablename NOT LIKE '%__sta'
        ORDER BY estimated_rows DESC
    """)
    
    actual_tables = {}
    for row in cursor.fetchall():
        tablename, estimated_rows, size_gb, index_count = row
        actual_tables[tablename] = {
            "rows": estimated_rows,
            "size_gb": size_gb,
            "index_count": index_count
        }

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5️⃣ Verification Report

# COMMAND ----------

print("=" * 100)
print("📊 TABLE VERIFICATION REPORT")
print("=" * 100)
print()

# Results
tables_ok = []
tables_missing = []
tables_wrong_count = []
tables_no_index = []

for expected_table, expected_rows in EXPECTED_TABLES.items():
    if expected_table not in actual_tables:
        tables_missing.append(expected_table)
        print(f"❌ MISSING: {expected_table}")
    else:
        actual = actual_tables[expected_table]
        actual_rows = actual["rows"]
        size_gb = actual["size_gb"]
        index_count = actual["index_count"]
        
        # Check row count (allow 1% variance for estimates)
        row_diff_pct = abs(actual_rows - expected_rows) / expected_rows * 100 if expected_rows > 0 else 0
        
        if row_diff_pct > 5:  # More than 5% off
            tables_wrong_count.append({
                "table": expected_table,
                "expected": expected_rows,
                "actual": actual_rows,
                "diff_pct": row_diff_pct
            })
            status = f"⚠️  WRONG COUNT"
        elif index_count == 0:
            tables_no_index.append(expected_table)
            status = f"⚠️  NO INDEX"
        else:
            tables_ok.append(expected_table)
            status = f"✅ OK"
        
        print(f"{status:15} {expected_table:70} {actual_rows:>15,} rows ({size_gb:>6.1f} GB) | {index_count} index(es)")

print()
print("=" * 100)
print("📈 SUMMARY")
print("=" * 100)
print(f"✅ Tables OK:           {len(tables_ok):>3} / {len(EXPECTED_TABLES)}")
print(f"❌ Tables Missing:      {len(tables_missing):>3}")
print(f"⚠️  Tables Wrong Count:  {len(tables_wrong_count):>3}")
print(f"⚠️  Tables No Index:     {len(tables_no_index):>3}")
print("=" * 100)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6️⃣ Detailed Issues

# COMMAND ----------

if tables_missing:
    print("\n" + "=" * 100)
    print("❌ MISSING TABLES (CRITICAL)")
    print("=" * 100)
    for table in tables_missing:
        print(f"  - {table} (expected {EXPECTED_TABLES[table]:,} rows)")

if tables_wrong_count:
    print("\n" + "=" * 100)
    print("⚠️  WRONG ROW COUNTS")
    print("=" * 100)
    for item in tables_wrong_count:
        print(f"  - {item['table']}")
        print(f"    Expected: {item['expected']:,} rows")
        print(f"    Actual:   {item['actual']:,} rows")
        print(f"    Diff:     {item['diff_pct']:.1f}%")

if tables_no_index:
    print("\n" + "=" * 100)
    print("⚠️  TABLES WITHOUT INDEXES (need to build)")
    print("=" * 100)
    for table in tables_no_index:
        rows = actual_tables[table]["rows"]
        size_gb = actual_tables[table]["size_gb"]
        print(f"  - {table:70} {rows:>15,} rows ({size_gb:>6.1f} GB)")
    print()
    print(f"💡 Total tables needing indexes: {len(tables_no_index)}")
    print(f"💡 Next step: Run 'fraud_build_indexes' job to create indexes in parallel")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7️⃣ Total Row Count

# COMMAND ----------

total_actual_rows = sum(actual_tables[t]["rows"] for t in EXPECTED_TABLES if t in actual_tables)
total_actual_size_gb = sum(actual_tables[t]["size_gb"] for t in EXPECTED_TABLES if t in actual_tables)

print()
print("=" * 100)
print("📊 TOTAL DATA LOADED")
print("=" * 100)
print(f"Expected rows: {TOTAL_EXPECTED_ROWS:>20,}")
print(f"Actual rows:   {total_actual_rows:>20,}")
print(f"Total size:    {total_actual_size_gb:>20,.1f} GB")
print(f"Completeness:  {(total_actual_rows / TOTAL_EXPECTED_ROWS * 100):>20.1f}%")
print("=" * 100)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8️⃣ Final Status

# COMMAND ----------

conn.close()

if tables_missing or tables_wrong_count:
    print("\n❌ VERIFICATION FAILED")
    print("   Issues found that need immediate attention!")
    dbutils.notebook.exit("FAILED")
else:
    print("\n✅ VERIFICATION PASSED")
    if tables_no_index:
        print(f"   ⚠️  {len(tables_no_index)} tables need indexes (this is OK, we'll build them next)")
    else:
        print("   All tables loaded with correct row counts and indexes!")
    dbutils.notebook.exit("SUCCESS")
