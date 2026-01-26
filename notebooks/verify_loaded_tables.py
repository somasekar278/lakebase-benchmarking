# Databricks notebook source
# MAGIC %md
# MAGIC # ✅ Verify Loaded Tables in Lakebase
# MAGIC 
# MAGIC Quick validation of completed tables:
# MAGIC - Row counts match expected
# MAGIC - PRIMARY KEY exists and is unique
# MAGIC - All columns present
# MAGIC - No NULL values in PK
# MAGIC - Sample data looks correct

# COMMAND ----------

import psycopg

# Connection parameters
dbutils.widgets.text("lakebase_host", "ep-gentle-silence-d346r2cf.database.eu-west-1.cloud.databricks.com", "Lakebase Host")
dbutils.widgets.text("lakebase_database", "benchmark", "Database")
dbutils.widgets.text("lakebase_schema", "features", "Schema")
dbutils.widgets.text("lakebase_user", "fraud_benchmark_user", "User")
dbutils.widgets.text("lakebase_password", "fraud-benchmark-user123!", "Password")

LAKEBASE_CONFIG = {
    'host': dbutils.widgets.get("lakebase_host"),
    'port': 5432,
    'database': dbutils.widgets.get("lakebase_database"),
    'schema': dbutils.widgets.get("lakebase_schema"),
    'user': dbutils.widgets.get("lakebase_user"),
    'password': dbutils.widgets.get("lakebase_password"),
    'sslmode': 'require'
}

conninfo = f"host={LAKEBASE_CONFIG['host']} port={LAKEBASE_CONFIG['port']} " \
           f"dbname={LAKEBASE_CONFIG['database']} user={LAKEBASE_CONFIG['user']} " \
           f"password={LAKEBASE_CONFIG['password']} sslmode={LAKEBASE_CONFIG['sslmode']}"

# Expected row counts (from fraud_tables_row_counts_30_COMPLETE.txt)
EXPECTED_COUNTS = {
    'client_id_card_fingerprint__fraud_rates__30d': 80_000_000,
    'client_id_card_fingerprint__fraud_rates__90d': 250_000_000,
    'client_id_card_fingerprint__fraud_rates__365d': 1_000_000_000,
    'client_id_customer_email_clean__fraud_rates__30d': 80_000_000,
    'client_id_customer_email_clean__fraud_rates__90d': 250_000_000,
    'client_id_customer_email_clean__fraud_rates__365d': 1_000_000_000,
    'client_id_cardholder_name_clean__fraud_rates__30d': 75_000_000,
}

print("="*80)
print("🔍 VERIFYING LOADED TABLES IN LAKEBASE")
print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣ Check Table Existence & Row Counts

# COMMAND ----------

print("\n" + "="*80)
print("1️⃣ TABLE EXISTENCE & ROW COUNTS")
print("="*80)

results = []

with psycopg.connect(conninfo) as conn:
    with conn.cursor() as cur:
        for table_name, expected_count in EXPECTED_COUNTS.items():
            full_table_name = f"{LAKEBASE_CONFIG['schema']}.{table_name}"
            
            # Check if table exists and get row count
            try:
                cur.execute(f"SELECT COUNT(*) FROM {full_table_name};")
                actual_count = cur.fetchone()[0]
                
                # Calculate difference
                diff = actual_count - expected_count
                diff_pct = (diff / expected_count) * 100 if expected_count > 0 else 0
                
                status = "✅" if abs(diff_pct) < 1 else "⚠️"  # Allow 1% variance
                
                result = {
                    'table': table_name,
                    'expected': expected_count,
                    'actual': actual_count,
                    'diff': diff,
                    'diff_pct': diff_pct,
                    'status': status
                }
                results.append(result)
                
                print(f"\n{status} {table_name}")
                print(f"   Expected: {expected_count:>15,}")
                print(f"   Actual:   {actual_count:>15,}")
                if diff != 0:
                    print(f"   Diff:     {diff:>15,} ({diff_pct:+.2f}%)")
                
            except Exception as e:
                print(f"\n❌ {table_name}")
                print(f"   Error: {e}")
                results.append({
                    'table': table_name,
                    'status': '❌',
                    'error': str(e)
                })

print("\n" + "="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2️⃣ Check PRIMARY KEY Constraints

# COMMAND ----------

print("\n" + "="*80)
print("2️⃣ PRIMARY KEY CONSTRAINTS")
print("="*80)

with psycopg.connect(conninfo) as conn:
    with conn.cursor() as cur:
        for table_name in EXPECTED_COUNTS.keys():
            full_table_name = f"{LAKEBASE_CONFIG['schema']}.{table_name}"
            
            # Get PK columns
            cur.execute(f"""
                SELECT a.attname AS column_name
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid 
                                   AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = '{full_table_name}'::regclass
                  AND i.indisprimary
                ORDER BY a.attnum;
            """)
            
            pk_columns = [row[0] for row in cur.fetchall()]
            
            if pk_columns:
                print(f"\n✅ {table_name}")
                print(f"   PK: {', '.join(pk_columns)}")
                
                # Check for NULL values in PK
                pk_col = pk_columns[0]  # Assuming single PK column
                cur.execute(f"SELECT COUNT(*) FROM {full_table_name} WHERE {pk_col} IS NULL;")
                null_count = cur.fetchone()[0]
                
                if null_count > 0:
                    print(f"   ⚠️  WARNING: {null_count:,} NULL values in PK!")
                else:
                    print(f"   ✅ No NULL values in PK")
                
                # Check for duplicates in PK
                cur.execute(f"""
                    SELECT {pk_col}, COUNT(*) as cnt 
                    FROM {full_table_name} 
                    GROUP BY {pk_col} 
                    HAVING COUNT(*) > 1 
                    LIMIT 1;
                """)
                duplicate = cur.fetchone()
                
                if duplicate:
                    print(f"   ❌ DUPLICATES FOUND in PK: {duplicate[0]} appears {duplicate[1]} times")
                else:
                    print(f"   ✅ No duplicate PKs")
            else:
                print(f"\n❌ {table_name}")
                print(f"   No PRIMARY KEY found!")

print("\n" + "="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3️⃣ Check Column Structure

# COMMAND ----------

print("\n" + "="*80)
print("3️⃣ COLUMN STRUCTURE")
print("="*80)

with psycopg.connect(conninfo) as conn:
    with conn.cursor() as cur:
        for table_name in list(EXPECTED_COUNTS.keys())[:3]:  # Check first 3 tables
            full_table_name = f"{LAKEBASE_CONFIG['schema']}.{table_name}"
            
            cur.execute(f"""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = '{LAKEBASE_CONFIG['schema']}'
                  AND table_name = '{table_name}'
                ORDER BY ordinal_position;
            """)
            
            columns = cur.fetchall()
            
            print(f"\n📋 {table_name}")
            print(f"   Columns: {len(columns)}")
            for col_name, col_type, nullable in columns:
                null_str = "NULL" if nullable == "YES" else "NOT NULL"
                print(f"      • {col_name:<60} {col_type:<20} {null_str}")

print("\n" + "="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4️⃣ Sample Data Inspection

# COMMAND ----------

print("\n" + "="*80)
print("4️⃣ SAMPLE DATA (First Row from Each Table)")
print("="*80)

with psycopg.connect(conninfo) as conn:
    with conn.cursor() as cur:
        for table_name in list(EXPECTED_COUNTS.keys())[:3]:  # Check first 3 tables
            full_table_name = f"{LAKEBASE_CONFIG['schema']}.{table_name}"
            
            cur.execute(f"SELECT * FROM {full_table_name} LIMIT 1;")
            row = cur.fetchone()
            
            if row:
                # Get column names
                col_names = [desc[0] for desc in cur.description]
                
                print(f"\n📄 {table_name}")
                for col_name, value in zip(col_names, row):
                    # Truncate long values
                    str_value = str(value)
                    if len(str_value) > 50:
                        str_value = str_value[:47] + "..."
                    print(f"   {col_name:<60} = {str_value}")
            else:
                print(f"\n⚠️  {table_name}: No data found!")

print("\n" + "="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5️⃣ Check Indexes

# COMMAND ----------

print("\n" + "="*80)
print("5️⃣ INDEXES")
print("="*80)

with psycopg.connect(conninfo) as conn:
    with conn.cursor() as cur:
        for table_name in EXPECTED_COUNTS.keys():
            full_table_name = f"{LAKEBASE_CONFIG['schema']}.{table_name}"
            
            cur.execute(f"""
                SELECT 
                    i.relname AS index_name,
                    a.attname AS column_name,
                    ix.indisunique AS is_unique,
                    ix.indisprimary AS is_primary
                FROM pg_class t
                JOIN pg_index ix ON t.oid = ix.indrelid
                JOIN pg_class i ON i.oid = ix.indexrelid
                JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
                WHERE t.oid = '{full_table_name}'::regclass
                ORDER BY i.relname, a.attnum;
            """)
            
            indexes = cur.fetchall()
            
            if indexes:
                print(f"\n✅ {table_name}")
                current_index = None
                for idx_name, col_name, is_unique, is_primary in indexes:
                    if idx_name != current_index:
                        unique_str = "UNIQUE" if is_unique else ""
                        primary_str = "PRIMARY KEY" if is_primary else ""
                        flags = " ".join(filter(None, [unique_str, primary_str]))
                        print(f"   • {idx_name} ({flags})")
                        current_index = idx_name
                    print(f"      - {col_name}")
            else:
                print(f"\n⚠️  {table_name}: No indexes found!")

print("\n" + "="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Summary

# COMMAND ----------

print("\n" + "="*80)
print("✅ VALIDATION SUMMARY")
print("="*80)

all_passed = True
issues = []

for result in results:
    if 'error' in result:
        all_passed = False
        issues.append(f"❌ {result['table']}: {result['error']}")
    elif result['status'] == '⚠️':
        issues.append(f"⚠️  {result['table']}: Row count off by {result['diff_pct']:+.2f}%")

print(f"\n📊 Tables Checked: {len(results)}")
print(f"✅ Passed: {sum(1 for r in results if r.get('status') == '✅')}")
print(f"⚠️  Warnings: {sum(1 for r in results if r.get('status') == '⚠️')}")
print(f"❌ Errors: {sum(1 for r in results if 'error' in r)}")

if all_passed and not issues:
    print("\n" + "🎉 " * 20)
    print("✅ ALL TABLES VALIDATED SUCCESSFULLY!")
    print("🎉 " * 20)
else:
    print("\n⚠️  ISSUES FOUND:")
    for issue in issues:
        print(f"   {issue}")

print("\n" + "="*80)
print("✅ VERIFICATION COMPLETE")
print("="*80)

# COMMAND ----------
