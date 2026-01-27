"""
Get Last Successful Benchmark Run ID
=====================================
Run this in Databricks before starting a new benchmark to avoid resampling keys.
Keys sampling takes 1+ hour, so reusing keys from a previous run saves time.

Usage:
    1. Run this cell
    2. Copy the printed run_id
    3. Set the "reuse_run_id" widget parameter in the benchmark notebook
"""

import psycopg

# Connection details (same as benchmark)
DBHOST = dbutils.widgets.get("dbhost")
DBPORT = int(dbutils.widgets.get("dbport") or "5432")
DBUSER = dbutils.secrets.get("pg-secrets", "db_user")
DBPASS = dbutils.secrets.get("pg-secrets", "db_password")
DBNAME = "db"
SCHEMA = "features"

# Connect
conn = psycopg.connect(
    host=DBHOST,
    port=DBPORT,
    user=DBUSER,
    password=DBPASS,
    dbname=DBNAME,
    autocommit=True
)

print("=" * 80)
print("🔍 SEARCHING FOR LAST SUCCESSFUL RUN WITH KEYS...")
print("=" * 80)
print()

# Query to find the most recent run with persisted keys
query = f"""
WITH run_stats AS (
  SELECT 
    k.run_id,
    COUNT(DISTINCT k.entity_name || k.key_type) as entity_keytype_count,
    COUNT(*) as total_keys,
    MIN(r.run_ts) as run_timestamp,
    COUNT(DISTINCT r.fetch_mode) as mode_count,
    ARRAY_AGG(DISTINCT r.fetch_mode ORDER BY r.fetch_mode) as modes
  FROM {SCHEMA}.zipfian_keys_per_run k
  LEFT JOIN {SCHEMA}.zipfian_feature_serving_results_v5 r 
    ON k.run_id = r.run_id
  GROUP BY k.run_id
)
SELECT 
  run_id,
  run_timestamp,
  entity_keytype_count,
  total_keys,
  mode_count,
  modes
FROM run_stats
ORDER BY run_timestamp DESC NULLS LAST
LIMIT 5;
"""

with conn.cursor() as cur:
    cur.execute(query)
    rows = cur.fetchall()
    
    if not rows:
        print("❌ No previous runs with persisted keys found!")
        print("   You'll need to sample new keys (this will take ~1 hour).")
    else:
        print("📊 RECENT RUNS WITH KEYS:")
        print()
        print(f"{'Run ID':<12} {'Timestamp':<20} {'Entities':<10} {'Total Keys':<12} {'Modes':<8} {'Mode List'}")
        print("-" * 100)
        
        for i, row in enumerate(rows):
            run_id, run_ts, entity_count, total_keys, mode_count, modes = row
            ts_str = str(run_ts)[:19] if run_ts else "N/A"
            mode_list = str(modes) if modes else "[]"
            
            marker = "👉 LATEST" if i == 0 else ""
            print(f"{run_id:<12} {ts_str:<20} {entity_count:<10} {total_keys:<12} {mode_count or 0:<8} {mode_list} {marker}")
        
        print()
        print("=" * 80)
        print("✅ RECOMMENDED RUN_ID TO REUSE:")
        print("=" * 80)
        latest_run_id = rows[0][0]
        print()
        print(f"    {latest_run_id}")
        print()
        print("=" * 80)
        print("📝 HOW TO USE:")
        print("=" * 80)
        print(f'1. In the benchmark notebook, set widget: reuse_run_id = "{latest_run_id}"')
        print("2. Run the benchmark - it will skip key sampling and reuse these keys")
        print("3. Key sampling takes 1+ hour, so this saves significant time!")
        print()
        
        # Additional validation
        print("🔍 VALIDATING KEYS FOR LATEST RUN:")
        print()
        validation_query = f"""
        SELECT 
          entity_name,
          key_type,
          COUNT(*) as key_count
        FROM {SCHEMA}.zipfian_keys_per_run
        WHERE run_id = %s
        GROUP BY entity_name, key_type
        ORDER BY entity_name, key_type;
        """
        
        with conn.cursor() as cur2:
            cur2.execute(validation_query, (latest_run_id,))
            key_rows = cur2.fetchall()
            
            print(f"{'Entity':<25} {'Key Type':<10} {'Count'}")
            print("-" * 50)
            for entity, key_type, count in key_rows:
                print(f"{entity:<25} {key_type:<10} {count:>6}")
        
        print()
        print("=" * 80)

conn.close()
