#!/usr/bin/env python3
"""
Test RPC Request JSON Mode - Isolated Testing
Run this BEFORE running the full benchmark to validate the implementation.

Usage:
    python test_rpc_mode.py --host <lakebase_host> --user <user> --password <password>
"""

import psycopg
import json
import time
import argparse

def test_sql_function(conn):
    """Test 1: Verify SQL function exists and is callable."""
    print("\n" + "="*80)
    print("TEST 1: SQL Function Exists")
    print("="*80)
    
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 
                proname AS function_name,
                pg_get_function_arguments(oid) AS arguments,
                pg_get_function_result(oid) AS returns
            FROM pg_proc 
            WHERE proname = 'fetch_request_features' 
              AND pronamespace = 'features'::regnamespace
        """)
        result = cur.fetchone()
        
        if result:
            print("✅ Function exists!")
            print(f"   Name: {result[0]}")
            print(f"   Args: {result[1]}")
            print(f"   Returns: {result[2]}")
            return True
        else:
            print("❌ Function NOT found!")
            print("   Deploy with: ./deploy_rpc_function.sh <host> <user> benchmark")
            return False

def get_sample_keys(conn):
    """Test 2: Get real sample keys from database."""
    print("\n" + "="*80)
    print("TEST 2: Get Sample Keys")
    print("="*80)
    
    keys = {}
    
    with conn.cursor() as cur:
        # Card fingerprint
        cur.execute("""
            SELECT hash_key 
            FROM features.client_id_card_fingerprint__fraud_rates__30d 
            LIMIT 1
        """)
        keys['card_fingerprint'] = cur.fetchone()[0]
        
        # Customer email
        cur.execute("""
            SELECT hash_key 
            FROM features.client_id_customer_email_clean__fraud_rates__30d 
            LIMIT 1
        """)
        keys['customer_email'] = cur.fetchone()[0]
        
        # Cardholder name (note: _clean suffix)
        cur.execute("""
            SELECT hash_key 
            FROM features.client_id_cardholder_name_clean__fraud_rates__30d 
            LIMIT 1
        """)
        keys['cardholder_name'] = cur.fetchone()[0]
    
    print("✅ Sample keys retrieved:")
    for entity, key in keys.items():
        print(f"   {entity:20} → {key}")
    
    return keys

def test_function_call(conn, keys):
    """Test 3: Call RPC function with sample keys."""
    print("\n" + "="*80)
    print("TEST 3: RPC Function Call")
    print("="*80)
    
    start = time.perf_counter()
    
    with conn.cursor() as cur:
        cur.execute(
            "SELECT features.fetch_request_features(%s, %s, %s)",
            (keys['card_fingerprint'], keys['customer_email'], keys['cardholder_name'])
        )
        result = cur.fetchone()[0]
    
    end = time.perf_counter()
    latency_ms = (end - start) * 1000
    
    print(f"✅ Function executed successfully!")
    print(f"   Latency: {latency_ms:.2f}ms")
    print(f"   Result type: {type(result)}")
    
    return result, latency_ms

def validate_structure(result):
    """Test 4: Validate JSON structure."""
    print("\n" + "="*80)
    print("TEST 4: Validate JSON Structure")
    print("="*80)
    
    expected_entities = ['card_fingerprint', 'customer_email', 'cardholder_name']
    expected_groups = ['fraud_rates', 'time_since', 'good_rates']
    
    errors = []
    
    # Check top-level entities
    for entity in expected_entities:
        if entity not in result:
            errors.append(f"Missing entity: {entity}")
        else:
            print(f"✅ Entity '{entity}' present")
            
            # Check feature groups
            entity_data = result[entity]
            for group in expected_groups:
                if group not in entity_data:
                    errors.append(f"Missing group '{group}' in entity '{entity}'")
                else:
                    group_data = entity_data[group]
                    if not isinstance(group_data, list):
                        errors.append(f"Group '{group}' in '{entity}' is not a list (got {type(group_data)})")
                    else:
                        print(f"   → Group '{group}': {len(group_data)} rows")
    
    if errors:
        print("\n❌ Structure validation FAILED:")
        for error in errors:
            print(f"   • {error}")
        return False
    else:
        print("\n✅ Structure validation PASSED!")
        return True

def test_payload_size(result):
    """Test 5: Check payload size."""
    print("\n" + "="*80)
    print("TEST 5: Payload Size")
    print("="*80)
    
    payload_bytes = len(json.dumps(result))
    payload_kb = payload_bytes / 1024
    
    print(f"✅ Payload size: {payload_bytes:,} bytes ({payload_kb:.2f} KB)")
    
    if payload_kb > 100:
        print(f"⚠️  WARNING: Payload is large (>{payload_kb:.1f}KB). Consider compression.")
    else:
        print(f"✅ Payload size is reasonable")
    
    return payload_bytes

def test_performance(conn, keys, iterations=10):
    """Test 6: Performance test with multiple iterations."""
    print("\n" + "="*80)
    print(f"TEST 6: Performance Test ({iterations} iterations)")
    print("="*80)
    
    latencies = []
    
    for i in range(iterations):
        start = time.perf_counter()
        
        with conn.cursor() as cur:
            cur.execute(
                "SELECT features.fetch_request_features(%s, %s, %s)",
                (keys['card_fingerprint'], keys['customer_email'], keys['cardholder_name'])
            )
            _ = cur.fetchone()[0]
        
        end = time.perf_counter()
        latency_ms = (end - start) * 1000
        latencies.append(latency_ms)
        
        if i < 3 or i == iterations - 1:
            print(f"   Iteration {i+1:2}: {latency_ms:6.2f}ms")
    
    import statistics
    avg = statistics.mean(latencies)
    p50 = statistics.median(latencies)
    p99 = sorted(latencies)[int(len(latencies) * 0.99)] if len(latencies) >= 10 else max(latencies)
    
    print(f"\n✅ Performance Summary:")
    print(f"   Average: {avg:.2f}ms")
    print(f"   P50:     {p50:.2f}ms")
    print(f"   P99:     {p99:.2f}ms")
    print(f"   Min:     {min(latencies):.2f}ms")
    print(f"   Max:     {max(latencies):.2f}ms")
    
    return latencies

def main():
    parser = argparse.ArgumentParser(description="Test RPC Request JSON Mode")
    parser.add_argument("--host", required=True, help="Lakebase host")
    parser.add_argument("--user", required=True, help="Lakebase user")
    parser.add_argument("--password", required=True, help="Lakebase password")
    parser.add_argument("--database", default="benchmark", help="Database name")
    parser.add_argument("--iterations", type=int, default=10, help="Performance test iterations")
    
    args = parser.parse_args()
    
    print("\n" + "="*80)
    print("🧪 RPC REQUEST JSON MODE - TESTING")
    print("="*80)
    print(f"Host:     {args.host}")
    print(f"User:     {args.user}")
    print(f"Database: {args.database}")
    print("="*80)
    
    # Connect to database
    conn = psycopg.connect(
        host=args.host,
        port=5432,
        dbname=args.database,
        user=args.user,
        password=args.password,
        sslmode="require",
        connect_timeout=10
    )
    
    try:
        # Run tests
        if not test_sql_function(conn):
            print("\n❌ CRITICAL: SQL function not found. Deploy first!")
            return False
        
        keys = get_sample_keys(conn)
        result, latency = test_function_call(conn, keys)
        
        if not validate_structure(result):
            print("\n❌ CRITICAL: JSON structure validation failed!")
            return False
        
        payload_bytes = test_payload_size(result)
        latencies = test_performance(conn, keys, args.iterations)
        
        # Final summary
        print("\n" + "="*80)
        print("✅ ALL TESTS PASSED!")
        print("="*80)
        print(f"Function:     features.fetch_request_features()")
        print(f"Latency:      {latency:.2f}ms (first call)")
        print(f"Performance:  {statistics.mean(latencies):.2f}ms avg, {max(latencies):.2f}ms max")
        print(f"Payload:      {payload_bytes:,} bytes")
        print(f"Structure:    ✅ Valid (3 entities × 3 groups each)")
        print("="*80)
        print("\n✅ Ready to run benchmark with: fetch_mode='rpc_request_json'")
        print("   Or: run_all_modes=true to include RPC in full sweep")
        print()
        
        return True
        
    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        conn.close()

if __name__ == "__main__":
    import statistics
    success = main()
    exit(0 if success else 1)
