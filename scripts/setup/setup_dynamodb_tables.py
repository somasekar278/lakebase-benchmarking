"""
Create DynamoDB tables matching the Lakebase schema.
Run this once to set up DynamoDB tables.
"""
import boto3
import sys

# AWS Configuration
AWS_REGION = "us-west-2"  # Change to your preferred region

# Table definitions matching Lakebase schema
TABLES = {
    'fraud_reports_365d': {
        'AttributeDefinitions': [
            {'AttributeName': 'primary_key', 'AttributeType': 'S'}
        ],
        'KeySchema': [
            {'AttributeName': 'primary_key', 'KeyType': 'HASH'}
        ],
        'BillingMode': 'PAY_PER_REQUEST'  # On-demand pricing
    },
    'good_rate_90d_lag_730d': {
        'AttributeDefinitions': [
            {'AttributeName': 'primary_key', 'AttributeType': 'S'}
        ],
        'KeySchema': [
            {'AttributeName': 'primary_key', 'KeyType': 'HASH'}
        ],
        'BillingMode': 'PAY_PER_REQUEST'
    },
    'distinct_counts_amount_stats_365d': {
        'AttributeDefinitions': [
            {'AttributeName': 'primary_key', 'AttributeType': 'S'}
        ],
        'KeySchema': [
            {'AttributeName': 'primary_key', 'KeyType': 'HASH'}
        ],
        'BillingMode': 'PAY_PER_REQUEST'
    },
    'request_capture_times': {
        'AttributeDefinitions': [
            {'AttributeName': 'primary_key', 'AttributeType': 'S'}
        ],
        'KeySchema': [
            {'AttributeName': 'primary_key', 'KeyType': 'HASH'}
        ],
        'BillingMode': 'PAY_PER_REQUEST'
    }
}

def create_tables():
    """Create all DynamoDB tables"""
    dynamodb = boto3.client('dynamodb', region_name=AWS_REGION)
    
    print("=" * 80)
    print("Creating DynamoDB Tables")
    print("=" * 80)
    
    for table_name, config in TABLES.items():
        print(f"\n📦 Creating table: {table_name}")
        
        try:
            # Check if table exists
            try:
                dynamodb.describe_table(TableName=table_name)
                print(f"   ⚠️  Table already exists, skipping...")
                continue
            except dynamodb.exceptions.ResourceNotFoundException:
                pass
            
            # Create table
            response = dynamodb.create_table(
                TableName=table_name,
                **config
            )
            
            print(f"   ✅ Table creation initiated")
            print(f"   ⏳ Waiting for table to become active...")
            
            # Wait for table to be active
            waiter = dynamodb.get_waiter('table_exists')
            waiter.wait(TableName=table_name)
            
            print(f"   ✅ Table {table_name} is now active")
            
        except Exception as e:
            print(f"   ❌ Error creating table {table_name}: {e}")
            sys.exit(1)
    
    print("\n" + "=" * 80)
    print("✅ All tables created successfully!")
    print("=" * 80)
    
    print("\n📊 Table Summary:")
    for table_name in TABLES.keys():
        response = dynamodb.describe_table(TableName=table_name)
        table = response['Table']
        print(f"   • {table_name}")
        print(f"     - Status: {table['TableStatus']}")
        print(f"     - Billing: {table.get('BillingModeSummary', {}).get('BillingMode', 'PROVISIONED')}")
        print(f"     - ARN: {table['TableArn']}")
    
    print("\n🎯 Next Steps:")
    print("   1. Run: python scripts/setup/load_dynamodb_from_lakebase.py")
    print("   2. This will copy data from Lakebase to DynamoDB")
    print("   3. Then run benchmark_dynamodb in Databricks")

if __name__ == "__main__":
    print("\n⚙️  AWS Region:", AWS_REGION)
    confirm = input("\nCreate DynamoDB tables? (yes/no): ")
    
    if confirm.lower() == 'yes':
        create_tables()
    else:
        print("Cancelled.")

