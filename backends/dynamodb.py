"""
DynamoDB backend implementation.

DynamoDB is a key-value store, so schema translation is required.
"""

import time
import boto3
from typing import Dict, List, Any
from core.backend import Backend, BackendType, QueryResult


class DynamoDBBackend(Backend):
    """
    DynamoDB backend for benchmarking.
    
    Note: DynamoDB is schema-less and optimized for key-value lookups.
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.backend_type = BackendType.DYNAMODB
        self.connection = None  # boto3 client
        self.table_prefix = config.get('table_prefix', 'benchmark_')
        self.region = config.get('region', 'us-east-1')
    
    def connect(self) -> bool:
        """Connect to DynamoDB"""
        try:
            self.connection = boto3.client(
                'dynamodb',
                region_name=self.region,
                aws_access_key_id=self.config.get('access_key_id'),
                aws_secret_access_key=self.config.get('secret_access_key')
            )
            
            # Test connection
            self.connection.list_tables(Limit=1)
            return True
        
        except Exception as e:
            print(f"❌ DynamoDB connection failed: {e}")
            return False
    
    def disconnect(self) -> bool:
        """Disconnect from DynamoDB"""
        # boto3 client doesn't need explicit disconnect
        self.connection = None
        return True
    
    def create_tables(self, workload) -> bool:
        """
        Create DynamoDB tables based on workload definition.
        
        Note: DynamoDB tables are created with primary_key as partition key.
        """
        if not self.connection:
            return False
        
        try:
            for table_def in workload.get_tables():
                table_name = self.table_prefix + table_def.name
                
                # Check if table exists
                existing_tables = self.connection.list_tables()['TableNames']
                if table_name in existing_tables:
                    print(f"⚠️  Table {table_name} already exists, skipping...")
                    continue
                
                # Create table with primary_key as partition key
                self.connection.create_table(
                    TableName=table_name,
                    KeySchema=[
                        {
                            'AttributeName': 'primary_key',
                            'KeyType': 'HASH'  # Partition key
                        }
                    ],
                    AttributeDefinitions=[
                        {
                            'AttributeName': 'primary_key',
                            'AttributeType': 'S'  # String
                        }
                    ],
                    BillingMode='PAY_PER_REQUEST',  # On-demand pricing
                    # Or use ProvisionedThroughput for reserved capacity:
                    # ProvisionedThroughput={
                    #     'ReadCapacityUnits': 1000,
                    #     'WriteCapacityUnits': 1000
                    # }
                )
                
                print(f"✅ Created DynamoDB table: {table_name}")
            
            # Wait for tables to be active
            print("⏳ Waiting for tables to be active...")
            for table_def in workload.get_tables():
                table_name = self.table_prefix + table_def.name
                waiter = self.connection.get_waiter('table_exists')
                waiter.wait(TableName=table_name)
            
            print("✅ All tables active")
            return True
        
        except Exception as e:
            print(f"❌ Table creation failed: {e}")
            return False
    
    def load_data(self, table_name: str, data: List[Dict[str, Any]]) -> bool:
        """
        Load data into DynamoDB table.
        
        Uses batch_write_item for efficient bulk loading.
        """
        if not self.connection or not data:
            return False
        
        try:
            full_table_name = self.table_prefix + table_name
            
            # DynamoDB batch_write_item accepts max 25 items at a time
            batch_size = 25
            
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                
                # Convert data to DynamoDB format
                request_items = {
                    full_table_name: [
                        {
                            'PutRequest': {
                                'Item': self._convert_to_dynamodb_item(row)
                            }
                        }
                        for row in batch
                    ]
                }
                
                self.connection.batch_write_item(RequestItems=request_items)
            
            print(f"✅ Loaded {len(data)} items into {full_table_name}")
            return True
        
        except Exception as e:
            print(f"❌ Data load failed: {e}")
            return False
    
    def query(self, query_pattern, keys: List[str]) -> QueryResult:
        """
        Execute query (key lookup) in DynamoDB.
        
        Uses batch_get_item for multiple keys.
        """
        if not self.connection:
            return QueryResult(
                success=False,
                latency_ms=0,
                rows_returned=0,
                error="Not connected"
            )
        
        try:
            start = time.time()
            
            table = query_pattern.tables[0]
            full_table_name = self.table_prefix + table
            
            # DynamoDB batch_get_item accepts max 100 keys at a time
            all_items = []
            batch_size = 100
            
            for i in range(0, len(keys), batch_size):
                batch_keys = keys[i:i + batch_size]
                
                response = self.connection.batch_get_item(
                    RequestItems={
                        full_table_name: {
                            'Keys': [{'primary_key': {'S': key}} for key in batch_keys]
                        }
                    }
                )
                
                all_items.extend(response.get('Responses', {}).get(full_table_name, []))
            
            latency_ms = (time.time() - start) * 1000
            
            return QueryResult(
                success=True,
                latency_ms=latency_ms,
                rows_returned=len(all_items),
                metadata={'backend': 'dynamodb', 'method': 'batch_get_item'}
            )
        
        except Exception as e:
            latency_ms = (time.time() - start) * 1000 if 'start' in locals() else 0
            return QueryResult(
                success=False,
                latency_ms=latency_ms,
                rows_returned=0,
                error=str(e)
            )
    
    def batch_query(self, query_patterns: List, keys_dict: Dict[str, List[str]]) -> QueryResult:
        """
        Execute batch query across multiple tables.
        
        DynamoDB's batch_get_item supports multiple tables in one request.
        """
        if not self.connection:
            return QueryResult(
                success=False,
                latency_ms=0,
                rows_returned=0,
                error="Not connected"
            )
        
        try:
            start = time.time()
            
            # Build request for multiple tables
            request_items = {}
            
            for table_name, keys in keys_dict.items():
                if not keys:
                    continue
                
                full_table_name = self.table_prefix + table_name
                
                # DynamoDB batch_get_item max 100 keys total across all tables
                request_items[full_table_name] = {
                    'Keys': [{'primary_key': {'S': key}} for key in keys[:100]]
                }
            
            # Execute batch get
            response = self.connection.batch_get_item(RequestItems=request_items)
            
            # Count total items returned
            total_items = sum(
                len(items) for items in response.get('Responses', {}).values()
            )
            
            latency_ms = (time.time() - start) * 1000
            
            return QueryResult(
                success=True,
                latency_ms=latency_ms,
                rows_returned=total_items,
                metadata={
                    'backend': 'dynamodb',
                    'method': 'batch_get_item',
                    'tables': len(keys_dict)
                }
            )
        
        except Exception as e:
            latency_ms = (time.time() - start) * 1000 if 'start' in locals() else 0
            return QueryResult(
                success=False,
                latency_ms=latency_ms,
                rows_returned=0,
                error=str(e)
            )
    
    def cleanup(self) -> bool:
        """Delete all tables"""
        if not self.connection:
            return False
        
        try:
            # List and delete all tables with our prefix
            tables = self.connection.list_tables()['TableNames']
            
            for table_name in tables:
                if table_name.startswith(self.table_prefix):
                    self.connection.delete_table(TableName=table_name)
                    print(f"🗑️  Deleted table: {table_name}")
            
            return True
        
        except Exception as e:
            print(f"❌ Cleanup failed: {e}")
            return False
    
    def supports_feature(self, feature: str) -> bool:
        """Check feature support"""
        supported = {
            'key_value_lookup', 'batch_operations', 'secondary_indexes',
            'streams', 'ttl', 'transactions'
        }
        return feature.lower() in supported
    
    def _convert_to_dynamodb_item(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert a Python dict to DynamoDB item format.
        
        Example:
            {'name': 'John', 'age': 30} 
            → {'name': {'S': 'John'}, 'age': {'N': '30'}}
        """
        item = {}
        
        for key, value in row.items():
            if value is None:
                continue
            elif isinstance(value, str):
                item[key] = {'S': value}
            elif isinstance(value, (int, float)):
                item[key] = {'N': str(value)}
            elif isinstance(value, bool):
                item[key] = {'BOOL': value}
            elif isinstance(value, bytes):
                item[key] = {'B': value}
            elif isinstance(value, dict):
                item[key] = {'M': self._convert_to_dynamodb_item(value)}
            elif isinstance(value, list):
                item[key] = {'L': [self._convert_to_dynamodb_item({'v': v})['v'] for v in value]}
            else:
                # Fallback to string
                item[key] = {'S': str(value)}
        
        return item
    
    def estimate_cost(self, num_reads: int, num_writes: int, data_size_gb: float) -> Dict[str, float]:
        """
        Estimate DynamoDB costs.
        
        Args:
            num_reads: Number of read operations
            num_writes: Number of write operations
            data_size_gb: Data size in GB
        
        Returns:
            Dict with cost breakdown
        """
        # DynamoDB pricing (example, adjust for region)
        # On-demand pricing (us-east-1):
        read_cost_per_million = 0.25  # $0.25 per million read request units
        write_cost_per_million = 1.25  # $1.25 per million write request units
        storage_cost_per_gb = 0.25  # $0.25 per GB-month
        
        read_cost = (num_reads / 1_000_000) * read_cost_per_million
        write_cost = (num_writes / 1_000_000) * write_cost_per_million
        storage_cost = data_size_gb * storage_cost_per_gb
        
        return {
            'read_cost': read_cost,
            'write_cost': write_cost,
            'storage_cost': storage_cost,
            'total_monthly': read_cost + write_cost + storage_cost,
            'currency': 'USD'
        }

