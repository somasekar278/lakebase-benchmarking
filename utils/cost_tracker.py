"""
Cost tracking and analysis for multi-backend benchmarking.

Tracks costs for:
- Data loading
- Query execution
- Storage
- Network egress
- Additional services (backup, replication, etc.)
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from datetime import datetime
from enum import Enum


class CostCategory(Enum):
    """Categories of costs"""
    DATA_LOADING = "data_loading"
    QUERY_EXECUTION = "query_execution"
    STORAGE = "storage"
    NETWORK = "network"
    COMPUTE = "compute"
    BACKUP = "backup"
    REPLICATION = "replication"
    OTHER = "other"


@dataclass
class CostItem:
    """Single cost item"""
    category: CostCategory
    description: str
    amount: float
    currency: str = "USD"
    unit: str = ""  # e.g., "per GB", "per hour", "per 1M requests"
    quantity: float = 1.0
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def total_cost(self) -> float:
        """Total cost for this item"""
        return self.amount * self.quantity


@dataclass
class BenchmarkCost:
    """Cost summary for a benchmark run"""
    backend_name: str
    items: List[CostItem] = field(default_factory=list)
    currency: str = "USD"
    region: str = "us-east-1"
    
    def add_item(self, item: CostItem):
        """Add a cost item"""
        self.items.append(item)
    
    def add_cost(
        self,
        category: CostCategory,
        description: str,
        amount: float,
        quantity: float = 1.0,
        unit: str = "",
        **metadata
    ):
        """Add a cost item (convenience method)"""
        item = CostItem(
            category=category,
            description=description,
            amount=amount,
            currency=self.currency,
            unit=unit,
            quantity=quantity,
            metadata=metadata
        )
        self.add_item(item)
    
    def get_total_by_category(self, category: CostCategory) -> float:
        """Get total cost for a category"""
        return sum(
            item.total_cost for item in self.items
            if item.category == category
        )
    
    def get_total(self) -> float:
        """Get total cost"""
        return sum(item.total_cost for item in self.items)
    
    def get_breakdown(self) -> Dict[str, float]:
        """Get cost breakdown by category"""
        breakdown = {}
        for category in CostCategory:
            total = self.get_total_by_category(category)
            if total > 0:
                breakdown[category.value] = total
        return breakdown
    
    def get_summary(self) -> Dict[str, Any]:
        """Get cost summary"""
        return {
            'backend': self.backend_name,
            'total': self.get_total(),
            'currency': self.currency,
            'region': self.region,
            'breakdown': self.get_breakdown(),
            'items': len(self.items)
        }


class CostModel:
    """
    Base class for backend-specific cost models.
    
    Each backend implements its own pricing model.
    """
    
    def __init__(self, region: str = "us-east-1", currency: str = "USD"):
        self.region = region
        self.currency = currency
        self.pricing = self._load_pricing()
    
    def _load_pricing(self) -> Dict[str, Any]:
        """
        Load pricing data for this backend.
        Override in subclasses.
        """
        return {}
    
    def calculate_data_loading_cost(self, num_rows: int, data_size_gb: float) -> CostItem:
        """Calculate cost of loading data"""
        raise NotImplementedError
    
    def calculate_query_cost(self, num_queries: int, data_scanned_gb: float = 0) -> CostItem:
        """Calculate cost of executing queries"""
        raise NotImplementedError
    
    def calculate_storage_cost(self, data_size_gb: float, duration_hours: float) -> CostItem:
        """Calculate storage cost"""
        raise NotImplementedError
    
    def calculate_compute_cost(self, duration_hours: float, compute_units: float = 1.0) -> CostItem:
        """Calculate compute cost (for compute-based backends)"""
        raise NotImplementedError


class LakebaseCostModel(CostModel):
    """
    Cost model for Lakebase (Databricks compute).
    
    Based on:
    - Compute hours (DBU - Databricks Units)
    - Storage (Delta Lake storage)
    """
    
    def _load_pricing(self) -> Dict[str, Any]:
        """Load Lakebase/Databricks pricing"""
        # Note: These are example prices, adjust for actual region/tier
        return {
            'dbu_cost_per_hour': 0.55,  # SQL Compute DBU cost
            'storage_cost_per_gb_month': 0.15,  # Delta Lake storage
            'network_egress_per_gb': 0.09,  # Network egress
        }
    
    def calculate_data_loading_cost(self, num_rows: int, data_size_gb: float) -> CostItem:
        """
        Calculate cost of loading data to Lakebase.
        
        Primarily compute cost (Spark cluster running).
        """
        # Estimate: 1GB takes ~1 minute on medium cluster (~2 DBU)
        hours = (data_size_gb / 60) * 2  # 2 DBU cluster
        dbu_cost = hours * self.pricing['dbu_cost_per_hour']
        
        return CostItem(
            category=CostCategory.DATA_LOADING,
            description=f"Data loading to Lakebase ({num_rows:,} rows, {data_size_gb:.2f} GB)",
            amount=dbu_cost,
            currency=self.currency,
            unit="DBU hours",
            quantity=1.0,
            metadata={
                'num_rows': num_rows,
                'data_size_gb': data_size_gb,
                'estimated_hours': hours
            }
        )
    
    def calculate_query_cost(self, num_queries: int, data_scanned_gb: float = 0) -> CostItem:
        """
        Calculate cost of executing queries.
        
        Based on compute time (SQL Warehouse).
        """
        # Estimate: 100 queries take ~1 minute on SQL warehouse (~1 DBU)
        hours = (num_queries / 100 / 60) * 1  # 1 DBU SQL warehouse
        dbu_cost = hours * self.pricing['dbu_cost_per_hour']
        
        return CostItem(
            category=CostCategory.QUERY_EXECUTION,
            description=f"Query execution on Lakebase ({num_queries:,} queries)",
            amount=dbu_cost,
            currency=self.currency,
            unit="DBU hours",
            quantity=1.0,
            metadata={
                'num_queries': num_queries,
                'data_scanned_gb': data_scanned_gb,
                'estimated_hours': hours
            }
        )
    
    def calculate_storage_cost(self, data_size_gb: float, duration_hours: float) -> CostItem:
        """Calculate storage cost"""
        # Monthly storage cost, pro-rated to hours
        hours_per_month = 730
        storage_cost = data_size_gb * self.pricing['storage_cost_per_gb_month'] * (duration_hours / hours_per_month)
        
        return CostItem(
            category=CostCategory.STORAGE,
            description=f"Storage cost ({data_size_gb:.2f} GB for {duration_hours:.1f} hours)",
            amount=storage_cost,
            currency=self.currency,
            unit="GB-hours",
            quantity=1.0,
            metadata={
                'data_size_gb': data_size_gb,
                'duration_hours': duration_hours
            }
        )
    
    def calculate_compute_cost(self, duration_hours: float, compute_units: float = 1.0) -> CostItem:
        """Calculate compute cost"""
        cost = duration_hours * compute_units * self.pricing['dbu_cost_per_hour']
        
        return CostItem(
            category=CostCategory.COMPUTE,
            description=f"Compute cost ({duration_hours:.2f} hours @ {compute_units} DBU)",
            amount=cost,
            currency=self.currency,
            unit="DBU hours",
            quantity=1.0,
            metadata={
                'duration_hours': duration_hours,
                'compute_units': compute_units
            }
        )


class DynamoDBCostModel(CostModel):
    """
    Cost model for DynamoDB.
    
    Based on:
    - Read/Write Request Units (on-demand or provisioned)
    - Storage
    - Optional: Streams, backups, global tables
    """
    
    def _load_pricing(self) -> Dict[str, Any]:
        """Load DynamoDB pricing"""
        # US East (N. Virginia) pricing
        return {
            'write_request_per_million': 1.25,  # $1.25 per million write requests
            'read_request_per_million': 0.25,   # $0.25 per million read requests
            'storage_per_gb_month': 0.25,        # $0.25 per GB-month
            'backup_per_gb': 0.10,               # $0.10 per GB for on-demand backup
            'restore_per_gb': 0.15,              # $0.15 per GB for restore
        }
    
    def calculate_data_loading_cost(self, num_rows: int, data_size_gb: float) -> CostItem:
        """
        Calculate cost of loading data to DynamoDB.
        
        Each item write = 1 WCU (for items up to 1KB).
        """
        # Assume average item size ~1KB (1 WCU per write)
        num_writes = num_rows
        cost = (num_writes / 1_000_000.0) * self.pricing['write_request_per_million']
        
        return CostItem(
            category=CostCategory.DATA_LOADING,
            description=f"Data loading to DynamoDB ({num_rows:,} items)",
            amount=cost,
            currency=self.currency,
            unit=f"{num_writes:,} write requests",
            quantity=1.0,  # Cost already calculated in amount
            metadata={
                'num_rows': num_rows,
                'data_size_gb': data_size_gb,
                'num_writes': num_writes,
                'assumed_item_size_kb': 1
            }
        )
    
    def calculate_query_cost(self, num_queries: int, data_scanned_gb: float = 0) -> CostItem:
        """
        Calculate cost of executing queries.
        
        Assumes GetItem or batch_get_item (strongly consistent reads).
        """
        # Each strongly consistent read of up to 4KB = 1 RCU
        # Assume 25 keys per query, 1KB per item → 25 RCUs per query
        rcus_per_query = 25
        total_rcus = num_queries * rcus_per_query
        cost = (total_rcus / 1_000_000) * self.pricing['read_request_per_million']
        
        return CostItem(
            category=CostCategory.QUERY_EXECUTION,
            description=f"Query execution on DynamoDB ({num_queries:,} queries, {total_rcus:,} RCUs)",
            amount=cost,
            currency=self.currency,
            unit=f"{total_rcus:,} read requests",
            quantity=1.0,  # Cost already calculated in amount
            metadata={
                'num_queries': num_queries,
                'rcus_per_query': rcus_per_query,
                'total_rcus': total_rcus
            }
        )
    
    def calculate_storage_cost(self, data_size_gb: float, duration_hours: float) -> CostItem:
        """Calculate storage cost"""
        # Monthly storage cost, pro-rated to hours
        hours_per_month = 730
        storage_cost = data_size_gb * self.pricing['storage_per_gb_month'] * (duration_hours / hours_per_month)
        
        return CostItem(
            category=CostCategory.STORAGE,
            description=f"Storage cost ({data_size_gb:.2f} GB for {duration_hours:.1f} hours)",
            amount=storage_cost,
            currency=self.currency,
            unit="GB-hours",
            quantity=1.0,
            metadata={
                'data_size_gb': data_size_gb,
                'duration_hours': duration_hours
            }
        )
    
    def calculate_compute_cost(self, duration_hours: float, compute_units: float = 1.0) -> CostItem:
        """DynamoDB is serverless - no compute cost"""
        return CostItem(
            category=CostCategory.COMPUTE,
            description="No compute cost (serverless)",
            amount=0.0,
            currency=self.currency,
            unit="N/A",
            quantity=0,
            metadata={'serverless': True}
        )


class AuroraCostModel(CostModel):
    """
    Cost model for Aurora.
    
    Based on:
    - Instance hours (compute)
    - Storage (per GB-month)
    - I/O requests
    """
    
    def _load_pricing(self) -> Dict[str, Any]:
        """Load Aurora pricing"""
        # US East (N. Virginia) pricing
        return {
            'db_r6g_2xlarge_per_hour': 0.816,  # 8 vCPU, 64 GB RAM
            'storage_per_gb_month': 0.10,       # $0.10 per GB-month
            'io_request_per_million': 0.20,     # $0.20 per million I/O requests
            'backup_storage_per_gb': 0.021,     # $0.021 per GB-month (beyond free tier)
        }
    
    def calculate_data_loading_cost(self, num_rows: int, data_size_gb: float) -> CostItem:
        """Calculate cost of loading data to Aurora"""
        # Cost = instance hours + I/O requests
        # Estimate: 10GB/hour loading rate
        hours = data_size_gb / 10
        instance_cost = hours * self.pricing['db_r6g_2xlarge_per_hour']
        
        # I/O requests for writes
        io_requests = num_rows * 5  # Assume 5 I/O per row (inserts + index updates)
        io_cost = (io_requests / 1_000_000) * self.pricing['io_request_per_million']
        
        total_cost = instance_cost + io_cost
        
        return CostItem(
            category=CostCategory.DATA_LOADING,
            description=f"Data loading to Aurora ({num_rows:,} rows, {data_size_gb:.2f} GB)",
            amount=total_cost,
            currency=self.currency,
            unit="instance hours + I/O",
            quantity=1.0,
            metadata={
                'num_rows': num_rows,
                'data_size_gb': data_size_gb,
                'instance_hours': hours,
                'io_requests': io_requests,
                'instance_cost': instance_cost,
                'io_cost': io_cost
            }
        )
    
    def calculate_query_cost(self, num_queries: int, data_scanned_gb: float = 0) -> CostItem:
        """Calculate cost of executing queries"""
        # Assume 1 hour of query execution
        hours = 1.0
        instance_cost = hours * self.pricing['db_r6g_2xlarge_per_hour']
        
        # I/O requests for reads
        io_requests = num_queries * 10  # Assume 10 I/O per query
        io_cost = (io_requests / 1_000_000) * self.pricing['io_request_per_million']
        
        total_cost = instance_cost + io_cost
        
        return CostItem(
            category=CostCategory.QUERY_EXECUTION,
            description=f"Query execution on Aurora ({num_queries:,} queries)",
            amount=total_cost,
            currency=self.currency,
            unit="instance hours + I/O",
            quantity=1.0,
            metadata={
                'num_queries': num_queries,
                'instance_hours': hours,
                'io_requests': io_requests
            }
        )
    
    def calculate_storage_cost(self, data_size_gb: float, duration_hours: float) -> CostItem:
        """Calculate storage cost"""
        hours_per_month = 730
        storage_cost = data_size_gb * self.pricing['storage_per_gb_month'] * (duration_hours / hours_per_month)
        
        return CostItem(
            category=CostCategory.STORAGE,
            description=f"Storage cost ({data_size_gb:.2f} GB for {duration_hours:.1f} hours)",
            amount=storage_cost,
            currency=self.currency,
            unit="GB-hours",
            quantity=1.0
        )
    
    def calculate_compute_cost(self, duration_hours: float, compute_units: float = 1.0) -> CostItem:
        """Calculate compute cost (instance hours)"""
        cost = duration_hours * self.pricing['db_r6g_2xlarge_per_hour']
        
        return CostItem(
            category=CostCategory.COMPUTE,
            description=f"Compute cost ({duration_hours:.2f} hours)",
            amount=cost,
            currency=self.currency,
            unit="instance hours",
            quantity=1.0
        )


class CosmosDBCostModel(CostModel):
    """
    Cost model for Azure Cosmos DB.
    
    Based on:
    - Request Units per second (RU/s)
    - Storage
    - Optional: Multi-region replication, backups
    """
    
    def _load_pricing(self) -> Dict[str, Any]:
        """Load Cosmos DB pricing"""
        # US East pricing (provisioned throughput)
        return {
            'cost_per_100_rus_hour': 0.008,  # $0.008 per 100 RU/s per hour
            'storage_per_gb_month': 0.25,     # $0.25 per GB-month
            'backup_per_gb': 0.12,            # $0.12 per GB for backup storage
        }
    
    def calculate_data_loading_cost(self, num_rows: int, data_size_gb: float) -> CostItem:
        """Calculate cost of loading data to Cosmos DB"""
        # Estimate: 10 RU per write (1KB item)
        rus_per_write = 10
        total_rus = num_rows * rus_per_write
        
        # Assume loading takes 1 hour with auto-scale
        # Need to provision enough RU/s to handle load
        rus_per_second = total_rus / 3600
        hours = 1.0
        cost = (rus_per_second / 100) * self.pricing['cost_per_100_rus_hour'] * hours
        
        return CostItem(
            category=CostCategory.DATA_LOADING,
            description=f"Data loading to Cosmos DB ({num_rows:,} items)",
            amount=cost,
            currency=self.currency,
            unit="RU/s hours",
            quantity=1.0,
            metadata={
                'num_rows': num_rows,
                'rus_per_write': rus_per_write,
                'total_rus': total_rus,
                'rus_per_second': rus_per_second
            }
        )
    
    def calculate_query_cost(self, num_queries: int, data_scanned_gb: float = 0) -> CostItem:
        """Calculate cost of executing queries"""
        # Estimate: 5 RU per point read
        rus_per_query = 5 * 25  # 25 items per query
        total_rus = num_queries * rus_per_query
        
        # Assume 1 hour of query execution
        rus_per_second = total_rus / 3600
        hours = 1.0
        cost = (rus_per_second / 100) * self.pricing['cost_per_100_rus_hour'] * hours
        
        return CostItem(
            category=CostCategory.QUERY_EXECUTION,
            description=f"Query execution on Cosmos DB ({num_queries:,} queries)",
            amount=cost,
            currency=self.currency,
            unit="RU/s hours",
            quantity=1.0,
            metadata={
                'num_queries': num_queries,
                'rus_per_query': rus_per_query,
                'total_rus': total_rus
            }
        )
    
    def calculate_storage_cost(self, data_size_gb: float, duration_hours: float) -> CostItem:
        """Calculate storage cost"""
        hours_per_month = 730
        storage_cost = data_size_gb * self.pricing['storage_per_gb_month'] * (duration_hours / hours_per_month)
        
        return CostItem(
            category=CostCategory.STORAGE,
            description=f"Storage cost ({data_size_gb:.2f} GB for {duration_hours:.1f} hours)",
            amount=storage_cost,
            currency=self.currency,
            unit="GB-hours",
            quantity=1.0
        )
    
    def calculate_compute_cost(self, duration_hours: float, compute_units: float = 1.0) -> CostItem:
        """Cosmos DB: compute is RU/s based (included in query/load costs)"""
        return CostItem(
            category=CostCategory.COMPUTE,
            description="No separate compute cost (RU/s based)",
            amount=0.0,
            currency=self.currency,
            unit="N/A",
            quantity=0
        )


# Factory for cost models
COST_MODELS = {
    'lakebase': LakebaseCostModel,
    'dynamodb': DynamoDBCostModel,
    'aurora': AuroraCostModel,
    'cosmosdb': CosmosDBCostModel,
}


def get_cost_model(backend_name: str, region: str = "us-east-1") -> CostModel:
    """Get cost model for a backend"""
    if backend_name not in COST_MODELS:
        raise ValueError(f"Cost model for '{backend_name}' not found")
    
    return COST_MODELS[backend_name](region=region)


if __name__ == "__main__":
    # Example usage
    print("Cost Tracking System")
    print("=" * 70)
    
    # Example benchmark scenario
    num_rows = 100_000_000  # 100M rows
    data_size_gb = 50  # 50 GB
    num_queries = 100  # 100 benchmark iterations
    duration_hours = 2  # 2 hour benchmark
    
    print(f"\nScenario: {num_rows:,} rows, {data_size_gb} GB, {num_queries} queries, {duration_hours}h\n")
    
    for backend_name in ['lakebase', 'dynamodb', 'aurora', 'cosmosdb']:
        print(f"\n{backend_name.upper()}:")
        print("-" * 70)
        
        model = get_cost_model(backend_name)
        cost_summary = BenchmarkCost(backend_name=backend_name)
        
        # Calculate costs
        load_cost = model.calculate_data_loading_cost(num_rows, data_size_gb)
        query_cost = model.calculate_query_cost(num_queries)
        storage_cost = model.calculate_storage_cost(data_size_gb, duration_hours)
        
        cost_summary.add_item(load_cost)
        cost_summary.add_item(query_cost)
        cost_summary.add_item(storage_cost)
        
        # Print breakdown
        print(f"  Data Loading:   ${load_cost.total_cost:.4f}")
        print(f"  Query Execution: ${query_cost.total_cost:.4f}")
        print(f"  Storage:        ${storage_cost.total_cost:.4f}")
        print(f"  TOTAL:          ${cost_summary.get_total():.4f}")
    
    print("\n" + "=" * 70)

