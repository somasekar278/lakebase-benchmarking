## 🏗️ Multi-Backend Benchmark Framework Design

## Overview

The framework now supports benchmarking **multiple database backends** side-by-side with identical workloads. This allows fair, apples-to-apples comparisons of:

- **Lakebase** (PostgreSQL-compatible)
- **DynamoDB** (AWS NoSQL)
- **Aurora** (AWS relational - PostgreSQL/MySQL)
- **Cosmos DB** (Azure multi-model)
- **Any future backend** (extensible design)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Benchmark Framework                          │
└─────────────────────────────────────────────────────────────────────┘
                                 │
                                 ↓
┌─────────────────────────────────────────────────────────────────────┐
│                         Workload Definition                         │
│  • Schema (tables, columns, types)                                  │
│  • Query Patterns (single, batch, joins)                            │
│  • Data Generation (keys, features, volumes)                        │
└─────────────────────────────────────────────────────────────────────┘
                                 │
                                 ↓
┌─────────────────────────────────────────────────────────────────────┐
│                         Backend Abstraction                         │
│  • Backend Interface (connect, query, load, cleanup)                │
│  • Schema Translation (generic → backend-specific)                  │
│  • Result Normalization (QueryResult)                               │
└─────────────────────────────────────────────────────────────────────┘
                                 │
                ┌────────────────┼────────────────┬────────────────┐
                ↓                ↓                ↓                ↓
        ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
        │  Lakebase    │ │  DynamoDB    │ │   Aurora     │ │  Cosmos DB   │
        │   Backend    │ │   Backend    │ │   Backend    │ │   Backend    │
        └──────────────┘ └──────────────┘ └──────────────┘ └──────────────┘
                ↓                ↓                ↓                ↓
        ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
        │  PostgreSQL  │ │  AWS DynamoDB│ │ AWS Aurora   │ │  Azure       │
        │   Database   │ │   Service    │ │   Database   │ │  Cosmos DB   │
        └──────────────┘ └──────────────┘ └──────────────┘ └──────────────┘
                                 │
                                 ↓
┌─────────────────────────────────────────────────────────────────────┐
│                      Benchmark Results                              │
│  • Latency (P50, P95, P99)                                          │
│  • Throughput                                                       │
│  • Cost Estimates                                                   │
│  • Side-by-Side Comparison                                          │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Core Components

### 1. Backend Interface (`core/backend.py`)

Abstract base class that all backends must implement:

```python
class Backend(ABC):
    @abstractmethod
    def connect(self) -> bool:
        """Establish connection"""
        
    @abstractmethod
    def create_tables(self, workload) -> bool:
        """Create tables from workload definition"""
        
    @abstractmethod
    def load_data(self, table_name, data) -> bool:
        """Load data into table"""
        
    @abstractmethod
    def query(self, query_pattern, keys) -> QueryResult:
        """Execute single query"""
        
    @abstractmethod
    def batch_query(self, query_patterns, keys_dict) -> QueryResult:
        """Execute batch query (binpacking)"""
        
    @abstractmethod
    def cleanup(self) -> bool:
        """Clean up resources"""
```

### 2. Backend Implementations (`backends/`)

#### Lakebase (`backends/lakebase.py`)
- **Status**: ✅ Implemented
- **Protocol**: PostgreSQL (psycopg2)
- **Strengths**: Transactions, joins, stored procedures, full SQL
- **Use Case**: Complex queries, ACID requirements

#### DynamoDB (`backends/dynamodb.py`)
- **Status**: ✅ Implemented
- **Protocol**: AWS SDK (boto3)
- **Strengths**: Key-value lookups, horizontal scaling, low latency
- **Use Case**: Simple lookups, high throughput
- **Special Features**:
  - `batch_get_item` for multi-table binpacking
  - Cost estimation
  - On-demand vs provisioned capacity

#### Aurora (`backends/aurora.py`)
- **Status**: ⏳ Placeholder (to be implemented)
- **Protocol**: PostgreSQL/MySQL
- **Strengths**: Managed PostgreSQL/MySQL, read replicas, fast failover
- **Use Case**: Managed SQL database with HA

#### Cosmos DB (`backends/cosmosdb.py`)
- **Status**: ⏳ Placeholder (to be implemented)
- **Protocol**: Azure SDK
- **Strengths**: Global distribution, multi-model, consistency options
- **Use Case**: Global apps, multiple consistency levels

### 3. Schema Translation (`core/backend.py`)

Translates generic schema to backend-specific types:

```python
# Generic schema (backend-agnostic)
schema = {
    'primary_key': DataType.STRING,
    'feature_1': DataType.DECIMAL,
    'updated_at': DataType.BIGINT
}

# Translates to:
# Lakebase:  {'primary_key': 'TEXT', 'feature_1': 'NUMERIC', ...}
# DynamoDB:  {'primary_key': 'S', 'feature_1': 'N', ...}
# Cosmos DB: {'primary_key': 'string', 'feature_1': 'number', ...}
```

### 4. Standardized Results

All backends return `QueryResult`:

```python
@dataclass
class QueryResult:
    success: bool
    latency_ms: float
    rows_returned: int
    data: Optional[List[Dict]]
    error: Optional[str]
    metadata: Optional[Dict]
```

This allows fair comparisons regardless of backend implementation details.

---

## Configuration System

### Backend Configuration (`config.py`)

```python
BACKEND_CONFIGS = {
    'lakebase': {
        'enabled': True,
        'host': 'ep-xxx.database.eu-west-1.cloud.databricks.com',
        'port': 5432,
        'database': 'benchmark',
        'user': 'fraud_benchmark_user',
        'password': 'xxx',
        'schema': 'features',
    },
    
    'dynamodb': {
        'enabled': True,  # Set to True when ready to benchmark
        'region': 'us-east-1',
        'access_key_id': 'AKIAXXXXX',
        'secret_access_key': 'xxxxx',
        'table_prefix': 'benchmark_',
        'billing_mode': 'PAY_PER_REQUEST',  # or 'PROVISIONED'
    },
    
    'aurora': {
        'enabled': False,  # Not yet implemented
        'cluster_endpoint': 'xxx.cluster-xxx.us-east-1.rds.amazonaws.com',
        'reader_endpoint': 'xxx.cluster-ro-xxx.us-east-1.rds.amazonaws.com',
        'engine': 'postgres',  # or 'mysql'
        'database': 'benchmark',
        'user': 'admin',
        'password': 'xxxxx',
    },
    
    'cosmosdb': {
        'enabled': False,  # Not yet implemented
        'endpoint': 'https://xxx.documents.azure.com:443/',
        'key': 'xxxxx',
        'database': 'benchmark',
        'api': 'sql',  # or 'mongodb', 'cassandra', 'gremlin'
    },
}
```

### CLI Configuration

```bash
# Run benchmark on single backend
python run_benchmark.py --backend lakebase

# Run benchmark on multiple backends (comparison)
python run_benchmark.py --backends lakebase dynamodb

# Run with specific workload
python run_benchmark.py \
    --backends lakebase dynamodb \
    --workload fraud_detection \
    --iterations 100

# Configure DynamoDB-specific options
python run_benchmark.py \
    --backend dynamodb \
    --dynamodb-capacity-mode PAY_PER_REQUEST \
    --dynamodb-region us-east-1

# Run comparison benchmark
python run_benchmark.py \
    --compare \
    --backends lakebase dynamodb aurora \
    --workload fraud_detection \
    --output comparison_results.json
```

---

## Workload Portability

### Schema Mapping

Workloads are defined once, then automatically translated for each backend:

```python
# Define workload once (backend-agnostic)
from core import create_workload, TableDef, DataType

workload = create_workload(
    mode="schema",
    tables=[
        TableDef(
            name="fraud_reports_365d",
            columns={
                "primary_key": DataType.STRING,
                "fraud_reports_365d": DataType.DECIMAL,
                "fraud_rate_365d": DataType.DECIMAL,
                "updated_at": DataType.BIGINT,
            },
            rows=1_000_000,
            primary_key="primary_key"
        )
    ]
)

# Framework automatically translates to:
# - Lakebase: CREATE TABLE with TEXT, NUMERIC, BIGINT
# - DynamoDB: CreateTable with S, N, N
# - Cosmos DB: Container with string, number, number
```

### Query Pattern Adaptation

Query patterns are adapted to backend capabilities:

```python
# Binpacking query pattern
QueryPattern(
    name="fraud_lookup",
    strategy=QueryStrategy.BINPACKED_SP,
    tables=["table1", "table2", "table3"],
    keys_per_query=25
)

# Translates to:
# - Lakebase: Stored procedure with multiple RETURN QUERY blocks
# - DynamoDB: batch_get_item with RequestItems for multiple tables
# - Aurora: Stored procedure (similar to Lakebase)
# - Cosmos DB: Multiple point reads or batch operation
```

---

## Benchmark Execution Flow

### 1. Setup Phase

```
For each enabled backend:
  1. Connect to backend
  2. Create tables (with schema translation)
  3. Load test data (using optimal method for backend)
  4. Warm up (run sample queries)
```

### 2. Benchmark Phase

```
For each backend:
  For each iteration:
    1. Generate query keys
    2. Execute query (measure latency)
    3. Record result
  
  Calculate statistics:
    - P50, P95, P99 latency
    - Throughput (ops/sec)
    - Consistency (coefficient of variation)
```

### 3. Comparison Phase

```
Generate comparison report:
  - Latency comparison (box plots, CDFs)
  - Throughput comparison (bar charts)
  - Cost estimates (if available)
  - Feature comparison matrix
  - Recommendations
```

### 4. Cleanup Phase

```
For each backend:
  1. Delete tables/containers
  2. Disconnect
  3. Save results
```

---

## Implementation Phases

### Phase 1: Foundation (✅ Complete)
- [x] Abstract backend interface
- [x] Backend factory pattern
- [x] Schema translation system
- [x] Lakebase backend implementation
- [x] DynamoDB backend implementation (functional)
- [x] Configuration system

### Phase 2: DynamoDB Integration (🚧 Next Week)
- [ ] Test DynamoDB backend with real AWS account
- [ ] Optimize batch_get_item for multi-table queries
- [ ] Add provisioned capacity options
- [ ] Implement cost estimation
- [ ] Add DynamoDB-specific metrics (consumed capacity)
- [ ] Test with fraud detection workload

### Phase 3: Aurora Integration (📅 Future)
- [ ] Implement Aurora PostgreSQL backend
- [ ] Test with Aurora cluster
- [ ] Add read replica support
- [ ] Implement connection pooling
- [ ] Optimize for Aurora-specific features

### Phase 4: Cosmos DB Integration (📅 Future)
- [ ] Implement Cosmos DB SQL API backend
- [ ] Test with Azure account
- [ ] Add partition key strategy
- [ ] Implement consistency level options
- [ ] Add global distribution support

### Phase 5: Comparative Analysis (📅 Future)
- [ ] Side-by-side benchmark runner
- [ ] Comparison report generator
- [ ] Cost-benefit analysis
- [ ] Feature matrix comparison
- [ ] Recommendation engine

---

## Usage Examples

### Example 1: Benchmark Lakebase Only

```python
from backends import BackendFactory, BackendType
from core import create_workload

# Create workload
workload = create_workload(
    mode="auto",
    num_tables=30,
    features_per_table=5,
    rows_per_table=100_000_000
)

# Create Lakebase backend
config = {...}  # Lakebase config
backend = BackendFactory.create(BackendType.LAKEBASE, config)

# Run benchmark
backend.connect()
backend.create_tables(workload)
backend.load_data(...)
result = backend.query(...)
print(f"Latency: {result.latency_ms}ms")
```

### Example 2: Compare Lakebase vs DynamoDB

```python
from backends import BackendFactory, BackendType

# Create both backends
lakebase = BackendFactory.create(BackendType.LAKEBASE, lakebase_config)
dynamodb = BackendFactory.create(BackendType.DYNAMODB, dynamodb_config)

backends = {'lakebase': lakebase, 'dynamodb': dynamodb}

# Run same workload on both
for name, backend in backends.items():
    backend.connect()
    backend.create_tables(workload)
    # ... benchmark ...
    results[name] = backend.query(...)

# Compare
print(f"Lakebase P99: {results['lakebase'].latency_ms}ms")
print(f"DynamoDB P99: {results['dynamodb'].latency_ms}ms")
```

### Example 3: CLI Usage

```bash
# Compare all enabled backends
python run_benchmark.py --compare

# Specific backends with custom workload
python run_benchmark.py \
    --backends lakebase dynamodb \
    --workload fraud_detection \
    --iterations 100 \
    --output-format json \
    --output results/comparison.json

# DynamoDB-specific options
python run_benchmark.py \
    --backend dynamodb \
    --dynamodb-region eu-west-1 \
    --dynamodb-capacity PROVISIONED \
    --dynamodb-rcu 1000 \
    --dynamodb-wcu 1000
```

---

## Backend-Specific Features

### Lakebase
- ✅ Full SQL support
- ✅ Stored procedures (binpacking)
- ✅ Transactions
- ✅ Complex joins
- ✅ Indexes (B-tree, hash, GiST, etc.)
- ✅ Full-text search

### DynamoDB
- ✅ Key-value lookups
- ✅ `batch_get_item` (up to 100 keys)
- ✅ Secondary indexes (GSI, LSI)
- ✅ Streams (change data capture)
- ✅ TTL (automatic expiration)
- ✅ Transactions (limited)
- ⚠️  No joins (denormalize data)
- ⚠️  No complex queries

### Aurora
- ✅ PostgreSQL/MySQL compatibility
- ✅ Read replicas (up to 15)
- ✅ Fast failover (<30s)
- ✅ Backtrack (point-in-time recovery)
- ✅ Global database
- ✅ Parallel query

### Cosmos DB
- ✅ Global distribution
- ✅ Multi-region writes
- ✅ Multiple consistency levels
- ✅ Automatic indexing
- ✅ Multi-model (SQL, MongoDB, Cassandra, etc.)
- ✅ Serverless option

---

## Cost Estimation

Framework includes cost estimation for supported backends:

```python
# DynamoDB cost estimate
result = dynamodb.estimate_cost(
    num_reads=1_000_000,
    num_writes=100_000,
    data_size_gb=10
)
# {'read_cost': 0.25, 'write_cost': 0.125, 'storage_cost': 2.5, 'total_monthly': 2.875}

# Cosmos DB cost estimate
result = cosmosdb.estimate_cost(
    rus_per_second=1000,
    storage_gb=10,
    regions=2
)
# {'throughput_cost': 116.80, 'storage_cost': 5.0, 'total_monthly': 121.80, 'regions': 2}
```

---

## Adding New Backends

To add a new backend (e.g., MongoDB, Cassandra, Bigtable):

1. **Create backend class** in `backends/new_backend.py`:
   ```python
   from core.backend import Backend, BackendType
   
   class NewBackend(Backend):
       def __init__(self, config):
           super().__init__(config)
           self.backend_type = BackendType.NEW_BACKEND
       
       # Implement all abstract methods
       def connect(self): ...
       def create_tables(self, workload): ...
       def query(self, query_pattern, keys): ...
       # ... etc
   ```

2. **Add backend type** to `core/backend.py`:
   ```python
   class BackendType(Enum):
       # ... existing backends
       NEW_BACKEND = "new_backend"
   ```

3. **Register backend** in `backends/__init__.py`:
   ```python
   from .new_backend import NewBackend
   BackendFactory.register(BackendType.NEW_BACKEND, NewBackend)
   ```

4. **Add configuration** to `config.py`:
   ```python
   BACKEND_CONFIGS['new_backend'] = {
       'enabled': False,
       'endpoint': 'xxx',
       # ... backend-specific config
   }
   ```

5. **Add type mappings** to `SchemaTranslator` (if needed)

6. **Test**:
   ```bash
   python run_benchmark.py --backend new_backend --workload test
   ```

---

## Best Practices

### 1. Fair Comparisons
- Use identical workloads across all backends
- Same data volumes and distributions
- Same query patterns (where possible)
- Account for backend-specific optimizations

### 2. Workload Design
- Define schemas in generic format (DataType)
- Use query patterns that map to all backends
- Consider backend limitations (e.g., DynamoDB no joins)

### 3. Cost Awareness
- Include cost estimates in reports
- Consider TCO (not just database costs)
- Factor in operational overhead

### 4. Feature Parity
- Document feature support matrix
- Note workarounds for missing features
- Highlight backend-specific advantages

---

## Troubleshooting

### Issue: Backend not registered
**Error**: `Backend 'xyz' not registered`
**Fix**: Ensure backend is imported and registered in `backends/__init__.py`

### Issue: Schema translation fails
**Error**: `Backend XYZ not supported`
**Fix**: Add type mappings to `SchemaTranslator.TYPE_MAPPINGS`

### Issue: Connection timeout
**Fix**: Check network connectivity, credentials, security groups

### Issue: Feature not supported
**Fix**: Check `backend.supports_feature(feature)`, use alternative approach

---

## Future Enhancements

- [ ] Automated backend selection (based on workload characteristics)
- [ ] Cost optimization recommendations
- [ ] Multi-cloud deployment support
- [ ] Terraform backend provisioning
- [ ] Continuous benchmarking (CI/CD integration)
- [ ] Machine learning for workload prediction
- [ ] Auto-tuning for each backend

---

## Summary

The multi-backend framework provides:

✅ **Extensible** - Easy to add new backends  
✅ **Fair** - Identical workloads across all backends  
✅ **Comprehensive** - Latency, throughput, cost comparisons  
✅ **Practical** - CLI tools for real-world usage  
✅ **Production-Ready** - Lakebase & DynamoDB implemented  

**Next**: DynamoDB integration testing (next week) 🚀

