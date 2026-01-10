"""
Database backend implementations for benchmarking.

Available backends:
- Lakebase (PostgreSQL-compatible)
- DynamoDB (AWS)
- Aurora (AWS PostgreSQL/MySQL)
- Cosmos DB (Azure)
"""

from core.backend import BackendType, Backend, BackendFactory

# Import backend implementations
from .lakebase import LakebaseBackend
from .dynamodb import DynamoDBBackend
from .aurora import AuroraBackend
from .cosmosdb import CosmosDBBackend

# Register backends with factory
BackendFactory.register(BackendType.LAKEBASE, LakebaseBackend)
BackendFactory.register(BackendType.DYNAMODB, DynamoDBBackend)
BackendFactory.register(BackendType.AURORA, AuroraBackend)
BackendFactory.register(BackendType.COSMOSDB, CosmosDBBackend)

__all__ = [
    'Backend',
    'BackendType',
    'BackendFactory',
    'LakebaseBackend',
    'DynamoDBBackend',
    'AuroraBackend',
    'CosmosDBBackend',
]

