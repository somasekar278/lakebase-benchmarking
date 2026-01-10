"""
Abstract backend interface for multi-database benchmarking.

Supports: Lakebase, DynamoDB, Aurora, Cosmos DB, and extensible to others.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from enum import Enum


class BackendType(Enum):
    """Supported database backends"""
    LAKEBASE = "lakebase"
    DYNAMODB = "dynamodb"
    AURORA = "aurora"
    COSMOSDB = "cosmosdb"
    MONGODB = "mongodb"
    CASSANDRA = "cassandra"
    BIGTABLE = "bigtable"


@dataclass
class BackendConfig:
    """Configuration for a database backend"""
    backend_type: BackendType
    name: str
    config: Dict[str, Any]
    enabled: bool = True
    description: str = ""


@dataclass
class QueryResult:
    """Standardized query result across all backends"""
    success: bool
    latency_ms: float
    rows_returned: int
    data: Optional[List[Dict[str, Any]]] = None
    error: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class Backend(ABC):
    """
    Abstract base class for database backends.
    
    All backends must implement this interface to be benchmarked.
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.backend_type = None  # Set by subclass
        self.connection = None
        self.cost_tracker = None  # Will be initialized when backend_type is set
        self.cost_model = None
    
    @abstractmethod
    def connect(self) -> bool:
        """
        Establish connection to the database.
        
        Returns:
            True if connection successful, False otherwise
        """
        pass
    
    @abstractmethod
    def disconnect(self) -> bool:
        """
        Close connection to the database.
        
        Returns:
            True if disconnection successful, False otherwise
        """
        pass
    
    @abstractmethod
    def create_tables(self, workload) -> bool:
        """
        Create tables based on workload definition.
        
        Args:
            workload: Workload object with table definitions
        
        Returns:
            True if tables created successfully, False otherwise
        """
        pass
    
    @abstractmethod
    def load_data(self, table_name: str, data: List[Dict[str, Any]]) -> bool:
        """
        Load data into a table.
        
        Args:
            table_name: Name of the table
            data: List of records to insert
        
        Returns:
            True if data loaded successfully, False otherwise
        """
        pass
    
    @abstractmethod
    def query(self, query_pattern, keys: List[str]) -> QueryResult:
        """
        Execute a query based on query pattern.
        
        Args:
            query_pattern: QueryPattern object defining the query
            keys: List of keys to query
        
        Returns:
            QueryResult with timing and data
        """
        pass
    
    @abstractmethod
    def batch_query(self, query_patterns: List, keys_dict: Dict[str, List[str]]) -> QueryResult:
        """
        Execute multiple queries (for binpacking scenarios).
        
        Args:
            query_patterns: List of QueryPattern objects
            keys_dict: Dict mapping table names to key lists
        
        Returns:
            QueryResult with combined timing and data
        """
        pass
    
    @abstractmethod
    def cleanup(self) -> bool:
        """
        Clean up resources (delete tables, etc.).
        
        Returns:
            True if cleanup successful, False otherwise
        """
        pass
    
    def get_backend_info(self) -> Dict[str, Any]:
        """
        Get information about the backend.
        
        Returns:
            Dict with backend metadata
        """
        return {
            'type': self.backend_type.value if self.backend_type else 'unknown',
            'name': self.__class__.__name__,
            'config': self._get_safe_config(),
            'connected': self.connection is not None,
        }
    
    def _get_safe_config(self) -> Dict[str, Any]:
        """
        Get config with sensitive values masked.
        
        Returns:
            Config dict with passwords/tokens masked
        """
        safe_config = self.config.copy()
        sensitive_keys = ['password', 'token', 'secret', 'key', 'access_key', 'secret_key']
        
        for key in safe_config:
            if any(sensitive in key.lower() for sensitive in sensitive_keys):
                if isinstance(safe_config[key], str) and len(safe_config[key]) > 0:
                    safe_config[key] = '***' + safe_config[key][-4:]
        
        return safe_config
    
    def validate_config(self) -> bool:
        """
        Validate backend configuration.
        
        Returns:
            True if config is valid, False otherwise
        """
        # Override in subclasses for specific validation
        return True
    
    def supports_feature(self, feature: str) -> bool:
        """
        Check if backend supports a specific feature.
        
        Args:
            feature: Feature name (e.g., 'transactions', 'indexes', 'stored_procedures')
        
        Returns:
            True if feature is supported, False otherwise
        """
        # Override in subclasses
        return False
    
    def initialize_cost_tracking(self):
        """
        Initialize cost tracking for this backend.
        Should be called after backend_type is set.
        """
        if self.backend_type:
            try:
                # Import here to avoid circular dependency
                from utils.cost_tracker import BenchmarkCost, get_cost_model
                
                self.cost_tracker = BenchmarkCost(
                    backend_name=self.backend_type.value,
                    region=self.config.get('region', 'us-east-1')
                )
                self.cost_model = get_cost_model(
                    self.backend_type.value,
                    region=self.config.get('region', 'us-east-1')
                )
            except Exception as e:
                print(f"⚠️  Cost tracking unavailable: {e}")
    
    def get_cost_summary(self) -> Dict[str, Any]:
        """
        Get cost summary for this backend.
        
        Returns:
            Dict with cost breakdown
        """
        if self.cost_tracker:
            return self.cost_tracker.get_summary()
        return {'error': 'Cost tracking not initialized'}
    
    def get_total_cost(self) -> float:
        """
        Get total cost for this backend.
        
        Returns:
            Total cost in USD
        """
        if self.cost_tracker:
            return self.cost_tracker.get_total()
        return 0.0


class BackendFactory:
    """
    Factory for creating backend instances.
    """
    
    _backends = {}
    
    @classmethod
    def register(cls, backend_type: BackendType, backend_class):
        """Register a backend implementation"""
        cls._backends[backend_type] = backend_class
    
    @classmethod
    def create(cls, backend_type: BackendType, config: Dict[str, Any]) -> Backend:
        """
        Create a backend instance.
        
        Args:
            backend_type: Type of backend to create
            config: Configuration dict for the backend
        
        Returns:
            Backend instance
        
        Raises:
            ValueError: If backend type is not registered
        """
        if backend_type not in cls._backends:
            available = ', '.join([bt.value for bt in cls._backends.keys()])
            raise ValueError(
                f"Backend '{backend_type.value}' not registered. "
                f"Available backends: {available}"
            )
        
        return cls._backends[backend_type](config)
    
    @classmethod
    def list_backends(cls) -> List[BackendType]:
        """List all registered backends"""
        return list(cls._backends.keys())
    
    @classmethod
    def is_registered(cls, backend_type: BackendType) -> bool:
        """Check if a backend is registered"""
        return backend_type in cls._backends


# Type mapping for schema translation across backends
class DataType(Enum):
    """Generic data types that map to backend-specific types"""
    STRING = "string"
    INTEGER = "integer"
    BIGINT = "bigint"
    DECIMAL = "decimal"
    BOOLEAN = "boolean"
    TIMESTAMP = "timestamp"
    BINARY = "binary"
    JSON = "json"


class SchemaTranslator:
    """
    Translates generic schema definitions to backend-specific schemas.
    """
    
    # Backend-specific type mappings
    TYPE_MAPPINGS = {
        BackendType.LAKEBASE: {
            DataType.STRING: "TEXT",
            DataType.INTEGER: "INTEGER",
            DataType.BIGINT: "BIGINT",
            DataType.DECIMAL: "NUMERIC",
            DataType.BOOLEAN: "BOOLEAN",
            DataType.TIMESTAMP: "TIMESTAMPTZ",
            DataType.BINARY: "BYTEA",
            DataType.JSON: "JSONB",
        },
        BackendType.DYNAMODB: {
            DataType.STRING: "S",
            DataType.INTEGER: "N",
            DataType.BIGINT: "N",
            DataType.DECIMAL: "N",
            DataType.BOOLEAN: "BOOL",
            DataType.TIMESTAMP: "N",  # Unix timestamp
            DataType.BINARY: "B",
            DataType.JSON: "M",  # Map
        },
        BackendType.AURORA: {
            # Same as PostgreSQL/Lakebase for Aurora PostgreSQL
            DataType.STRING: "TEXT",
            DataType.INTEGER: "INTEGER",
            DataType.BIGINT: "BIGINT",
            DataType.DECIMAL: "NUMERIC",
            DataType.BOOLEAN: "BOOLEAN",
            DataType.TIMESTAMP: "TIMESTAMPTZ",
            DataType.BINARY: "BYTEA",
            DataType.JSON: "JSONB",
        },
        BackendType.COSMOSDB: {
            # Cosmos DB is schema-less, but types for validation
            DataType.STRING: "string",
            DataType.INTEGER: "number",
            DataType.BIGINT: "number",
            DataType.DECIMAL: "number",
            DataType.BOOLEAN: "boolean",
            DataType.TIMESTAMP: "string",  # ISO 8601
            DataType.BINARY: "string",  # Base64
            DataType.JSON: "object",
        },
    }
    
    @classmethod
    def translate_type(cls, data_type: DataType, backend_type: BackendType) -> str:
        """Translate a generic data type to backend-specific type"""
        if backend_type not in cls.TYPE_MAPPINGS:
            raise ValueError(f"Backend {backend_type} not supported")
        
        return cls.TYPE_MAPPINGS[backend_type].get(data_type, "TEXT")
    
    @classmethod
    def translate_schema(cls, generic_schema: Dict, backend_type: BackendType) -> Dict:
        """Translate a full schema to backend-specific format"""
        translated = {}
        
        for column, data_type in generic_schema.items():
            if isinstance(data_type, DataType):
                translated[column] = cls.translate_type(data_type, backend_type)
            else:
                translated[column] = data_type  # Already backend-specific
        
        return translated


if __name__ == "__main__":
    # Example usage
    print("Backend Framework")
    print("=" * 60)
    
    print("\nSupported Backend Types:")
    for bt in BackendType:
        print(f"  • {bt.value}")
    
    print("\nGeneric Data Types:")
    for dt in DataType:
        print(f"  • {dt.value}")
    
    print("\nType Mapping Example (STRING type):")
    for backend in [BackendType.LAKEBASE, BackendType.DYNAMODB, BackendType.COSMOSDB]:
        translated = SchemaTranslator.translate_type(DataType.STRING, backend)
        print(f"  {backend.value:15} → {translated}")
    
    print("\n" + "=" * 60)

