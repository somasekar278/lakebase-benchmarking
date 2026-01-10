# =============================================================================
# Terraform Variables for Lakebase Benchmark Framework
# =============================================================================

# -----------------------------------------------------------------------------
# Provider Configuration
# -----------------------------------------------------------------------------

variable "databricks_host" {
  description = "Databricks workspace URL"
  type        = string
  default     = "https://fe-sandbox-one-env-som-workspace.cloud.databricks.com"
}

variable "databricks_token" {
  description = "Databricks personal access token"
  type        = string
  sensitive   = true
}

variable "aws_region" {
  description = "AWS region for Lakebase and other resources"
  type        = string
  default     = "eu-west-1"
}

# -----------------------------------------------------------------------------
# Lakebase Configuration (Coming Soon - TF Support Expected)
# -----------------------------------------------------------------------------

variable "lakebase_project_id" {
  description = "Lakebase project ID (will be used when TF support is available)"
  type        = string
  default     = null
}

variable "lakebase_endpoint" {
  description = "Lakebase database endpoint"
  type        = string
  default     = "ep-autumn-fire-d318blbk.database.eu-west-1.cloud.databricks.com"
}

variable "lakebase_database" {
  description = "Lakebase database name"
  type        = string
  default     = "benchmark"
}

variable "lakebase_user" {
  description = "Lakebase benchmark user"
  type        = string
  default     = "fraud_benchmark_user"
}

variable "lakebase_password" {
  description = "Lakebase benchmark user password"
  type        = string
  sensitive   = true
}

# -----------------------------------------------------------------------------
# Benchmark Configuration
# -----------------------------------------------------------------------------

variable "workload_name" {
  description = "Name of the workload to benchmark"
  type        = string
  default     = "fraud_detection"
}

variable "num_tables" {
  description = "Number of tables for auto-generated workloads"
  type        = number
  default     = 30
}

variable "features_per_table" {
  description = "Number of features per table"
  type        = number
  default     = 5
}

variable "rows_per_table" {
  description = "Number of rows per table"
  type        = number
  default     = 100000000
}

variable "use_binpacking" {
  description = "Whether to use stored procedure binpacking"
  type        = bool
  default     = true
}

# -----------------------------------------------------------------------------
# Databricks Job Configuration
# -----------------------------------------------------------------------------

variable "job_cluster_size" {
  description = "Cluster node type for jobs"
  type        = string
  default     = "m6i.2xlarge"
}

variable "job_num_workers" {
  description = "Number of workers for job clusters"
  type        = number
  default     = 4
}

variable "spark_version" {
  description = "Spark version for clusters"
  type        = string
  default     = "16.4.x-scala2.12"
}

# -----------------------------------------------------------------------------
# Notebook Paths
# -----------------------------------------------------------------------------

variable "notebook_base_path" {
  description = "Base path for notebooks in Databricks workspace"
  type        = string
  default     = "/Users/som.natarajan@databricks.com"
}

# -----------------------------------------------------------------------------
# Unity Catalog Configuration (for bulk loading)
# -----------------------------------------------------------------------------

variable "unity_catalog_name" {
  description = "Unity Catalog name for volumes"
  type        = string
  default     = "main"
}

variable "unity_catalog_schema" {
  description = "Unity Catalog schema for volumes"
  type        = string
  default     = "default"
}

variable "unity_catalog_volume" {
  description = "Unity Catalog volume name for bulk loading data"
  type        = string
  default     = "benchmark_data"
}

# -----------------------------------------------------------------------------
# Data Loading Strategy
# -----------------------------------------------------------------------------

variable "bulk_load_threshold" {
  description = "Row count threshold above which to use bulk load (COPY) instead of JDBC"
  type        = number
  default     = 100000000  # 100M rows
}

# -----------------------------------------------------------------------------
# Tags
# -----------------------------------------------------------------------------

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default = {
    Project     = "lakebase-benchmark"
    ManagedBy   = "terraform"
    Environment = "benchmark"
  }
}

# -----------------------------------------------------------------------------
# Feature Flags
# -----------------------------------------------------------------------------

variable "deploy_data_loading_jobs" {
  description = "Whether to deploy data loading jobs"
  type        = bool
  default     = true
}

variable "deploy_benchmark_jobs" {
  description = "Whether to deploy benchmark jobs"
  type        = bool
  default     = true
}

variable "enable_lakebase_resources" {
  description = "Enable Lakebase resource creation (when TF support is available)"
  type        = bool
  default     = false  # Set to true when Lakebase TF provider is available
}

