# =============================================================================
# Main Terraform Configuration
# =============================================================================

terraform {
  required_version = ">= 1.0"
  
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.0"
    }
    
    # Lakebase provider (when available)
    # lakebase = {
    #   source  = "databricks/lakebase"
    #   version = "~> 1.0"
    # }
  }
}

# =============================================================================
# Providers
# =============================================================================

provider "databricks" {
  host  = var.databricks_host
  token = var.databricks_token
}

# Lakebase provider configuration (coming soon)
# provider "lakebase" {
#   host     = var.lakebase_endpoint
#   database = var.lakebase_database
#   username = var.lakebase_user
#   password = var.lakebase_password
# }

# =============================================================================
# Data Sources
# =============================================================================

data "databricks_current_user" {
  description = "Get current user information"
}

# =============================================================================
# Local Values
# =============================================================================

locals {
  # Notebook paths
  notebook_path_base        = "${var.notebook_base_path}/lakebase_benchmark"
  notebook_path_setup       = "${local.notebook_path_base}/setup"
  notebook_path_data_load   = "${local.notebook_path_base}/data_loading"
  notebook_path_benchmarks  = "${local.notebook_path_base}/benchmarks"
  
  # Common job settings
  job_timeout_seconds = 0  # No timeout
  max_retries        = 2
  
  # Tags with dynamic values
  common_tags = merge(
    var.tags,
    {
      Workload    = var.workload_name
      DeployedBy  = data.databricks_current_user.user_name
      DeployedAt  = timestamp()
    }
  )
}

