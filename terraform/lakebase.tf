# =============================================================================
# Lakebase Resources (Terraform Support Coming Soon)
# =============================================================================
# 
# These resources will be enabled when Databricks releases Terraform support
# for Lakebase Autoscale (expected in next few weeks).
#
# To enable: Set variable enable_lakebase_resources = true
#
# Expected Terraform Resources (based on typical patterns):
# - lakebase_project
# - lakebase_database
# - lakebase_schema
# - lakebase_table
# - lakebase_role
# - lakebase_grant
# =============================================================================

# -----------------------------------------------------------------------------
# Lakebase Project (Placeholder)
# -----------------------------------------------------------------------------

# resource "lakebase_project" "benchmark" {
#   count = var.enable_lakebase_resources ? 1 : 0
# 
#   name        = "lakebase-benchmark-${var.workload_name}"
#   description = "Lakebase benchmark project for ${var.workload_name}"
#   region      = var.aws_region
#   
#   # Autoscale settings
#   compute_type = "autoscale"
#   min_cu       = 0   # Scale to zero
#   max_cu       = 16  # Adjust based on workload
#   
#   tags = local.common_tags
# }

# -----------------------------------------------------------------------------
# Lakebase Database (Placeholder)
# -----------------------------------------------------------------------------

# resource "lakebase_database" "benchmark" {
#   count = var.enable_lakebase_resources ? 1 : 0
#   
#   project_id = lakebase_project.benchmark[0].id
#   name       = var.lakebase_database
# }

# -----------------------------------------------------------------------------
# Lakebase Schema (Placeholder)
# -----------------------------------------------------------------------------

# resource "lakebase_schema" "features" {
#   count = var.enable_lakebase_resources ? 1 : 0
#   
#   project_id  = lakebase_project.benchmark[0].id
#   database    = lakebase_database.benchmark[0].name
#   name        = "features"
#   description = "Feature tables for benchmarking"
# }

# -----------------------------------------------------------------------------
# Lakebase User/Role (Placeholder)
# -----------------------------------------------------------------------------

# resource "lakebase_role" "benchmark_user" {
#   count = var.enable_lakebase_resources ? 1 : 0
#   
#   project_id = lakebase_project.benchmark[0].id
#   name       = var.lakebase_user
#   password   = var.lakebase_password
#   
#   # Permissions
#   login       = true
#   inherit     = false
#   create_db   = false
#   create_role = false
#   superuser   = false
# }

# -----------------------------------------------------------------------------
# Schema Grants (Placeholder)
# -----------------------------------------------------------------------------

# resource "lakebase_grant" "schema_usage" {
#   count = var.enable_lakebase_resources ? 1 : 0
#   
#   project_id = lakebase_project.benchmark[0].id
#   schema     = lakebase_schema.features[0].name
#   role       = lakebase_role.benchmark_user[0].name
#   privilege  = "USAGE"
# }

# resource "lakebase_grant" "schema_create" {
#   count = var.enable_lakebase_resources ? 1 : 0
#   
#   project_id = lakebase_project.benchmark[0].id
#   schema     = lakebase_schema.features[0].name
#   role       = lakebase_role.benchmark_user[0].name
#   privilege  = "CREATE"
# }

# -----------------------------------------------------------------------------
# Table Grants (Placeholder)
# -----------------------------------------------------------------------------

# resource "lakebase_grant" "tables_select" {
#   count = var.enable_lakebase_resources ? 1 : 0
#   
#   project_id = lakebase_project.benchmark[0].id
#   schema     = lakebase_schema.features[0].name
#   tables     = "ALL TABLES"
#   role       = lakebase_role.benchmark_user[0].name
#   privilege  = "SELECT"
# }

# resource "lakebase_grant" "tables_insert" {
#   count = var.enable_lakebase_resources ? 1 : 0
#   
#   project_id = lakebase_project.benchmark[0].id
#   schema     = lakebase_schema.features[0].name
#   tables     = "ALL TABLES"
#   role       = lakebase_role.benchmark_user[0].name
#   privilege  = "INSERT"
# }

# -----------------------------------------------------------------------------
# Data API Configuration (Placeholder)
# -----------------------------------------------------------------------------

# resource "lakebase_data_api" "benchmark" {
#   count = var.enable_lakebase_resources ? 1 : 0
#   
#   project_id     = lakebase_project.benchmark[0].id
#   enabled        = true
#   exposed_schemas = ["features"]
#   aggregates_enabled = true
# }

# resource "lakebase_role" "authenticator" {
#   count = var.enable_lakebase_resources ? 1 : 0
#   
#   project_id = lakebase_project.benchmark[0].id
#   name       = "authenticator"
#   login      = true
#   inherit    = false
# }

# =============================================================================
# Manual Setup (Until TF Support Available)
# =============================================================================
#
# For now, create Lakebase resources manually using:
#
# 1. Lakebase Console UI:
#    - Create project: lakebase-benchmark-{workload_name}
#    - Create database: benchmark
#    - Create schema: features
#
# 2. SQL Scripts:
#    Run: scripts/setup/setup_lakebase.sql
#    This creates:
#    - Schema and tables
#    - Users and roles
#    - Grants
#
# 3. Python Scripts:
#    Run: python scripts/setup/setup_data_api.py --user your.email@company.com
#    This sets up:
#    - Data API infrastructure
#    - User roles and permissions
#
# 4. Store connection details in terraform.tfvars:
#    lakebase_endpoint  = "ep-XXX.database.REGION.cloud.databricks.com"
#    lakebase_database  = "benchmark"
#    lakebase_user      = "fraud_benchmark_user"
#    lakebase_password  = "your_password"
#
# =============================================================================

# Output placeholder values (for manual setup reference)
output "lakebase_manual_setup_instructions" {
  value = <<-EOT
    ==================================================================================
    LAKEBASE MANUAL SETUP REQUIRED (Terraform support coming soon)
    ==================================================================================
    
    1. Create Lakebase project in Databricks console:
       - Name: lakebase-benchmark-${var.workload_name}
       - Type: Autoscale
       - Region: ${var.aws_region}
    
    2. Run SQL setup:
       psql -h ${var.lakebase_endpoint} -d ${var.lakebase_database} -f ../scripts/setup/setup_lakebase.sql
    
    3. Run Data API setup:
       python ../scripts/setup/setup_data_api.py --user your.email@company.com
    
    4. Update terraform.tfvars with Lakebase connection details
    
    5. When Terraform support is available:
       - Set enable_lakebase_resources = true
       - Uncomment resources in lakebase.tf
       - Run: terraform apply
    
    ==================================================================================
  EOT
  
  description = "Instructions for manual Lakebase setup until Terraform support is available"
}

