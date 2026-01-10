# =============================================================================
# Terraform Outputs
# =============================================================================

# -----------------------------------------------------------------------------
# Deployment Summary
# -----------------------------------------------------------------------------

output "deployment_summary" {
  value = <<-EOT
    ==================================================================================
    LAKEBASE BENCHMARK FRAMEWORK - DEPLOYMENT SUMMARY
    ==================================================================================
    
    Workload: ${var.workload_name}
    Region:   ${var.aws_region}
    
    DATABRICKS RESOURCES (Deployed):
    --------------------------------
    ✅ Notebooks:
       - Base Path:       ${databricks_directory.benchmark_base.path}
       - Data Loading:    ${databricks_directory.data_loading.path}
       - Benchmarks:      ${databricks_directory.benchmarks.path}
    
    ✅ Jobs:
       ${var.deploy_data_loading_jobs ? "- Data Loading:    ${try(databricks_job.load_flexible_schema[0].url, "N/A")}" : "- Data Loading: Disabled"}
       ${var.deploy_benchmark_jobs ? "- Benchmarks:      ${try(databricks_job.benchmark_flexible[0].url, "N/A")}" : "- Benchmarks: Disabled"}
    
    LAKEBASE RESOURCES (Manual Setup Required):
    -------------------------------------------
    ⏳ Terraform support coming soon (next few weeks)
    
    Manual setup steps:
    1. Create Lakebase project via UI
    2. Run: psql -h ${var.lakebase_endpoint} -d ${var.lakebase_database} -f ../scripts/setup/setup_lakebase.sql
    3. Run: python ../scripts/setup/setup_data_api.py
    
    NEXT STEPS:
    -----------
    1. Verify notebooks in Databricks workspace
    2. Run data loading job to create test data
    3. Run benchmark jobs to measure performance
    4. When Lakebase TF support is available:
       - Set enable_lakebase_resources = true
       - Uncomment resources in lakebase.tf
       - Run: terraform apply
    
    ==================================================================================
  EOT
  
  description = "Summary of deployed resources and next steps"
}

# -----------------------------------------------------------------------------
# Resource URLs
# -----------------------------------------------------------------------------

output "databricks_workspace_url" {
  value       = var.databricks_host
  description = "Databricks workspace URL"
}

output "lakebase_endpoint" {
  value       = var.lakebase_endpoint
  description = "Lakebase database endpoint"
}

# -----------------------------------------------------------------------------
# Configuration Reference
# -----------------------------------------------------------------------------

output "benchmark_config" {
  value = {
    workload_name       = var.workload_name
    num_tables          = var.num_tables
    features_per_table  = var.features_per_table
    rows_per_table      = var.rows_per_table
    use_binpacking      = var.use_binpacking
    job_cluster_size    = var.job_cluster_size
    spark_version       = var.spark_version
  }
  description = "Benchmark configuration parameters"
}

