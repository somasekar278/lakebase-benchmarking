# =============================================================================
# Benchmark Jobs
# =============================================================================

# -----------------------------------------------------------------------------
# Flexible Benchmark Job (Direct Connection)
# -----------------------------------------------------------------------------

resource "databricks_job" "benchmark_flexible" {
  count = var.deploy_benchmark_jobs ? 1 : 0
  
  name = "Lakebase Benchmark - Flexible (${var.workload_name})"
  
  tags = merge(
    local.common_tags,
    {
      JobType = "benchmark"
      Method  = "direct_connection"
    }
  )
  
  max_retries = 0  # Don't retry benchmarks
  timeout_seconds = local.job_timeout_seconds
  
  task {
    task_key = "run_benchmark"
    
    notebook_task {
      notebook_path = databricks_notebook.benchmark_flexible.path
      base_parameters = {
        num_tables          = tostring(var.num_tables)
        features_per_table  = tostring(var.features_per_table)
        num_keys            = "25"
        num_iterations      = "100"
        num_warmup          = "5"
      }
    }
    
    new_cluster {
      num_workers   = 0  # Single node for benchmark
      spark_version = var.spark_version
      node_type_id  = var.job_cluster_size
      
      spark_conf = {
        "spark.databricks.delta.preview.enabled" = "true"
      }
    }
  }
}

# -----------------------------------------------------------------------------
# Benchmark Comparison Job (Direct vs Data API)
# -----------------------------------------------------------------------------

resource "databricks_job" "benchmark_comparison" {
  count = var.deploy_benchmark_jobs ? 1 : 0
  
  name = "Lakebase Benchmark - Comparison (${var.workload_name})"
  
  tags = merge(
    local.common_tags,
    {
      JobType = "benchmark"
      Method  = "comparison"
    }
  )
  
  max_retries = 0  # Don't retry benchmarks
  timeout_seconds = local.job_timeout_seconds
  
  task {
    task_key = "run_comparison"
    
    notebook_task {
      notebook_path = databricks_notebook.benchmark_comparison.path
      base_parameters = {
        num_tables          = tostring(var.num_tables)
        features_per_table  = tostring(var.features_per_table)
        num_keys            = "25"
        num_iterations      = "100"
        num_warmup          = "5"
      }
    }
    
    new_cluster {
      num_workers   = 0  # Single node for benchmark
      spark_version = var.spark_version
      node_type_id  = var.job_cluster_size
      
      spark_conf = {
        "spark.databricks.delta.preview.enabled" = "true"
      }
    }
  }
}

# -----------------------------------------------------------------------------
# Fraud Detection Benchmark Job (Schema-Defined)
# -----------------------------------------------------------------------------

resource "databricks_job" "benchmark_fraud_detection" {
  count = var.workload_name == "fraud_detection" && var.deploy_benchmark_jobs ? 1 : 0
  
  name = "Lakebase Benchmark - Fraud Detection"
  
  tags = merge(
    local.common_tags,
    {
      JobType  = "benchmark"
      Workload = "fraud_detection"
    }
  )
  
  max_retries = 0
  timeout_seconds = local.job_timeout_seconds
  
  task {
    task_key = "run_fraud_benchmark"
    
    notebook_task {
      notebook_path = databricks_notebook.benchmark_lakebase.path
      base_parameters = {
        num_iterations = "100"
        num_warmup     = "5"
      }
    }
    
    new_cluster {
      num_workers   = 0  # Single node
      spark_version = var.spark_version
      node_type_id  = var.job_cluster_size
      
      spark_conf = {
        "spark.databricks.delta.preview.enabled" = "true"
      }
    }
  }
}

# -----------------------------------------------------------------------------
# Outputs
# -----------------------------------------------------------------------------

output "benchmark_flexible_job_id" {
  value       = var.deploy_benchmark_jobs ? databricks_job.benchmark_flexible[0].id : null
  description = "ID of the flexible benchmark job"
}

output "benchmark_flexible_job_url" {
  value       = var.deploy_benchmark_jobs ? databricks_job.benchmark_flexible[0].url : null
  description = "URL of the flexible benchmark job"
}

output "benchmark_comparison_job_id" {
  value       = var.deploy_benchmark_jobs ? databricks_job.benchmark_comparison[0].id : null
  description = "ID of the comparison benchmark job"
}

output "benchmark_comparison_job_url" {
  value       = var.deploy_benchmark_jobs ? databricks_job.benchmark_comparison[0].url : null
  description = "URL of the comparison benchmark job"
}

output "benchmark_fraud_job_id" {
  value       = var.workload_name == "fraud_detection" && var.deploy_benchmark_jobs ? databricks_job.benchmark_fraud_detection[0].id : null
  description = "ID of the fraud detection benchmark job"
}

output "benchmark_fraud_job_url" {
  value       = var.workload_name == "fraud_detection" && var.deploy_benchmark_jobs ? databricks_job.benchmark_fraud_detection[0].url : null
  description = "URL of the fraud detection benchmark job"
}

