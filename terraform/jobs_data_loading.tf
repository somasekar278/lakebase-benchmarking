# =============================================================================
# Data Loading Jobs
# =============================================================================

# -----------------------------------------------------------------------------
# Flexible Schema Data Loading Job
# -----------------------------------------------------------------------------

resource "databricks_job" "load_flexible_schema" {
  count = var.deploy_data_loading_jobs ? 1 : 0
  
  name = "Lakebase Benchmark - Load Data (${var.workload_name})"
  
  tags = merge(
    local.common_tags,
    {
      JobType = "data_loading"
    }
  )
  
  max_retries = local.max_retries
  timeout_seconds = local.job_timeout_seconds
  
  # Task 1: Load Table 0
  task {
    task_key = "load_table_00"
    
    notebook_task {
      notebook_path = databricks_notebook.load_flexible_schema.path
      base_parameters = {
        table_index = "0"
        num_tables  = tostring(var.num_tables)
        rows        = tostring(var.rows_per_table)
      }
    }
    
    new_cluster {
      num_workers   = var.job_num_workers
      spark_version = var.spark_version
      node_type_id  = var.job_cluster_size
      
      spark_conf = {
        "spark.databricks.delta.preview.enabled" = "true"
      }
    }
  }
  
  # Task 2: Load Table 1 (sequential)
  task {
    task_key = "load_table_01"
    
    depends_on {
      task_key = "load_table_00"
    }
    
    notebook_task {
      notebook_path = databricks_notebook.load_flexible_schema.path
      base_parameters = {
        table_index = "1"
        num_tables  = tostring(var.num_tables)
        rows        = tostring(var.rows_per_table)
      }
    }
    
    new_cluster {
      num_workers   = var.job_num_workers
      spark_version = var.spark_version
      node_type_id  = var.job_cluster_size
      
      spark_conf = {
        "spark.databricks.delta.preview.enabled" = "true"
      }
    }
  }
  
  # Task 3: Load Table 2 (sequential)
  task {
    task_key = "load_table_02"
    
    depends_on {
      task_key = "load_table_01"
    }
    
    notebook_task {
      notebook_path = databricks_notebook.load_flexible_schema.path
      base_parameters = {
        table_index = "2"
        num_tables  = tostring(var.num_tables)
        rows        = tostring(var.rows_per_table)
      }
    }
    
    new_cluster {
      num_workers   = var.job_num_workers
      spark_version = var.spark_version
      node_type_id  = var.job_cluster_size
      
      spark_conf = {
        "spark.databricks.delta.preview.enabled" = "true"
      }
    }
  }
  
  # Task 4: Load Table 3 (sequential)
  task {
    task_key = "load_table_03"
    
    depends_on {
      task_key = "load_table_02"
    }
    
    notebook_task {
      notebook_path = databricks_notebook.load_flexible_schema.path
      base_parameters = {
        table_index = "3"
        num_tables  = tostring(var.num_tables)
        rows        = tostring(var.rows_per_table)
      }
    }
    
    new_cluster {
      num_workers   = var.job_num_workers
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

output "data_loading_job_id" {
  value       = var.deploy_data_loading_jobs ? databricks_job.load_flexible_schema[0].id : null
  description = "ID of the data loading job"
}

output "data_loading_job_url" {
  value       = var.deploy_data_loading_jobs ? databricks_job.load_flexible_schema[0].url : null
  description = "URL of the data loading job in Databricks UI"
}

