# =============================================================================
# Bulk Data Loading Jobs (PostgreSQL COPY for > 100M rows)
# =============================================================================

# -----------------------------------------------------------------------------
# Bulk Load Job (for large tables using COPY)
# -----------------------------------------------------------------------------

resource "databricks_job" "load_bulk_copy" {
  count = var.deploy_data_loading_jobs ? 1 : 0
  
  name = "Lakebase Benchmark - Bulk Load (COPY) - ${var.workload_name}"
  
  tags = merge(
    local.common_tags,
    {
      JobType    = "data_loading"
      LoadMethod = "postgresql_copy"
      Optimized  = "large_tables"
    }
  )
  
  max_retries = local.max_retries
  timeout_seconds = local.job_timeout_seconds
  
  # Task 1: Load Table 0 (Bulk - 1B rows)
  task {
    task_key = "bulk_load_table_00"
    
    notebook_task {
      notebook_path = "${local.notebook_path_data_load}/load_bulk_copy"
      base_parameters = {
        table_index = "0"
        num_tables  = tostring(var.num_tables)
        rows        = "1000000000"  # 1B rows - optimal for COPY
        volume_name = var.unity_catalog_volume
        catalog     = var.unity_catalog_name
        schema_uc   = var.unity_catalog_schema
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
  
  # Task 2: Load Table 1 (Bulk - 100M rows)
  task {
    task_key = "bulk_load_table_01"
    
    depends_on {
      task_key = "bulk_load_table_00"
    }
    
    notebook_task {
      notebook_path = "${local.notebook_path_data_load}/load_bulk_copy"
      base_parameters = {
        table_index = "1"
        num_tables  = tostring(var.num_tables)
        rows        = "100000000"  # 100M rows
        volume_name = var.unity_catalog_volume
        catalog     = var.unity_catalog_name
        schema_uc   = var.unity_catalog_schema
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
  
  # Task 3: Load Table 2 (Bulk - 100M rows)
  task {
    task_key = "bulk_load_table_02"
    
    depends_on {
      task_key = "bulk_load_table_01"
    }
    
    notebook_task {
      notebook_path = "${local.notebook_path_data_load}/load_bulk_copy"
      base_parameters = {
        table_index = "2"
        num_tables  = tostring(var.num_tables)
        rows        = "100000000"  # 100M rows
        volume_name = var.unity_catalog_volume
        catalog     = var.unity_catalog_name
        schema_uc   = var.unity_catalog_schema
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
  
  # Task 4: Load Table 3 (Bulk - 100M rows)
  task {
    task_key = "bulk_load_table_03"
    
    depends_on {
      task_key = "bulk_load_table_02"
    }
    
    notebook_task {
      notebook_path = "${local.notebook_path_data_load}/load_bulk_copy"
      base_parameters = {
        table_index = "3"
        num_tables  = tostring(var.num_tables)
        rows        = "100000000"  # 100M rows
        volume_name = var.unity_catalog_volume
        catalog     = var.unity_catalog_name
        schema_uc   = var.unity_catalog_schema
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

output "bulk_load_job_id" {
  value       = var.deploy_data_loading_jobs ? databricks_job.load_bulk_copy[0].id : null
  description = "ID of the bulk load job (PostgreSQL COPY)"
}

output "bulk_load_job_url" {
  value       = var.deploy_data_loading_jobs ? databricks_job.load_bulk_copy[0].url : null
  description = "URL of the bulk load job in Databricks UI"
}

# -----------------------------------------------------------------------------
# Documentation
# -----------------------------------------------------------------------------

# When to use this job vs regular JDBC loading:
#
# ✅ Use Bulk Load (COPY) when:
#    - Tables have > 100M rows
#    - Initial data loading (not incremental)
#    - Network is unstable (single operation vs streaming)
#    - Need maximum throughput (10-100x faster)
#
# ✅ Use JDBC Load when:
#    - Tables have < 100M rows
#    - Incremental/upsert operations
#    - Need Spark parallelism for data generation
#    - Source is already in Delta Lake
#
# Performance Comparison (typical):
# ---------------------------------
# 1B rows with JDBC:   ~5-10 hours (often fails)
# 1B rows with COPY:   ~10-30 minutes (reliable)
#
# Speedup: 10-30x for large tables

