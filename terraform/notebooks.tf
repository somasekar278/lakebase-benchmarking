# =============================================================================
# Databricks Notebooks Deployment
# =============================================================================

# -----------------------------------------------------------------------------
# Workspace Directories
# -----------------------------------------------------------------------------

resource "databricks_directory" "benchmark_base" {
  path = local.notebook_path_base
}

resource "databricks_directory" "setup" {
  path       = local.notebook_path_setup
  depends_on = [databricks_directory.benchmark_base]
}

resource "databricks_directory" "data_loading" {
  path       = local.notebook_path_data_load
  depends_on = [databricks_directory.benchmark_base]
}

resource "databricks_directory" "benchmarks" {
  path       = local.notebook_path_benchmarks
  depends_on = [databricks_directory.benchmark_base]
}

# -----------------------------------------------------------------------------
# Data Loading Notebooks
# -----------------------------------------------------------------------------

resource "databricks_notebook" "load_flexible_schema" {
  source = "../notebooks/data_loading/load_flexible_schema.py"
  path   = "${local.notebook_path_data_load}/load_flexible_schema"
  language = "PYTHON"
  format   = "SOURCE"
}

resource "databricks_notebook" "load_bulk_copy" {
  source = "../notebooks/data_loading/load_bulk_copy.py"
  path   = "${local.notebook_path_data_load}/load_bulk_copy"
  language = "PYTHON"
  format   = "SOURCE"
}

# -----------------------------------------------------------------------------
# Benchmark Notebooks
# -----------------------------------------------------------------------------

resource "databricks_notebook" "benchmark_flexible" {
  source = "../notebooks/benchmarks/benchmark_flexible.py"
  path   = "${local.notebook_path_benchmarks}/benchmark_flexible"
  language = "PYTHON"
  format   = "SOURCE"
}

resource "databricks_notebook" "benchmark_lakebase" {
  source = "../notebooks/benchmarks/benchmark_lakebase.py"
  path   = "${local.notebook_path_benchmarks}/benchmark_lakebase"
  language = "PYTHON"
  format   = "SOURCE"
}

resource "databricks_notebook" "benchmark_comparison" {
  source = "../notebooks/benchmarks/benchmark_flexible_with_data_api.py"
  path   = "${local.notebook_path_benchmarks}/benchmark_comparison"
  language = "PYTHON"
  format   = "SOURCE"
}

# -----------------------------------------------------------------------------
# Outputs
# -----------------------------------------------------------------------------

output "notebook_paths" {
  value = {
    base             = databricks_directory.benchmark_base.path
    data_loading     = databricks_directory.data_loading.path
    benchmarks       = databricks_directory.benchmarks.path
    load_flexible    = databricks_notebook.load_flexible_schema.path
    load_bulk_copy   = databricks_notebook.load_bulk_copy.path
    benchmark_flex   = databricks_notebook.benchmark_flexible.path
    benchmark_lake   = databricks_notebook.benchmark_lakebase.path
    benchmark_comp   = databricks_notebook.benchmark_comparison.path
  }
  description = "Deployed notebook paths in Databricks workspace"
}

