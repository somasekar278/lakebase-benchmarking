# 🚀 Terraform Deployment

Deploy the Lakebase Benchmark Framework infrastructure using Terraform.

---

## 📋 What Gets Deployed

### ✅ Deployed Now (Fully Supported)
- **Databricks Notebooks** - All benchmark and data loading notebooks
- **Databricks Jobs** - Automated data loading and benchmarking
- **Workspace Directories** - Organized folder structure

### ⏳ Coming Soon (Terraform Support Expected)
- **Lakebase Project** - Autoscale compute
- **Lakebase Database & Schema** - Automated setup
- **Lakebase Users & Roles** - Permission management
- **Lakebase Grants** - Automated permissions
- **Data API Configuration** - REST API setup

> **Note:** Lakebase Autoscale Terraform support is expected in the next few weeks. Until then, Lakebase resources must be created manually (see [Manual Setup](#manual-lakebase-setup)).

---

## 🚦 Quick Start

### 1. Prerequisites

- Terraform >= 1.0
- Databricks workspace with access token
- Lakebase project (manual setup for now)

### 2. Configure Variables

```bash
cd terraform/
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars` with your values:

```hcl
databricks_host  = "https://your-workspace.cloud.databricks.com"
databricks_token = "dapi..."

lakebase_endpoint = "ep-XXX.database.eu-west-1.cloud.databricks.com"
lakebase_database = "benchmark"
lakebase_user     = "fraud_benchmark_user"
lakebase_password = "your_password"

workload_name      = "fraud_detection"
num_tables         = 30
features_per_table = 5
rows_per_table     = 100000000
```

### 3. Initialize Terraform

```bash
terraform init
```

### 4. Plan Deployment

```bash
terraform plan
```

Review the resources that will be created.

### 5. Deploy

```bash
terraform apply
```

Type `yes` to confirm.

---

## 📦 Module Structure

```
terraform/
├── main.tf                     # Provider configuration, locals
├── variables.tf                # Input variables
├── lakebase.tf                 # Lakebase resources (commented, ready for TF support)
├── notebooks.tf                # Databricks notebook deployment
├── jobs_data_loading.tf        # Data loading job definitions
├── jobs_benchmarks.tf          # Benchmark job definitions
├── outputs.tf                  # Output values
├── terraform.tfvars.example    # Example configuration
└── README.md                   # This file
```

---

## ⚙️ Configuration

### Workload Types

**Option 1: Auto-Generated (Flexible)**
```hcl
workload_name      = "generic_benchmark"
num_tables         = 30
features_per_table = 5
rows_per_table     = 100000000
use_binpacking     = true
```

**Option 2: Schema-Defined (Fraud Detection)**
```hcl
workload_name = "fraud_detection"
# Uses predefined schemas from scripts/setup/schema.py
```

### Cluster Configuration

**Compute-Optimized:**
```hcl
job_cluster_size = "m6i.2xlarge"  # 8 cores, 32GB
job_num_workers  = 4
```

**Memory-Optimized:**
```hcl
job_cluster_size = "r6i.2xlarge"  # 8 cores, 64GB
job_num_workers  = 4
```

### Deployment Flags

```hcl
deploy_data_loading_jobs = true   # Deploy data loading jobs
deploy_benchmark_jobs    = true   # Deploy benchmark jobs
enable_lakebase_resources = false # Set to true when TF support available
```

---

## 🔧 Manual Lakebase Setup

Until Terraform support is available, set up Lakebase manually:

### 1. Create Lakebase Project

Via Databricks Console:
- Navigate to "Compute" → "Lakebase"
- Create new project: `lakebase-benchmark-{workload_name}`
- Type: **Autoscale**
- Region: Match your benchmarking region (e.g., `eu-west-1`)

### 2. Create Database & Schema

```bash
# Connect using psql
psql -h <lakebase_endpoint> -d postgres -U admin

# Create database
CREATE DATABASE benchmark;

# Connect to new database
\c benchmark

# Create schema
CREATE SCHEMA features;
```

### 3. Run SQL Setup Script

```bash
cd ../scripts/setup/
psql -h <lakebase_endpoint> -d benchmark -U admin -f setup_lakebase.sql
```

This creates:
- Tables with proper schemas
- Users and roles
- Schema and table grants

### 4. Setup Data API (Optional)

```bash
cd ../scripts/setup/
python setup_data_api.py --user your.email@company.com
```

### 5. Update terraform.tfvars

Add your Lakebase connection details to `terraform.tfvars`.

---

## 🎯 Usage

### Deploy Everything

```bash
terraform apply
```

### Deploy Only Notebooks

```bash
terraform apply -target=databricks_directory.benchmark_base \
                -target=databricks_notebook.load_flexible_schema \
                -target=databricks_notebook.benchmark_flexible
```

### Deploy Only Jobs

```bash
terraform apply -target=databricks_job.load_flexible_schema \
                -target=databricks_job.benchmark_flexible
```

### Disable Data Loading Jobs

In `terraform.tfvars`:
```hcl
deploy_data_loading_jobs = false
```

Then:
```bash
terraform apply
```

---

## 📊 Post-Deployment

After successful deployment, Terraform will output:

```
deployment_summary = <<-EOT
==================================================================================
LAKEBASE BENCHMARK FRAMEWORK - DEPLOYMENT SUMMARY
==================================================================================

Workload: fraud_detection
Region:   eu-west-1

DATABRICKS RESOURCES (Deployed):
--------------------------------
✅ Notebooks:
   - Base Path:       /Users/your.email@databricks.com/lakebase_benchmark
   - Data Loading:    /Users/your.email@databricks.com/lakebase_benchmark/data_loading
   - Benchmarks:      /Users/your.email@databricks.com/lakebase_benchmark/benchmarks

✅ Jobs:
   - Data Loading:    https://your-workspace.cloud.databricks.com/#job/12345
   - Benchmarks:      https://your-workspace.cloud.databricks.com/#job/12346

...
EOT
```

### Run Data Loading

1. Go to Databricks workspace
2. Navigate to **Jobs** → Find "Lakebase Benchmark - Load Data"
3. Click **Run Now**
4. Monitor progress (loads tables sequentially to avoid networking issues)

### Run Benchmarks

1. Wait for data loading to complete
2. Navigate to **Jobs** → Find "Lakebase Benchmark - Flexible"
3. Click **Run Now**
4. View results in notebook output

---

## 🔮 When Lakebase Terraform Support Arrives

1. **Update `terraform.tfvars`:**
   ```hcl
   enable_lakebase_resources = true
   ```

2. **Uncomment Resources in `lakebase.tf`:**
   - Remove the `#` comment characters from resource blocks
   - Adjust resource names/properties as needed for the actual provider

3. **Re-plan and Apply:**
   ```bash
   terraform plan   # Review what will be created
   terraform apply  # Create Lakebase resources
   ```

4. **Migrate Existing Resources (if needed):**
   ```bash
   # Import existing resources instead of recreating
   terraform import lakebase_project.benchmark <project_id>
   terraform import lakebase_database.benchmark <database_id>
   ```

---

## 🗑️ Cleanup

### Destroy All Resources

```bash
terraform destroy
```

**Note:** This will:
- ✅ Delete Databricks jobs and notebooks
- ❌ **NOT** delete Lakebase resources (manual setup for now)

### Manual Lakebase Cleanup

```sql
-- Drop tables
DROP TABLE IF EXISTS features.fraud_reports_365d CASCADE;
DROP TABLE IF EXISTS features.good_rate_90d_lag_730d CASCADE;
-- ... repeat for all tables

-- Drop schema
DROP SCHEMA IF EXISTS features CASCADE;

-- Drop database (if needed)
DROP DATABASE IF EXISTS benchmark;
```

---

## 🔒 Security Best Practices

1. **Never commit `terraform.tfvars`** - Contains sensitive credentials
   ```bash
   echo "terraform.tfvars" >> .gitignore
   ```

2. **Use environment variables** (alternative to tfvars):
   ```bash
   export TF_VAR_databricks_token="dapi..."
   export TF_VAR_lakebase_password="..."
   ```

3. **Use Terraform Cloud/Enterprise** for state management:
   ```hcl
   terraform {
     backend "remote" {
       organization = "your-org"
       workspaces {
         name = "lakebase-benchmark"
       }
     }
   }
   ```

4. **Rotate credentials regularly** - Update tokens and passwords

---

## 🐛 Troubleshooting

### Error: Notebook not found

**Cause:** Notebook path doesn't exist in source

**Fix:**
```bash
# Ensure notebooks exist
ls -la ../notebooks/benchmarks/
ls -la ../notebooks/data_loading/

# Update notebook paths in notebooks.tf if needed
```

### Error: Unauthorized

**Cause:** Invalid Databricks token

**Fix:**
1. Generate new PAT in Databricks workspace (User Settings → Access Tokens)
2. Update `databricks_token` in `terraform.tfvars`

### Job fails with networking error

**Cause:** Parallel data loading overwhelming Lakebase

**Fix:** Already configured - jobs run sequentially using `depends_on`

### Lakebase connection timeout

**Cause:** Autoscale cold start (scaled to zero)

**Fix:** Run a warmup query first:
```sql
SELECT 1;  -- Wakes up the cluster
```

---

## 📚 Additional Resources

- [Terraform Databricks Provider Docs](https://registry.terraform.io/providers/databricks/databricks/latest/docs)
- [Lakebase Documentation](https://docs.databricks.com/lakebase/) (when available)
- [Framework Usage Examples](../USAGE_EXAMPLES.md)
- [Framework Design](../FRAMEWORK_DESIGN.md)

---

## 🤝 Contributing

To add new resources to the Terraform configuration:

1. Create a new `.tf` file in `terraform/`
2. Add variables to `variables.tf`
3. Add outputs to `outputs.tf`
4. Update this README
5. Test with `terraform plan`

---

## 📝 Notes

- Terraform state is stored locally by default
- For production, use remote state (S3, Terraform Cloud, etc.)
- Lakebase resources are placeholders until TF support is available
- Jobs use sequential execution to avoid networking issues
- Benchmarks run on single-node clusters for consistency

---

**Questions?** Check [FRAMEWORK_DESIGN.md](../FRAMEWORK_DESIGN.md) or [USAGE_EXAMPLES.md](../USAGE_EXAMPLES.md)

