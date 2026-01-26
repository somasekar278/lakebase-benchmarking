# Databricks notebook source
# MAGIC %md
# MAGIC # 📊 View Zipfian Benchmark Report
# MAGIC 
# MAGIC This notebook displays the generated HTML report inline.

# COMMAND ----------

dbutils.widgets.text("run_id", "e1f75f29", "Run ID")

# COMMAND ----------

run_id = dbutils.widgets.get("run_id")
report_path = f"/dbfs/FileStore/benchmark_reports/{run_id}/report.html"

print(f"📊 Loading report for run: {run_id}")
print(f"   Path: {report_path}")

# Read the HTML file
with open(report_path, 'r') as f:
    html_content = f.read()

print(f"✅ Report loaded ({len(html_content):,} bytes)")

# Display it
displayHTML(html_content)
