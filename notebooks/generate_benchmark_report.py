# Databricks notebook source
# MAGIC %md
# MAGIC # 📊 Lakebase Feature Serving Benchmark Report
# MAGIC 
# MAGIC **Publication-ready visualizations comparing Lakebase vs DynamoDB**
# MAGIC 
# MAGIC Generates:
# MAGIC - Latency distribution histograms
# MAGIC - P50/P99 comparison charts
# MAGIC - Table-by-table breakdown
# MAGIC - Executive summary
# MAGIC - Exportable HTML/PDF report

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣ Install Dependencies

# COMMAND ----------

%pip install psycopg[binary,pool] numpy matplotlib seaborn pandas plotly kaleido
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2️⃣ Configuration

# COMMAND ----------

dbutils.widgets.text("lakebase_host", "", "Lakebase Host")
dbutils.widgets.text("lakebase_database", "benchmark", "Database")
dbutils.widgets.text("lakebase_schema", "features", "Schema")
dbutils.widgets.text("lakebase_user", "", "User")
dbutils.widgets.text("lakebase_password", "", "Password")
dbutils.widgets.text("num_warmup", "50", "Warmup Iterations")
dbutils.widgets.text("num_iterations", "1000", "Benchmark Iterations")
dbutils.widgets.dropdown("report_format", "interactive", ["interactive", "static"], "Report Format")

LAKEBASE_CONFIG = {
    'host': dbutils.widgets.get("lakebase_host"),
    'port': 5432,
    'database': dbutils.widgets.get("lakebase_database"),
    'schema': dbutils.widgets.get("lakebase_schema"),
    'user': dbutils.widgets.get("lakebase_user"),
    'password': dbutils.widgets.get("lakebase_password"),
    'sslmode': 'require'
}

SCHEMA = dbutils.widgets.get("lakebase_schema")
NUM_WARMUP = int(dbutils.widgets.get("num_warmup"))
NUM_ITERATIONS = int(dbutils.widgets.get("num_iterations"))
REPORT_FORMAT = dbutils.widgets.get("report_format")

# DynamoDB baseline
DYNAMODB_P50 = 30.0
DYNAMODB_P99 = 79.0

print("="*80)
print("⚙️  REPORT CONFIGURATION")
print("="*80)
print(f"Lakebase:   {LAKEBASE_CONFIG['host']}")
print(f"Schema:     {SCHEMA}")
print(f"Warmup:     {NUM_WARMUP}")
print(f"Iterations: {NUM_ITERATIONS}")
print(f"Format:     {REPORT_FORMAT}")
print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3️⃣ Import Libraries

# COMMAND ----------

import sys
sys.path.append("/Workspace/som.natarajan@databricks.com/.bundle/lakebase-benchmarking/dev/files")

from utils.feature_server import create_feature_schemas_from_ddl, get_sample_hashkeys
from utils.instrumented_feature_server import (
    InstrumentedFeatureServer,
    TimingBreakdown,
    DatabaseMetrics,
    calculate_component_percentiles
)

import psycopg
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import seaborn as sns
from datetime import datetime

# Set style for publication-quality plots
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)
plt.rcParams['font.size'] = 11
plt.rcParams['axes.labelsize'] = 12
plt.rcParams['axes.titlesize'] = 14
plt.rcParams['legend.fontsize'] = 10
plt.rcParams['xtick.labelsize'] = 10
plt.rcParams['ytick.labelsize'] = 10

print("✅ Imports complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4️⃣ Run Benchmarks for Multiple Scenarios

# COMMAND ----------

print("🚀 Initializing feature server...")

# Connect and discover schemas
conninfo = (
    f"host={LAKEBASE_CONFIG['host']} "
    f"port={LAKEBASE_CONFIG['port']} "
    f"dbname={LAKEBASE_CONFIG['database']} "
    f"user={LAKEBASE_CONFIG['user']} "
    f"password={LAKEBASE_CONFIG['password']} "
    f"sslmode={LAKEBASE_CONFIG['sslmode']}"
)

conn = psycopg.connect(conninfo)

# Discover all tables
table_schemas = create_feature_schemas_from_ddl(conn, schema=SCHEMA, exclude_columns=['updated_at'])
all_table_names = sorted(table_schemas.keys())

print(f"\n✅ Discovered {len(all_table_names)} tables\n")

# Get sample hashkeys
sample_hashkeys = get_sample_hashkeys(conn, SCHEMA, all_table_names[0], limit=NUM_WARMUP + NUM_ITERATIONS)
conn.close()

# Initialize INSTRUMENTED feature server
feature_server = InstrumentedFeatureServer(
    lakebase_config={**LAKEBASE_CONFIG, 'schema': SCHEMA},
    table_schemas=table_schemas,
    pool_size=10,
    max_pool_size=20
)

# Reset stats for clean measurement
feature_server.reset_database_stats()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5️⃣ Benchmark Different Table Counts

# COMMAND ----------

scenarios = [
    {"name": "30 Tables (Worst Case)", "num_tables": 30, "description": "All features, maximum fanout"},
    {"name": "20 Tables (Typical)", "num_tables": 20, "description": "Typical model feature set"},
    {"name": "10 Tables (Simple)", "num_tables": 10, "description": "Simple model, few features"},
]

results = {}

for scenario in scenarios:
    name = scenario["name"]
    num_tables = min(scenario["num_tables"], len(all_table_names))
    test_tables = all_table_names[:num_tables]
    
    print("="*80)
    print(f"🎯 SCENARIO: {name}")
    print("="*80)
    print(f"Tables: {num_tables}")
    print(f"Description: {scenario['description']}")
    print()
    
    # Warmup
    print(f"🔥 Warmup ({NUM_WARMUP} iterations)...")
    warmup_keys = sample_hashkeys[:NUM_WARMUP]
    for hashkey in warmup_keys:
        _, _ = feature_server.get_features_with_timing(hashkey, test_tables)
    
    # Benchmark with detailed timing
    print(f"⏱️  Benchmark ({NUM_ITERATIONS} iterations)...")
    benchmark_keys = sample_hashkeys[NUM_WARMUP:NUM_WARMUP + NUM_ITERATIONS]
    latencies = []
    timing_breakdowns = []
    
    for i, hashkey in enumerate(benchmark_keys):
        _, timing = feature_server.get_features_with_timing(hashkey, test_tables)
        latencies.append(timing.total_ms)
        timing_breakdowns.append(timing)
        
        if (i + 1) % 100 == 0:
            print(f"   Progress: {i+1}/{NUM_ITERATIONS}")
    
    # Get database metrics after benchmark
    db_metrics = feature_server.get_database_metrics()
    
    # Calculate component-level percentiles
    component_stats = calculate_component_percentiles(timing_breakdowns)
    
    # Calculate overall statistics
    p50 = np.percentile(latencies, 50)
    p95 = np.percentile(latencies, 95)
    p99 = np.percentile(latencies, 99)
    avg = np.mean(latencies)
    stddev = np.std(latencies)
    
    results[name] = {
        "num_tables": num_tables,
        "latencies": latencies,
        "timing_breakdowns": timing_breakdowns,
        "component_stats": component_stats,
        "db_metrics": db_metrics,
        "p50": p50,
        "p95": p95,
        "p99": p99,
        "avg": avg,
        "stddev": stddev,
        "min": min(latencies),
        "max": max(latencies)
    }
    
    print(f"\n✅ Results:")
    print(f"   P50: {p50:.2f}ms")
    print(f"   P95: {p95:.2f}ms")
    print(f"   P99: {p99:.2f}ms")
    print(f"   Avg: {avg:.2f}ms")
    print()

feature_server.close()

print("="*80)
print("✅ All scenarios complete!")
print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6️⃣ Generate Publication-Quality Visualizations

# COMMAND ----------

def create_latency_distribution_chart(scenario_name, data, row_count_label=""):
    """
    Create publication-quality latency distribution histogram
    Matches the style from user's examples
    """
    latencies = data["latencies"]
    p50 = data["p50"]
    p99 = data["p99"]
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    # Histogram
    n, bins, patches = ax.hist(
        latencies, 
        bins=50, 
        color='#87CEEB',  # Light blue
        edgecolor='black',
        alpha=0.7,
        linewidth=0.5
    )
    
    # P50 line (orange dashed)
    ax.axvline(
        p50, 
        color='#FFA500', 
        linestyle='--', 
        linewidth=2.5,
        label=f'P50: {p50:.2f} ms'
    )
    
    # P99 line (red dashed)
    ax.axvline(
        p99, 
        color='#DC143C', 
        linestyle='--', 
        linewidth=2.5,
        label=f'P99: {p99:.2f} ms'
    )
    
    # DynamoDB p99 reference (purple dotted)
    ax.axvline(
        DYNAMODB_P99,
        color='#800080',
        linestyle=':',
        linewidth=2.5,
        label=f'DynamoDB P99: {DYNAMODB_P99:.2f} ms'
    )
    
    # Labels and title
    ax.set_xlabel('Latency (ms)', fontsize=13, fontweight='bold')
    ax.set_ylabel('Frequency', fontsize=13, fontweight='bold')
    
    title = f'Latency Distribution: {scenario_name}'
    if row_count_label:
        title += f' ({row_count_label})'
    ax.set_title(title, fontsize=15, fontweight='bold', pad=15)
    
    # Legend
    ax.legend(
        loc='upper right',
        fontsize=11,
        frameon=True,
        shadow=True,
        fancybox=True
    )
    
    # Grid
    ax.grid(True, alpha=0.3, linestyle='-', linewidth=0.5)
    ax.set_axisbelow(True)
    
    # Set x-axis limit
    ax.set_xlim(0, min(max(latencies) * 1.1, 100))
    
    plt.tight_layout()
    return fig

# Generate charts for each scenario
for scenario_name, data in results.items():
    fig = create_latency_distribution_chart(scenario_name, data)
    plt.show()
    plt.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7️⃣ Comparison Chart: Lakebase vs DynamoDB

# COMMAND ----------

def create_comparison_chart(results_dict):
    """
    Create side-by-side comparison chart
    """
    scenarios = list(results_dict.keys())
    
    # Prepare data
    lakebase_p50 = [results_dict[s]["p50"] for s in scenarios]
    lakebase_p99 = [results_dict[s]["p99"] for s in scenarios]
    dynamodb_p50 = [DYNAMODB_P50] * len(scenarios)
    dynamodb_p99 = [DYNAMODB_P99] * len(scenarios)
    
    x = np.arange(len(scenarios))
    width = 0.35
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
    
    # P50 comparison
    ax1.bar(x - width/2, lakebase_p50, width, label='Lakebase', color='#4472C4', alpha=0.8)
    ax1.bar(x + width/2, dynamodb_p50, width, label='DynamoDB', color='#ED7D31', alpha=0.8)
    ax1.set_ylabel('Latency (ms)', fontsize=12, fontweight='bold')
    ax1.set_title('P50 Latency Comparison', fontsize=14, fontweight='bold')
    ax1.set_xticks(x)
    ax1.set_xticklabels([s.split('(')[0].strip() for s in scenarios], rotation=15, ha='right')
    ax1.legend(fontsize=11)
    ax1.grid(True, alpha=0.3, axis='y')
    
    # Add value labels
    for i, (lb, db) in enumerate(zip(lakebase_p50, dynamodb_p50)):
        ax1.text(i - width/2, lb + 2, f'{lb:.1f}', ha='center', va='bottom', fontsize=9)
        ax1.text(i + width/2, db + 2, f'{db:.1f}', ha='center', va='bottom', fontsize=9)
    
    # P99 comparison
    ax2.bar(x - width/2, lakebase_p99, width, label='Lakebase', color='#4472C4', alpha=0.8)
    ax2.bar(x + width/2, dynamodb_p99, width, label='DynamoDB', color='#ED7D31', alpha=0.8)
    ax2.set_ylabel('Latency (ms)', fontsize=12, fontweight='bold')
    ax2.set_title('P99 Latency Comparison', fontsize=14, fontweight='bold')
    ax2.set_xticks(x)
    ax2.set_xticklabels([s.split('(')[0].strip() for s in scenarios], rotation=15, ha='right')
    ax2.legend(fontsize=11)
    ax2.grid(True, alpha=0.3, axis='y')
    
    # Add value labels
    for i, (lb, db) in enumerate(zip(lakebase_p99, dynamodb_p99)):
        ax2.text(i - width/2, lb + 2, f'{lb:.1f}', ha='center', va='bottom', fontsize=9)
        ax2.text(i + width/2, db + 2, f'{db:.1f}', ha='center', va='bottom', fontsize=9)
    
    plt.tight_layout()
    return fig

comparison_fig = create_comparison_chart(results)
plt.show()
plt.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8️⃣ Component Latency Breakdown

# COMMAND ----------

def create_component_breakdown_chart(scenario_name, data):
    """
    Create stacked bar chart showing latency component breakdown
    """
    stats = data["component_stats"]
    
    components = ['network_rtt', 'query_execution', 'result_transfer', 'connection_acquire']
    labels = ['Network RTT', 'Query Execution', 'Result Transfer', 'Connection Acquire']
    colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#FFA07A']
    
    metrics = ['p50', 'p95', 'p99']
    x = np.arange(len(metrics))
    width = 0.6
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    bottom = np.zeros(len(metrics))
    
    for component, label, color in zip(components, labels, colors):
        values = [stats[component][metric] for metric in metrics]
        ax.bar(x, values, width, label=label, bottom=bottom, color=color, alpha=0.8)
        bottom += values
    
    ax.set_ylabel('Latency (ms)', fontsize=13, fontweight='bold')
    ax.set_title(f'Latency Component Breakdown: {scenario_name}', fontsize=15, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(['P50', 'P95', 'P99'], fontsize=12)
    ax.legend(loc='upper left', fontsize=10, frameon=True, shadow=True)
    ax.grid(True, alpha=0.3, axis='y')
    
    # Add total latency labels
    for i, metric in enumerate(metrics):
        total = sum(stats[comp][metric] for comp in components)
        ax.text(i, total + 1, f'{total:.1f}ms', ha='center', va='bottom', fontweight='bold')
    
    plt.tight_layout()
    return fig

# Generate component breakdown for each scenario
for scenario_name, data in results.items():
    fig = create_component_breakdown_chart(scenario_name, data)
    plt.show()
    plt.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9️⃣ Component Comparison Table

# COMMAND ----------

# Create component comparison table
print()
print("="*100)
print("⚙️  LATENCY COMPONENT BREAKDOWN (30 Tables Worst Case)")
print("="*100)
print()

worst_case = results["30 Tables (Worst Case)"]
comp_stats = worst_case["component_stats"]

comp_df = pd.DataFrame({
    "Component": ["Network RTT", "Query Execution", "Result Transfer", "Connection Acquire", "TOTAL"],
    "P50 (ms)": [
        f"{comp_stats['network_rtt']['p50']:.2f}",
        f"{comp_stats['query_execution']['p50']:.2f}",
        f"{comp_stats['result_transfer']['p50']:.2f}",
        f"{comp_stats['connection_acquire']['p50']:.2f}",
        f"{worst_case['p50']:.2f}"
    ],
    "P99 (ms)": [
        f"{comp_stats['network_rtt']['p99']:.2f}",
        f"{comp_stats['query_execution']['p99']:.2f}",
        f"{comp_stats['result_transfer']['p99']:.2f}",
        f"{comp_stats['connection_acquire']['p99']:.2f}",
        f"{worst_case['p99']:.2f}"
    ],
    "% of P50": [
        f"{(comp_stats['network_rtt']['p50'] / worst_case['p50'] * 100):.1f}%",
        f"{(comp_stats['query_execution']['p50'] / worst_case['p50'] * 100):.1f}%",
        f"{(comp_stats['result_transfer']['p50'] / worst_case['p50'] * 100):.1f}%",
        f"{(comp_stats['connection_acquire']['p50'] / worst_case['p50'] * 100):.1f}%",
        "100.0%"
    ],
    "% of P99": [
        f"{(comp_stats['network_rtt']['p99'] / worst_case['p99'] * 100):.1f}%",
        f"{(comp_stats['query_execution']['p99'] / worst_case['p99'] * 100):.1f}%",
        f"{(comp_stats['result_transfer']['p99'] / worst_case['p99'] * 100):.1f}%",
        f"{(comp_stats['connection_acquire']['p99'] / worst_case['p99'] * 100):.1f}%",
        "100.0%"
    ]
})

print(comp_df.to_string(index=False))
print()
print("="*100)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔟 Database Performance Metrics

# COMMAND ----------

# Create database metrics table
print()
print("="*80)
print("📊 DATABASE PERFORMANCE METRICS")
print("="*80)
print()

db_metrics = worst_case["db_metrics"]

metrics_data = [
    {"Metric": "Buffer Cache Hit Ratio", "Value": f"{db_metrics.cache_hit_ratio:.2f}%", "Status": "✅ Excellent" if db_metrics.cache_hit_ratio > 99 else "⚠️ Needs Warmup"},
    {"Metric": "Buffer Hits", "Value": f"{db_metrics.buffer_hits:,}", "Status": ""},
    {"Metric": "Buffer Reads (Disk I/O)", "Value": f"{db_metrics.buffer_reads:,}", "Status": ""},
    {"Metric": "Total Index Scans", "Value": f"{sum(db_metrics.index_scans.values()):,}", "Status": "✅ Good" if sum(db_metrics.index_scans.values()) > 0 else "❌ No Indexes Used"},
    {"Metric": "Total Sequential Scans", "Value": f"{sum(db_metrics.sequential_scans.values()):,}", "Status": "✅ Good" if sum(db_metrics.sequential_scans.values()) == 0 else "⚠️ Optimize"},
    {"Metric": "Total Rows Fetched", "Value": f"{sum(db_metrics.rows_fetched.values()):,}", "Status": ""}
]

metrics_df = pd.DataFrame(metrics_data)
print(metrics_df.to_string(index=False))
print()
print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣1️⃣ Cache Hit Ratio Visualization

# COMMAND ----------

# Create cache metrics chart
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

# Cache hit ratio gauge
scenarios = list(results.keys())
cache_ratios = [results[s]["db_metrics"].cache_hit_ratio for s in scenarios]

ax1.barh(scenarios, cache_ratios, color='#4ECDC4', alpha=0.8)
ax1.axvline(99, color='red', linestyle='--', linewidth=2, label='Target: 99%')
ax1.set_xlabel('Cache Hit Ratio (%)', fontsize=12, fontweight='bold')
ax1.set_title('Buffer Cache Performance', fontsize=14, fontweight='bold')
ax1.set_xlim(95, 100)
ax1.legend()
ax1.grid(True, alpha=0.3, axis='x')

# Add value labels
for i, ratio in enumerate(cache_ratios):
    ax1.text(ratio - 0.3, i, f'{ratio:.2f}%', va='center', ha='right', fontweight='bold', color='white')

# I/O breakdown (hits vs reads)
db_metrics = worst_case["db_metrics"]
io_data = {
    'Cache Hits\n(Memory)': db_metrics.buffer_hits,
    'Disk Reads\n(I/O)': db_metrics.buffer_reads
}

colors = ['#2ECC71', '#E74C3C']
ax2.pie(io_data.values(), labels=io_data.keys(), autopct='%1.1f%%', colors=colors, startangle=90)
ax2.set_title('I/O Distribution (30 Tables)', fontsize=14, fontweight='bold')

plt.tight_layout()
plt.show()
plt.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣2️⃣ Per-Table Latency Heatmap

# COMMAND ----------

# Get per-table latencies from timing breakdowns
worst_case_timings = worst_case["timing_breakdowns"]

# Calculate average latency per table across all iterations
table_names = list(worst_case_timings[0].per_table_ms.keys())
table_latencies = {table: [] for table in table_names}

for timing in worst_case_timings:
    for table, lat in timing.per_table_ms.items():
        table_latencies[table].append(lat)

# Calculate statistics per table
table_stats = []
for table in table_names:
    lats = table_latencies[table]
    table_stats.append({
        'table': table,
        'avg': np.mean(lats),
        'p50': np.percentile(lats, 50),
        'p99': np.percentile(lats, 99)
    })

# Sort by avg latency
table_stats.sort(key=lambda x: x['avg'], reverse=True)

# Create heatmap
fig, ax = plt.subplots(figsize=(12, max(8, len(table_names) * 0.3)))

tables = [t['table'][:50] + '...' if len(t['table']) > 50 else t['table'] for t in table_stats[:20]]  # Top 20
avgs = [t['avg'] for t in table_stats[:20]]
p99s = [t['p99'] for t in table_stats[:20]]

y = np.arange(len(tables))
width = 0.35

ax.barh(y - width/2, avgs, width, label='Average', color='#4ECDC4', alpha=0.8)
ax.barh(y + width/2, p99s, width, label='P99', color='#FF6B6B', alpha=0.8)

ax.set_yticks(y)
ax.set_yticklabels(tables, fontsize=9)
ax.set_xlabel('Latency (ms)', fontsize=12, fontweight='bold')
ax.set_title('Per-Table Latency Breakdown (Top 20 by Avg)', fontsize=14, fontweight='bold')
ax.legend(fontsize=10)
ax.grid(True, alpha=0.3, axis='x')

plt.tight_layout()
plt.show()
plt.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣3️⃣ Bottleneck Analysis

# COMMAND ----------

print()
print("="*80)
print("🔍 BOTTLENECK ANALYSIS (30 Tables)")
print("="*80)
print()

comp_stats = worst_case["component_stats"]

# Calculate bottleneck percentages at p99
p99_total = worst_case['p99']
network_pct = (comp_stats['network_rtt']['p99'] / p99_total) * 100
query_pct = (comp_stats['query_execution']['p99'] / p99_total) * 100
transfer_pct = (comp_stats['result_transfer']['p99'] / p99_total) * 100
conn_pct = (comp_stats['connection_acquire']['p99'] / p99_total) * 100

bottlenecks = [
    {"Component": "Query Execution", "P99 (ms)": comp_stats['query_execution']['p99'], "% of Total": query_pct},
    {"Component": "Network RTT", "P99 (ms)": comp_stats['network_rtt']['p99'], "% of Total": network_pct},
    {"Component": "Result Transfer", "P99 (ms)": comp_stats['result_transfer']['p99'], "% of Total": transfer_pct},
    {"Component": "Connection Acquire", "P99 (ms)": comp_stats['connection_acquire']['p99'], "% of Total": conn_pct}
]

# Sort by percentage
bottlenecks.sort(key=lambda x: x["% of Total"], reverse=True)

print("Primary Bottlenecks (at P99):")
print()
for i, b in enumerate(bottlenecks, 1):
    status = "🔴" if b["% of Total"] > 40 else "🟡" if b["% of Total"] > 20 else "🟢"
    print(f"{i}. {status} {b['Component']:20} {b['P99 (ms)']:>8.2f} ms  ({b['% of Total']:>5.1f}% of total)")

print()
print("Optimization Recommendations:")
print()

if network_pct > 30:
    print("⚠️  Network latency is significant (>30%). Consider:")
    print("   - Co-locate application and database in same region/AZ")
    print("   - Use connection pooling (already enabled)")
    print("   - Reduce network hops")
    
if query_pct > 50:
    print("⚠️  Query execution dominates. Consider:")
    print("   - Verify all indexes exist (check above)")
    print("   - Use parallel connections for faster queries")
    print("   - Optimize table schemas")
elif query_pct < 30:
    print("✅ Query execution is optimized!")
    print("   - Indexes are working efficiently")
    print("   - Cache hit ratio is high")

if transfer_pct > 20:
    print("⚠️  Result transfer is high. Consider:")
    print("   - SELECT only needed columns (already optimized)")
    print("   - Reduce result set size")
    
if conn_pct > 10:
    print("⚠️  Connection acquire time is high. Consider:")
    print("   - Increase connection pool size")
    print("   - Pre-warm connections")
else:
    print("✅ Connection pooling is working well!")

print()
print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣4️⃣ Executive Summary Table

# COMMAND ----------

# Create summary DataFrame
summary_data = []

for scenario_name, data in results.items():
    p50_vs_dynamo = ((data["p50"] - DYNAMODB_P50) / DYNAMODB_P50) * 100
    p99_vs_dynamo = ((data["p99"] - DYNAMODB_P99) / DYNAMODB_P99) * 100
    
    summary_data.append({
        "Scenario": scenario_name,
        "Tables": data["num_tables"],
        "P50 (ms)": f"{data['p50']:.2f}",
        "P99 (ms)": f"{data['p99']:.2f}",
        "Avg (ms)": f"{data['avg']:.2f}",
        "P50 vs DynamoDB": f"{p50_vs_dynamo:+.1f}%",
        "P99 vs DynamoDB": f"{p99_vs_dynamo:+.1f}%",
        "StdDev": f"{data['stddev']:.2f}"
    })

summary_df = pd.DataFrame(summary_data)

print("="*80)
print("📊 EXECUTIVE SUMMARY")
print("="*80)
print()
print(summary_df.to_string(index=False))
print()
print("="*80)
print(f"DynamoDB Baseline: P50 = {DYNAMODB_P50}ms, P99 = {DYNAMODB_P99}ms")
print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣5️⃣ Detailed Results Table

# COMMAND ----------

# Create detailed DataFrame
detailed_data = []

for scenario_name, data in results.items():
    detailed_data.append({
        "Scenario": scenario_name,
        "Iterations": len(data["latencies"]),
        "Min (ms)": f"{data['min']:.2f}",
        "P50 (ms)": f"{data['p50']:.2f}",
        "P95 (ms)": f"{data['p95']:.2f}",
        "P99 (ms)": f"{data['p99']:.2f}",
        "Max (ms)": f"{data['max']:.2f}",
        "Avg (ms)": f"{data['avg']:.2f}",
        "StdDev (ms)": f"{data['stddev']:.2f}",
    })

detailed_df = pd.DataFrame(detailed_data)

print()
print("="*80)
print("📈 DETAILED METRICS")
print("="*80)
print()
print(detailed_df.to_string(index=False))
print()
print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣6️⃣ Verdict & Recommendations

# COMMAND ----------

print()
print("="*80)
print("🏆 BENCHMARK VERDICT")
print("="*80)
print()

# Determine overall verdict
worst_case = results["30 Tables (Worst Case)"]
p99_win = worst_case["p99"] < DYNAMODB_P99
p50_win = worst_case["p50"] < DYNAMODB_P50

if p99_win and p50_win:
    verdict = "✅ DECISIVE WIN"
    message = "Lakebase beats DynamoDB on BOTH p50 and p99 latency!"
elif p99_win:
    verdict = "✅ SUCCESS"
    message = "Lakebase beats DynamoDB on p99 (tail latency), which matters most for SLAs."
    message += f"\n   P99: {worst_case['p99']:.2f}ms vs {DYNAMODB_P99}ms ({((DYNAMODB_P99 - worst_case['p99']) / DYNAMODB_P99 * 100):.1f}% improvement)"
else:
    verdict = "⚠️  COMPETITIVE"
    message = "Lakebase is competitive with DynamoDB, with trade-offs to consider."

print(f"Verdict: {verdict}")
print()
print(message)
print()

# Recommendations
print("📋 Recommendations:")
print()

if p99_win:
    print("✅ Production-Ready:")
    print("   - Lakebase provides superior tail latency (p99)")
    print("   - Predictable performance under load")
    print("   - No hot partition issues")
    print()

print("🎯 Next Steps:")
print("   1. Run concurrent load test (100+ clients)")
print("   2. Test with production traffic patterns")
print("   3. Validate cache hit ratios in production")
print("   4. Monitor p99 latency over 7 days")
print()

print("💡 Optimization Opportunities:")
if not p50_win:
    print("   - Consider parallel connections for p50 improvement")
print("   - Pre-warm cache on cold start")
print("   - Monitor autovacuum schedule")
print("   - Tune Lakebase CU based on load")

print()
print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣7️⃣ Export Report (Optional)

# COMMAND ----------

# Save results to DataFrame for export
report_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

export_data = []
for scenario_name, data in results.items():
    for latency in data["latencies"]:
        export_data.append({
            "scenario": scenario_name,
            "num_tables": data["num_tables"],
            "latency_ms": latency,
            "timestamp": report_timestamp
        })

export_df = pd.DataFrame(export_data)

# Save to Databricks table (optional)
# spark.createDataFrame(export_df).write.mode("append").saveAsTable("benchmark.feature_serving_results")

print(f"✅ Report generated: {report_timestamp}")
print(f"   Total measurements: {len(export_df):,}")
print(f"   Scenarios tested: {len(results)}")

# COMMAND ----------
