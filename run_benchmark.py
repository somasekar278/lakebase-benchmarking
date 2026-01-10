#!/usr/bin/env python3
"""
Lakebase Benchmarking Framework - CLI Tool

This CLI provides easy access to all framework capabilities:
- Run benchmarks on multiple backends (Lakebase, DynamoDB, Aurora, Cosmos DB)
- Generate flexible schemas for custom workloads
- Compare performance and cost across backends
- Track and report detailed metrics

Repository: lakebase-benchmarking

Usage Examples:
    # List available backends
    python run_benchmark.py --list-backends
    
    # Run benchmark on Lakebase
    python run_benchmark.py --backend lakebase --num-tables 30 --features-per-table 5
    
    # Compare Lakebase vs DynamoDB
    python run_benchmark.py --backends lakebase dynamodb --compare --show-costs
    
    # Generate schema for custom workload
    python run_benchmark.py --generate-schema --num-tables 50 --features-per-table 3 --output schema_50t
    
    # Run with existing schema
    python run_benchmark.py --backend lakebase --use-schema generated/schema_50t
"""

import argparse
import sys
import json
import time
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

try:
    from config import (
        BACKEND_CONFIGS, BENCHMARK_CONFIG, FLEXIBLE_SCHEMA_CONFIG,
        get_enabled_backends, get_backend_config, validate_backend_config,
        print_config
    )
    from core.backend import BackendType, BackendFactory
    from core.workload import create_workload, QueryStrategy, Workload
    from utils.cost_tracker import get_cost_model, BenchmarkCost
    from scripts.setup.flexible_schema_generator import FlexibleSchemaGenerator
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Make sure you're running from the project root directory")
    sys.exit(1)


class BenchmarkCLI:
    """Main CLI handler for benchmarking framework"""
    
    def __init__(self):
        self.results = {}
        self.start_time = None
    
    def list_backends(self):
        """List all available backends and their status"""
        print("\n" + "=" * 80)
        print("AVAILABLE BACKENDS")
        print("=" * 80)
        
        for backend_name, config in BACKEND_CONFIGS.items():
            enabled = config.get('enabled', False)
            status = "✅ Enabled" if enabled else "❌ Disabled"
            
            print(f"\n{backend_name.upper()}")
            print(f"  Status: {status}")
            
            if enabled:
                is_valid, msg = validate_backend_config(backend_name)
                if is_valid:
                    print(f"  Config: ✅ Valid")
                    # Show key config details
                    if backend_name == 'lakebase':
                        print(f"  Host: {config.get('host', 'N/A')}")
                        print(f"  Database: {config.get('database', 'N/A')}")
                    elif backend_name == 'dynamodb':
                        print(f"  Region: {config.get('region', 'N/A')}")
                        print(f"  Mode: {config.get('billing_mode', 'N/A')}")
                else:
                    print(f"  Config: ❌ {msg}")
        
        print("\n" + "=" * 80)
        enabled_list = get_enabled_backends()
        if enabled_list:
            print(f"✅ Enabled backends: {', '.join(enabled_list)}")
        else:
            print("❌ No backends enabled")
            print("\n💡 To enable a backend, edit config.py and set enabled=True")
        print("=" * 80 + "\n")
    
    def generate_schema(
        self,
        num_tables: int,
        features_per_table: int,
        rows_per_table: int,
        output_dir: str
    ):
        """Generate flexible schema for custom workload"""
        print("\n" + "=" * 80)
        print("SCHEMA GENERATION")
        print("=" * 80)
        print(f"Tables: {num_tables}")
        print(f"Features per table: {features_per_table}")
        print(f"Total features: {num_tables * features_per_table}")
        print(f"Rows per table: {rows_per_table:,}")
        print(f"Output directory: {output_dir}")
        print("=" * 80 + "\n")
        
        # Create output directory
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Generate schema
        print("📝 Generating schema...")
        generator = FlexibleSchemaGenerator(
            num_tables=num_tables,
            features_per_table=features_per_table,
            rows_per_table=rows_per_table
        )
        
        # Generate SQL files
        setup_sql_file = output_path / "setup.sql"
        stored_proc_file = output_path / "stored_procedure.sql"
        
        generator.generate_setup_sql(output_file=str(setup_sql_file))
        generator.generate_stored_procedure(output_file=str(stored_proc_file))
        
        # Generate Python config
        config_file = output_path / "schema_config.py"
        generator.generate_python_config(output_file=str(config_file))
        
        # Generate README
        readme_file = output_path / "README.md"
        with open(readme_file, 'w') as f:
            f.write(f"# Generated Schema: {num_tables} Tables\n\n")
            f.write(f"**Configuration:**\n")
            f.write(f"- Tables: {num_tables}\n")
            f.write(f"- Features per table: {features_per_table}\n")
            f.write(f"- Total features: {num_tables * features_per_table}\n")
            f.write(f"- Rows per table: {rows_per_table:,}\n")
            f.write(f"- Generated: {datetime.now().isoformat()}\n\n")
            f.write(f"**Files:**\n")
            f.write(f"- `setup.sql` - Table creation and permissions\n")
            f.write(f"- `stored_procedure.sql` - Binpacked lookup procedure\n")
            f.write(f"- `schema_config.py` - Python configuration\n\n")
            f.write(f"**Usage:**\n")
            f.write(f"```bash\n")
            f.write(f"# Run setup SQL\n")
            f.write(f"psql -h HOST -p PORT -d DATABASE -U USER -f setup.sql\n")
            f.write(f"psql -h HOST -p PORT -d DATABASE -U USER -f stored_procedure.sql\n")
            f.write(f"\n")
            f.write(f"# Run benchmark\n")
            f.write(f"python run_benchmark.py --backend lakebase --use-schema {output_dir}\n")
            f.write(f"```\n")
        
        print(f"\n✅ Schema generated successfully!")
        print(f"📁 Output directory: {output_path.absolute()}")
        print(f"📄 Files created:")
        print(f"   - {setup_sql_file.name}")
        print(f"   - {stored_proc_file.name}")
        print(f"   - {config_file.name}")
        print(f"   - {readme_file.name}")
        print(f"\n💡 Next steps:")
        print(f"   1. Review generated SQL files")
        print(f"   2. Run setup.sql on your database")
        print(f"   3. Run stored_procedure.sql on your database")
        print(f"   4. Load data using Databricks notebooks")
        print(f"   5. Run benchmark: python run_benchmark.py --backend lakebase --use-schema {output_dir}")
    
    def validate_backends(self, backend_names: List[str]) -> bool:
        """Validate that all requested backends are available and configured"""
        print("\n🔍 Validating backends...")
        all_valid = True
        
        for backend_name in backend_names:
            if backend_name not in BACKEND_CONFIGS:
                print(f"❌ Backend '{backend_name}' not found in configuration")
                all_valid = False
                continue
            
            if not BACKEND_CONFIGS[backend_name].get('enabled', False):
                print(f"❌ Backend '{backend_name}' is disabled in config.py")
                all_valid = False
                continue
            
            is_valid, msg = validate_backend_config(backend_name)
            if not is_valid:
                print(f"❌ Backend '{backend_name}': {msg}")
                all_valid = False
            else:
                print(f"✅ Backend '{backend_name}': Configuration valid")
        
        return all_valid
    
    def run_benchmarks(
        self,
        backend_names: List[str],
        num_tables: int,
        features_per_table: int,
        rows_per_table: int,
        num_iterations: int,
        num_warmup: int,
        keys_per_table: int,
        compare: bool = False,
        show_costs: bool = False,
        output_file: Optional[str] = None
    ):
        """Run benchmarks on specified backends"""
        self.start_time = time.time()
        
        # Validate backends
        if not self.validate_backends(backend_names):
            print("\n❌ Backend validation failed. Exiting.")
            return
        
        # Print configuration
        print("\n" + "=" * 80)
        print("BENCHMARK CONFIGURATION")
        print("=" * 80)
        print(f"Backends: {', '.join(backend_names)}")
        print(f"Tables: {num_tables}")
        print(f"Features per table: {features_per_table}")
        print(f"Total features: {num_tables * features_per_table}")
        print(f"Rows per table: {rows_per_table:,}")
        print(f"Iterations: {num_iterations}")
        print(f"Warmup iterations: {num_warmup}")
        print(f"Keys per lookup: {keys_per_table}")
        print(f"Compare mode: {compare}")
        print(f"Show costs: {show_costs}")
        print("=" * 80 + "\n")
        
        # Note: This is a simplified implementation
        # Full implementation would include actual benchmark execution
        
        print("⚠️  Note: Full benchmark execution requires data to be loaded.")
        print("   Use Databricks notebooks to load data first.")
        print("   This CLI currently generates schemas and validates configuration.")
        print("\n💡 For complete benchmark execution, use:")
        print("   1. Generate schema: python run_benchmark.py --generate-schema --num-tables 30 --features-per-table 5")
        print("   2. Load data: Use Databricks notebooks in notebooks/data_loading/")
        print("   3. Run benchmarks: Use notebooks in notebooks/benchmarks/")
        
        self.results = {
            'configuration': {
                'backends': backend_names,
                'num_tables': num_tables,
                'features_per_table': features_per_table,
                'rows_per_table': rows_per_table,
                'num_iterations': num_iterations
            },
            'status': 'configuration_validated',
            'message': 'Use Databricks notebooks for full benchmark execution'
        }
        
        if output_file:
            with open(output_file, 'w') as f:
                json.dump(self.results, f, indent=2)
            print(f"\n📄 Results saved to: {output_file}")
    
    def show_configuration(self):
        """Show current framework configuration"""
        print("\n" + "=" * 80)
        print("FRAMEWORK CONFIGURATION")
        print("=" * 80)
        print_config()
        print("=" * 80 + "\n")


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description="Lakebase Benchmarking Framework",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List available backends
  python run_benchmark.py --list-backends
  
  # Show framework configuration
  python run_benchmark.py --show-config
  
  # Generate schema for custom workload
  python run_benchmark.py --generate-schema --num-tables 30 --features-per-table 5 --output generated/schema_30t
  
  # Run benchmark on Lakebase (requires data loaded)
  python run_benchmark.py --backend lakebase --num-tables 30 --features-per-table 5
  
  # Compare multiple backends
  python run_benchmark.py --backends lakebase dynamodb --compare --show-costs
  
For more information, see:
  - FLEXIBLE_BENCHMARK_GUIDE.md
  - BACKEND_DESIGN.md
  - COST_ANALYSIS_DESIGN.md
        """
    )
    
    # Action commands
    parser.add_argument('--list-backends', action='store_true',
                       help='List all available backends and their status')
    parser.add_argument('--show-config', action='store_true',
                       help='Show current framework configuration')
    parser.add_argument('--generate-schema', action='store_true',
                       help='Generate flexible schema for custom workload')
    
    # Backend selection
    parser.add_argument('--backend', type=str,
                       help='Single backend to benchmark (e.g., lakebase)')
    parser.add_argument('--backends', nargs='+', type=str,
                       help='Multiple backends to benchmark (e.g., lakebase dynamodb)')
    
    # Workload configuration
    parser.add_argument('--num-tables', type=int,
                       default=FLEXIBLE_SCHEMA_CONFIG['default_num_tables'],
                       help=f"Number of tables (default: {FLEXIBLE_SCHEMA_CONFIG['default_num_tables']})")
    parser.add_argument('--features-per-table', type=int,
                       default=FLEXIBLE_SCHEMA_CONFIG['default_features_per_table'],
                       help=f"Features per table (default: {FLEXIBLE_SCHEMA_CONFIG['default_features_per_table']})")
    parser.add_argument('--rows-per-table', type=int,
                       default=FLEXIBLE_SCHEMA_CONFIG['default_rows_per_table'],
                       help=f"Rows per table (default: {FLEXIBLE_SCHEMA_CONFIG['default_rows_per_table']:,})")
    
    # Benchmark configuration
    parser.add_argument('--iterations', type=int,
                       default=BENCHMARK_CONFIG['num_iterations'],
                       help=f"Number of benchmark iterations (default: {BENCHMARK_CONFIG['num_iterations']})")
    parser.add_argument('--warmup', type=int,
                       default=BENCHMARK_CONFIG['num_warmup'],
                       help=f"Number of warmup iterations (default: {BENCHMARK_CONFIG['num_warmup']})")
    parser.add_argument('--keys-per-table', type=int,
                       default=BENCHMARK_CONFIG['keys_per_table'],
                       help=f"Keys to lookup per table (default: {BENCHMARK_CONFIG['keys_per_table']})")
    
    # Output options
    parser.add_argument('--compare', action='store_true',
                       help='Generate comparison report across backends')
    parser.add_argument('--show-costs', action='store_true',
                       help='Include cost analysis in results')
    parser.add_argument('--output', type=str,
                       help='Output file for benchmark results (JSON format)')
    parser.add_argument('--output-dir', type=str,
                       default='generated',
                       help='Output directory for generated schemas (default: generated)')
    
    args = parser.parse_args()
    
    # Create CLI instance
    cli = BenchmarkCLI()
    
    # Handle actions
    if args.list_backends:
        cli.list_backends()
        return
    
    if args.show_config:
        cli.show_configuration()
        return
    
    if args.generate_schema:
        cli.generate_schema(
            num_tables=args.num_tables,
            features_per_table=args.features_per_table,
            rows_per_table=args.rows_per_table,
            output_dir=args.output_dir
        )
        return
    
    # Determine backends to benchmark
    backend_names = []
    if args.backend:
        backend_names = [args.backend]
    elif args.backends:
        backend_names = args.backends
    else:
        # No action specified, show help
        parser.print_help()
        return
    
    # Run benchmarks
    cli.run_benchmarks(
        backend_names=backend_names,
        num_tables=args.num_tables,
        features_per_table=args.features_per_table,
        rows_per_table=args.rows_per_table,
        num_iterations=args.iterations,
        num_warmup=args.warmup,
        keys_per_table=args.keys_per_table,
        compare=args.compare,
        show_costs=args.show_costs,
        output_file=args.output
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Benchmark interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

