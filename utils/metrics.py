"""
Performance metrics collection and analysis utilities.
"""
import time
import numpy as np
from typing import List, Dict
import json


class PerformanceMetrics:
    """Collect and analyze performance metrics for benchmarks."""
    
    def __init__(self, name: str):
        """
        Initialize metrics collector.
        
        Args:
            name: Name of the benchmark
        """
        self.name = name
        self.latencies = []
        self.errors = []
        self.start_time = None
        self.end_time = None
    
    def record_latency(self, latency_ms: float):
        """Record a latency measurement in milliseconds."""
        self.latencies.append(latency_ms)
    
    def record_error(self, error: str):
        """Record an error."""
        self.errors.append(error)
    
    def start(self):
        """Start timing the benchmark."""
        self.start_time = time.time()
    
    def end(self):
        """End timing the benchmark."""
        self.end_time = time.time()
    
    def get_summary(self) -> Dict:
        """
        Get summary statistics.
        
        Returns:
            Dictionary with performance metrics
        """
        if not self.latencies:
            return {
                'name': self.name,
                'error': 'No latency data collected',
                'num_errors': len(self.errors)
            }
        
        latencies_array = np.array(self.latencies)
        
        summary = {
            'name': self.name,
            'num_requests': len(self.latencies),
            'num_errors': len(self.errors),
            'total_time_sec': self.end_time - self.start_time if self.end_time else None,
            'latency_ms': {
                'min': float(np.min(latencies_array)),
                'max': float(np.max(latencies_array)),
                'mean': float(np.mean(latencies_array)),
                'median': float(np.median(latencies_array)),
                'p50': float(np.percentile(latencies_array, 50)),
                'p95': float(np.percentile(latencies_array, 95)),
                'p99': float(np.percentile(latencies_array, 99)),
                'std': float(np.std(latencies_array))
            }
        }
        
        return summary
    
    def print_summary(self, budget_ms: float = 120.0):
        """
        Print formatted summary with pass/fail assessment.
        
        Args:
            budget_ms: Latency budget in milliseconds
        """
        summary = self.get_summary()
        
        print("\n" + "="*70)
        print(f"Benchmark: {summary['name']}")
        print("="*70)
        
        if 'error' in summary:
            print(f"❌ {summary['error']}")
            return
        
        print(f"Total Requests:  {summary['num_requests']}")
        print(f"Errors:          {summary['num_errors']}")
        
        if summary['total_time_sec']:
            print(f"Total Time:      {summary['total_time_sec']:.2f}s")
            print(f"Throughput:      {summary['num_requests']/summary['total_time_sec']:.1f} req/sec")
        
        print("\nLatency (ms):")
        latency = summary['latency_ms']
        print(f"  Min:     {latency['min']:8.2f} ms")
        print(f"  Mean:    {latency['mean']:8.2f} ms")
        print(f"  Median:  {latency['median']:8.2f} ms")
        print(f"  P95:     {latency['p95']:8.2f} ms")
        print(f"  P99:     {latency['p99']:8.2f} ms  ⚠️  CRITICAL for strict SLA")
        print(f"  Max:     {latency['max']:8.2f} ms")
        print(f"  StdDev:  {latency['std']:8.2f} ms")
        
        # Calculate P99.9 for ultra-strict SLA
        latencies_array = np.array(self.latencies)
        p999 = float(np.percentile(latencies_array, 99.9))
        print(f"  P99.9:   {p999:8.2f} ms  ⚠️  For mission-critical systems")
        
        # Assessment against budget
        print(f"\n📊 Assessment (Budget: {budget_ms}ms):")
        p95 = latency['p95']
        p99 = latency['p99']
        
        # P95 Assessment
        if p95 < budget_ms:
            margin = budget_ms - p95
            pct = (margin / budget_ms) * 100
            print(f"✅ P95: {p95:.2f}ms ({margin:.2f}ms / {pct:.1f}% under budget)")
        else:
            overage = p95 - budget_ms
            pct = (overage / budget_ms) * 100
            print(f"❌ P95: {p95:.2f}ms ({overage:.2f}ms / {pct:.1f}% over budget)")
        
        # P99 Assessment (Critical for production)
        safe_p99_budget = budget_ms * 0.67  # P99 should be < 2/3 of budget
        if p99 < safe_p99_budget:
            margin = safe_p99_budget - p99
            print(f"✅ P99: {p99:.2f}ms (safe for production, {margin:.2f}ms margin)")
        elif p99 < budget_ms:
            overage = p99 - safe_p99_budget
            print(f"⚠️  P99: {p99:.2f}ms (works but tight, {overage:.2f}ms over safe target)")
        else:
            overage = p99 - budget_ms
            print(f"❌ P99: {p99:.2f}ms (exceeds budget by {overage:.2f}ms - RISKY)")
        
        # Consistency Assessment
        cv = (latency['std'] / latency['mean']) * 100  # Coefficient of variation
        print(f"\n📈 Consistency:")
        if cv < 20:
            print(f"✅ Excellent (CV: {cv:.1f}%) - Very predictable")
        elif cv < 40:
            print(f"⚠️  Moderate (CV: {cv:.1f}%) - Some variance")
        else:
            print(f"❌ Poor (CV: {cv:.1f}%) - High variance, unpredictable")
        
        # Production Readiness for Strict SLA
        print(f"\n🎯 Production Readiness (Strict SLA):")
        checks = []
        checks.append(("P95 < budget", p95 < budget_ms))
        checks.append(("P99 < 67% of budget", p99 < safe_p99_budget))
        checks.append(("Max < 83% of budget", latency['max'] < budget_ms * 0.83))
        checks.append(("Low variance (CV<30%)", cv < 30))
        checks.append(("No errors", summary['num_errors'] == 0))
        
        passed = sum(1 for _, check in checks if check)
        for check_name, result in checks:
            status = "✅" if result else "❌"
            print(f"  {status} {check_name}")
        
        print(f"\n  Score: {passed}/{len(checks)} checks passed")
        if passed == len(checks):
            print(f"  ✅ PRODUCTION READY for strict SLA")
        elif passed >= len(checks) - 1:
            print(f"  ⚠️  MARGINAL - Review failing checks")
        else:
            print(f"  ❌ NOT RECOMMENDED for mission-critical real-time system")
        
        print("="*70 + "\n")
    
    def save_to_file(self, filepath: str):
        """
        Save metrics to JSON file.
        
        Args:
            filepath: Path to save the metrics
        """
        summary = self.get_summary()
        summary['raw_latencies'] = self.latencies
        summary['errors'] = self.errors
        
        with open(filepath, 'w') as f:
            json.dump(summary, f, indent=2)
        
        print(f"💾 Metrics saved to: {filepath}")


class Timer:
    """Context manager for timing operations."""
    
    def __init__(self):
        self.start_time = None
        self.end_time = None
        self.elapsed_ms = None
    
    def __enter__(self):
        self.start_time = time.perf_counter()
        return self
    
    def __exit__(self, *args):
        self.end_time = time.perf_counter()
        self.elapsed_ms = (self.end_time - self.start_time) * 1000  # Convert to ms
    
    def get_elapsed_ms(self) -> float:
        """Get elapsed time in milliseconds."""
        return self.elapsed_ms

