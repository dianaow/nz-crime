#!/usr/bin/env python3
"""
Practical Examples for Performance Benchmarking

This file shows how to use the performance monitoring system
for specific scenarios and real-world testing.
"""

import asyncio
import time
from performance_monitor import PerformanceMonitor, interpret_benchmarks
from app import CrimeDataTransformer  # Your data processing class
import logging

logger = logging.getLogger(__name__)

async def example_api_load_test():
    """
    Example: Load test your API to measure response times under load
    """
    print("🔥 Running API Load Test Example")
    print("-" * 50)
    
    monitor = PerformanceMonitor(api_base_url="http://localhost:8000")
    
    # Test with increasing load
    load_levels = [10, 50, 100, 200]
    
    for num_requests in load_levels:
        print(f"\n📈 Testing with {num_requests} concurrent requests...")
        
        with monitor.measure_time(f"load_test_{num_requests}_requests"):
            results = await monitor.benchmark_api_response_times(num_requests=num_requests)
        
        # Print results for this load level
        for endpoint, metrics in results.items():
            print(f"  {endpoint}:")
            print(f"    Cached avg: {metrics['cached_avg']:.2f}ms")
            print(f"    Uncached: {metrics['uncached_avg']:.2f}ms")
            print(f"    95th percentile: {metrics['cached_p95']:.2f}ms")

def example_data_ingestion_benchmark():
    """
    Example: Benchmark your actual data ingestion process
    """
    print("⚡ Running Data Ingestion Benchmark Example")
    print("-" * 50)
    
    monitor = PerformanceMonitor()
    
    # Create a sample transformer (you'd use your actual data file)
    def simulate_data_processing():
        """Simulate processing a batch of crime data"""
        # This would be your actual data processing
        transformer = CrimeDataTransformer(input_file="sample_data.csv")
        # Add your actual processing steps here
        time.sleep(2)  # Simulate processing time
        return {"records_processed": 10000}
    
    # Measure memory usage during data processing
    print("💾 Measuring memory usage during data processing...")
    memory_results = monitor.benchmark_memory_usage(simulate_data_processing)
    
    print(f"Peak memory usage: {memory_results['peak_memory_mb']:.2f} MB")
    print(f"Memory delta: {memory_results['memory_delta_mb']:.2f} MB")
    print(f"Processing time: {memory_results['execution_time_seconds']:.2f} seconds")

async def example_cache_performance_test():
    """
    Example: Test cache performance with realistic usage patterns
    """
    print("🎯 Running Cache Performance Test Example")
    print("-" * 50)
    
    monitor = PerformanceMonitor()
    
    # Test cache performance with different patterns
    cache_scenarios = [
        {"name": "Cold Cache", "requests": 50, "clear_cache": True},
        {"name": "Warm Cache", "requests": 100, "clear_cache": False},
        {"name": "Hot Cache", "requests": 200, "clear_cache": False}
    ]
    
    for scenario in cache_scenarios:
        print(f"\n🔥 Testing {scenario['name']} scenario...")
        
        if scenario['clear_cache']:
            # Clear cache for cold cache test
            try:
                import aiohttp
                async with aiohttp.ClientSession() as session:
                    await session.delete(f"{monitor.api_base_url}/api/cache/clear")
            except:
                pass
        
        cache_results = await monitor.benchmark_cache_hit_rate(num_requests=scenario['requests'])
        
        if cache_results:
            print(f"  Hit rate: {cache_results['hit_rate_percent']:.1f}%")
            print(f"  Hits: {cache_results['hits']}")
            print(f"  Misses: {cache_results['misses']}")

def example_database_performance_analysis():
    """
    Example: Analyze database query performance patterns
    """
    print("🗄️ Running Database Performance Analysis Example")
    print("-" * 50)
    
    monitor = PerformanceMonitor()
    
    # Test different query complexities
    query_scenarios = [
        {"name": "Simple Queries", "num_queries": 20},
        {"name": "Complex Queries", "num_queries": 50},
        {"name": "Heavy Load", "num_queries": 100}
    ]
    
    for scenario in query_scenarios:
        print(f"\n⚡ Testing {scenario['name']}...")
        
        db_results = monitor.benchmark_database_query_performance(
            num_queries=scenario['num_queries']
        )
        
        if db_results:
            print(f"  Average query time: {db_results['average_query_time_ms']:.2f}ms")
            print(f"  95th percentile: {db_results['p95_query_time_ms']:.2f}ms")
            print(f"  Min/Max: {db_results['min_query_time_ms']:.2f}ms / {db_results['max_query_time_ms']:.2f}ms")

async def example_comprehensive_system_test():
    """
    Example: Run a comprehensive system performance test
    """
    print("🚀 Running Comprehensive System Performance Test")
    print("=" * 60)
    
    monitor = PerformanceMonitor()
    
    # 1. Baseline system performance
    print("\n1️⃣ Measuring baseline system performance...")
    await monitor.benchmark_api_response_times(num_requests=30)
    
    # 2. Cache performance
    print("\n2️⃣ Analyzing cache performance...")
    await monitor.benchmark_cache_hit_rate(num_requests=50)
    
    # 3. Database performance
    print("\n3️⃣ Testing database performance...")
    monitor.benchmark_database_query_performance(num_queries=25)
    
    # 4. Data ingestion simulation
    print("\n4️⃣ Simulating data ingestion...")
    monitor.simulate_data_ingestion_benchmark(num_records=5000)
    
    # 5. Generate comprehensive report
    print("\n5️⃣ Generating performance report...")
    report = monitor.generate_performance_report()
    interpretations = interpret_benchmarks(report)
    
    # Display results
    print("\n" + "="*60)
    print("📊 COMPREHENSIVE PERFORMANCE RESULTS")
    print("="*60)
    
    for metric_type, interpretation in interpretations.items():
        print(f"\n{metric_type.replace('_', ' ').title()}:")
        print(f"  {interpretation}")
    
    # Show key metrics
    print(f"\n📈 Summary Statistics:")
    for metric_type, stats in report['summary'].items():
        print(f"  {metric_type.title()}: {stats['average']:.2f} (avg), {stats['min']:.2f}-{stats['max']:.2f} (range)")
    
    # Save detailed report
    monitor.save_report(report)
    print(f"\n💾 Detailed report saved with {report['total_metrics_collected']} metrics")

def example_custom_metric_tracking():
    """
    Example: Track custom metrics for your specific use case
    """
    print("📊 Custom Metric Tracking Example")
    print("-" * 50)
    
    monitor = PerformanceMonitor()
    
    # Example: Track custom business metrics
    with monitor.measure_time("custom_data_transformation", {"batch_size": 1000}):
        # Your custom operation here
        time.sleep(1)  # Simulate work
        
        # Record custom metrics
        monitor.record_metric(
            metric_name="records_processed_per_second",
            value=1000,
            unit="records/sec",
            context={"operation": "data_transformation"}
        )
        
        monitor.record_metric(
            metric_name="error_rate",
            value=0.5,
            unit="%",
            context={"operation": "data_validation"}
        )
    
    print("✅ Custom metrics recorded successfully")

def interpret_your_results(report):
    """
    Example: How to interpret your benchmark results
    """
    print("\n🎯 HOW TO INTERPRET YOUR RESULTS")
    print("=" * 50)
    
    # API Response Time Guidelines
    print("\n📡 API Response Time Guidelines:")
    print("  ✅ Excellent: <50ms (cached), <200ms (uncached)")
    print("  ✅ Good: 50-100ms (cached), 200-500ms (uncached)")
    print("  ⚠️  Fair: 100-200ms (cached), 500ms-1s (uncached)")
    print("  ❌ Poor: >200ms (cached), >1s (uncached)")
    
    # Cache Hit Rate Guidelines
    print("\n🎯 Cache Hit Rate Guidelines:")
    print("  ✅ Excellent: >95% hit rate")
    print("  ✅ Good: 85-95% hit rate")
    print("  ⚠️  Fair: 70-85% hit rate")
    print("  ❌ Poor: <70% hit rate")
    
    # Memory Usage Guidelines
    print("\n💾 Memory Usage Guidelines:")
    print("  ✅ Excellent: <1GB for typical operations")
    print("  ✅ Good: 1-2GB for large datasets")
    print("  ⚠️  Fair: 2-4GB (monitor for optimization)")
    print("  ❌ Poor: >4GB (needs optimization)")
    
    # Database Query Guidelines
    print("\n🗄️ Database Query Guidelines:")
    print("  ✅ Excellent: <10ms average")
    print("  ✅ Good: 10-50ms average")
    print("  ⚠️  Fair: 50-100ms average")
    print("  ❌ Poor: >100ms average")
    
    # Data Ingestion Guidelines
    print("\n⚡ Data Ingestion Guidelines:")
    print("  ✅ Excellent: >100,000 records/minute")
    print("  ✅ Good: 50,000-100,000 records/minute")
    print("  ⚠️  Fair: 25,000-50,000 records/minute")
    print("  ❌ Poor: <25,000 records/minute")

async def main():
    """Run example benchmarks"""
    print("🚀 Performance Benchmarking Examples")
    print("=" * 60)
    
    # Choose which examples to run
    examples = [
        ("API Load Test", example_api_load_test),
        ("Data Ingestion Benchmark", example_data_ingestion_benchmark),
        ("Cache Performance Test", example_cache_performance_test),
        ("Database Performance Analysis", example_database_performance_analysis),
        ("Custom Metric Tracking", example_custom_metric_tracking),
        ("Comprehensive System Test", example_comprehensive_system_test)
    ]
    
    print("\nAvailable examples:")
    for i, (name, _) in enumerate(examples, 1):
        print(f"  {i}. {name}")
    
    print("\nRunning comprehensive system test...")
    await example_comprehensive_system_test()
    
    # Show interpretation guidelines
    interpret_your_results({})

if __name__ == "__main__":
    asyncio.run(main()) 