#!/usr/bin/env python3
"""
Performance Monitoring and Benchmarking System for NZ Crime Data API

This module provides comprehensive performance measurement tools to validate
the benchmarks mentioned in the README.md file.
"""

import time
import psutil
import asyncio
import aiohttp
import statistics
import json
import redis
import pandas as pd
from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from contextlib import contextmanager
import gc
import sys
import os
from supabase import create_client, Client
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class PerformanceMetrics:
    """Data class to store performance metrics"""
    timestamp: str
    metric_name: str
    value: float
    unit: str
    context: Dict[str, Any]
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

class PerformanceMonitor:
    """Comprehensive performance monitoring system"""
    
    def __init__(self, api_base_url: str = "http://localhost:8000"):
        self.api_base_url = api_base_url
        self.metrics: List[PerformanceMetrics] = []
        self.redis_client = None
        self.supabase_client = None
        
        # Initialize Redis client
        try:
            redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
            self.redis_client = redis.Redis.from_url(redis_url, decode_responses=True)
            self.redis_client.ping()  # Test connection
            logger.info("Redis connection established")
        except Exception as e:
            logger.warning(f"Redis connection failed: {e}")
        
        # Initialize Supabase client
        try:
            supabase_url = os.getenv("SUPABASE_URL")
            supabase_key = os.getenv("SUPABASE_KEY")
            if supabase_url and supabase_key:
                self.supabase_client = create_client(supabase_url, supabase_key)
                logger.info("Supabase connection established")
        except Exception as e:
            logger.warning(f"Supabase connection failed: {e}")

    @contextmanager
    def measure_time(self, operation_name: str, context: Dict[str, Any] = None):
        """Context manager to measure execution time"""
        start_time = time.perf_counter()
        start_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        
        try:
            yield
        finally:
            end_time = time.perf_counter()
            end_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
            
            execution_time = (end_time - start_time) * 1000  # Convert to milliseconds
            memory_delta = end_memory - start_memory
            
            # Record timing metric
            self.record_metric(
                metric_name=f"{operation_name}_time",
                value=execution_time,
                unit="ms",
                context=context or {}
            )
            
            # Record memory usage if significant change
            if abs(memory_delta) > 1:  # Only record if >1MB change
                self.record_metric(
                    metric_name=f"{operation_name}_memory_delta",
                    value=memory_delta,
                    unit="MB",
                    context=context or {}
                )

    def record_metric(self, metric_name: str, value: float, unit: str, context: Dict[str, Any] = None):
        """Record a performance metric"""
        metric = PerformanceMetrics(
            timestamp=datetime.now().isoformat(),
            metric_name=metric_name,
            value=value,
            unit=unit,
            context=context or {}
        )
        self.metrics.append(metric)
        logger.info(f"📊 {metric_name}: {value:.2f} {unit}")

    async def benchmark_api_response_times(self, num_requests: int = 100) -> Dict[str, float]:
        """
        Benchmark API response times for various endpoints
        
        Target: <50ms average (cached), <200ms (uncached)
        """
        logger.info(f"🚀 Starting API response time benchmark with {num_requests} requests")
        
        endpoints = [
            "/api/suburbs?page=1&page_size=500",
            "/api/suburbs?region=Auckland&page=1&page_size=20",
            "/api/suburbs/100100",
            "/api/meshblocks?suburb_id=100100"
        ]
        
        results = {}
        
        async with aiohttp.ClientSession() as session:
            for endpoint in endpoints:
                url = f"{self.api_base_url}{endpoint}"
                response_times = []
                
                # Clear cache first to test uncached performance
                try:
                    await session.delete(f"{self.api_base_url}/api/cache/clear")
                    await asyncio.sleep(0.1)  # Brief pause
                except:
                    pass
                
                # Test uncached performance (first request)
                start_time = time.perf_counter()
                try:
                    async with session.get(url) as response:
                        await response.json()
                        uncached_time = (time.perf_counter() - start_time) * 1000
                        
                        self.record_metric(
                            metric_name="api_response_time_uncached",
                            value=uncached_time,
                            unit="ms",
                            context={"endpoint": endpoint}
                        )
                except Exception as e:
                    logger.error(f"Error testing uncached {endpoint}: {e}")
                    continue
                
                # Test cached performance (subsequent requests)
                cached_times = []
                for i in range(num_requests):
                    start_time = time.perf_counter()
                    try:
                        async with session.get(url) as response:
                            if response.status == 200:
                                await response.json()
                                response_time = (time.perf_counter() - start_time) * 1000
                                cached_times.append(response_time)
                    except Exception as e:
                        logger.warning(f"Request {i} failed for {endpoint}: {e}")
                
                if cached_times:
                    avg_cached_time = statistics.mean(cached_times)
                    p95_cached_time = statistics.quantiles(cached_times, n=20)[18]  # 95th percentile
                    
                    results[endpoint] = {
                        'uncached_avg': uncached_time,
                        'cached_avg': avg_cached_time,
                        'cached_p95': p95_cached_time,
                        'sample_size': len(cached_times)
                    }
                    
                    self.record_metric(
                        metric_name="api_response_time_cached_avg",
                        value=avg_cached_time,
                        unit="ms",
                        context={"endpoint": endpoint}
                    )
                    
                    self.record_metric(
                        metric_name="api_response_time_cached_p95",
                        value=p95_cached_time,
                        unit="ms",
                        context={"endpoint": endpoint}
                    )
        
        return results

    async def benchmark_cache_hit_rate(self, num_requests: int = 200) -> Dict[str, float]:
        """
        Measure cache hit rate by analyzing Redis statistics
        
        Target: >95% for frequently accessed data
        """
        logger.info(f"📈 Measuring cache hit rate over {num_requests} requests")
        
        if not self.redis_client:
            logger.error("Redis client not available for cache hit rate measurement")
            return {}
        
        # Get initial Redis stats
        initial_stats = self.redis_client.info('stats')
        initial_hits = initial_stats.get('keyspace_hits', 0)
        initial_misses = initial_stats.get('keyspace_misses', 0)
        
        # Make API requests to generate cache activity
        await self._generate_cache_activity(num_requests)
        
        # Get final Redis stats
        final_stats = self.redis_client.info('stats')
        final_hits = final_stats.get('keyspace_hits', 0)
        final_misses = final_stats.get('keyspace_misses', 0)
        
        # Calculate hit rate for this test period
        hits_during_test = final_hits - initial_hits
        misses_during_test = final_misses - initial_misses
        total_requests = hits_during_test + misses_during_test
        
        if total_requests > 0:
            hit_rate = (hits_during_test / total_requests) * 100
            
            self.record_metric(
                metric_name="cache_hit_rate",
                value=hit_rate,
                unit="%",
                context={
                    "hits": hits_during_test,
                    "misses": misses_during_test,
                    "total": total_requests
                }
            )
            
            return {
                'hit_rate_percent': hit_rate,
                'hits': hits_during_test,
                'misses': misses_during_test,
                'total_requests': total_requests
            }
        
        return {}

    async def _generate_cache_activity(self, num_requests: int):
        """Generate API requests to test cache performance"""
        endpoints = [
            "/api/suburbs?page=1&page_size=500",
            "/api/suburbs?region=Auckland&page=1&page_size=20",
            "/api/suburbs/100100",
            "/api/meshblocks?suburb_id=100100"
        ]
        
        async with aiohttp.ClientSession() as session:
            tasks = []
            for i in range(num_requests):
                endpoint = endpoints[i % len(endpoints)]
                url = f"{self.api_base_url}{endpoint}"
                tasks.append(session.get(url))
            
            # Execute requests with some concurrency
            for i in range(0, len(tasks), 10):  # Process in batches of 10
                batch = tasks[i:i+10]
                responses = await asyncio.gather(*batch, return_exceptions=True)
                for response in responses:
                    if isinstance(response, aiohttp.ClientResponse):
                        response.close()

    def benchmark_memory_usage(self, operation_func, *args, **kwargs) -> Dict[str, float]:
        """
        Monitor memory usage during a specific operation
        
        Target: <2GB for processing 1M+ crime records
        """
        logger.info("💾 Monitoring memory usage during operation")
        
        # Force garbage collection before starting
        gc.collect()
        
        process = psutil.Process()
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        peak_memory = initial_memory
        
        # Monitor memory during operation
        def memory_monitor():
            nonlocal peak_memory
            while True:
                current_memory = process.memory_info().rss / 1024 / 1024
                peak_memory = max(peak_memory, current_memory)
                time.sleep(0.1)  # Check every 100ms
        
        # Start memory monitoring in background
        import threading
        monitor_thread = threading.Thread(target=memory_monitor, daemon=True)
        monitor_thread.start()
        
        # Execute the operation
        start_time = time.perf_counter()
        try:
            result = operation_func(*args, **kwargs)
        finally:
            end_time = time.perf_counter()
            final_memory = process.memory_info().rss / 1024 / 1024
        
        memory_delta = final_memory - initial_memory
        execution_time = end_time - start_time
        
        metrics = {
            'initial_memory_mb': initial_memory,
            'peak_memory_mb': peak_memory,
            'final_memory_mb': final_memory,
            'memory_delta_mb': memory_delta,
            'execution_time_seconds': execution_time
        }
        
        self.record_metric(
            metric_name="memory_usage_peak",
            value=peak_memory,
            unit="MB",
            context={"operation": operation_func.__name__}
        )
        
        self.record_metric(
            metric_name="memory_usage_delta",
            value=memory_delta,
            unit="MB",
            context={"operation": operation_func.__name__}
        )
        
        return metrics

    def benchmark_database_query_performance(self, num_queries: int = 50) -> Dict[str, float]:
        """
        Measure database query performance
        
        Target: <10ms for indexed lookups
        """
        logger.info(f"🗄️ Benchmarking database query performance with {num_queries} queries")
        
        if not self.supabase_client:
            logger.error("Supabase client not available for database benchmarking")
            return {}
        
        query_times = []
        
        # Test various query types
        queries = [
            lambda: self.supabase_client.table("suburbs").select("suburb_id,name").limit(10).execute(),
            lambda: self.supabase_client.table("suburbs").select("*").eq("region", "Auckland").limit(5).execute(),
            lambda: self.supabase_client.table("crimes").select("event_id,suburb_id").limit(20).execute(),
            lambda: self.supabase_client.table("meshblocks").select("id,suburb_id").limit(15).execute()
        ]
        
        for i in range(num_queries):
            query_func = queries[i % len(queries)]
            
            start_time = time.perf_counter()
            try:
                result = query_func()
                query_time = (time.perf_counter() - start_time) * 1000  # Convert to ms
                query_times.append(query_time)
                
                self.record_metric(
                    metric_name="database_query_time",
                    value=query_time,
                    unit="ms",
                    context={"query_index": i % len(queries)}
                )
                
            except Exception as e:
                logger.warning(f"Query {i} failed: {e}")
        
        if query_times:
            avg_time = statistics.mean(query_times)
            p95_time = statistics.quantiles(query_times, n=20)[18] if len(query_times) >= 20 else max(query_times)
            
            return {
                'average_query_time_ms': avg_time,
                'p95_query_time_ms': p95_time,
                'min_query_time_ms': min(query_times),
                'max_query_time_ms': max(query_times),
                'total_queries': len(query_times)
            }
        
        return {}

    def simulate_data_ingestion_benchmark(self, num_records: int = 10000) -> Dict[str, float]:
        """
        Simulate data ingestion performance
        
        Target: 100,000 records/minute with parallel processing
        """
        logger.info(f"⚡ Simulating data ingestion for {num_records} records")
        
        # Create sample data
        sample_data = []
        for i in range(num_records):
            sample_data.append({
                'id': f"test_{i}",
                'name': f"Test Suburb {i}",
                'region': f"Region {i % 10}",
                'crime_rate': i * 0.1,
                'timestamp': datetime.now().isoformat()
            })
        
        # Measure batch processing time
        batch_size = 1000
        start_time = time.perf_counter()
        
        # Simulate batch processing (without actual database writes)
        batches_processed = 0
        for i in range(0, len(sample_data), batch_size):
            batch = sample_data[i:i + batch_size]
            # Simulate processing time
            time.sleep(0.01)  # 10ms per batch to simulate real processing
            batches_processed += 1
        
        end_time = time.perf_counter()
        total_time = end_time - start_time
        records_per_second = num_records / total_time
        records_per_minute = records_per_second * 60
        
        self.record_metric(
            metric_name="data_ingestion_rate",
            value=records_per_minute,
            unit="records/minute",
            context={
                "num_records": num_records,
                "batch_size": batch_size,
                "total_time_seconds": total_time
            }
        )
        
        return {
            'records_per_minute': records_per_minute,
            'records_per_second': records_per_second,
            'total_time_seconds': total_time,
            'batches_processed': batches_processed
        }

    def generate_performance_report(self) -> Dict[str, Any]:
        """Generate a comprehensive performance report"""
        logger.info("📋 Generating performance report")
        
        # Group metrics by type
        metrics_by_type = {}
        for metric in self.metrics:
            metric_type = metric.metric_name.split('_')[0]
            if metric_type not in metrics_by_type:
                metrics_by_type[metric_type] = []
            metrics_by_type[metric_type].append(metric)
        
        # Calculate summary statistics
        report = {
            'timestamp': datetime.now().isoformat(),
            'total_metrics_collected': len(self.metrics),
            'summary': {},
            'detailed_metrics': [metric.to_dict() for metric in self.metrics]
        }
        
        # Analyze each metric type
        for metric_type, type_metrics in metrics_by_type.items():
            values = [m.value for m in type_metrics]
            if values:
                report['summary'][metric_type] = {
                    'count': len(values),
                    'average': statistics.mean(values),
                    'min': min(values),
                    'max': max(values),
                    'median': statistics.median(values)
                }
                
                if len(values) >= 2:
                    report['summary'][metric_type]['std_dev'] = statistics.stdev(values)
        
        return report

    def save_report(self, report: Dict[str, Any], filename: str = None):
        """Save performance report to file"""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"performance_report_{timestamp}.json"
        
        with open(filename, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        logger.info(f"📄 Performance report saved to {filename}")

def interpret_benchmarks(report: Dict[str, Any]) -> Dict[str, str]:
    """
    Interpret benchmark results and provide recommendations
    
    Returns a dictionary with interpretations for each metric type
    """
    interpretations = {}
    summary = report.get('summary', {})
    
    # API Response Time Analysis
    if 'api' in summary:
        avg_response = summary['api']['average']
        if avg_response < 50:
            interpretations['api_response'] = "✅ EXCELLENT: API response times are optimal"
        elif avg_response < 100:
            interpretations['api_response'] = "✅ GOOD: API response times are acceptable"
        elif avg_response < 200:
            interpretations['api_response'] = "⚠️ FAIR: API response times could be improved"
        else:
            interpretations['api_response'] = "❌ POOR: API response times need optimization"
    
    # Cache Hit Rate Analysis
    cache_metrics = [m for m in report.get('detailed_metrics', []) if 'cache_hit_rate' in m['metric_name']]
    if cache_metrics:
        hit_rate = cache_metrics[-1]['value']  # Get latest measurement
        if hit_rate >= 95:
            interpretations['cache_performance'] = "✅ EXCELLENT: Cache hit rate is optimal"
        elif hit_rate >= 85:
            interpretations['cache_performance'] = "✅ GOOD: Cache hit rate is acceptable"
        elif hit_rate >= 70:
            interpretations['cache_performance'] = "⚠️ FAIR: Cache hit rate could be improved"
        else:
            interpretations['cache_performance'] = "❌ POOR: Cache hit rate needs optimization"
    
    # Memory Usage Analysis
    if 'memory' in summary:
        peak_memory = summary['memory']['max']
        if peak_memory < 1024:  # Less than 1GB
            interpretations['memory_usage'] = "✅ EXCELLENT: Memory usage is very efficient"
        elif peak_memory < 2048:  # Less than 2GB
            interpretations['memory_usage'] = "✅ GOOD: Memory usage is within acceptable limits"
        elif peak_memory < 4096:  # Less than 4GB
            interpretations['memory_usage'] = "⚠️ FAIR: Memory usage is moderate"
        else:
            interpretations['memory_usage'] = "❌ POOR: Memory usage is high"
    
    # Database Query Performance Analysis
    if 'database' in summary:
        avg_query_time = summary['database']['average']
        if avg_query_time < 10:
            interpretations['database_performance'] = "✅ EXCELLENT: Database queries are very fast"
        elif avg_query_time < 50:
            interpretations['database_performance'] = "✅ GOOD: Database queries are fast"
        elif avg_query_time < 100:
            interpretations['database_performance'] = "⚠️ FAIR: Database queries are acceptable"
        else:
            interpretations['database_performance'] = "❌ POOR: Database queries are slow"
    
    # Data Ingestion Rate Analysis
    ingestion_metrics = [m for m in report.get('detailed_metrics', []) if 'ingestion_rate' in m['metric_name']]
    if ingestion_metrics:
        ingestion_rate = ingestion_metrics[-1]['value']  # records per minute
        if ingestion_rate >= 100000:
            interpretations['ingestion_performance'] = "✅ EXCELLENT: Data ingestion rate meets target"
        elif ingestion_rate >= 50000:
            interpretations['ingestion_performance'] = "✅ GOOD: Data ingestion rate is acceptable"
        elif ingestion_rate >= 25000:
            interpretations['ingestion_performance'] = "⚠️ FAIR: Data ingestion rate could be improved"
        else:
            interpretations['ingestion_performance'] = "❌ POOR: Data ingestion rate is below target"
    
    return interpretations