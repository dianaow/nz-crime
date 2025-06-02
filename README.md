# NZ Crime Data Processing & API System

A high-performance data processing and API system for New Zealand crime statistics, featuring optimized data ingestion, caching, and efficient geographic data handling.

## 📋 Table of Contents

- [Overview](#overview)
- [System Architecture](#system-architecture)
- [Components](#components)
  - [app.py - Data Transformation Engine](#apppy---data-transformation-engine)
  - [api.py - High-Performance API](#apipy---high-performance-api)
  - [convert_shpfile.py - Geographic Data Converter](#convert_shpfilepy---geographic-data-converter)
- [Performance Optimizations](#performance-optimizations)
- [Installation & Setup](#installation--setup)
- [Usage](#usage)
- [API Endpoints](#api-endpoints)
- [Configuration](#configuration)

## 🎯 Overview

This system processes large-scale New Zealand crime data, transforms it for efficient storage in Supabase, and provides a high-performance API with caching. The system is designed to handle millions of crime records while maintaining sub-second response times.

### Key Features

- **Parallel Processing**: Multi-threaded data transformation and ingestion
- **Intelligent Caching**: Redis-based caching with automatic invalidation
- **Geographic Integration**: Shapefile processing with centroid calculation
- **Memory Optimization**: Chunked processing to handle large datasets
- **Error Resilience**: Retry mechanisms and graceful error handling
- **Performance Monitoring**: Built-in memory usage tracking and logging

## 🏗️ System Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Raw Data      │    │  Transformation │    │   Supabase DB   │
│   (CSV/SHP)     │───▶│    Engine       │───▶│   (Optimized)   │
│                 │    │   (app.py)      │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                        │
                                                        ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Client Apps   │◀───│   FastAPI       │◀───│   Redis Cache   │
│                 │    │   (api.py)      │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 🔧 Components

### `app.py` - Data Transformation Engine

The core data processing engine that transforms raw crime data into an optimized format for the API.

#### **Key Functionality**

- **Crime Data Processing**: Transforms raw CSV crime data into normalized records
- **Geographic Integration**: Links crime data with suburb and meshblock geometries
- **Statistical Calculations**: Computes crime rates, trends, and safety scores
- **Data Validation**: Ensures data integrity and handles missing values

#### **Performance Optimizations**

##### 🚀 **Parallel Processing Strategy**
```python
# Multi-threaded processing with configurable workers
max_workers = multiprocessing.cpu_count()
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    # Process data chunks in parallel
```

- **Chunked Processing**: Processes data in 50,000-record chunks to optimize memory usage
- **Batch Processing**: Groups suburbs and meshblocks into batches of 500-1000 for parallel processing
- **Thread Pool Execution**: Utilizes all available CPU cores for maximum throughput

##### 💾 **Memory Management**
```python
def _monitor_memory_usage(self) -> None:
    """Monitor and log memory usage"""
    if memory_info.rss > 1024 * 1024 * 1024:  # If using more than 1GB
        gc.collect()  # Trigger garbage collection
```

- **Memory Monitoring**: Continuous tracking of memory usage with automatic garbage collection
- **Chunked Data Loading**: Processes large files in manageable chunks to prevent memory overflow
- **Efficient Data Structures**: Uses pandas vectorized operations for optimal performance

##### 🔄 **Supabase Ingestion Optimization**
```python
def _save_batch_to_supabase(self, batch_data: List[Dict], table_name: str, use_upsert: bool) -> int:
    # Exponential backoff retry mechanism
    # Automatic batch size reduction on timeout
    # Parallel batch processing
```

- **Batch Upserts**: Processes data in optimized batch sizes (250-1000 records)
- **Retry Logic**: Exponential backoff with automatic batch splitting on timeouts
- **Connection Pooling**: Efficient database connection management
- **Error Recovery**: Graceful handling of network issues and database constraints

##### 📊 **Data Processing Pipeline**
1. **Load Phase**: Chunked CSV reading with encoding detection
2. **Transform Phase**: Parallel data cleaning and normalization
3. **Enrich Phase**: Geographic data integration and statistical calculations
4. **Load Phase**: Optimized batch insertion into Supabase

### `api.py` - High-Performance API

A FastAPI-based REST API optimized for high-throughput data retrieval with intelligent caching.

#### **Key Functionality**

- **Suburb Data API**: Comprehensive suburb information with crime statistics with geogr 
- **Crime Events API**: Individual crime record access with filtering
- **Report Generation**: PDF report URLs and widget embed codes

#### **Performance Optimizations**

##### ⚡ **Caching Strategy**
```python
def cache_response(prefix: str, ttl_seconds: int = 300):
    cache_key = f"{prefix}:{json.dumps(sorted_kwargs, sort_keys=True)}"
```

```python
@cache_response(prefix="suburbs_list", ttl_seconds=86400)
async def get_suburbs(params: Dict[str, Any] = Depends(get_query_params)):
    # Cached for 24 hours with parameter-based cache keys
```

- **Redis Caching**: All API responses cached with configurable TTL
- **Parameter-Based Keys**: Cache keys include query parameters for precise cache hits
- **Cache Invalidation**: Automatic cache clearing endpoints
- **Hit Rate Optimization**: 24-hour cache for static data, shorter TTL for dynamic content

##### 🔍 **Query Optimization**

- **Pagination Support**: Configurable page sizes (1-500 records) for optimal performance
- **Index-Optimized Queries**: Leverages database indexes for fast lookups

##### 🗜️ **Response Optimization**
```python
# Gzip compression for large responses
app.add_middleware(GZipMiddleware, minimum_size=1000)

# CORS optimization for web applications
app.add_middleware(CORSMiddleware, allow_origins=["*"])
```

- **Gzip Compression**: Automatic compression for responses >1KB
- **CORS Optimization**: Efficient cross-origin request handling

### `convert_shpfile.py` - Geographic Data Converter

Specialized tool for converting ESRI Shapefiles to optimized GeoJSON format with performance enhancements.

#### **Key Functionality**

- **Shapefile Conversion**: ESRI Shapefile to GeoJSON transformation
- **Coordinate System Transformation**: Automatic projection to WGS84
- **Centroid Calculation**: Accurate geographic center calculation
- **Geometry Simplification**: Optional polygon simplification for web performance

## 🚀 Performance Optimizations Summary

### Data Ingestion Performance

| Strategy | Implementation |
|----------|----------------|
| **Parallel Processing** | ThreadPoolExecutor with CPU core count workers for faster processing |
| **Chunked Loading** | 50,000 record chunks for memory reduction |
| **Batch Upserts** | 250-1000 record batches for faster database writes |
| **Retry Logic** | Exponential backoff with batch splitting |
| **Memory Management** | Automatic garbage collection |

### Data Extraction Performance

| Strategy | Implementation |
|----------|----------------|
| **Redis Caching** | 24-hour TTL for static data for faster responses |
| **Gzip Compression** | Automatic compression >1KB for response size reduction |
| **Database Indexing** | Optimized indexes on filter columns for faster queries |
| **Connection Pooling** | Supabase connection optimization for latency reduction -Not implemented currently|


## 🛠️ Installation & Setup

### Prerequisites
```bash
# Python 3.9
pip install fastapi gunicorn uvicorn pandas numpy geopandas shapely
pip install supabase redis pydantic python-dotenv tqdm psutil
```

### Environment Configuration
```bash
# .env file
SUPABASE_URL=your_supabase_url
SUPABASE_KEY=your_supabase_anon_key
SUPABASE_SERVICE_ROLE_KEY=your_service_role_key
REDIS_URL=redis://localhost:6379
```

### Database Setup



## 🚀 Usage

### Data Processing
```bash
# Process crime data with parallel optimization
python app.py --input victimization_time_place.csv --max-workers 8

# Convert shapefiles with geometry simplification
python convert_shpfile.py suburbs.shp --simplify --tolerance 0.001
```

### API Server
```bash
# Start high-performance API server
uvicorn api:app --host 0.0.0.0 --port 8000 --workers 4
```

### Performance Monitoring
```bash
# Run comprehensive performance benchmark
python performance_monitor.py

# Run specific benchmark examples
python benchmark_examples.py
```

## 📡 API Endpoints

### Core Endpoints
- `GET /api/suburbs` - List suburbs with filtering and pagination
- `GET /api/suburbs/{suburb_id}` - Detailed suburb information
- `GET /api/crimes` - Crime events with advanced filtering
- `GET /api/meshblocks` - Geographic meshblock data
- `DELETE /api/cache/clear` - Cache management

## ⚙️ Configuration

### Performance Tuning
```python
# app.py configuration
CHUNK_SIZE = 50000          # Records per processing chunk
BATCH_SIZE = 1000           # Records per database batch
MAX_WORKERS = cpu_count()   # Parallel processing threads

# api.py configuration
CACHE_TTL = 86400          # Cache time-to-live (24 hours)
GZIP_MIN_SIZE = 1000       # Minimum size for compression
MAX_PAGE_SIZE = 500        # Maximum records per page
```

### Memory Optimization
```python
# Memory thresholds
MEMORY_WARNING_THRESHOLD = 1024 * 1024 * 1024  # 1GB
GC_TRIGGER_PROBABILITY = 0.1                    # 10% random GC
```

---
