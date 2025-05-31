#!/usr/bin/env python3
"""
Shapefile to GeoJSON Converter

This script converts ESRI Shapefiles (.shp) to GeoJSON format.
It supports optional geometry simplification for better web performance,
sorting by columns, and splitting into batches.
"""

import argparse
import sys
from pathlib import Path
import geopandas as gpd
from typing import Optional
import pyproj
print(pyproj.__version__)

def convert_shapefile_to_geojson(
    input_path: str,
    output_path: Optional[str] = None,
    simplify: bool = False,
    tolerance: float = 0.001,
    sort_column: Optional[str] = None,
    batch_size: Optional[int] = None
) -> None:
    """
    Convert a shapefile to GeoJSON format.
    
    Args:
        input_path (str): Path to the input shapefile
        output_path (str, optional): Path for the output GeoJSON file. If not provided,
                                   will use the input filename with .geojson extension
        simplify (bool): Whether to simplify the geometry for better web performance
        tolerance (float): Tolerance value for geometry simplification
        sort_column (str, optional): Column name to sort the data by
        batch_size (int, optional): Number of features per batch file
    """
    try:
        # Validate input file exists
        input_path = Path(input_path)
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_path}")

        # Set default output path if not provided
        if output_path is None:
            output_path = input_path.with_suffix('.geojson')
        else:
            output_path = Path(output_path)

        # Read the shapefile
        print(f"Reading shapefile: {input_path}")
        gdf = gpd.read_file(input_path)
        
        # Sort the data if requested
        if sort_column:
            if sort_column not in gdf.columns:
                raise ValueError(f"Sort column '{sort_column}' not found in the data")
            print(f"Sorting data by column: {sort_column}")
            gdf = gdf.sort_values(by=sort_column)

        # Print the current coordinate system
        print(f"Input coordinate system: {gdf.crs}")

        # Calculate centroids using a projected CRS first for accurate calculation
        print("Calculating centroids for each polygon...")
        original_crs = gdf.crs
        
        # Project to a suitable projected CRS (e.g., Web Mercator) for centroid calculation
        gdf_projected = gdf.to_crs(epsg=3857)  # Web Mercator projection
        centroids_projected = gdf_projected.geometry.centroid
        
        # Convert centroids back to original CRS
        centroids = gpd.GeoSeries(centroids_projected, crs=3857).to_crs(original_crs)
        
        # Transform to WGS84 (EPSG:4326) if not already in that coordinate system
        gdf = gdf.to_crs(epsg=4326)
        centroids = centroids.to_crs(epsg=4326)
        print(f"Transformed coordinate system: {gdf.crs}")

        # Add latitude/longitude columns from the accurately calculated centroids
        gdf['longitude'] = centroids.x
        gdf['latitude'] = centroids.y

        # Simplify geometry if requested
        if simplify:
            print(f"Simplifying geometry with tolerance: {tolerance}")
            gdf["geometry"] = gdf["geometry"].simplify(
                tolerance=tolerance,
                preserve_topology=True
            )

        # Save as single file or multiple batches
        if batch_size is None:
            # Save as single GeoJSON
            print(f"Saving to GeoJSON: {output_path}")
            gdf.to_file(output_path, driver="GeoJSON")
            print("Conversion completed successfully!")
        else:
            # Save in batches
            total_features = len(gdf)
            num_batches = (total_features + batch_size - 1) // batch_size
            print(f"Splitting into {num_batches} batches of {batch_size} features each")
            
            output_stem = output_path.stem
            output_parent = output_path.parent
            
            for i in range(num_batches):
                start_idx = i * batch_size
                end_idx = min((i + 1) * batch_size, total_features)
                batch_df = gdf.iloc[start_idx:end_idx]
                
                batch_filename = output_parent / f"{output_stem}_batch_{i+1}.geojson"
                print(f"Saving batch {i+1} to: {batch_filename}")
                batch_df.to_file(batch_filename, driver="GeoJSON")
            
            print(f"Successfully saved {num_batches} batch files!")

        print(f"Added latitude and longitude columns to the output file(s).")

    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(
        description="Convert ESRI Shapefiles to GeoJSON format"
    )
    parser.add_argument(
        "input_file",
        help="Path to the input shapefile (.shp)"
    )
    parser.add_argument(
        "-o", "--output",
        help="Path for the output GeoJSON file (default: same as input with .geojson extension)"
    )
    parser.add_argument(
        "-s", "--simplify",
        action="store_true",
        help="Simplify geometry for better web performance"
    )
    parser.add_argument(
        "-t", "--tolerance",
        type=float,
        default=0.001,
        help="Tolerance value for geometry simplification (default: 0.001)"
    )
    parser.add_argument(
        "--sort",
        help="Column name to sort the data by"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        help="Number of features per batch file (e.g., 500)"
    )

    args = parser.parse_args()
    
    convert_shapefile_to_geojson(
        input_path=args.input_file,
        output_path=args.output,
        simplify=args.simplify,
        tolerance=args.tolerance,
        sort_column=args.sort,
        batch_size=args.batch_size
    )

if __name__ == "__main__":
    main()
