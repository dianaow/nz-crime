#!/usr/bin/env python3
"""
Shapefile to GeoJSON Converter

This script converts ESRI Shapefiles (.shp) to GeoJSON format.
It supports optional geometry simplification for better web performance.
"""

import argparse
import sys
from pathlib import Path
import geopandas as gpd
from typing import Optional

def convert_shapefile_to_geojson(
    input_path: str,
    output_path: Optional[str] = None,
    simplify: bool = False,
    tolerance: float = 0.001
) -> None:
    """
    Convert a shapefile to GeoJSON format.
    
    Args:
        input_path (str): Path to the input shapefile
        output_path (str, optional): Path for the output GeoJSON file. If not provided,
                                   will use the input filename with .geojson extension
        simplify (bool): Whether to simplify the geometry for better web performance
        tolerance (float): Tolerance value for geometry simplification
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

        # Calculate centroids and add latitude/longitude columns
        print("Calculating centroids for each polygon...")
        centroids = gdf.geometry.centroid
        gdf['longitude'] = centroids.x
        gdf['latitude'] = centroids.y

        # Simplify geometry if requested
        if simplify:
            print(f"Simplifying geometry with tolerance: {tolerance}")
            gdf["geometry"] = gdf["geometry"].simplify(
                tolerance=tolerance,
                preserve_topology=True
            )

        # Save as GeoJSON
        print(f"Saving to GeoJSON: {output_path}")
        gdf.to_file(output_path, driver="GeoJSON")
        print("Conversion completed successfully!")
        print(f"Added latitude and longitude columns to the output file.")

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

    args = parser.parse_args()
    
    convert_shapefile_to_geojson(
        input_path=args.input_file,
        output_path=args.output,
        simplify=args.simplify,
        tolerance=args.tolerance
    )

if __name__ == "__main__":
    main()
