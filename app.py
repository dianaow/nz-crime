import pandas as pd
import numpy as np
import uuid
import json
import hashlib
from datetime import datetime, timedelta
import requests
from typing import Dict, List, Tuple, Any, Optional, Generator
import os
from dotenv import load_dotenv
from supabase import create_client, Client
import time
from shapely.geometry import shape
import geopandas as gpd
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing
from tqdm import tqdm
import logging
from functools import lru_cache
import psutil
import gc

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crime_data_transform.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class CrimeDataTransformer:
    def __init__(self, input_file: str, output_dir: str = 'output', batch_size: int = 10000):
        """
        Initialize the crime data transformer
        
        Args:
            input_file: Path to the crime data CSV file
            output_dir: Directory to save output files
            batch_size: Number of rows to process in each batch
        """
        self.input_file = input_file
        self.output_dir = output_dir
        self.df_crime = None
        self.df_suburbs = None
        self.df_crimes = None
        self.df_census = None
        self.suburbs_geojson = None
        self.geometry_index = {}  # New index for geometry lookups
        self.neo4j_user = os.getenv("NEO4J_USER")
        self.neo4j_password = os.getenv("NEO4J_PASSWORD")
        self.batch_size = batch_size 
        self.max_workers = multiprocessing.cpu_count()  # Number of worker threads

        # Initialize Supabase client from environment variables
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        if supabase_url and supabase_key:
            self.supabase = create_client(supabase_url, supabase_key)
        else:
            self.supabase = None
            logger.warning("Supabase credentials not found in environment variables")
        
        # Create output directory if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
    def _load_census_data(self) -> None:
        """Load and process the census data"""
        try:
            self.df_census = pd.read_csv('rawdata/nzcensus.csv')
            # Filter for 2023 data only
            self.df_census = self.df_census[self.df_census['Census year'] == 2023]
            logger.info(f"Loaded {len(self.df_census)} census records")
        except Exception as e:
            logger.error(f"Error loading census data: {str(e)}")
            self.df_census = None
            
    def _create_geometry_index(self) -> None:
        """
        Create an index mapping suburb names to their geometries and coordinates.
        This eliminates the need for linear search through GeoJSON features.
        """
        if not self.suburbs_geojson:
            raise ValueError("Suburbs geojson data not loaded")
            
        logger.info("Creating geometry index...")
        for feature in self.suburbs_geojson['features']:
            suburb_name = feature['properties']['AU2013_V_1']
            if suburb_name:  # Only index if suburb name exists
                self.geometry_index[suburb_name] = {
                    'geometry': feature['geometry'],
                    'lat': feature['properties']['latitude'],
                    'lng': feature['properties']['longitude']
                }
        logger.info(f"Created geometry index with {len(self.geometry_index)} suburbs")

    def _load_suburbs_geojson(self) -> None:
        """Load the suburbs geojson data and create the geometry index"""
        try:
            with open('./rawdata/areaunits.geojson', 'r') as f:
                self.suburbs_geojson = json.load(f)
            logger.info("Loaded suburbs geojson data")
            # Create the geometry index after loading the data
            self._create_geometry_index()
        except Exception as e:
            logger.error(f"Error loading suburbs geojson data: {str(e)}")
            self.suburbs_geojson = None
            
    @lru_cache(maxsize=1000)
    def _get_suburb_geometry(self, suburb_name: str) -> Tuple[float, float, Dict]:
        """
        Get the geometry data for a suburb using the geometry index
        
        Args:
            suburb_name: Name of the suburb to look up
            
        Returns:
            Tuple of (latitude, longitude, geometry)
        """
        if not self.geometry_index:
            raise ValueError("Geometry index not created")
            
        # Look up the suburb in the index
        suburb_data = self.geometry_index.get(suburb_name)
        if suburb_data:
            return suburb_data['lat'], suburb_data['lng'], suburb_data['geometry']
                    
        return None, None, None
        
    def _get_suburb_population(self, suburb_name: str) -> int:
        """
        Get the population for a suburb from the census data
        
        Args:
            suburb_name: Name of the suburb to look up
            
        Returns:
            Population count for the suburb
        """
        if self.df_census is None:
            return 10000  # Fallback to placeholder if census data not loaded
            
        # Try to find exact match
        match = self.df_census[self.df_census['Area'] == suburb_name]
        if not match.empty:
            return match['Value'].iloc[0]
            
        # If no exact match, try to find partial match
        # This handles cases where the names might be slightly different
        for area in self.df_census['Area']:
            if suburb_name.lower() in area.lower() or area.lower() in suburb_name.lower():
                return self.df_census[self.df_census['Area'] == area]['Value'].iloc[0]
                
        # If no match found, return placeholder
        return 10000
        
    def load_data(self) -> None:
        """Load and preprocess the crime data CSV"""
        logger.info(f"Reading crime data from {self.input_file}")
        
        # Try different encodings
        encodings = ['utf-8', 'utf-16', 'latin1', 'iso-8859-1', 'cp1252']
        
        for encoding in encodings:
            try:
                # Read the CSV file with tab delimiter
                self.df_crime = pd.read_csv(self.input_file, encoding=encoding, sep='\t')
                logger.info(f"Successfully read file with {encoding} encoding")
                break
            except UnicodeDecodeError:
                continue
            except Exception as e:
                logger.error(f"Error reading file with {encoding} encoding: {str(e)}")
                continue
        
        if self.df_crime is None:
            raise ValueError("Could not read the CSV file with any of the attempted encodings")
        
        logger.info(f"Loaded {len(self.df_crime)} crime records")
        
    def _clean_text(self, text: str) -> str:
        """
        Clean text by removing special characters and normalizing spaces
        
        Args:
            text: Input text to clean
            
        Returns:
            Cleaned text string
        """
        if pd.isna(text):
            return text
            
        # Replace special characters with spaces
        text = str(text)
        text = text.replace('.', ' ')  # Replace dots with spaces
        text = text.replace('-', ' ')  # Replace hyphens with spaces
        text = text.replace('_', ' ')  # Replace underscores with spaces
        
        # Normalize spaces (replace multiple spaces with single space)
        text = ' '.join(text.split())
        
        return text.strip()
        
    def transform_data(self) -> None:
        """Transform the crime data into suburbs and crimes tables"""
        if self.df_crime is None:
            self.load_data()
            
        logger.info("Starting data transformation...")
        
        # Strip whitespace from column names and values
        self.df_crime.columns = self.df_crime.columns.str.strip()
        for col in self.df_crime.select_dtypes(include=['object']).columns:
            self.df_crime[col] = self.df_crime[col].str.strip()
            
        # Clean special characters from Area Unit and Territorial Authority columns
        self.df_crime['Area Unit'] = self.df_crime['Area Unit'].apply(self._clean_text)
        self.df_crime['Territorial Authority'] = self.df_crime['Territorial Authority'].apply(self._clean_text)
                    
        logger.info("Data transformation completed")
    
    def _load_data_in_chunks(self, file_path: str, chunk_size: int = 10000) -> Generator[pd.DataFrame, None, None]:
        """
        Load data in chunks to manage memory usage
        
        Args:
            file_path: Path to the CSV file
            chunk_size: Size of each chunk to read
            
        Returns:
            Generator yielding DataFrame chunks
        """
        encodings = ['utf-8', 'utf-16', 'latin1', 'iso-8859-1', 'cp1252']
        
        for encoding in encodings:
            try:
                for chunk in pd.read_csv(file_path, encoding=encoding, sep='\t', chunksize=chunk_size):
                    yield chunk
                break
            except UnicodeDecodeError:
                continue
            except Exception as e:
                logger.error(f"Error reading file with {encoding} encoding: {str(e)}")
                continue

    def _process_suburb_batch(self, suburb_batch: List[Dict]) -> List[Dict]:
        """
        Process a batch of suburbs in parallel
        
        Args:
            suburb_batch: List of suburb data dictionaries
            
        Returns:
            List of processed suburb data dictionaries
        """
        processed_suburbs = []
        
        for suburb_data in suburb_batch:
            try:
                # Process each suburb
                suburb_name = suburb_data['name']
                lat, lng, geometry = self._get_suburb_geometry(suburb_name)
                
                if geometry:
                    # Convert GeoJSON to PostGIS geometry
                    shapely_geom = shape(geometry)
                    wkt_geometry = shapely_geom.wkt
                else:
                    wkt_geometry = {}
                
                suburb_data.update({
                    'lat': lat,
                    'lng': lng,
                    'geometry': wkt_geometry
                })
                
                processed_suburbs.append(suburb_data)
                
            except Exception as e:
                logger.error(f"Error processing suburb {suburb_name}: {str(e)}")
                continue
                
        return processed_suburbs

    def _create_suburbs_table(self) -> None:
        """Create the suburbs table with aggregated crime data using parallel processing"""
        logger.info("Creating suburbs table...")
        
        # Group by suburb and calculate crime stats
        suburb_groups = self.df_crime.groupby('Area Unit')
        
        # Create a list to store suburb data
        suburbs_data = []
        
        # Process suburbs in parallel
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []
            
            # Submit suburb processing tasks
            for suburb_name, suburb_df in suburb_groups:
                if pd.isna(suburb_name) or suburb_name == '':
                    continue
                    
                suburb_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, suburb_name))
                region = suburb_df['Territorial Authority'].iloc[0] if not suburb_df['Territorial Authority'].empty else ''
                
                # Calculate crime statistics
                suburb_df['Date'] = pd.to_datetime(suburb_df['Year Month'], format='%B %Y')
                current_date = datetime.now()
                one_year_ago = current_date - timedelta(days=365)
                last_12m_data = suburb_df[suburb_df['Date'] >= one_year_ago]
                
                total_crimes_12m = len(last_12m_data)
                population = self._get_suburb_population(suburb_name)
                crime_rate_per_1000 = (total_crimes_12m / population) * 1000 if population > 0 else 0
                
                # Calculate crime trend
                suburb_df['YearMonth'] = suburb_df['Date'].dt.to_period('M')
                monthly_crimes = suburb_df.groupby('YearMonth').size()
                
                if len(monthly_crimes) >= 2:
                    recent_months = monthly_crimes.iloc[-3:] if len(monthly_crimes) >= 3 else monthly_crimes.iloc[-2:]
                    previous_months = monthly_crimes.iloc[-6:-3] if len(monthly_crimes) >= 6 else monthly_crimes.iloc[:-3]
                    
                    recent_avg = recent_months.mean() if not recent_months.empty else 0
                    previous_avg = previous_months.mean() if not previous_months.empty else 0
                    
                    if recent_avg > previous_avg * 1.1:
                        crime_trend = "up"
                    elif recent_avg < previous_avg * 0.9:
                        crime_trend = "down"
                    else:
                        crime_trend = "flat"
                        
                    trend_3m_change = ((recent_avg - previous_avg) / previous_avg) * 100 if previous_avg > 0 else 0
                else:
                    crime_trend = "flat"
                    trend_3m_change = 0
                
                crime_breakdown = suburb_df.groupby('ANZSOC Group').size().to_dict()
                
                suburb_data = {
                    'suburb_id': suburb_id,
                    'name': suburb_name,
                    'region': region,
                    'council': '',
                    'lat': None,
                    'lng': None,
                    'geometry': None,
                    'safety_score': 0,
                    'crime_rate_per_1000': crime_rate_per_1000,
                    'total_crimes_12m': total_crimes_12m,
                    'crime_trend': crime_trend,
                    'rank_in_region': 0,
                    'crime_breakdown': json.dumps(crime_breakdown),
                    'trend_3m_change': trend_3m_change,
                    'report_url': '',
                    'widget_embed_code': '',
                    'summary_text': '',
                    'tags': json.dumps([])
                }
                
                futures.append(executor.submit(self._process_suburb_batch, [suburb_data]))
            
            # Collect results with progress bar
            for future in tqdm(as_completed(futures), total=len(futures), desc="Processing suburbs"):
                try:
                    processed_suburbs = future.result()
                    suburbs_data.extend(processed_suburbs)
                except Exception as e:
                    logger.error(f"Error processing suburb batch: {str(e)}")
        
        # Create DataFrame from suburbs data
        self.df_suburbs = pd.DataFrame(suburbs_data)
        
        if not self.df_suburbs.empty:
            # Calculate ranks and clean data
            self.df_suburbs['rank_in_region'] = self.df_suburbs.groupby('region')['crime_rate_per_1000'].rank(
                ascending=False, method='dense'
            ).astype(int)
            
            # Clean NaN values
            self.df_suburbs = self.df_suburbs.replace([np.inf, -np.inf], np.nan)
            self.df_suburbs = self.df_suburbs.fillna({
                'crime_rate_per_1000': 0,
                'total_crimes_12m': 0,
                'trend_3m_change': 0,
                'lat': 0,
                'lng': 0,
                'safety_score': 0,
                'rank_in_region': 0
            })
            
            # Convert JSON strings
            self.df_suburbs['crime_breakdown'] = self.df_suburbs['crime_breakdown'].apply(
                lambda x: json.loads(x) if isinstance(x, str) else {}
            )
            self.df_suburbs['crime_breakdown'] = self.df_suburbs['crime_breakdown'].apply(
                lambda x: json.loads(x) if isinstance(x, str) else {}
            )
            self.df_suburbs['tags'] = self.df_suburbs['tags'].apply(
                lambda x: json.loads(x) if isinstance(x, str) else []
            )
            
        logger.info(f"Created suburbs table with {len(self.df_suburbs)} records")
    
    def _create_crimes_table(self) -> None:
        """Create the crimes table with individual crime records"""
        logger.info("Creating crimes table...")
        
        # Create a list to store crime data
        crimes_data = []
        
        # Create a mapping of suburb names to their IDs from df_suburbs
        suburb_id_map = dict(zip(self.df_suburbs['name'], self.df_suburbs['suburb_id']))
        print(suburb_id_map)
        # Process each crime record
        for _, row in self.df_crime.iterrows():
            # Skip records with empty suburb names
            if pd.isna(row['Area Unit']) or row['Area Unit'] == '':
                continue
                
            # Generate UUID for crime_id
            crime_id = str(uuid.uuid4())
            
            # Get suburb_id from the mapping
            suburb_id = suburb_id_map.get(row['Area Unit'])
            # if suburb_id is None:
            #     logger.warning(f"Could not find suburb_id for Area Unit: {row['Area Unit']}")
            #     continue
            
            # Convert date to datetime
            date = pd.to_datetime(row['Year Month'], format='%B %Y')
            
            # Create crime record
            crime_record = {
                'event_id': crime_id,
                'suburb_id': suburb_id,
                'victimisation_date': date.strftime('%Y-%m-%d'),
                'offence_code': row['ANZSOC Division'],
                'offence_category': row['ANZSOC Division'],
                'offence_description': row['ANZSOC Group'],
                'area_unit': row['Area Unit'],
                'district': row['Territorial Authority'],
                'meshblock_code': row.get('Meshblock', ''),  # fallback to empty string
                'location_type': row.get('Locn Type Division', '')  # fallback to empty string
            }
            
            crimes_data.append(crime_record)
            
        # Create crimes dataframe
        self.df_crimes = pd.DataFrame(crimes_data)
        
        logger.info(f"Created {len(self.df_crimes)} crime records")
    
    def save_to_csv(self) -> None:
        """Save the transformed data to CSV files"""            
        logger.info("Saving transformed data to CSV files...")
        
        # Save suburbs data
        suburbs_file = os.path.join(self.output_dir, 'suburbs.csv')
        self.df_suburbs.to_csv(suburbs_file, index=False)
        logger.info(f"Saved suburbs data to {suburbs_file}")
        
        # Save crimes data
        crimes_file = os.path.join(self.output_dir, 'crimes.csv')
        self.df_crimes.to_csv(crimes_file, index=False)
        logger.info(f"Saved crimes data to {crimes_file}")
        
    def save_to_json(self) -> None:
        """Save the transformed data to JSON files (useful for Supabase import)"""
        logger.info("Saving transformed data to JSON files...")
        
        # Save suburbs data
        suburbs_file = os.path.join(self.output_dir, 'suburbs.json')
        suburbs_json = self.df_suburbs.to_dict(orient='records')
        with open(suburbs_file, 'w') as f:
            json.dump(suburbs_json, f, indent=2)
        logger.info(f"Saved suburbs data to {suburbs_file}")
        
        # Save crimes data
        crimes_file = os.path.join(self.output_dir, 'crimes.json')
        crimes_json = self.df_crimes.to_dict(orient='records')
        with open(crimes_file, 'w') as f:
            json.dump(crimes_json, f, indent=2)
        logger.info(f"Saved crimes data to {crimes_file}")
        
    def save_to_supabase(self) -> None:
        """
        Efficiently save data to Supabase tables using batch processing.
        Handles both suburbs (small dataset) and crimes (large dataset) tables.
        """
        if not self.supabase:
            logger.error("Supabase client not initialized")
            return

        # Process suburbs table (small dataset)
        if self.df_suburbs is not None and not self.df_suburbs.empty:
            logger.info(f"Importing {len(self.df_suburbs)} suburbs to Supabase...")
            try:
                # Convert DataFrame to list of dictionaries
                suburbs_data = self.df_suburbs.to_dict(orient='records')
                
                # Insert all suburbs at once (small dataset)
                self.supabase.table('suburbs').insert(suburbs_data).execute()
                logger.info("Successfully imported suburbs")
            except Exception as e:
                logger.error(f"Error importing suburbs: {str(e)}")
                raise

        # Process crimes table (large dataset)
        if self.df_crimes is not None and not self.df_crimes.empty:
            logger.info(f"Importing {len(self.df_crimes)} crimes to Supabase...")
            try:
                # Process in batches
                batch_size = self.batch_size
                total_rows = len(self.df_crimes)
                
                with tqdm(total=total_rows, desc="Importing crimes") as pbar:
                    for start_idx in range(0, total_rows, batch_size):
                        end_idx = min(start_idx + batch_size, total_rows)
                        batch = self.df_crimes.iloc[start_idx:end_idx]
                        
                        # Convert batch to list of dictionaries
                        batch_data = batch.to_dict(orient='records')
                        
                        # Insert batch
                        self.supabase.table('crimes').insert(batch_data).execute()
                        
                        # Update progress
                        pbar.update(len(batch))
                        
                        # Monitor memory usage
                        self._monitor_memory_usage()
                
                logger.info("Successfully imported all crimes")
            except Exception as e:
                logger.error(f"Error importing crimes: {str(e)}")
                raise

    def _monitor_memory_usage(self) -> None:
        """Monitor and log memory usage"""
        process = psutil.Process()
        memory_info = process.memory_info()
        logger.info(f"Memory usage: {memory_info.rss / 1024 / 1024:.2f} MB")
        
        if memory_info.rss > 1024 * 1024 * 1024:  # If using more than 1GB
            logger.warning("High memory usage detected, triggering garbage collection")
            gc.collect()

def process_large_dataset(input_file: str, batch_size: int = 10000) -> None:
    """
    Process a large dataset in batches to avoid memory issues
    
    Args:
        input_file: Path to the input CSV file
        batch_size: Number of rows to process in each batch
    """
    logger.info(f"Processing large dataset in batches of {batch_size}")
    
    # Create output directory
    output_dir = 'output'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Create transformer instance
    transformer = CrimeDataTransformer(input_file=input_file, output_dir=output_dir, batch_size=batch_size)

    # Load census and geojson data
    transformer._load_census_data()
    transformer._load_suburbs_geojson()
    
    # Load and transform data once
    logger.info("Loading and transforming data...")
    transformer.load_data()
    transformer.transform_data()
    
    # Create suburbs table once
    logger.info("Creating suburbs table...")
    transformer._create_suburbs_table()
    
    # Process crimes in parallel
    logger.info("Processing crimes data...")
    
    def process_crime_chunk(chunk):
        chunk_transformer = CrimeDataTransformer(input_file=None, output_dir=output_dir, batch_size=batch_size)
        chunk_transformer.df_crime = chunk
        chunk_transformer.df_suburbs = transformer.df_suburbs  # Share suburbs data
        chunk_transformer.transform_data()
        chunk_transformer._create_crimes_table()
        return chunk_transformer.df_crimes
    
    # Use multiple threads for processing
    num_threads = multiprocessing.cpu_count()
    crimes_data = []
    
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        
        # Submit crime processing tasks
        for chunk in transformer._load_data_in_chunks(input_file, batch_size):
            futures.append(executor.submit(process_crime_chunk, chunk))
        
        # Collect results with progress bar
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing crime chunks"):
            try:
                df_crimes = future.result()
                crimes_data.extend(df_crimes.to_dict(orient='records'))
                
                # Monitor memory usage
                transformer._monitor_memory_usage()
                
            except Exception as e:
                logger.error(f"Error processing crime chunk: {str(e)}")
    
    # Create final crimes DataFrame
    transformer.df_crimes = pd.DataFrame(crimes_data)
    
    # Save data to Supabase
    if transformer.supabase:
        transformer.save_to_supabase()
    
    logger.info("Completed processing all data")

def main():
    """Main function to run the data transformation"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Transform crime data for Supabase import')
    parser.add_argument('--input', required=True, help='Input CSV file')
    parser.add_argument('--output-dir', default='output', help='Output directory')
    parser.add_argument('--batch-size', type=int, default=10000, help='Batch size for large dataset processing')
    parser.add_argument('--max-workers', type=int, default=None, help='Maximum number of worker threads')

    args = parser.parse_args()
    
    # Set number of worker threads if specified
    if args.max_workers:
        multiprocessing.cpu_count = lambda: args.max_workers
    
    process_large_dataset(args.input, args.batch_size)

if __name__ == "__main__":
    main()