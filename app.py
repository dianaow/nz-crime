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
import random

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
    def __init__(self, input_file: str, output_dir: str = 'output'):
        """
        Initialize the crime data transformer
        
        Args:
            input_file: Path to the crime data CSV file
            output_dir: Directory to save output files
        """
        self.input_file = input_file
        self.output_dir = output_dir
        self.df_crime = None
        self.df_suburbs = None
        self.df_crimes = None
        self.df_census = None
        self.geo_df = None
        self.geo_lookup = {}
        self.suburbs_geojson = None
        self.meshblocks_geojson = None  # Store meshblock GeoJSON
        self.geometry_index = {}  # New index for geometry lookups
        self.meshblock_geometries = {}  # Store meshblock geometries
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

    def _load_data(self) -> None:
        """Load and preprocess the crime data CSV"""
        logger.info(f"Reading crime data from {self.input_file}")
        
        # Try different encodings
        encodings = ['utf-8', 'utf-16', 'latin1', 'iso-8859-1', 'cp1252']
        
        for encoding in encodings:
            try:
                # Read the CSV file with tab delimiter
                self.df_crime = pd.read_csv(self.input_file, encoding=encoding, sep='\t')
                logger.info(f"Found {self.df_crime['Area Unit'].nunique()} unique Area Units in the dataset")
                
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

    def _load_census_data(self) -> None:
        """Load census data for population information"""
        census_file = os.path.join(os.path.dirname(self.input_file), 'nzcensus.csv')
        if os.path.exists(census_file):
            self.census_df = pd.read_csv(census_file)
            logger.info(f"Loaded census data with {len(self.census_df)} records")
        else:
            logger.warning(f"Census file not found: {census_file}")
            self.census_df = pd.DataFrame()
            
    def _load_geographic_areas_data(self) -> None:
        """Load geographic areas data for suburb information"""
        if self.input_file is None:
            logger.warning("Cannot load geographic areas data: input_file is None")
            self.geo_df = pd.DataFrame()
            self.geo_lookup = {}
            return
            
        geo_file = os.path.join(os.path.dirname(self.input_file), 'geographic-areas-table-2023.csv')
        if os.path.exists(geo_file):
            # Only load the columns we need to save memory
            self.geo_df = pd.read_csv(geo_file, usecols=['AU2017_name', 'AU2017_name', 'SA22023_code', 'SA22023_name_ascii', 'REGC2023_name', 'TA2023_name', 'MB2023_code', 'MB2013_code', 'UR2023_name'])
            # Create a lookup dictionary for suburb matching
            self.geo_lookup = {}
            meshblock_lookup = {}
            # Create a lookup for meshblock codes
            self.meshblock_lookup = {}
            
            # First pass to collect meshblocks by suburb
            for _, row in self.geo_df.iterrows():
                suburb_name = row['AU2017_name']
                if suburb_name and not pd.isna(suburb_name):
                    if suburb_name not in meshblock_lookup:
                        meshblock_lookup[suburb_name] = []
                    
                    # Add meshblock to the suburb's array if it exists
                    if not pd.isna(row['MB2023_code']):
                        meshblock_lookup[suburb_name].append(row['MB2023_code'])

                # Create a mapping from old meshblock codes to new ones
                if not pd.isna(row['MB2013_code']) and not pd.isna(row['MB2023_code']):
                    self.meshblock_lookup[str(row['MB2013_code'])] = str(row['MB2023_code'])
            
            # Second pass to create the complete lookup
            for _, row in self.geo_df.iterrows():
                suburb_name = row['AU2017_name']
                if suburb_name and not pd.isna(suburb_name):
                    self.geo_lookup[suburb_name] = {
                        'SA22023_code': row['SA22023_code'],
                        'SA22023_name': row['SA22023_name_ascii'],
                        'REGC2023_name': row['REGC2023_name'],
                        'TA2023_name': row['TA2023_name'],
                        'UR2023_name': row['UR2023_name'],
                        'meshblocks': meshblock_lookup.get(suburb_name, [])
                    }
            
            # Analyze the mapping distribution in geo file
            # Analyze the mapping distribution in geo file
            geo_mapping_counts = self.geo_df.groupby('AU2017_name')['SA22023_name_ascii'].nunique()
            logger.info("\nA=SA22023 to AU2017 mapping distribution in geo file:")
            logger.info(f"Number of AU2017 areas with 1 SA2023: {len(geo_mapping_counts[geo_mapping_counts == 1])}")
            logger.info(f"Number of AU2017 areas with 2-5 SA2023: {len(geo_mapping_counts[(geo_mapping_counts > 1) & (geo_mapping_counts <= 5)])}")
            logger.info(f"Number of AU2017 areas with >5 SA2023: {len(geo_mapping_counts[geo_mapping_counts > 5])}")
            
            geo_mapping_counts1 = self.geo_df.groupby('SA22023_name_ascii')['AU2017_name'].nunique()
            logger.info("\nAU2017 to SA22023 mapping distribution in geo file:")
            logger.info(f"Number of SA22023 areas with 1 AU2017: {len(geo_mapping_counts1[geo_mapping_counts1 == 1])}")
            logger.info(f"Number of SA22023 areas with 2-5 AU2017: {len(geo_mapping_counts1[(geo_mapping_counts1 > 1) & (geo_mapping_counts <= 5)])}")
            logger.info(f"Number of SA22023 areas with >5 AU2017: {len(geo_mapping_counts1[geo_mapping_counts1 > 5])}")
            
            logger.info(f"Created lookup for {len(self.geo_lookup)} suburbs with meshblock codes")
            logger.info(f"Created lookup for {len(self.meshblock_lookup)} meshblock codes")
        else:
            logger.warning(f"Geographic areas file not found: {geo_file}")
            self.geo_df = pd.DataFrame()
            self.geo_lookup = {}
            self.meshblock_lookup = {}

    def _load_suburbs_geojson(self) -> None:
        """Load the suburbs geojson data and create the geometry index"""
        try:
            with open('./rawdata/suburbs.geojson', 'r') as f:
                self.suburbs_geojson = json.load(f)
            logger.info("Loaded suburbs geojson data")

            #  Create an index mapping suburb names to their geometries and coordinates.
            for feature in self.suburbs_geojson['features']:
                suburb_name = feature['properties']['SA22023__2']
                if suburb_name:  # Only index if suburb name exists
                    self.geometry_index[suburb_name] = {
                        'geometry': feature['geometry'],
                        'lat': feature['properties']['latitude'],
                        'lng': feature['properties']['longitude']
                    }
            logger.info(f"Created geometry index with {len(self.geometry_index)} suburbs")

        except Exception as e:
            logger.error(f"Error loading suburbs geojson data: {str(e)}")
            self.suburbs_geojson = None

    def _load_meshblocks_geojson(self) -> None:
        """
        Load the meshblocks geojson data and create the geometry index.
        """
        try:
            with open('./rawdata/meshblocks.geojson', 'r') as f:
                self.meshblocks_geojson = json.load(f)
            logger.info("Loaded meshblocks geojson data")

            # Create an index mapping meshblock IDs to their geometries
            for feature in self.meshblocks_geojson['features']:
                mb_id = feature['properties']['MB2023_V1_']       
                if mb_id:
                    try:
                        # Strip leading zeros from meshblock ID
                        mb_id = str(int(mb_id))  # Convert to int to remove leading zeros, then back to string
                        
                        # Store the raw geometry instead of converting to WKT here
                        self.meshblock_geometries[str(mb_id)] = {
                            'geometry': feature['geometry'],  # Store raw GeoJSON geometry
                            'lat': feature['properties']['latitude'],
                            'lng': feature['properties']['longitude']
                        }
                    except Exception as e:
                        logger.warning(f"Error processing geometry for meshblock {mb_id}: {str(e)}")             
            logger.info(f"Created meshblock geometry index with {len(self.meshblock_geometries)} meshblocks")
    
        except Exception as e:
            logger.error(f"Error loading meshblocks geojson data: {str(e)}")
            self.meshblocks_geojson = None
            self.meshblock_geometries = {}
    
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
        # text = text.replace('-', ' ')  # Replace hyphens with spaces
        # text = text.replace('_', ' ')  # Replace underscores with spaces
        
        # Normalize spaces (replace multiple spaces with single space)
        text = ' '.join(text.split())
        
        return text.strip()
        
    def _create_crimes_table(self) -> None:
        """Create the crimes table with individual crime records using parallel processing"""
        if self.df_crime is None or self.df_crime.empty:
            logger.warning("No crime data available to process")
            return

        # Define a function to process crime chunks in parallel
        def process_crime_chunk(chunk):
            # Clean and transform the data using vectorized operations where possible
            chunk.columns = chunk.columns.str.strip()
            
            # Clean text columns in bulk
            text_columns = chunk.select_dtypes(include=['object']).columns
            chunk[text_columns] = chunk[text_columns].apply(lambda x: x.str.strip())
            
            # Clean special characters from Area Unit and Territorial Authority columns
            chunk['Area Unit'] = chunk['Area Unit'].apply(self._clean_text)
            chunk['Territorial Authority'] = chunk['Territorial Authority'].apply(self._clean_text)

            # Convert suburb name and suburb_id columns to 2023 format using vectorized operations
            chunk['suburb_id'] = chunk['Area Unit'].map(lambda x: self.geo_lookup.get(x, {}).get('SA22023_code', ''))
            
            # Convert meshblock codes to 2023 format
            chunk['Meshblock'] = chunk['Meshblock'].astype(str)
            chunk['MB2023_code'] = chunk['Meshblock'].map(lambda x: self.meshblock_lookup.get(x, x))

            # Create ANZSOC division code mapping
            anzsoc_divisions = {
                "1": "HOMICIDE AND RELATED OFFENCES",
                "2": "ACTS INTENDED TO CAUSE INJURY",
                "3": "SEXUAL ASSAULT AND RELATED OFFENCES",
                "4": "DANGEROUS OR NEGLIGENT ACTS ENDANGERING PERSONS",
                "5": "ABDUCTION, HARASSMENT AND OTHER RELATED OFFENCES AGAINST A PERSON",
                "6": "ROBBERY, EXTORTION AND RELATED OFFENCES",
                "7": "UNLAWFUL ENTRY WITH INTENT/BURGLARY, BREAK AND ENTER",
                "8": "THEFT AND RELATED OFFENCES",
                "9": "FRAUD, DECEPTION AND RELATED OFFENCES",
                "10": "ILLICIT DRUG OFFENCES",
                "11": "PROHIBITED AND REGULATED WEAPONS AND EXPLOSIVES OFFENCES",
                "12": "PROPERTY DAMAGE AND ENVIRONMENTAL POLLUTION",
                "13": "PUBLIC ORDER OFFENCES",
                "14": "TRAFFIC AND VEHICLE REGULATORY OFFENCES",
                "15": "OFFENCES AGAINST JUSTICE PROCEDURES, GOVERNMENT SECURITY AND GOVERNMENT OPERATIONS",
                "16": "MISCELLANEOUS OFFENCES"
            }
            
            # Create reverse lookup for extracting code from division name
            anzsoc_reverse_lookup = {v.lower(): k for k, v in anzsoc_divisions.items()}
            
            # Process dates in bulk
            chunk['victimisation_date'] = pd.to_datetime(chunk['Year Month'], format='%B %Y', errors='coerce').dt.strftime('%Y-%m-%d')
            
            # Extract offence codes in bulk
            chunk['offence_code'] = chunk['ANZSOC Division'].apply(
                lambda x: x[:2] if pd.notna(x) and len(str(x)) >= 2 and str(x)[:2].isdigit() 
                else anzsoc_reverse_lookup.get(str(x).lower(), "") if pd.notna(x) else ""
            )
            
            # Create crime records in bulk
            crime_records = []
            for _, row in chunk.iterrows():
                if pd.isna(row['suburb_id']) or row['suburb_id'] == '':
                    continue
                    
                crime_record = {
                    'event_id': str(uuid.uuid4()),
                    'suburb_id': row['suburb_id'],
                    'victimisation_date': row['victimisation_date'],
                    'offence_code': row['offence_code'] if row['offence_code'] else None,
                    'offence_category': row['ANZSOC Division'] if pd.notna(row['ANZSOC Division']) else None,
                    'offence_description': row['ANZSOC Group'] if pd.notna(row['ANZSOC Group']) else None,
                    'meshblock_code': str(row['MB2023_code']) if pd.notna(row['MB2023_code']) else None,
                    'AU2017_name': row['Area Unit'] if pd.notna(row['Area Unit']) else None
                }
                crime_records.append(crime_record)
            
            return crime_records
        
        # Create chunks of the crime data for parallel processing
        df_chunks = []
        chunk_size = 50000  # Increased chunk size for better performance
        
        # If df_crime is already loaded as a whole, split it into chunks
        if isinstance(self.df_crime, pd.DataFrame):
            num_chunks = max(1, len(self.df_crime) // chunk_size)
            df_chunks = np.array_split(self.df_crime, num_chunks)
        else:
            # If we need to load from file in chunks, load_data_in_chunks will handle it
            logger.warning("Crime data not loaded as DataFrame, chunking not possible")
            return
            
        logger.info(f"Processing {len(df_chunks)} chunks of crime data in parallel")
        
        # Process chunks in parallel
        crimes_data = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []
            
            # Submit chunk processing tasks
            for chunk in df_chunks:
                futures.append(executor.submit(process_crime_chunk, chunk))
            
            # Collect results with progress bar
            for future in tqdm(as_completed(futures), total=len(futures), desc="Processing crime chunks"):
                try:
                    chunk_crimes = future.result()
                    crimes_data.extend(chunk_crimes)
                    
                    # Monitor memory usage periodically
                    if random.random() < 0.1:  # Check memory usage randomly to reduce overhead
                        self._monitor_memory_usage()
                        
                except Exception as e:
                    logger.error(f"Error processing crime chunk: {str(e)}")
        
        # Create final crimes DataFrame
        self.df_crimes = pd.DataFrame(crimes_data)
        
        logger.info(f"Created {len(self.df_crimes)} crime records using parallel processing")

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

    @lru_cache(maxsize=1000)  
    def _get_suburb_population(self, suburb_name: str) -> int:
        """
        Get the population for a suburb from the census data
        
        Args:
            suburb_name: Name of the suburb to look up
            
        Returns:
            Population count for the suburb
        """
        if self.df_census is None:
            #print("Census data not loaded", suburb_name)
            return 10000  # Fallback to placeholder if census data not loaded
            
        # Try to find exact match
        match = self.df_census[(self.df_census['Area'] == suburb_name) & (self.df_census['Year'] == 2023)]
        if not match.empty:
            return match['Value'].iloc[0]
            
        # If no exact match, try to find partial match
        # This handles cases where the names might be slightly different
        for area in self.df_census['Area']:
            if suburb_name.lower() in area.lower() or area.lower() in suburb_name.lower():
                return self.df_census[self.df_census['Area'] == area]['Value'].iloc[0]
                
        # If no match found, return placeholder
        return 10000
              
    def _process_suburb_batch(self, suburb_data_input: List[Dict]) -> List[Dict]:
        """
        Process a batch of suburbs in parallel
        
        Args:
            suburb_data_input: List containing suburb name and dataframe tuple
            
        Returns:
            List of processed suburb data dictionaries
        """
        processed_suburbs = []
        
        for item in suburb_data_input:
            suburb_name = item['AU2017_name']
            suburb_df = item['suburb_df']
            
            try:
                # Calculate crime statistics using victimisation_date
                suburb_df['Date'] = pd.to_datetime(suburb_df['victimisation_date'])
                current_date = datetime.now()
                one_year_ago = current_date - timedelta(days=365)
                last_12m_data = suburb_df[suburb_df['Date'] >= one_year_ago]
                total_crimes_12m = len(last_12m_data)

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
                
                crime_breakdown = suburb_df.groupby('offence_code').size().to_dict()
                
                # Get geographic data from the lookup
                geo_data = self.geo_lookup.get(suburb_name, {})
                suburb_id = geo_data.get('SA22023_code') or str(uuid.uuid5(uuid.NAMESPACE_DNS, suburb_name))
                suburb_name_2023 = geo_data.get('SA22023_name', suburb_name)
                region = geo_data.get('REGC2023_name')
                council = geo_data.get('TA2023_name', '')
                location_type = geo_data.get('UR2023_name', '')
                #meshblocks = geo_data.get('meshblocks', [])

                # Calculate crime rate based on suburb population
                population = self._get_suburb_population(suburb_name_2023)
                crime_rate_per_1000 = (total_crimes_12m / population) * 1000 if population > 0 else 0
                
                # Create the suburb data dictionary
                suburb_data = {
                    'suburb_id': suburb_id,
                    'AU2017_name': suburb_name,
                    'name': suburb_name_2023,
                    'region': region,
                    'council': council,
                    'location_type': location_type,
                    #'meshblocks': meshblocks,  # Store as list
                    'lat': None,
                    'lng': None,
                    'geometry': None,
                    'safety_score': 0,
                    'crime_rate_per_1000': crime_rate_per_1000,
                    'total_crimes_12m': int(total_crimes_12m) if total_crimes_12m < 32767 else 32767,  # Cast to smallint with upper limit
                    'crime_trend': crime_trend,
                    'rank_in_region': 0,
                    'crime_breakdown': json.dumps(crime_breakdown),
                    'trend_3m_change': trend_3m_change,
                    'report_url': '',
                    'widget_embed_code': '',
                    'summary_text': '',
                    'tags': []
                }
                
                # Get and process the geometry data
                lat, lng, geometry = self._get_suburb_geometry(suburb_name_2023)
                wkt_geometry = None  # Default to None (NULL in DB)

                if geometry:
                    try:
                        shapely_geom = shape(geometry)
                        if shapely_geom.is_valid:
                            wkt_geometry = shapely_geom.wkt
                        else:
                            logger.warning(f"Invalid geometry for suburb {suburb_name}, using NULL geometry")
                    except Exception as geom_error:
                        logger.warning(f"Error converting geometry for suburb {suburb_name}: {str(geom_error)}")

                suburb_data.update({
                    'lat': lat,
                    'lng': lng,
                    'geometry': wkt_geometry  # This will map to NULL in Supabase if None
                })

                processed_suburbs.append(suburb_data)

            except Exception as e:
                logger.error(f"Error processing suburb {suburb_name}: {str(e)}")
                continue
    
        return processed_suburbs

    def _create_suburbs_table(self) -> None:
        """Create the suburbs table with aggregated crime data using parallel processing"""
        if self.df_crimes is None or self.df_crimes.empty:
            logger.warning("No crime data available to process")
            return
        
        # Group by suburb_id to prepare suburb data
        suburb_groups = self.df_crimes.groupby('suburb_id')
        logger.info(f"Found {len(suburb_groups)} unique suburbs to process")
        
        # Prepare data for parallel processing
        suburb_inputs = []
        
        # Extract suburb data
        for suburb_id, suburb_df in suburb_groups:
            if pd.isna(suburb_id) or suburb_id == '':
                logger.warning(f"Skipping suburb {suburb_id} due to empty ID")
                continue
                
            # Get the original Area Unit name from the first record in df_crimes
            area_unit = suburb_df['AU2017_name'].iloc[0] if not suburb_df.empty else ''
                
            # Create a dictionary with the suburb data
            suburb_input = {
                'AU2017_name': area_unit,
                'suburb_id': suburb_id,
                'suburb_df': suburb_df
            }
            suburb_inputs.append(suburb_input)
        
        # Process suburbs in parallel
        processed_suburbs = []
        
        # Split into batches for parallel processing
        batch_size = 500  # Process in batches of 500 suburbs
        suburb_batches = [suburb_inputs[i:i + batch_size] for i in range(0, len(suburb_inputs), batch_size)]
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []
            
            # Submit suburb processing tasks
            for batch in suburb_batches:
                futures.append(executor.submit(self._process_suburb_batch, batch))
            
            # Collect results with progress bar
            for future in tqdm(as_completed(futures), total=len(futures), desc="Processing suburbs"):
                try:
                    batch_results = future.result()
                    processed_suburbs.extend(batch_results)
                except Exception as e:
                    logger.error(f"Error processing suburb batch: {str(e)}")
        
        # Create final suburbs DataFrame
        self.df_suburbs = pd.DataFrame(processed_suburbs)
        
        if not self.df_suburbs.empty:
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
            
            # self.df_suburbs['meshblocks'] = self.df_suburbs['meshblocks'].apply(
            #     lambda x: x if isinstance(x, list) else []
            # )
            
            if 'tags' in self.df_suburbs.columns:
                self.df_suburbs['tags'] = self.df_suburbs['tags'].apply(
                    lambda x: x if isinstance(x, list) else []
                )
                
            self.df_suburbs['crime_breakdown'] = self.df_suburbs['crime_breakdown'].apply(
                lambda x: json.loads(x) if isinstance(x, str) else {}
            )
            
        logger.info(f"Created suburbs table with {len(self.df_suburbs)} records")
 
    def _process_meshblock_batch(self, meshblock_batch: List[Dict]) -> List[Dict]:
        """
        Process a batch of meshblocks in parallel
        
        Args:
            meshblock_batch: List of meshblock data dictionaries
            
        Returns:
            List of processed meshblock data dictionaries
        """
        processed_meshblocks = []
        
        for meshblock_data in meshblock_batch:
            try:
                mb_id = meshblock_data['id']
                
                # Get geometry data from the meshblock_geometries dictionary
                geometry_data = self.meshblock_geometries.get(str(mb_id))
                
                if geometry_data:
                    wkt_geometry = None  # Default to None (NULL in DB)
                    
                    # Convert GeoJSON geometry to WKT format using Shapely - similar to suburb processing
                    if geometry_data.get('geometry'):
                        try:
                            shapely_geom = shape(geometry_data['geometry'])
                            if shapely_geom.is_valid:
                                wkt_geometry = shapely_geom.wkt
                            else:
                                logger.warning(f"Invalid geometry for meshblock {mb_id}, using NULL geometry")
                        except Exception as geom_error:
                            logger.warning(f"Error converting geometry for meshblock {mb_id}: {str(geom_error)}")
                    
                    meshblock_data.update({
                        'lat': geometry_data.get('lat'),
                        'lng': geometry_data.get('lng'),
                        'geometry': wkt_geometry
                    })
                
                processed_meshblocks.append(meshblock_data)
                
            except Exception as e:
                logger.error(f"Error processing meshblock {mb_id}: {str(e)}")
                continue
        
        return processed_meshblocks

    def _create_meshblocks_table(self) -> None:
        """
        Process meshblocks with crime counts and geometries using parallel processing
        """
        if self.df_crimes is None or self.df_crimes.empty:
            logger.warning("No crime data available for meshblock crime calculation")
            return
            
        # Get valid suburb_ids from the crimes data instead of suburbs table
        valid_suburb_ids = set(self.df_crimes['suburb_id'].unique())
        logger.info(f"Found {len(valid_suburb_ids)} valid suburb IDs from crimes data")
            
        # Group by meshblock and get the first suburb_id for each
        meshblock_suburbs = self.df_crimes.groupby('meshblock_code')['suburb_id'].first()
        
        # Count crime occurrences of each meshblock
        crime_counts = self.df_crimes['meshblock_code'].value_counts().to_dict()
        
        # Create a list of meshblock record dictionaries
        meshblocks_list = []
        
        for mb_code, count in crime_counts.items():
            # Skip empty/invalid codes
            if pd.isna(mb_code) or mb_code == '':
                continue
            
            # Get the suburb ID from the crime data
            suburb_id = meshblock_suburbs.get(mb_code)
            
            # Only create record if suburb_id is valid
            if suburb_id is not None and suburb_id in valid_suburb_ids:
                # Create a record for this meshblock
                meshblock_record = {
                    'id': mb_code,
                    'suburb_id': suburb_id,
                    'crime_count': count,
                    'lat': 0,
                    'lng': 0,
                    'geometry': None
                }
                
                meshblocks_list.append(meshblock_record)
            else:
                logger.warning(f"Skipping meshblock {mb_code} due to invalid suburb_id: {suburb_id}")
        
        logger.info(f"Created {len(meshblocks_list)} meshblock records from crime data")

        # Process meshblocks in parallel
        processed_meshblocks = []
        
        # Split into batches for parallel processing
        batch_size = 1000
        meshblock_batches = [meshblocks_list[i:i + batch_size] for i in range(0, len(meshblocks_list), batch_size)]
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []
            
            # Submit meshblock processing tasks
            for batch in meshblock_batches:
                futures.append(executor.submit(self._process_meshblock_batch, batch))
            
            # Collect results with progress bar
            for future in tqdm(as_completed(futures), total=len(futures), desc="Processing meshblocks"):
                try:
                    batch_results = future.result()
                    processed_meshblocks.extend(batch_results)
                except Exception as e:
                    logger.error(f"Error processing meshblock batch: {str(e)}")
        
        # Create final meshblocks DataFrame
        self.df_meshblocks = pd.DataFrame(processed_meshblocks)
        self.df_meshblocks = self.df_meshblocks.replace([np.inf, -np.inf, np.nan], None)

        logger.info(f"Processed {len(self.df_meshblocks)} meshblocks with crime data and geometries")

    def _save_batch_to_supabase(self, batch_data: List[Dict], table_name: str, use_upsert: bool) -> int:
        """
        Process and save a batch of data to Supabase
        
        Args:
            batch_data: List of dictionaries to save
            table_name: Name of the Supabase table
            use_upsert: Whether to use upsert (True) or insert (False)
            
        Returns:
            Number of processed items
        """
        max_retries = 5
        retry_delay = 3  # seconds
        
        # Try inserting with retries
        for retry in range(max_retries):
            try:
                # Insert or upsert batch based on parameter
                if use_upsert:
                    self.supabase.table(table_name).upsert(batch_data).execute()
                else:
                    self.supabase.table(table_name).insert(batch_data).execute()
                
                return len(batch_data)  # Return number of processed items for progress tracking
                
            except Exception as e:
                error_message = str(e)
                logger.warning(f"Error on attempt {retry+1}/{max_retries} for {table_name} batch: {error_message}")
                
                # If we get a timeout or server disconnection, reduce batch size
                if "timeout" in error_message.lower() or "disconnect" in error_message.lower():
                    if len(batch_data) > 1:
                        # Split batch in half and try each half separately
                        mid_point = len(batch_data) // 2
                        logger.info(f"Timeout detected, splitting batch of {len(batch_data)} into two smaller batches")
                        
                        # Process first half
                        first_half = batch_data[:mid_point]
                        success_count = self._save_batch_to_supabase(first_half, table_name, use_upsert)
                        
                        # Process second half
                        second_half = batch_data[mid_point:]
                        success_count += self._save_batch_to_supabase(second_half, table_name, use_upsert)
                        
                        return success_count
                    else:
                        logger.error(f"Cannot split batch further, single record is causing timeout")
                
                if retry < max_retries - 1:
                    # Exponential backoff with increasing delay
                    sleep_time = retry_delay * (2 ** retry)
                    logger.info(f"Retrying in {sleep_time} seconds...")
                    time.sleep(sleep_time)
                else:
                    logger.error(f"Failed to import {table_name} batch after {max_retries} attempts")
                    raise

    def _save_dataframe_to_supabase(self, df: pd.DataFrame, table_name: str, use_upsert: bool = True, 
                          batch_size: Optional[int] = None, desc: str = None) -> None:
        """
        Generic method to save a DataFrame to Supabase table using parallel batch processing.
        
        Args:
            df: DataFrame to save
            table_name: Name of the Supabase table
            use_upsert: Whether to use upsert (True) or insert (False)
            batch_size: Optional custom batch size
            desc: Description for the progress bar
        """
        if not self.supabase:
            logger.error("Supabase client not initialized")
            return

        if df is None or df.empty:
            logger.warning(f"No data available to save to {table_name}")
            return
            
        # Use provided batch size or default to a reasonable size
        supabase_batch_size = batch_size or 1000
            
        # Use table name for progress bar description if not provided
        progress_desc = desc or f"Importing {table_name}"

        logger.info(f"Importing {len(df)} records to {table_name} using parallel processing...")
        
        # Convert DataFrame to list of dictionaries
        data_records = df.to_dict(orient='records')
        total_records = len(data_records)
        
        # Create batches for parallel processing
        batches = []
        for start_idx in range(0, total_records, supabase_batch_size):
            end_idx = min(start_idx + supabase_batch_size, total_records)
            batch = data_records[start_idx:end_idx]
            batches.append(batch)
        
        # Set up progress bar
        progress_bar = tqdm(total=total_records, desc=progress_desc)
        
        # Process in parallel with ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=min(self.max_workers, len(batches))) as executor:
            futures = []
            
            # Submit batch processing tasks
            for batch in batches:
                futures.append(executor.submit(
                    self._save_batch_to_supabase, 
                    batch_data=batch, 
                    table_name=table_name, 
                    use_upsert=use_upsert
                ))
            
            # Collect results and update progress
            for future in as_completed(futures):
                try:
                    processed_count = future.result()
                    progress_bar.update(processed_count)
                    
                    # Monitor memory usage periodically
                    if random.random() < 0.1:  # Check memory usage randomly to reduce overhead
                        self._monitor_memory_usage()
                        
                except Exception as e:
                    logger.error(f"Error processing batch for {table_name}: {str(e)}")
            
            progress_bar.close()
        
        logger.info(f"Successfully imported all data to {table_name} using parallel processing")

    def save_suburbs_to_supabase(self) -> None:
        """
        Efficiently save suburbs data to Supabase table using batch processing.
        """
        self._save_dataframe_to_supabase(
            df=self.df_suburbs,
            table_name='suburbs',
            use_upsert=True,
            batch_size=250,
            desc="Importing suburbs"
        )

    def save_crimes_to_supabase(self) -> None:
        """
        Efficiently save crimes data to Supabase table using batch processing.
        """
        self.df_crimes = self.df_crimes.drop('AU2017_name', axis=1)
        self._save_dataframe_to_supabase(
            df=self.df_crimes,
            table_name='crimes',
            use_upsert=True,
            batch_size=500,
            desc="Importing crimes"
        )

    def save_meshblocks_to_supabase(self) -> None:
        """
        Save meshblock data to Supabase table using batch processing
        """
        self._save_dataframe_to_supabase(
            df=self.df_meshblocks,
            table_name='meshblocks',
            batch_size=250,
            use_upsert=True,
            desc="Importing meshblocks"
        )
        
    def save_to_supabase(self) -> None:
        """
        Save all data to Supabase tables using the individual saving methods
        """
        if not self.supabase:
            logger.error("Supabase client not initialized")
            return
            
        # Save all data types
        logger.info("Starting to save all data to Supabase...")
        self.save_suburbs_to_supabase()
        self.save_meshblocks_to_supabase()
        self.save_crimes_to_supabase()
        
        logger.info("Completed saving all data to Supabase")

    def save_to_csv(self) -> None:
        """Save the transformed data to CSV files"""            
        logger.info("Saving transformed data to CSV files...")
        
        # Save suburbs data
        suburbs_file = os.path.join(self.output_dir, 'suburbs.csv')
        self.df_suburbs.to_csv(suburbs_file, index=False)
        logger.info(f"Saved suburbs data to {suburbs_file}")

        # Save meshblocks data
        meshblocks_file = os.path.join(self.output_dir, 'meshblocks.csv')
        self.df_meshblocks.to_csv(meshblocks_file, index=False)
        logger.info(f"Saved meshblocks data to {meshblocks_file}")

        # Save crimes data
        crimes_file = os.path.join(self.output_dir, 'crimes.csv')
        self.df_crimes.to_csv(crimes_file, index=False)
        logger.info(f"Saved crimes data to {crimes_file}")

    def _monitor_memory_usage(self) -> None:
        """Monitor and log memory usage"""
        process = psutil.Process()
        memory_info = process.memory_info()
        logger.info(f"Memory usage: {memory_info.rss / 1024 / 1024:.2f} MB")
        
        if memory_info.rss > 1024 * 1024 * 1024:  # If using more than 1GB
            logger.warning("High memory usage detected, triggering garbage collection")
            gc.collect()

def process_large_dataset(input_file: str) -> None:
    """
    Process a large dataset in batches to avoid memory issues
    
    Args:
        input_file: Path to the input CSV file
        batch_size: Number of rows to process in each batch
    """
    # Create output directory
    output_dir = 'output'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Create transformer instance
    transformer = CrimeDataTransformer(input_file=input_file, output_dir=output_dir)

    # Load census and geojson data
    transformer._load_census_data()
    transformer._load_geographic_areas_data()
    transformer._load_suburbs_geojson()
    transformer._load_meshblocks_geojson()
    
    # Load crimes data
    transformer._load_data()
    
    logger.info("Creating crimes table...")
    transformer._create_crimes_table()

    logger.info("Creating suburbs table...")
    transformer._create_suburbs_table()
    
    logger.info("Creating meshblocks table...")
    transformer._create_meshblocks_table()

    # # Save data to Supabase
    if transformer.supabase:
        transformer.save_to_supabase()

    # #Save data to CSV
    #transformer.save_to_csv()

    logger.info("Completed processing all data")

def main():
    """Main function to run the data transformation"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Transform crime data for Supabase import')
    parser.add_argument('--input', required=True, help='Input CSV file')
    parser.add_argument('--output-dir', default='output', help='Output directory')
    parser.add_argument('--max-workers', type=int, default=None, help='Maximum number of worker threads')

    args = parser.parse_args()
    
    # Set number of worker threads if specified
    if args.max_workers:
        multiprocessing.cpu_count = lambda: args.max_workers
    
    process_large_dataset(args.input)

if __name__ == "__main__":
    main()