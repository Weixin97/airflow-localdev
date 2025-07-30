"""
SpaceX Data Extractor - No Pandas Required!
Fetches raw JSON from SpaceX API and loads directly to PostgreSQL
"""

import os
import requests
import psycopg2
import json
from datetime import datetime
import logging
from typing import List, Dict, Optional

# Setup logging
logger = logging.getLogger(__name__)

class SpaceXExtractor:
    def __init__(self):
        self.base_url = "https://api.spacexdata.com/v4"
        self.db_config = {
            'host': os.getenv('SPACEX_DB_HOST', 'spacex_postgres'),
            'port': int(os.getenv('SPACEX_DB_PORT', 5432)),
            'user': os.getenv('SPACEX_DB_USER', 'spacex_user'),
            'password': os.getenv('SPACEX_DB_PASSWORD', 'spacx_password'),
            'database': os.getenv('SPACEX_DB_NAME', 'spacex_db')
        }
    
    def get_connection(self):
        """Get PostgreSQL database connection"""
        try:
            conn = psycopg2.connect(**self.db_config)
            return conn
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    def log_pipeline_run(self, dag_id: str, task_id: str, status: str, records: int = 0, error: str = None):
        """Log pipeline execution to metadata table"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Fixed to match the actual table schema
            cursor.execute("""
                INSERT INTO bronze.pipeline_metadata (pipeline_name, status, records_processed, error_message)
                VALUES (%s, %s, %s, %s)
            """, (f"{dag_id}.{task_id}", status, records, error))
            
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as e:
            logger.error(f"Failed to log pipeline run: {e}")
    
    def extract_launches(self, **context) -> int:
        """Extract launch data from SpaceX API and load to bronze.launches"""
        logger.info("🚀 Extracting launches from SpaceX API...")
        
        try:
            # Fetch data from API
            response = requests.get(f"{self.base_url}/launches", timeout=60)
            response.raise_for_status()
            launches = response.json()
            
            logger.info(f"Retrieved {len(launches)} launches from API")
            
            # Load to PostgreSQL
            conn = self.get_connection()
            cursor = conn.cursor()
            
            inserted_count = 0
            updated_count = 0
            
            for launch in launches:
                cursor.execute("""
                    INSERT INTO bronze.launches (id, data, extracted_at, source_system)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (id) 
                    DO UPDATE SET 
                        data = EXCLUDED.data,
                        extracted_at = EXCLUDED.extracted_at
                    RETURNING (xmax = 0) AS inserted
                """, (
                    launch['id'], 
                    json.dumps(launch), 
                    datetime.now(),
                    'spacex_api_v4'
                ))
                
                is_inserted = cursor.fetchone()[0]
                if is_inserted:
                    inserted_count += 1
                else:
                    updated_count += 1
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"✅ Launches loaded: {inserted_count} new, {updated_count} updated")
            
            # Log success
            self.log_pipeline_run(
                context.get('dag').dag_id if context.get('dag') else 'manual',
                'extract_launches',
                'success',
                len(launches)
            )
            
            return len(launches)
            
        except Exception as e:
            logger.error(f"❌ Error extracting launches: {e}")
            # Log error
            self.log_pipeline_run(
                context.get('dag').dag_id if context.get('dag') else 'manual',
                'extract_launches',
                'failed',
                0,
                str(e)
            )
            raise
    
    def extract_starlink(self, **context) -> int:
        """Extract Starlink satellite data from SpaceX API"""
        logger.info("🛰️ Extracting Starlink satellites from SpaceX API...")
        
        try:
            # Fetch data from API
            response = requests.get(f"{self.base_url}/starlink", timeout=120)
            response.raise_for_status()
            satellites = response.json()
            
            logger.info(f"Retrieved {len(satellites)} satellites from API")
            
            # Load to PostgreSQL
            conn = self.get_connection()
            cursor = conn.cursor()
            
            inserted_count = 0
            updated_count = 0
            
            for satellite in satellites:
                cursor.execute("""
                    INSERT INTO bronze.starlink (id, data, extracted_at, source_system)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (id) 
                    DO UPDATE SET 
                        data = EXCLUDED.data,
                        extracted_at = EXCLUDED.extracted_at
                    RETURNING (xmax = 0) AS inserted
                """, (
                    satellite['id'], 
                    json.dumps(satellite), 
                    datetime.now(),
                    'spacex_api_v4'
                ))
                
                is_inserted = cursor.fetchone()[0]
                if is_inserted:
                    inserted_count += 1
                else:
                    updated_count += 1
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"✅ Starlink loaded: {inserted_count} new, {updated_count} updated")
            
            # Log success
            self.log_pipeline_run(
                context.get('dag').dag_id if context.get('dag') else 'manual',
                'extract_starlink',
                'success',
                len(satellites)
            )
            
            return len(satellites)
            
        except Exception as e:
            logger.error(f"❌ Error extracting Starlink data: {e}")
            # Log error
            self.log_pipeline_run(
                context.get('dag').dag_id if context.get('dag') else 'manual',
                'extract_starlink',
                'failed',
                0,
                str(e)
            )
            raise
    
    def extract_rockets(self, **context) -> int:
        """Extract rocket data from SpaceX API"""
        logger.info("🚀 Extracting rockets from SpaceX API...")
        
        try:
            # Fetch data from API
            response = requests.get(f"{self.base_url}/rockets", timeout=30)
            response.raise_for_status()
            rockets = response.json()
            
            logger.info(f"Retrieved {len(rockets)} rockets from API")
            
            # Load to PostgreSQL
            conn = self.get_connection()
            cursor = conn.cursor()
            
            for rocket in rockets:
                cursor.execute("""
                    INSERT INTO bronze.rockets (id, data, extracted_at, source_system)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (id) 
                    DO UPDATE SET 
                        data = EXCLUDED.data,
                        extracted_at = EXCLUDED.extracted_at
                """, (
                    rocket['id'], 
                    json.dumps(rocket), 
                    datetime.now(),
                    'spacex_api_v4'
                ))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"✅ Rockets loaded: {len(rockets)} records")
            
            # Log success
            self.log_pipeline_run(
                context.get('dag').dag_id if context.get('dag') else 'manual',
                'extract_rockets',
                'success',
                len(rockets)
            )
            
            return len(rockets)
            
        except Exception as e:
            logger.error(f"❌ Error extracting rockets: {e}")
            # Log error
            self.log_pipeline_run(
                context.get('dag').dag_id if context.get('dag') else 'manual',
                'extract_rockets',
                'failed',
                0,
                str(e)
            )
            raise
    
    def get_extraction_summary(self) -> Dict:
        """Get summary of extracted data"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Get counts from bronze tables
            cursor.execute("""
                SELECT 
                    (SELECT COUNT(*) FROM bronze.launches) as launches_count,
                    (SELECT COUNT(*) FROM bronze.starlink) as starlink_count,
                    (SELECT COUNT(*) FROM bronze.rockets) as rockets_count,
                    (SELECT MAX(extracted_at) FROM bronze.launches) as last_launch_update,
                    (SELECT MAX(extracted_at) FROM bronze.starlink) as last_starlink_update
            """)
            
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            
            return {
                'launches_count': result[0],
                'starlink_count': result[1],
                'rockets_count': result[2],
                'last_launch_update': result[3],
                'last_starlink_update': result[4]
            }
            
        except Exception as e:
            logger.error(f"Failed to get extraction summary: {e}")
            return {}

def test_connection() -> bool:
    """Test database connectivity"""
    try:
        extractor = SpaceXExtractor()
        conn = extractor.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        logger.info("✅ Database connection test successful")
        return True
        
    except Exception as e:
        logger.error(f"❌ Database connection test failed: {e}")
        return False