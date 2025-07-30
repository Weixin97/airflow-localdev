"""
Database utility functions for SpaceX ETL pipeline
"""

import os
import psycopg2
import logging
from typing import Dict, List

logger = logging.getLogger(__name__)

def get_db_connection():
    """Get database connection using environment variables"""
    config = {
        'host': os.getenv('SPACEX_DB_HOST', 'spacex-postgres'),
        'port': int(os.getenv('SPACEX_DB_PORT', 5432)),
        'user': os.getenv('SPACEX_DB_USER', 'spacex_user'),
        'password': os.getenv('SPACX_DB_PASSWORD', 'spacex_password'),
        'database': os.getenv('SPACEX_DB_NAME', 'spacex_db')
    }
    return psycopg2.connect(**config)

def check_data_quality(**context) -> Dict:
    """Check data quality in bronze tables"""
    logger.info("🔍 Running data quality checks...")
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Data quality checks
        checks = {}
        
        # Check for duplicate launches
        cursor.execute("SELECT COUNT(*), COUNT(DISTINCT id) FROM bronze.launches")
        total, unique = cursor.fetchone()
        checks['launches_duplicates'] = total - unique
        
        # Check for launches with missing data
        cursor.execute("""
            SELECT COUNT(*) FROM bronze.launches 
            WHERE data->>'name' IS NULL OR data->>'date_utc' IS NULL
        """)
        checks['launches_missing_data'] = cursor.fetchone()[0]
        
        # Check Starlink satellites with active status
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(CASE WHEN data->'spaceTrack'->>'DECAYED' = '0' THEN 1 END) as active,
                COUNT(CASE WHEN data->'spaceTrack'->>'DECAYED' = '1' THEN 1 END) as inactive
            FROM bronze.starlink
        """)
        total, active, inactive = cursor.fetchone()
        checks['starlink_total'] = total
        checks['starlink_active'] = active
        checks['starlink_inactive'] = inactive
        
        # Check recent data freshness (last 7 days)
        cursor.execute("""
            SELECT COUNT(*) FROM bronze.launches 
            WHERE extracted_at >= NOW() - INTERVAL '7 days'
        """)
        checks['recent_launches'] = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        logger.info(f"✅ Data quality checks completed: {checks}")
        return checks
        
    except Exception as e:
        logger.error(f"❌ Data quality check failed: {e}")
        raise

def get_pipeline_status() -> Dict:
    """Get status of recent pipeline runs"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Fixed to match the actual table schema
        cursor.execute("""
            SELECT 
                pipeline_name,
                status,
                records_processed,
                run_date,
                error_message
            FROM bronze.pipeline_metadata 
            WHERE run_date >= NOW() - INTERVAL '24 hours'
            ORDER BY run_date DESC
            LIMIT 10
        """)
        
        runs = []
        for row in cursor.fetchall():
            runs.append({
                'pipeline_name': row[0],  # Changed from task_id
                'status': row[1],
                'records_processed': row[2],
                'run_date': row[3],
                'error_message': row[4]
            })
        
        cursor.close()
        conn.close()
        
        return {'recent_runs': runs}
        
    except Exception as e:
        logger.error(f"Failed to get pipeline status: {e}")
        return {'recent_runs': []}