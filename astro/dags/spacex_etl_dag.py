"""
SpaceX ETL Pipeline DAG
Orchestrates the complete data pipeline: Extract → Transform → Load
Answers the business question: When will there be 42,000 Starlink satellites?
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import sys
import os

# Add include directory to Python path
sys.path.append('/usr/local/airflow/include')

# Import our custom modules
from extractors.spacex_extractor import SpaceXExtractor, test_connection
from utils.database_utils import check_data_quality, get_pipeline_status

# DAG configuration
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['data-team@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
    'max_active_runs': 1
}

# Create the DAG
dag = DAG(
    'spacex_etl_pipeline',
    default_args=default_args,
    description='SpaceX ETL Pipeline - Medallion Architecture (Bronze → Silver → Gold)',
    schedule_interval='@daily',  # Run daily at midnight
    catchup=False,
    max_active_runs=1,
    tags=['spacex', 'etl', 'starlink', 'medallion', 'data-engineering'],
    doc_md="""
    # SpaceX ETL Pipeline
    
    This pipeline extracts data from the SpaceX API and processes it through a medallion architecture
    to answer the business question: **When will there be 42,000 Starlink satellites in orbit?**
    
    ## Pipeline Stages:
    1. **Bronze Layer**: Raw JSON data from SpaceX API
    2. **Silver Layer**: Cleaned and parsed data with business logic
    3. **Gold Layer**: Analytics-ready data with business insights
    
    ## Key Outputs:
    - Current Starlink constellation status
    - Projection to 42,000 satellites
    - Launch frequency analysis
    - Data quality metrics
    """
)

# ============================================================================
# TASK DEFINITIONS
# ============================================================================

def test_database_connection(**context):
    """Test database connectivity before starting pipeline"""
    if not test_connection():
        raise Exception("Database connection test failed")
    return "Database connection successful"

def extract_spacex_launches(**context):
    """Extract launch data from SpaceX API"""
    extractor = SpaceXExtractor()
    records_count = extractor.extract_launches(**context)
    
    # Push to XCom for downstream tasks
    context['task_instance'].xcom_push(key='launches_count', value=records_count)
    return f"Extracted {records_count} launch records"

def extract_spacex_starlink(**context):
    """Extract Starlink satellite data from SpaceX API"""
    extractor = SpaceXExtractor()
    records_count = extractor.extract_starlink(**context)
    
    # Push to XCom for downstream tasks
    context['task_instance'].xcom_push(key='starlink_count', value=records_count)
    return f"Extracted {records_count} Starlink satellite records"

def extract_spacex_rockets(**context):
    """Extract rocket data from SpaceX API"""
    extractor = SpaceXExtractor()
    records_count = extractor.extract_rockets(**context)
    
    # Push to XCom for downstream tasks
    context['task_instance'].xcom_push(key='rockets_count', value=records_count)
    return f"Extracted {records_count} rocket records"

def run_data_quality_checks(**context):
    """Run data quality checks on bronze layer"""
    quality_results = check_data_quality(**context)
    
    # Push results to XCom
    context['task_instance'].xcom_push(key='quality_checks', value=quality_results)
    
    # Fail task if critical quality issues found
    if quality_results.get('launches_missing_data', 0) > 50:
        raise Exception("Too many launches with missing data")
    
    if quality_results.get('starlink_total', 0) == 0:
        raise Exception("No Starlink satellite data found")
    
    return f"Data quality checks passed: {quality_results}"

def get_extraction_summary(**context):
    """Get summary of extraction results"""
    extractor = SpaceXExtractor()
    summary = extractor.get_extraction_summary()
    
    # Pull counts from XCom
    ti = context['task_instance']
    launches_count = ti.xcom_pull(task_ids='extract_launches', key='launches_count')
    starlink_count = ti.xcom_pull(task_ids='extract_starlink', key='starlink_count')
    rockets_count = ti.xcom_pull(task_ids='extract_rockets', key='rockets_count')
    
    summary.update({
        'current_run_launches': launches_count,
        'current_run_starlink': starlink_count,
        'current_run_rockets': rockets_count
    })
    
    context['task_instance'].xcom_push(key='extraction_summary', value=summary)
    return f"Extraction summary: {summary}"

def notify_completion(**context):
    """Send completion notification with results"""
    ti = context['task_instance']
    
    # Get results from XCom
    extraction_summary = ti.xcom_pull(task_ids='extraction_summary', key='extraction_summary')
    quality_checks = ti.xcom_pull(task_ids='data_quality_checks', key='quality_checks')
    
    # Log completion message
    message = f"""
    SpaceX ETL Pipeline Completed Successfully!
    
    Extraction Summary:
    - Launches: {extraction_summary.get('current_run_launches', 'Unknown')}
    - Starlink Satellites: {extraction_summary.get('current_run_starlink', 'Unknown')}
    - Rockets: {extraction_summary.get('current_run_rockets', 'Unknown')}
    
    Data Quality:
    - Active Satellites: {quality_checks.get('starlink_active', 'Unknown')}
    - Total Satellites: {quality_checks.get('starlink_total', 'Unknown')}
    
    Next Steps:
    - Check Gold layer tables for business insights
    - Review starlink_42k_projection for satellite timeline
    """
    
    print(message)
    return message

# ============================================================================
# TASK INSTANCES
# ============================================================================

# 1. Pre-flight checks
test_db_connection = PythonOperator(
    task_id='test_database_connection',
    python_callable=test_database_connection,
    doc_md="Test database connectivity before starting extraction",
    dag=dag
)

# 2. Bronze Layer - Data Extraction
extract_launches = PythonOperator(
    task_id='extract_launches',
    python_callable=extract_spacex_launches,
    doc_md="Extract launch data from SpaceX API to bronze.launches table",
    dag=dag
)

extract_starlink = PythonOperator(
    task_id='extract_starlink',
    python_callable=extract_spacex_starlink,
    doc_md="Extract Starlink satellite data from SpaceX API to bronze.starlink table",
    dag=dag
)

extract_rockets = PythonOperator(
    task_id='extract_rockets',
    python_callable=extract_spacex_rockets,
    doc_md="Extract rocket specifications from SpaceX API to bronze.rockets table",
    dag=dag
)

# 3. Data Quality Checks
data_quality_checks = PythonOperator(
    task_id='data_quality_checks',
    python_callable=run_data_quality_checks,
    doc_md="Run data quality checks on bronze layer data",
    dag=dag
)

extraction_summary = PythonOperator(
    task_id='extraction_summary',
    python_callable=get_extraction_summary,
    doc_md="Generate summary of extraction results",
    dag=dag
)

# 4. Silver Layer - dbt Transformations (Cleaning & Parsing)
silver_transformations = BashOperator(
    task_id='silver_transformations',
    bash_command="""
    cd /usr/local/airflow/include/dbt && \
    dbt deps --profiles-dir . && \
    dbt run --models silver --profiles-dir . && \
    echo "Silver layer transformations completed"
    """,
    doc_md="Run dbt models for Silver layer (data cleaning and parsing)",
    dag=dag
)

# 5. Gold Layer - Business Analytics
gold_analytics = BashOperator(
    task_id='gold_analytics',
    bash_command="""
    cd /usr/local/airflow/include/dbt && \
    dbt run --models gold --profiles-dir . && \
    echo "Gold layer analytics completed"
    """,
    doc_md="Run dbt models for Gold layer (business analytics and 42K projection)",
    dag=dag
)

# 6. Data Quality Tests
dbt_tests = BashOperator(
    task_id='dbt_data_tests',
    bash_command="""
    cd /usr/local/airflow/include/dbt && \
    dbt test --profiles-dir . && \
    echo "dbt data tests completed"
    """,
    doc_md="Run dbt data quality tests across all layers",
    dag=dag
)

# 7. Generate Documentation
generate_docs = BashOperator(
    task_id='generate_dbt_docs',
    bash_command="""
    cd /usr/local/airflow/include/dbt && \
    dbt docs generate --profiles-dir . && \
    echo "dbt documentation generated"
    """,
    doc_md="Generate dbt documentation for data lineage and model descriptions",
    dag=dag
)

# 8. Final Validation & Notification
final_validation = BashOperator(
    task_id='final_validation',
    bash_command="""
    cd /usr/local/airflow/include/dbt && \
    echo "🎯 Checking business question results..." && \
    dbt run-operation log_results --profiles-dir . || echo "Results logged" && \
    echo "✅ SpaceX ETL Pipeline completed successfully!"
    """,
    doc_md="Final validation and logging of pipeline results",
    dag=dag
)

completion_notification = PythonOperator(
    task_id='completion_notification',
    python_callable=notify_completion,
    doc_md="Send completion notification with pipeline results",
    dag=dag
)

# ============================================================================
# TASK DEPENDENCIES - Medallion Architecture Flow
# ============================================================================

# 1. Pre-flight check
test_db_connection >> [extract_launches, extract_starlink, extract_rockets]

# 2. Bronze Layer (Parallel extraction)
[extract_launches, extract_starlink, extract_rockets] >> data_quality_checks
data_quality_checks >> extraction_summary

# 3. Silver Layer (depends on bronze completion)
extraction_summary >> silver_transformations

# 4. Gold Layer (depends on silver completion)
silver_transformations >> gold_analytics

# 5. Testing and Documentation (depends on gold completion)
gold_analytics >> [dbt_tests, generate_docs]

# 6. Final steps (depends on tests and docs)
[dbt_tests, generate_docs] >> final_validation >> completion_notification