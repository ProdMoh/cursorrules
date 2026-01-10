import logging
from datetime import datetime, timedelta
from typing import Dict, Any

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.exceptions import AirflowException

# Configure logging
logger = logging.getLogger(__name__)

# Default arguments for the DAG
# Production best practice: Use exponential backoff to prevent hammering APIs/DBs during outages
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
}

@dag(
    dag_id='etl_data_pipeline',
    default_args=DEFAULT_ARGS,
    description='Extract data from source systems, apply transformations, and load into the data warehouse.',
    schedule='@daily',
    start_date=datetime(2026, 1, 10),
    catchup=False,
    tags=['etl', 'production', 'data-warehouse'],
    doc_md="""
    ### ETL Data Pipeline
    This DAG performs a standard Extract-Transform-Load (ETL) process.
    
    **Idempotency Strategy:**
    - Tasks use `logical_date` (ds) to partition data.
    - The Load task performs an 'overwrite' on the specific partition to ensure re-runs don't duplicate data.
    
    **Late Data Handling:**
    - Uses `data_interval_start` to define the exact window of data being processed.
    """
)
def etl_data_pipeline():

    @task(task_id='extract_from_source')
    def extract_from_source(**kwargs) -> Dict[str, Any]:
        """
        Extracts raw data for the specific execution date.
        Uses logical_date to ensure idempotency.
        """
        context = get_current_context()
        ds = context['ds']
        data_interval_start = context['data_interval_start']
        
        logger.info(f"Extracting data for partition: {ds}")
        logger.info(f"Data window start: {data_interval_start}")
        
        # Simulation of extraction logic
        try:
            # In production, this would be a Hook call (e.g., S3Hook, PostgresHook)
            # Example: data = s3_hook.read_key(f'raw/source/{ds}/data.json')
            extracted_data = {"status": "success", "records_count": 1500, "partition": ds}
            
            if not extracted_data:
                raise ValueError(f"No data found for {ds}")
                
            return extracted_data
        except Exception as e:
            logger.error(f"Extraction failed: {str(e)}")
            raise AirflowException(f"Critical failure in extraction: {e}")

    @task(task_id='transform_data')
    def transform_data(raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Applies cleaning, aggregations, and joins.
        """
        logger.info(f"Transforming {raw_data['records_count']} records.")
        
        # Simulation of transformation logic
        # In production, this might involve Pandas, Spark, or SQL transformations
        transformed_data = {
            "status": "transformed",
            "processed_at": datetime.utcnow().isoformat(),
            "partition": raw_data['partition'],
            "metrics": {"sum_value": 10000, "avg_value": 6.6}
        }
        
        return transformed_data

    @task(task_id='load_to_dw')
    def load_to_dw(transformed_data: Dict[str, Any]):
        """
        Loads data into the Data Warehouse.
        Implements an overwrite-partition pattern for idempotency.
        """
        context = get_current_context()
        ds = context['ds']
        
        logger.info(f"Loading data into DW for partition {ds}")
        
        # IDEMPOTENCY STEP: 
        # In a real DW (BigQuery/Snowflake), you would use a MERGE statement 
        # or DELETE FROM table WHERE partition_date = '{{ ds }}' followed by INSERT.
        sql_cleanup = f"DELETE FROM analytics.fact_table WHERE report_date = '{ds}'"
        logger.info(f"Executing cleanup: {sql_cleanup}")
        
        # LOAD STEP:
        logger.info(f"Inserting transformed records for {ds}")
        
        return True

    @task(task_id='quality_check')
    def quality_check():
        """
        Final validation to ensure data integrity after load.
        """
        context = get_current_context()
        ds = context['ds']
        
        logger.info(f"Running post-load quality checks for {ds}")
        # Example: Check for nulls in primary keys or row count mismatches
        # In production, use Great Expectations or custom SQL check operators
        
        check_passed = True
        if not check_passed:
            raise AirflowException("Data quality check failed: Null values detected in PK.")
            
        logger.info("Quality check passed successfully.")

    # Define Task Dependencies
    raw_data = extract_from_source()
    transformed_data = transform_data(raw_data)
    load_complete = load_to_dw(transformed_data)
    
    # Ensure quality check runs after load
    load_complete >> quality_check()

# Instantiate the DAG
etl_pipeline_dag = etl_data_pipeline()