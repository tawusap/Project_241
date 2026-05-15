from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from pipeline.validation import validate_and_split
from pipeline.storage import save_clean_data, save_quarantine_data
from pipeline.star_schema import create_star_schema  # noqa: F401 (re-exported for tests)
from pipeline.reporting import generate_report
from pipeline.config import CLEAN_DATA_PATH, RAW_DATA_PATH

import os
import logging

logger = logging.getLogger(__name__)

# =============================================================================
# DAG DEFINITION
# Runs weekly; catchup=False means only the most-recent interval fires on start.
# =============================================================================
default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'csv_to_parquet_converter',
    default_args=default_args,
    description='Convert taxi data from CSV to Parquet format',
    schedule='@weekly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['data_conversion', 'csv', 'parquet'],
)


# =============================================================================
# TASK 1 — GET CSV FILES
# Scans RAW_DATA_PATH for new yellow_tripdata_*.csv files.
# Skips files that already have a corresponding silver Parquet output.
# Pushes: csv_files (new), skipped_files, total_files  →  XCom
# =============================================================================
def get_csv_files(**context):
    if not os.path.exists(RAW_DATA_PATH):
        logger.warning(f"Raw path {RAW_DATA_PATH} does not exist. Creating it.")
        os.makedirs(RAW_DATA_PATH, exist_ok=True)
        all_csv = []
    else:
        all_csv = sorted([
            f for f in os.listdir(RAW_DATA_PATH)
            if f.startswith('yellow_tripdata') and f.endswith('.csv')
        ])

    new_files = []
    skipped_files = []
    for f in all_csv:
        stem = f.replace('.csv', '')
        silver_path = os.path.join(CLEAN_DATA_PATH, f'{stem}_silver.parquet')
        if os.path.exists(silver_path):
            skipped_files.append(f)
        else:
            new_files.append(f)

    logger.info(f"Found {len(all_csv)} CSV files: {len(new_files)} new, {len(skipped_files)} already processed")

    if len(new_files) == 0 and len(skipped_files) == 0:
        logger.warning("No CSV files found! DAG will continue but no data will be processed.")

    context['task_instance'].xcom_push(key='csv_files', value=new_files)
    context['task_instance'].xcom_push(key='skipped_files', value=skipped_files)
    context['task_instance'].xcom_push(key='total_files', value=len(all_csv))

    return f"Found {len(all_csv)} CSV files: {len(new_files)} new, {len(skipped_files)} skipped"


# =============================================================================
# TASK DEFINITIONS & PIPELINE DEPENDENCIES
#
#   get_csv_files
#       └── validate_and_split
#               ├── save_clean_data ──── create_star_schema ──┐
#               └── save_quarantine_data ─────────────────────┴── generate_report
# =============================================================================
get_files_task = PythonOperator(
    task_id='get_csv_files',
    python_callable=get_csv_files,
    execution_timeout=timedelta(minutes=10),
    dag=dag,
)

validate_task = PythonOperator(
    task_id='validate_and_split',
    python_callable=validate_and_split,
    execution_timeout=timedelta(hours=3),
    dag=dag,
)

save_clean_task = PythonOperator(
    task_id='save_clean_data',
    python_callable=save_clean_data,
    execution_timeout=timedelta(minutes=30),
    dag=dag,
)

save_quarantine_task = PythonOperator(
    task_id='save_quarantine_data',
    python_callable=save_quarantine_data,
    execution_timeout=timedelta(minutes=30),
    dag=dag,
)

star_schema_task = PythonOperator(
    task_id='create_star_schema',
    python_callable=create_star_schema,
    execution_timeout=timedelta(hours=2),
    dag=dag,
)

report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    execution_timeout=timedelta(minutes=10),
    dag=dag,
)

get_files_task >> validate_task
validate_task >> [save_clean_task, save_quarantine_task]
save_clean_task >> star_schema_task
[star_schema_task, save_quarantine_task] >> report_task
