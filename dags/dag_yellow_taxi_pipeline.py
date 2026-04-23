from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

import pandas as pd
import os
import glob
import shutil
import logging

logger = logging.getLogger(__name__)

DATA_QUALITY_THRESHOLD = 80
CLEAN_DATA_PATH = '/opt/airflow/processed_data/silver'
QUARANTINE_DATA_PATH = '/opt/airflow/processed_data/quarantine'
GOLD_DIR = '/opt/airflow/processed_data/gold'

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


def get_csv_files(**context):
    archive_path = '/opt/airflow/archive/test_raw'

    if not os.path.exists(archive_path):
        logger.warning(f"Archive path {archive_path} does not exist. Creating it.")
        os.makedirs(archive_path, exist_ok=True)
        csv_files = []
    else:
        csv_files = sorted([
            f for f in os.listdir(archive_path)
            if f.startswith('yellow_tripdata') and f.endswith('.csv')
        ])

    logger.info(f"Found {len(csv_files)} CSV files in {archive_path}")

    if len(csv_files) == 0:
        logger.warning("No CSV files found! DAG will continue but no data will be processed.")

    context['task_instance'].xcom_push(key='csv_files', value=csv_files)
    context['task_instance'].xcom_push(key='total_files', value=len(csv_files))

    return f"Found {len(csv_files)} CSV files"


def validate_and_split(**context):
    archive_path = '/opt/airflow/archive/test_raw'

    csv_files = context['task_instance'].xcom_pull(
        key='csv_files',
        task_ids='get_csv_files'
    )

    if not csv_files:
        logger.warning("No CSV files to validate")
        empty_results = {
            'files_processed': 0,
            'total_records': 0,
            'clean_records': 0,
            'quarantine_records': 0,
            'details': []
        }
        context['task_instance'].xcom_push(key='validation_results', value=empty_results)
        context['task_instance'].xcom_push(key='clean_data_rows', value=0)
        context['task_instance'].xcom_push(key='quarantine_data_rows', value=0)
        return empty_results

    validation_results = {
        'files_processed': 0,
        'total_records': 0,
        'clean_records': 0,
        'quarantine_records': 0,
        'details': []
    }

    clean_temp_paths = []
    quarantine_temp_paths = []
    os.makedirs('/tmp/clean', exist_ok=True)
    os.makedirs('/tmp/quarantine', exist_ok=True)

    for csv_file in csv_files:
        csv_path = os.path.join(archive_path, csv_file)

        try:
            logger.info(f"Validating {csv_file}...")

            df = pd.read_csv(csv_path, low_memory=False)

            required_columns = [
                'tpep_pickup_datetime', 'tpep_dropoff_datetime',
                'fare_amount', 'trip_distance', 'passenger_count'
            ]
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                logger.error(f"Missing columns in {csv_file}: {missing_columns}")
                validation_results['details'].append({
                    'file': csv_file,
                    'error': f"Missing columns: {missing_columns}"
                })
                continue

            df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'], errors='coerce')
            df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'], errors='coerce')
            df['fare_amount'] = pd.to_numeric(df['fare_amount'], errors='coerce')
            df['trip_distance'] = pd.to_numeric(df['trip_distance'], errors='coerce')
            df['passenger_count'] = pd.to_numeric(df['passenger_count'], errors='coerce')

            initial_count = len(df)

            valid_mask = (
                (df['tpep_pickup_datetime'].notna()) &
                (df['tpep_dropoff_datetime'].notna()) &
                (df['fare_amount'].fillna(0) > 0) &
                (df['trip_distance'].fillna(0) > 0) &
                (df['passenger_count'].fillna(0) > 0) &
                (df['tpep_dropoff_datetime'] > df['tpep_pickup_datetime'])
            )

            clean_df = df[valid_mask].copy()
            quarantine_df = df[~valid_mask].copy()

            quarantine_df['source_file'] = csv_file

            if len(quarantine_df) > 0:
                flag_df = pd.DataFrame({
                    'Invalid pickup datetime':     quarantine_df['tpep_pickup_datetime'].isna(),
                    'Invalid dropoff datetime':    quarantine_df['tpep_dropoff_datetime'].isna(),
                    'Fare amount <= 0 or NaN':     quarantine_df['fare_amount'].isna() | (quarantine_df['fare_amount'] <= 0),
                    'Trip distance <= 0 or NaN':   quarantine_df['trip_distance'].isna() | (quarantine_df['trip_distance'] <= 0),
                    'Passenger count <= 0 or NaN': quarantine_df['passenger_count'].isna() | (quarantine_df['passenger_count'] <= 0),
                    'Dropoff before pickup':       (quarantine_df['tpep_dropoff_datetime'].notna() &
                                                    quarantine_df['tpep_pickup_datetime'].notna() &
                                                    (quarantine_df['tpep_dropoff_datetime'] <= quarantine_df['tpep_pickup_datetime'])),
                })
                quarantine_df['error_reason'] = flag_df.apply(
                    lambda row: '; '.join(col for col, v in row.items() if v) or 'Multiple validation failures',
                    axis=1
                )
                quarantine_df = pd.concat([quarantine_df, flag_df], axis=1)

            validation_results['files_processed'] += 1
            validation_results['total_records'] += initial_count
            validation_results['clean_records'] += len(clean_df)
            validation_results['quarantine_records'] += len(quarantine_df)

            clean_pct = round(len(clean_df) / initial_count * 100, 2) if initial_count > 0 else 0.0
            validation_results['details'].append({
                'file': csv_file,
                'total': initial_count,
                'clean': len(clean_df),
                'quarantine': len(quarantine_df),
                'clean_percentage': clean_pct
            })

            logger.info(
                f"✓ {csv_file}: {len(clean_df):,} clean, {len(quarantine_df):,} quarantine "
                f"({clean_pct}% clean)"
            )

            stem = csv_file.replace('.csv', '')
            clean_path = f'/tmp/clean/{stem}.parquet'
            quarantine_path = f'/tmp/quarantine/{stem}.parquet'

            clean_df.to_parquet(clean_path, compression='snappy', index=False)
            clean_temp_paths.append(clean_path)

            if len(quarantine_df) > 0:
                quarantine_df.to_parquet(quarantine_path, compression='snappy', index=False)
                quarantine_temp_paths.append(quarantine_path)

            del df, clean_df, quarantine_df

        except Exception as e:
            validation_results['details'].append({
                'file': csv_file,
                'error': str(e)
            })
            logger.error(f"✗ Error validating {csv_file}: {str(e)}")

    context['task_instance'].xcom_push(key='validation_results', value=validation_results)
    context['task_instance'].xcom_push(key='clean_data_rows', value=validation_results['clean_records'])
    context['task_instance'].xcom_push(key='quarantine_data_rows', value=validation_results['quarantine_records'])
    context['task_instance'].xcom_push(key='clean_temp_paths', value=clean_temp_paths)
    context['task_instance'].xcom_push(key='quarantine_temp_paths', value=quarantine_temp_paths)

    return validation_results


def save_clean_data(**context):
    os.makedirs(CLEAN_DATA_PATH, exist_ok=True)

    clean_temp_paths = context['task_instance'].xcom_pull(
        key='clean_temp_paths',
        task_ids='validate_and_split'
    ) or []

    if not clean_temp_paths:
        logger.warning("No clean data to save")
        context['task_instance'].xcom_push(key='clean_data_saved', value={'file': 'N/A', 'records': 0})
        return "No clean data"

    try:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        saved_files = []
        for src in clean_temp_paths:
            stem = os.path.basename(src).replace('.parquet', '')
            dest = os.path.join(CLEAN_DATA_PATH, f'{stem}_silver_{timestamp}.parquet')
            shutil.copy2(src, dest)
            saved_files.append(dest)
            os.remove(src)

        clean_records = context['task_instance'].xcom_pull(
            key='clean_data_rows', task_ids='validate_and_split'
        ) or 0
        logger.info(f"✓ Saved {clean_records:,} clean records across {len(saved_files)} files to {CLEAN_DATA_PATH}")

        context['task_instance'].xcom_push(
            key='clean_data_saved',
            value={'file': CLEAN_DATA_PATH, 'records': clean_records}
        )
        return f"Saved {clean_records:,} clean records"

    except Exception as e:
        logger.error(f"✗ Error saving clean data: {str(e)}")
        raise


def save_quarantine_data(**context):
    os.makedirs(QUARANTINE_DATA_PATH, exist_ok=True)

    quarantine_temp_paths = context['task_instance'].xcom_pull(
        key='quarantine_temp_paths',
        task_ids='validate_and_split'
    ) or []

    if not quarantine_temp_paths:
        logger.info("No quarantine data to save")
        context['task_instance'].xcom_push(key='quarantine_data_saved', value={'file': 'N/A', 'records': 0})
        return "No quarantine data"

    try:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        saved_files = []
        for src in quarantine_temp_paths:
            stem = os.path.basename(src).replace('.parquet', '')
            dest = os.path.join(QUARANTINE_DATA_PATH, f'{stem}_quarantine_{timestamp}.parquet')
            shutil.copy2(src, dest)
            saved_files.append(dest)
            os.remove(src)

        quarantine_records = context['task_instance'].xcom_pull(
            key='quarantine_data_rows', task_ids='validate_and_split'
        ) or 0
        logger.info(f"⚠ Saved {quarantine_records:,} quarantine records across {len(saved_files)} files to {QUARANTINE_DATA_PATH}")

        context['task_instance'].xcom_push(
            key='quarantine_data_saved',
            value={'file': QUARANTINE_DATA_PATH, 'records': quarantine_records}
        )
        return f"Saved {quarantine_records:,} quarantine records"

    except Exception as e:
        logger.error(f"✗ Error saving quarantine data: {str(e)}")
        raise


def create_gold_summary(**context):
    os.makedirs(GOLD_DIR, exist_ok=True)

    silver_files = sorted(glob.glob(os.path.join(CLEAN_DATA_PATH, "*.parquet")))

    if not silver_files:
        logger.info(f"[GOLD] ไม่พบไฟล์ parquet ใน {CLEAN_DATA_PATH} -- ข้ามการสร้าง Gold Layer")
        return {"status": "skipped", "reason": "silver_empty", "rows_written": 0}

    logger.info(f"[GOLD] พบไฟล์ใน Silver จำนวน {len(silver_files)} ไฟล์")

    partials = []
    for fp in silver_files:
        try:
            part = pd.read_parquet(fp, columns=[
                'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'trip_distance', 'total_amount'
            ])
            if "tpep_dropoff_datetime" in part.columns:
                part["trip_date"] = pd.to_datetime(part["tpep_dropoff_datetime"], errors="coerce").dt.date
            else:
                part["trip_date"] = pd.to_datetime(part["tpep_pickup_datetime"], errors="coerce").dt.date

            part = part.dropna(subset=["trip_date"])
            agg = (
                part.groupby("trip_date", as_index=False)
                    .agg(
                        total_trips  =("trip_date",     "count"),
                        total_revenue=("total_amount",  "sum"),
                        distance_sum =("trip_distance", "sum"),
                    )
            )
            partials.append(agg)
            del part, agg
            logger.info(f"   -> aggregated {os.path.basename(fp)}")
        except Exception as e:
            logger.warning(f"   -> [WARN] อ่านไฟล์ไม่สำเร็จ {fp}: {e}")

    if not partials:
        logger.info("[GOLD] ไม่มีไฟล์ที่อ่านได้ -- ข้าม Gold Layer")
        return {"status": "skipped", "reason": "no_readable_files", "rows_written": 0}

    combined = pd.concat(partials, ignore_index=True)
    daily_summary = (
        combined.groupby("trip_date", as_index=False)
                .agg(
                    total_trips  =("total_trips",   "sum"),
                    total_revenue=("total_revenue", "sum"),
                    distance_sum =("distance_sum",  "sum"),
                )
                .sort_values("trip_date")
                .reset_index(drop=True)
    )
    daily_summary["average_trip_distance"] = (daily_summary["distance_sum"] / daily_summary["total_trips"]).round(3)
    daily_summary["total_revenue"] = daily_summary["total_revenue"].round(2)
    daily_summary = daily_summary.drop(columns=["distance_sum"])

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(GOLD_DIR, f"daily_summary_{ts}.json")
    daily_summary.to_json(output_path, orient='records', indent=2, date_format='iso')

    logger.info(f"[GOLD] บันทึกไฟล์สำเร็จ -> {output_path}")
    logger.info(f"[GOLD] จำนวนแถว summary = {len(daily_summary):,}")

    result = {"status": "success", "rows_written": int(len(daily_summary)), "output_path": output_path}
    context['task_instance'].xcom_push(key='gold_summary', value=result)
    return result


def generate_report(**context):
    validation_results = context['task_instance'].xcom_pull(
        key='validation_results',
        task_ids='validate_and_split'
    )

    if not validation_results:
        logger.warning("No validation results found")
        return "No validation results"

    clean_saved = context['task_instance'].xcom_pull(
        key='clean_data_saved',
        task_ids='save_clean_data'
    ) or {'records': 0, 'file': 'N/A'}

    quarantine_saved = context['task_instance'].xcom_pull(
        key='quarantine_data_saved',
        task_ids='save_quarantine_data'
    ) or {'records': 0, 'file': 'N/A'}

    total_records = validation_results['total_records']
    clean_count = validation_results['clean_records']
    quarantine_count = validation_results['quarantine_records']
    clean_percentage = (clean_count / total_records * 100) if total_records > 0 else 0
    quarantine_percentage = (quarantine_count / total_records * 100) if total_records > 0 else 0

    logger.info("=" * 70)
    logger.info("DATA VALIDATION REPORT")
    logger.info("=" * 70)
    logger.info(f"Files Processed: {validation_results['files_processed']}")
    logger.info(f"Total Records: {total_records:,}")
    logger.info(f"Clean Records: {clean_count:,} ({clean_percentage:.2f}%)")
    logger.info(f"Quarantine Records: {quarantine_count:,} ({quarantine_percentage:.2f}%)")
    logger.info("")
    logger.info("File-by-File Details:")
    for detail in validation_results['details']:
        if 'error' in detail:
            logger.info(f"  ✗ {detail['file']}: ERROR - {detail['error']}")
        else:
            logger.info(
                f"  • {detail['file']}: {detail['clean']:,} clean, "
                f"{detail['quarantine']:,} quarantine ({detail['clean_percentage']:.2f}% clean)"
            )

    logger.info("")
    logger.info("Data Saved:")
    logger.info(f"  Clean Layer (Silver): {clean_saved['records']:,} records → {clean_saved.get('file', 'N/A')}")
    logger.info(f"  Quarantine Layer: {quarantine_saved['records']:,} records → {quarantine_saved.get('file', 'N/A')}")
    logger.info("=" * 70)

    if total_records == 0:
        logger.warning("⚠ No records were processed — skipping data quality check")
        context['task_instance'].xcom_push(key='alert_triggered', value=False)
    elif clean_percentage < DATA_QUALITY_THRESHOLD:
        alert_message = (
            f"⚠️ ALERT: Data Quality Below Threshold!\n"
            f"Clean Percentage: {clean_percentage:.2f}% "
            f"(Threshold: {DATA_QUALITY_THRESHOLD}%)\n"
            f"Total Records: {total_records:,}\n"
            f"Clean Records: {clean_count:,}\n"
            f"Quarantine Records: {quarantine_count:,}"
        )
        logger.warning(alert_message)
        context['task_instance'].xcom_push(key='alert_triggered', value=True)
    else:
        logger.info(f"✓ Data Quality Check Passed ({clean_percentage:.2f}% >= {DATA_QUALITY_THRESHOLD}%)")
        context['task_instance'].xcom_push(key='alert_triggered', value=False)

    return "Report generated successfully"


get_files_task = PythonOperator(
    task_id='get_csv_files',
    python_callable=get_csv_files,
    dag=dag,
)

validate_task = PythonOperator(
    task_id='validate_and_split',
    python_callable=validate_and_split,
    dag=dag,
)

save_clean_task = PythonOperator(
    task_id='save_clean_data',
    python_callable=save_clean_data,
    dag=dag,
)

save_quarantine_task = PythonOperator(
    task_id='save_quarantine_data',
    python_callable=save_quarantine_data,
    dag=dag,
)

gold_layer_task = PythonOperator(
    task_id='create_gold_summary',
    python_callable=create_gold_summary,
    dag=dag,
)

report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    dag=dag,
)

get_files_task >> validate_task
validate_task >> [save_clean_task, save_quarantine_task]
save_clean_task >> gold_layer_task
[gold_layer_task, save_quarantine_task] >> report_task
