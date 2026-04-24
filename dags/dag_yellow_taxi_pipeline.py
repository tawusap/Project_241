from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

import pandas as pd
import os
import glob
import shutil
import logging

logger = logging.getLogger(__name__)

CHUNK_SIZE = 100_000
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
        logger.warning(f"Raw path {archive_path} does not exist. Creating it.")
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
        context['task_instance'].xcom_push(key='clean_temp_paths', value=[])
        context['task_instance'].xcom_push(key='quarantine_temp_paths', value=[])
        return empty_results

    validation_results = {
        'files_processed': 0,
        'total_records': 0,
        'clean_records': 0,
        'quarantine_records': 0,
        'details': []
    }

    import pyarrow as pa
    import pyarrow.parquet as pq

    clean_temp_paths = []
    quarantine_temp_paths = []
    os.makedirs('/tmp/clean', exist_ok=True)
    os.makedirs('/tmp/quarantine', exist_ok=True)

    for csv_file in csv_files:
        csv_path = os.path.join(archive_path, csv_file)

        try:
            logger.info(f"Validating {csv_file}...")

            required_columns = [
                'tpep_pickup_datetime', 'tpep_dropoff_datetime',
                'fare_amount', 'trip_distance', 'passenger_count'
            ]
            header = pd.read_csv(csv_path, nrows=0)
            missing_columns = [col for col in required_columns if col not in header.columns]
            if missing_columns:
                logger.error(f"Missing columns in {csv_file}: {missing_columns}")
                validation_results['details'].append({
                    'file': csv_file,
                    'error': f"Missing columns: {missing_columns}"
                })
                continue

            stem = csv_file.replace('.csv', '')
            clean_path = f'/tmp/clean/{stem}.parquet'
            quarantine_path = f'/tmp/quarantine/{stem}.parquet'

            clean_writer = None
            quarantine_writer = None
            total_count = 0
            clean_count = 0
            quarantine_count = 0

            try:
                for chunk in pd.read_csv(csv_path, chunksize=CHUNK_SIZE, low_memory=False):
                    chunk['tpep_pickup_datetime'] = pd.to_datetime(chunk['tpep_pickup_datetime'], errors='coerce')
                    chunk['tpep_dropoff_datetime'] = pd.to_datetime(chunk['tpep_dropoff_datetime'], errors='coerce')
                    chunk['fare_amount'] = pd.to_numeric(chunk['fare_amount'], errors='coerce')
                    chunk['trip_distance'] = pd.to_numeric(chunk['trip_distance'], errors='coerce')
                    chunk['passenger_count'] = pd.to_numeric(chunk['passenger_count'], errors='coerce')

                    valid_mask = (
                        (chunk['tpep_pickup_datetime'].notna()) &
                        (chunk['tpep_dropoff_datetime'].notna()) &
                        (chunk['fare_amount'].fillna(0) > 0) &
                        (chunk['trip_distance'].fillna(0) > 0) &
                        (chunk['passenger_count'].fillna(0) > 0) &
                        (chunk['tpep_dropoff_datetime'] > chunk['tpep_pickup_datetime'])
                    )

                    clean_chunk = chunk[valid_mask].copy()
                    quarantine_chunk = chunk[~valid_mask].copy()
                    quarantine_chunk['source_file'] = csv_file

                    if len(quarantine_chunk) > 0:
                        flag_df = pd.DataFrame({
                            'Invalid pickup datetime':     quarantine_chunk['tpep_pickup_datetime'].isna(),
                            'Invalid dropoff datetime':    quarantine_chunk['tpep_dropoff_datetime'].isna(),
                            'Fare amount <= 0 or NaN':     quarantine_chunk['fare_amount'].isna() | (quarantine_chunk['fare_amount'] <= 0),
                            'Trip distance <= 0 or NaN':   quarantine_chunk['trip_distance'].isna() | (quarantine_chunk['trip_distance'] <= 0),
                            'Passenger count <= 0 or NaN': quarantine_chunk['passenger_count'].isna() | (quarantine_chunk['passenger_count'] <= 0),
                            'Dropoff before pickup':       (quarantine_chunk['tpep_dropoff_datetime'].notna() &
                                                            quarantine_chunk['tpep_pickup_datetime'].notna() &
                                                            (quarantine_chunk['tpep_dropoff_datetime'] <= quarantine_chunk['tpep_pickup_datetime'])),
                        })
                        quarantine_chunk['error_reason'] = flag_df.apply(
                            lambda row: '; '.join(col for col, v in row.items() if v) or 'Multiple validation failures',
                            axis=1
                        )

                    total_count += len(chunk)
                    clean_count += len(clean_chunk)
                    quarantine_count += len(quarantine_chunk)

                    if len(clean_chunk) > 0:
                        table = pa.Table.from_pandas(clean_chunk, preserve_index=False)
                        if clean_writer is None:
                            clean_writer = pq.ParquetWriter(clean_path, table.schema, compression='snappy')
                        clean_writer.write_table(table.cast(clean_writer.schema))

                    if len(quarantine_chunk) > 0:
                        table = pa.Table.from_pandas(quarantine_chunk, preserve_index=False)
                        if quarantine_writer is None:
                            quarantine_writer = pq.ParquetWriter(quarantine_path, table.schema, compression='snappy')
                        quarantine_writer.write_table(table.cast(quarantine_writer.schema))

                    del chunk, clean_chunk, quarantine_chunk

            finally:
                if clean_writer:
                    clean_writer.close()
                    clean_temp_paths.append(clean_path)
                if quarantine_writer:
                    quarantine_writer.close()
                    quarantine_temp_paths.append(quarantine_path)

            validation_results['files_processed'] += 1
            validation_results['total_records'] += total_count
            validation_results['clean_records'] += clean_count
            validation_results['quarantine_records'] += quarantine_count

            clean_pct = round(clean_count / total_count * 100, 2) if total_count > 0 else 0.0
            validation_results['details'].append({
                'file': csv_file,
                'total': total_count,
                'clean': clean_count,
                'quarantine': quarantine_count,
                'clean_percentage': clean_pct
            })

            logger.info(
                f"✓ {csv_file}: {clean_count:,} clean, {quarantine_count:,} quarantine "
                f"({clean_pct}% clean)"
            )

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
        context['task_instance'].xcom_push(key='silver_paths', value=[])
        context['task_instance'].xcom_push(key='clean_data_saved', value={'file': 'N/A', 'records': 0})
        return "No clean data"

    try:
        saved_files = []
        for src in clean_temp_paths:
            stem = os.path.basename(src).replace('.parquet', '')
            dest = os.path.join(CLEAN_DATA_PATH, f'{stem}_silver.parquet')
            shutil.move(src, dest)
            saved_files.append(dest)

        clean_records = context['task_instance'].xcom_pull(
            key='clean_data_rows', task_ids='validate_and_split'
        ) or 0
        logger.info(f"✓ Saved {clean_records:,} clean records across {len(saved_files)} files to {CLEAN_DATA_PATH}")

        context['task_instance'].xcom_push(key='silver_paths', value=saved_files)
        context['task_instance'].xcom_push(
            key='clean_data_saved',
            value={'file': CLEAN_DATA_PATH, 'records': clean_records}
        )
        return f"Saved {clean_records:,} clean records"

    except Exception as e:
        logger.error(f"✗ Error saving clean data: {str(e)}")
        raise


_ERROR_FOLDER_MAP = {
    'Invalid pickup datetime':     'invalid_pickup_datetime',
    'Invalid dropoff datetime':    'invalid_dropoff_datetime',
    'Fare amount <= 0 or NaN':     'invalid_fare',
    'Trip distance <= 0 or NaN':   'invalid_distance',
    'Passenger count <= 0 or NaN': 'invalid_passenger_count',
    'Dropoff before pickup':       'invalid_time_sequence',
}

_METADATA_COLS = ['source_file', 'error_reason']

_ERROR_KEEP_COLS = {
    'invalid_pickup_datetime':  _METADATA_COLS + ['tpep_pickup_datetime'],
    'invalid_dropoff_datetime': _METADATA_COLS + ['tpep_dropoff_datetime'],
    'invalid_fare':             _METADATA_COLS + ['fare_amount'],
    'invalid_distance':         _METADATA_COLS + ['trip_distance'],
    'invalid_passenger_count':  _METADATA_COLS + ['passenger_count'],
    'invalid_time_sequence':    _METADATA_COLS + ['tpep_pickup_datetime', 'tpep_dropoff_datetime'],
    'multiple_errors':          _METADATA_COLS + [
        'tpep_pickup_datetime', 'tpep_dropoff_datetime',
        'fare_amount', 'trip_distance', 'passenger_count',
    ],
}


def _classify_error_folder(error_reason: str) -> str:
    matched = [folder for label, folder in _ERROR_FOLDER_MAP.items() if label in error_reason]
    if len(matched) == 1:
        return matched[0]
    return 'multiple_errors'


def save_quarantine_data(**context):
    quarantine_temp_paths = context['task_instance'].xcom_pull(
        key='quarantine_temp_paths',
        task_ids='validate_and_split'
    ) or []

    if not quarantine_temp_paths:
        logger.info("No quarantine data to save")
        context['task_instance'].xcom_push(key='quarantine_data_saved', value={'file': 'N/A', 'records': 0})
        return "No quarantine data"

    try:
        total_saved = 0

        for src in quarantine_temp_paths:
            stem = os.path.basename(src).replace('.parquet', '')
            df = pd.read_parquet(src)

            df['_error_folder'] = df['error_reason'].apply(_classify_error_folder)

            for folder_name, group in df.groupby('_error_folder'):
                dest_dir = os.path.join(QUARANTINE_DATA_PATH, folder_name)
                os.makedirs(dest_dir, exist_ok=True)

                keep_cols = [c for c in _ERROR_KEEP_COLS[folder_name] if c in group.columns]
                dest = os.path.join(dest_dir, f'{stem}_quarantine.parquet')
                group[keep_cols].to_parquet(dest, compression='snappy', index=False)
                logger.info(f"  -> {folder_name}: {len(group):,} records, {len(keep_cols)} cols → {dest}")
                total_saved += len(group)

            os.remove(src)

        logger.info(f"⚠ Saved {total_saved:,} quarantine records to {QUARANTINE_DATA_PATH}/<error_type>/")

        context['task_instance'].xcom_push(
            key='quarantine_data_saved',
            value={'file': QUARANTINE_DATA_PATH, 'records': total_saved}
        )
        return f"Saved {total_saved:,} quarantine records"

    except Exception as e:
        logger.error(f"✗ Error saving quarantine data: {str(e)}")
        raise


def create_gold_summary(**context):
    import pyarrow.parquet as pq
    from collections import defaultdict

    os.makedirs(GOLD_DIR, exist_ok=True)

    silver_files = context['task_instance'].xcom_pull(
        key='silver_paths', task_ids='save_clean_data'
    ) or sorted(glob.glob(os.path.join(CLEAN_DATA_PATH, "*.parquet")))

    if not silver_files:
        logger.info(f"[GOLD] ไม่พบไฟล์ parquet ใน {CLEAN_DATA_PATH} -- ข้ามการสร้าง Gold Layer")
        return {"status": "skipped", "reason": "silver_empty", "rows_written": 0}

    logger.info(f"[GOLD] พบไฟล์ใน Silver จำนวน {len(silver_files)} ไฟล์")

    daily_agg = defaultdict(lambda: {'trips': 0, 'revenue': 0.0, 'dist_sum': 0.0, 'dist_count': 0})

    for fp in silver_files:
        try:
            pf = pq.ParquetFile(fp)
            schema_names = pf.schema_arrow.names

            if 'tpep_dropoff_datetime' in schema_names:
                date_col = 'tpep_dropoff_datetime'
            elif 'tpep_pickup_datetime' in schema_names:
                date_col = 'tpep_pickup_datetime'
            else:
                logger.warning(f"   -> [WARN] ไม่พบ datetime column ใน {fp} -- ข้าม")
                continue

            missing = {'total_amount', 'trip_distance'} - set(schema_names)
            if missing:
                raise ValueError(f"[GOLD] คอลัมน์จำเป็นหายไปจาก Silver: {missing}")

            row_count = 0
            for batch in pf.iter_batches(
                batch_size=CHUNK_SIZE,
                columns=[date_col, 'total_amount', 'trip_distance']
            ):
                chunk = batch.to_pandas()
                chunk[date_col] = pd.to_datetime(chunk[date_col], errors='coerce')
                chunk['trip_date'] = chunk[date_col].dt.date
                chunk = chunk.dropna(subset=['trip_date'])

                for date, grp in chunk.groupby('trip_date'):
                    daily_agg[date]['trips']      += len(grp)
                    daily_agg[date]['revenue']    += grp['total_amount'].sum()
                    daily_agg[date]['dist_sum']   += grp['trip_distance'].sum()
                    daily_agg[date]['dist_count'] += grp['trip_distance'].count()
                    row_count += len(grp)

                del chunk

            logger.info(f"   -> loaded {os.path.basename(fp)} | rows={row_count:,}")

        except Exception as e:
            logger.warning(f"   -> [WARN] อ่านไฟล์ไม่สำเร็จ {fp}: {e}")

    if not daily_agg:
        logger.info("[GOLD] ไม่มีข้อมูลที่อ่านได้ -- ข้าม Gold Layer")
        return {"status": "skipped", "reason": "no_readable_files", "rows_written": 0}

    rows = []
    for date, stats in sorted(daily_agg.items()):
        avg_dist = round(stats['dist_sum'] / stats['dist_count'], 3) if stats['dist_count'] > 0 else 0.0
        rows.append({
            'trip_date':             date,
            'total_trips':           stats['trips'],
            'total_revenue':         round(stats['revenue'], 2),
            'average_trip_distance': avg_dist,
        })
    daily_summary = pd.DataFrame(rows)

    output_path = os.path.join(GOLD_DIR, "daily_summary.parquet")
    daily_summary.to_parquet(output_path, index=False)

    logger.info(f"[GOLD] บันทึกไฟล์สำเร็จ -> {output_path}")
    logger.info(f"[GOLD] จำนวนแถว summary = {len(daily_summary):,}")

    result = {
        "status": "success",
        "rows_written": int(len(daily_summary)),
        "output_path": output_path,
    }
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
