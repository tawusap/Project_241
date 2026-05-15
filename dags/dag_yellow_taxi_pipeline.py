from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

import pandas as pd
import os
import glob
import re
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
    archive_path = '/opt/airflow/archive/raw'

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
    archive_path = '/opt/airflow/archive/raw'

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

            _m = re.search(r'(\d{4})-\d{2}', csv_file)
            expected_year = int(_m.group(1)) if _m else None

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

                    year_ok = (
                        chunk['tpep_pickup_datetime'].dt.year == expected_year
                        if expected_year else True
                    )

                    valid_mask = (
                        (chunk['tpep_pickup_datetime'].notna()) &
                        (chunk['tpep_dropoff_datetime'].notna()) &
                        (chunk['fare_amount'].fillna(0) > 0) &
                        (chunk['trip_distance'].fillna(0) > 0) &
                        (chunk['passenger_count'].isna() | (chunk['passenger_count'] > 0)) &
                        (chunk['tpep_dropoff_datetime'] > chunk['tpep_pickup_datetime']) &
                        year_ok
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
                            'Passenger count = 0':         quarantine_chunk['passenger_count'].notna() & (quarantine_chunk['passenger_count'] <= 0),
                            'Dropoff before pickup':       (quarantine_chunk['tpep_dropoff_datetime'].notna() &
                                                            quarantine_chunk['tpep_pickup_datetime'].notna() &
                                                            (quarantine_chunk['tpep_dropoff_datetime'] <= quarantine_chunk['tpep_pickup_datetime'])),
                            'Pickup year mismatch':        (quarantine_chunk['tpep_pickup_datetime'].notna() &
                                                            (quarantine_chunk['tpep_pickup_datetime'].dt.year != expected_year))
                                                           if expected_year else False,
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
    'Passenger count = 0':         'invalid_passenger_count',
    'Dropoff before pickup':       'invalid_time_sequence',
    'Pickup year mismatch':        'invalid_year',
}

_METADATA_COLS = ['source_file', 'error_reason']

_ERROR_KEEP_COLS = {
    'invalid_pickup_datetime':  _METADATA_COLS + ['tpep_pickup_datetime'],
    'invalid_dropoff_datetime': _METADATA_COLS + ['tpep_dropoff_datetime'],
    'invalid_fare':             _METADATA_COLS + ['fare_amount'],
    'invalid_distance':         _METADATA_COLS + ['trip_distance'],
    'invalid_passenger_count':  _METADATA_COLS + ['passenger_count'],
    'invalid_time_sequence':    _METADATA_COLS + ['tpep_pickup_datetime', 'tpep_dropoff_datetime'],
    'invalid_year':             _METADATA_COLS + ['tpep_pickup_datetime'],
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


def create_star_schema(**context):
    import pyarrow as pa
    import pyarrow.parquet as pq

    STAR_SCHEMA_DIR = os.path.join(GOLD_DIR, 'star_schema')
    os.makedirs(STAR_SCHEMA_DIR, exist_ok=True)

    silver_files = context['task_instance'].xcom_pull(
        key='silver_paths', task_ids='save_clean_data'
    ) or sorted(glob.glob(os.path.join(CLEAN_DATA_PATH, "*.parquet")))

    if not silver_files:
        logger.info("[STAR SCHEMA] ไม่พบไฟล์ใน Silver -- ข้ามการสร้าง Gold Layer")
        return {"status": "skipped", "reason": "silver_empty"}

    logger.info(f"[STAR SCHEMA] พบไฟล์ Silver {len(silver_files)} ไฟล์")

    # static dimension tables
    dim_time = pd.DataFrame({
        'time_key': range(24),
        'hour': range(24),
        'time_period': [
            'night' if h < 6 else 'morning' if h < 12 else 'afternoon' if h < 18 else 'evening'
            for h in range(24)
        ],
    })

    dim_vendor = pd.DataFrame([
        {'vendor_key': 1, 'vendor_id': 1, 'vendor_name': 'Creative Mobile Technologies'},
        {'vendor_key': 2, 'vendor_id': 2, 'vendor_name': 'VeriFone Inc.'},
        {'vendor_key': 0, 'vendor_id': 0, 'vendor_name': 'Unknown'},
    ])

    dim_payment_type = pd.DataFrame([
        {'payment_key': 1, 'payment_type_code': 1, 'payment_description': 'Credit card'},
        {'payment_key': 2, 'payment_type_code': 2, 'payment_description': 'Cash'},
        {'payment_key': 3, 'payment_type_code': 3, 'payment_description': 'No charge'},
        {'payment_key': 4, 'payment_type_code': 4, 'payment_description': 'Dispute'},
        {'payment_key': 5, 'payment_type_code': 5, 'payment_description': 'Unknown'},
        {'payment_key': 6, 'payment_type_code': 6, 'payment_description': 'Voided trip'},
        {'payment_key': 0, 'payment_type_code': 0, 'payment_description': 'Not recorded'},
    ])

    dim_rate_code = pd.DataFrame([
        {'rate_code_key': 1, 'rate_code_id': 1, 'rate_code_description': 'Standard rate'},
        {'rate_code_key': 2, 'rate_code_id': 2, 'rate_code_description': 'JFK'},
        {'rate_code_key': 3, 'rate_code_id': 3, 'rate_code_description': 'Newark'},
        {'rate_code_key': 4, 'rate_code_id': 4, 'rate_code_description': 'Nassau or Westchester'},
        {'rate_code_key': 5, 'rate_code_id': 5, 'rate_code_description': 'Negotiated fare'},
        {'rate_code_key': 6, 'rate_code_id': 6, 'rate_code_description': 'Group ride'},
        {'rate_code_key': 0, 'rate_code_id': 0, 'rate_code_description': 'Not recorded'},
    ])

    # Pass 1: scan only pickup datetime column to build dim_date (memory-light)
    logger.info("[STAR SCHEMA] Pass 1: สร้าง dim_date...")
    unique_dates = set()
    for fp in silver_files:
        try:
            pf = pq.ParquetFile(fp)
            if 'tpep_pickup_datetime' not in pf.schema_arrow.names:
                continue
            for batch in pf.iter_batches(batch_size=CHUNK_SIZE, columns=['tpep_pickup_datetime']):
                s = pd.to_datetime(batch.column('tpep_pickup_datetime').to_pandas(), errors='coerce')
                unique_dates.update(s.dt.date.dropna().unique())
            del pf
        except Exception as e:
            logger.warning(f"  -> [WARN] Pass 1 อ่าน {fp} ไม่สำเร็จ: {e}")

    if not unique_dates:
        logger.info("[STAR SCHEMA] ไม่มีข้อมูล datetime -- ข้าม")
        return {"status": "skipped", "reason": "no_readable_files"}

    dim_date_rows = []
    for d in sorted(unique_dates):
        dt = pd.Timestamp(d)
        dim_date_rows.append({
            'date_key':    int(dt.strftime('%Y%m%d')),
            'full_date':   d,
            'year':        dt.year,
            'month':       dt.month,
            'day':         dt.day,
            'day_of_week': dt.dayofweek,
            'day_name':    dt.day_name(),
            'month_name':  dt.month_name(),
            'quarter':     dt.quarter,
            'is_weekend':  dt.dayofweek >= 5,
        })
    dim_date = pd.DataFrame(dim_date_rows)
    logger.info(f"  -> dim_date: {len(dim_date):,} unique dates")

    # save dimension tables (all small, safe to hold in memory)
    dim_tables = {
        'dim_date':         dim_date,
        'dim_time':         dim_time,
        'dim_vendor':       dim_vendor,
        'dim_payment_type': dim_payment_type,
        'dim_rate_code':    dim_rate_code,
    }
    row_counts = {}
    for name, tbl in dim_tables.items():
        out = os.path.join(STAR_SCHEMA_DIR, f'{name}.parquet')
        tbl.to_parquet(out, compression='snappy', index=False)
        row_counts[name] = len(tbl)
        logger.info(f"  [GOLD] {name}.parquet | rows={len(tbl):,}")

    # FK lookup maps
    date_map = dict(zip(dim_date['full_date'], dim_date['date_key']))
    vendor_map = dict(zip(dim_vendor['vendor_id'], dim_vendor['vendor_key']))
    payment_map = dict(zip(dim_payment_type['payment_type_code'], dim_payment_type['payment_key']))
    rate_map = dict(zip(dim_rate_code['rate_code_id'], dim_rate_code['rate_code_key']))

    # Pass 2: build fact_trips streaming — one batch in memory at a time
    logger.info("[STAR SCHEMA] Pass 2: สร้าง fact_trips แบบ streaming...")
    MEASURE_COLS = (
        'passenger_count', 'trip_distance', 'fare_amount', 'extra', 'mta_tax',
        'tip_amount', 'tolls_amount', 'improvement_surcharge', 'congestion_surcharge', 'total_amount',
    )
    fact_path = os.path.join(STAR_SCHEMA_DIR, 'fact_trips.parquet')
    fact_writer = None
    trip_id_counter = 0
    total_fact_rows = 0

    try:
        for fp in silver_files:
            try:
                pf = pq.ParquetFile(fp)
                schema_names = set(pf.schema_arrow.names)
                file_rows = 0

                for batch in pf.iter_batches(batch_size=CHUNK_SIZE):
                    chunk = batch.to_pandas()
                    chunk['tpep_pickup_datetime'] = pd.to_datetime(chunk['tpep_pickup_datetime'], errors='coerce')
                    chunk['tpep_dropoff_datetime'] = pd.to_datetime(chunk['tpep_dropoff_datetime'], errors='coerce')
                    n = len(chunk)

                    fact = pd.DataFrame()
                    fact['trip_id']  = range(trip_id_counter, trip_id_counter + n)
                    fact['date_key'] = chunk['tpep_pickup_datetime'].dt.date.map(date_map).fillna(0).astype(int)
                    fact['time_key'] = chunk['tpep_pickup_datetime'].dt.hour.fillna(0).astype(int)

                    if 'VendorID' in schema_names:
                        fact['vendor_key'] = pd.to_numeric(chunk['VendorID'], errors='coerce').fillna(0).astype(int).map(vendor_map).fillna(0).astype(int)
                    else:
                        fact['vendor_key'] = 0

                    if 'payment_type' in schema_names:
                        fact['payment_key'] = pd.to_numeric(chunk['payment_type'], errors='coerce').fillna(0).astype(int).map(payment_map).fillna(0).astype(int)
                    else:
                        fact['payment_key'] = 0

                    if 'RatecodeID' in schema_names:
                        fact['rate_code_key'] = pd.to_numeric(chunk['RatecodeID'], errors='coerce').fillna(0).astype(int).map(rate_map).fillna(0).astype(int)
                    else:
                        fact['rate_code_key'] = 0

                    for loc_col in ('PULocationID', 'DOLocationID'):
                        fact[loc_col] = pd.to_numeric(chunk[loc_col], errors='coerce').fillna(0).astype(int) if loc_col in schema_names else 0

                    for measure in MEASURE_COLS:
                        fact[measure] = pd.to_numeric(chunk[measure], errors='coerce').fillna(0.0) if measure in schema_names else 0.0

                    fact['trip_duration_minutes'] = (
                        (chunk['tpep_dropoff_datetime'] - chunk['tpep_pickup_datetime'])
                        .dt.total_seconds().div(60).clip(lower=0).fillna(0.0).round(2)
                    )

                    table = pa.Table.from_pandas(fact, preserve_index=False)
                    if fact_writer is None:
                        fact_writer = pq.ParquetWriter(fact_path, table.schema, compression='snappy')
                    fact_writer.write_table(table.cast(fact_writer.schema))

                    trip_id_counter += n
                    file_rows += n
                    total_fact_rows += n
                    del chunk, fact, table

                logger.info(f"  -> {os.path.basename(fp)} | rows={file_rows:,}")

            except Exception as e:
                logger.warning(f"  -> [WARN] Pass 2 อ่าน {fp} ไม่สำเร็จ: {e}")

    finally:
        if fact_writer:
            fact_writer.close()

    row_counts['fact_trips'] = total_fact_rows
    logger.info(f"  [GOLD] fact_trips.parquet | rows={total_fact_rows:,}")
    logger.info(f"[STAR SCHEMA] บันทึก {len(row_counts)} ตารางสำเร็จ -> {STAR_SCHEMA_DIR}")

    result = {"status": "success", "tables": row_counts, "output_dir": STAR_SCHEMA_DIR}
    context['task_instance'].xcom_push(key='star_schema_result', value=result)
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

    star_result = context['task_instance'].xcom_pull(
        key='star_schema_result', task_ids='create_star_schema'
    ) or {}
    if star_result.get('status') == 'success':
        logger.info("")
        logger.info("Gold Layer (Star Schema):")
        for tbl, cnt in star_result.get('tables', {}).items():
            logger.info(f"  {tbl}: {cnt:,} rows")
        logger.info(f"  Output: {star_result.get('output_dir', 'N/A')}")
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

star_schema_task = PythonOperator(
    task_id='create_star_schema',
    python_callable=create_star_schema,
    dag=dag,
)

report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    dag=dag,
)

get_files_task >> validate_task
validate_task >> [save_clean_task, save_quarantine_task]
save_clean_task >> star_schema_task
[star_schema_task, save_quarantine_task] >> report_task
