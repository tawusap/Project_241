import os
import re
import shutil
import logging

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pipeline.config import (
    CHUNK_SIZE, RAW_DATA_PATH,
    TMP_CLEAN, TMP_QUARANTINE,
    _ERROR_KEEP_COLS, _classify_error_folder,
)

logger = logging.getLogger(__name__)


def validate_and_split(**context):
    """
    อ่าน CSV ทีละ chunk → validate → แยกเป็น clean (silver) และ quarantine
    เขียนผ่าน ParquetWriter แบบ streaming เพื่อควบคุม memory
    """
    csv_files = context['task_instance'].xcom_pull(
        key='csv_files',
        task_ids='get_csv_files'
    )

    # --- Early exit: ไม่มีไฟล์ใหม่ ---
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

    # ล้าง /tmp ก่อนทุกครั้ง ป้องกัน schema conflict เมื่อ DAG retry
    # (ParquetWriter จะ error ถ้า schema ของ chunk ใหม่ไม่ตรงกับ writer เดิม)
    for tmp_dir in [TMP_CLEAN, TMP_QUARANTINE]:
        if os.path.exists(tmp_dir):
            shutil.rmtree(tmp_dir)
        os.makedirs(tmp_dir)

    clean_temp_paths = []

    for csv_file in csv_files:
        csv_path = os.path.join(RAW_DATA_PATH, csv_file)

        try:
            logger.info(f"Validating {csv_file}...")

            # --- Step 1: ตรวจ schema ก่อน (อ่านแค่ header ไม่โหลดข้อมูล) ---
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
            clean_path = f'{TMP_CLEAN}/{stem}.parquet'

            # ดึงปีจากชื่อไฟล์ เช่น yellow_tripdata_2019-01.csv → 2019
            # ใช้ตรวจว่า pickup year ตรงกับปีในชื่อไฟล์
            _m = re.search(r'(\d{4})-\d{2}', csv_file)
            expected_year = int(_m.group(1)) if _m else None

            # ParquetWriter เปิดค้างไว้ระหว่าง chunk loop เพื่อ streaming write
            # quarantine_writers แยก writer ต่อ error folder
            clean_writer = None
            quarantine_writers = {}
            total_count = 0
            clean_count = 0
            quarantine_count = 0

            try:
                # --- Step 2: อ่าน CSV ทีละ CHUNK_SIZE rows ---
                for chunk in pd.read_csv(csv_path, chunksize=CHUNK_SIZE, low_memory=False):

                    # แปลง type ก่อน validate — errors='coerce' ทำให้ค่าที่ parse ไม่ได้กลายเป็น NaN
                    chunk['tpep_pickup_datetime'] = pd.to_datetime(chunk['tpep_pickup_datetime'], errors='coerce')
                    chunk['tpep_dropoff_datetime'] = pd.to_datetime(chunk['tpep_dropoff_datetime'], errors='coerce')
                    chunk['fare_amount'] = pd.to_numeric(chunk['fare_amount'], errors='coerce')
                    chunk['trip_distance'] = pd.to_numeric(chunk['trip_distance'], errors='coerce')
                    chunk['passenger_count'] = pd.to_numeric(chunk['passenger_count'], errors='coerce')

                    year_ok = (
                        chunk['tpep_pickup_datetime'].dt.year == expected_year
                        if expected_year else True
                    )

                    # --- Step 3: Validation rules (vectorized boolean mask) ---
                    # ใช้ mask แทน apply(row) เพราะเร็วกว่า 10-100x บน DataFrame ขนาดใหญ่
                    valid_mask = (
                        (chunk['tpep_pickup_datetime'].notna()) &           # rule 1: pickup ต้องมีค่า
                        (chunk['tpep_dropoff_datetime'].notna()) &           # rule 2: dropoff ต้องมีค่า
                        (chunk['fare_amount'].fillna(0) > 0) &              # rule 3: fare > 0
                        (chunk['trip_distance'].fillna(0) > 0) &            # rule 4: distance > 0
                        (chunk['passenger_count'].isna() | (chunk['passenger_count'] > 0)) &  # rule 5: passenger NULL หรือ > 0
                        (chunk['tpep_dropoff_datetime'] > chunk['tpep_pickup_datetime']) &     # rule 6: dropoff หลัง pickup
                        year_ok                                             # rule 7: ปีตรงกับชื่อไฟล์
                    )

                    clean_chunk = chunk[valid_mask].copy()
                    quarantine_chunk = chunk[~valid_mask].copy()
                    quarantine_chunk['source_file'] = csv_file

                    # --- Step 4: บันทึก error_reason ให้แต่ละ quarantine row ---
                    if len(quarantine_chunk) > 0:
                        flags = {
                            'Invalid pickup datetime':   quarantine_chunk['tpep_pickup_datetime'].isna(),
                            'Invalid dropoff datetime':  quarantine_chunk['tpep_dropoff_datetime'].isna(),
                            'Fare amount <= 0 or NaN':   quarantine_chunk['fare_amount'].isna() | (quarantine_chunk['fare_amount'] <= 0),
                            'Trip distance <= 0 or NaN': quarantine_chunk['trip_distance'].isna() | (quarantine_chunk['trip_distance'] <= 0),
                            'Passenger count = 0':       quarantine_chunk['passenger_count'].notna() & (quarantine_chunk['passenger_count'] <= 0),
                            'Dropoff before pickup':     (quarantine_chunk['tpep_dropoff_datetime'].notna() &
                                                          quarantine_chunk['tpep_pickup_datetime'].notna() &
                                                          (quarantine_chunk['tpep_dropoff_datetime'] <= quarantine_chunk['tpep_pickup_datetime'])),
                            'Pickup year mismatch':      (quarantine_chunk['tpep_pickup_datetime'].notna() &
                                                          (quarantine_chunk['tpep_pickup_datetime'].dt.year != expected_year))
                                                         if expected_year else False,
                        }
                        # สร้าง flag matrix ทีเดียว แล้ว join เป็น string
                        # เร็วกว่า apply(axis=1) เพราะใช้ numpy broadcasting
                        flag_matrix = pd.DataFrame(flags).values
                        col_names = list(flags.keys())
                        quarantine_chunk['error_reason'] = [
                            '; '.join(name for name, v in zip(col_names, row) if v)
                            for row in flag_matrix
                        ]

                    total_count += len(chunk)
                    clean_count += len(clean_chunk)
                    quarantine_count += len(quarantine_chunk)

                    # --- Step 5: เขียน clean rows ลง Parquet (streaming) ---
                    if len(clean_chunk) > 0:
                        table = pa.Table.from_pandas(clean_chunk, preserve_index=False)
                        if clean_writer is None:
                            clean_writer = pq.ParquetWriter(clean_path, table.schema, compression='snappy')
                        # cast เพื่อให้ schema ตรงกันทุก chunk (บาง chunk อาจมี type drift)
                        clean_writer.write_table(table.cast(clean_writer.schema))

                    # --- Step 6: เขียน quarantine rows แยก folder ตาม error type ---
                    if len(quarantine_chunk) > 0:
                        # เขียนตรงลง folder ที่ถูกต้องทันที ไม่ต้องโหลดซ้ำใน save_quarantine_data
                        quarantine_chunk['_folder'] = quarantine_chunk['error_reason'].apply(_classify_error_folder)
                        for folder_name, group in quarantine_chunk.groupby('_folder', sort=False):
                            keep_cols = [c for c in _ERROR_KEEP_COLS[folder_name] if c in group.columns]
                            folder_tmp = f'{TMP_QUARANTINE}/{folder_name}'
                            os.makedirs(folder_tmp, exist_ok=True)
                            table = pa.Table.from_pandas(group[keep_cols], preserve_index=False)
                            if folder_name not in quarantine_writers:
                                quarantine_writers[folder_name] = pq.ParquetWriter(
                                    f'{folder_tmp}/{stem}.parquet', table.schema, compression='snappy'
                                )
                            quarantine_writers[folder_name].write_table(table.cast(quarantine_writers[folder_name].schema))

                    # คืน memory ทันที ไม่รอให้ GC เก็บ
                    del chunk, clean_chunk, quarantine_chunk

            finally:
                # ปิด writer เสมอ แม้จะเกิด error กลางทาง
                if clean_writer:
                    clean_writer.close()
                    clean_temp_paths.append(clean_path)
                for w in quarantine_writers.values():
                    w.close()

            # --- Step 7: รวบรวม stats ของไฟล์นี้ ---
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

    # --- ส่งผลลัพธ์ไปให้ task ถัดไปผ่าน XCom ---
    context['task_instance'].xcom_push(key='validation_results', value=validation_results)
    context['task_instance'].xcom_push(key='clean_data_rows', value=validation_results['clean_records'])
    context['task_instance'].xcom_push(key='quarantine_data_rows', value=validation_results['quarantine_records'])
    context['task_instance'].xcom_push(key='clean_temp_paths', value=clean_temp_paths)

    return validation_results
