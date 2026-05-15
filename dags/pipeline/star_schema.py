import os
import logging

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pipeline.config import CHUNK_SIZE, GOLD_DIR

logger = logging.getLogger(__name__)

# =============================================================================
# STATIC DIMENSION TABLES
# ค่าเหล่านี้มาจาก NYC TLC Data Dictionary — ไม่เปลี่ยนแปลงตามข้อมูล
# =============================================================================
_DIM_TIME = pd.DataFrame({
    'time_key': range(24),
    'hour': range(24),
    'time_period': [
        'night' if h < 6 else 'morning' if h < 12 else 'afternoon' if h < 18 else 'evening'
        for h in range(24)
    ],
})

_DIM_VENDOR = pd.DataFrame([
    {'vendor_key': 1, 'vendor_id': 1, 'vendor_name': 'Creative Mobile Technologies'},
    {'vendor_key': 2, 'vendor_id': 2, 'vendor_name': 'VeriFone Inc.'},
    {'vendor_key': 0, 'vendor_id': 0, 'vendor_name': 'Unknown'},
])

_DIM_PAYMENT_TYPE = pd.DataFrame([
    {'payment_key': 1, 'payment_type_code': 1, 'payment_description': 'Credit card'},
    {'payment_key': 2, 'payment_type_code': 2, 'payment_description': 'Cash'},
    {'payment_key': 3, 'payment_type_code': 3, 'payment_description': 'No charge'},
    {'payment_key': 4, 'payment_type_code': 4, 'payment_description': 'Dispute'},
    {'payment_key': 5, 'payment_type_code': 5, 'payment_description': 'Unknown'},
    {'payment_key': 6, 'payment_type_code': 6, 'payment_description': 'Voided trip'},
    {'payment_key': 0, 'payment_type_code': 0, 'payment_description': 'Not recorded'},
])

_DIM_RATE_CODE = pd.DataFrame([
    {'rate_code_key': 1, 'rate_code_id': 1, 'rate_code_description': 'Standard rate'},
    {'rate_code_key': 2, 'rate_code_id': 2, 'rate_code_description': 'JFK'},
    {'rate_code_key': 3, 'rate_code_id': 3, 'rate_code_description': 'Newark'},
    {'rate_code_key': 4, 'rate_code_id': 4, 'rate_code_description': 'Nassau or Westchester'},
    {'rate_code_key': 5, 'rate_code_id': 5, 'rate_code_description': 'Negotiated fare'},
    {'rate_code_key': 6, 'rate_code_id': 6, 'rate_code_description': 'Group ride'},
    {'rate_code_key': 0, 'rate_code_id': 0, 'rate_code_description': 'Not recorded'},
])

# columns ที่เป็น measure ใน fact_trips
_MEASURE_COLS = (
    'passenger_count', 'trip_distance', 'fare_amount', 'extra', 'mta_tax',
    'tip_amount', 'tolls_amount', 'improvement_surcharge', 'congestion_surcharge', 'total_amount',
)


def _build_dim_date(silver_files: list) -> pd.DataFrame:
    """
    Pass 1 — อ่านแค่ column tpep_pickup_datetime จากทุก silver file
    เพื่อรวบรวม unique dates สำหรับสร้าง dim_date

    เหตุผลที่ใช้ 2 pass: dim_date ต้องพร้อมก่อน เพื่อสร้าง date_map
    ที่ใช้ lookup date_key ใน fact_trips (pass 2)
    """
    unique_dates = set()
    for fp in silver_files:
        try:
            pf = pq.ParquetFile(fp)
            if 'tpep_pickup_datetime' not in pf.schema_arrow.names:
                continue
            # iter_batches + columns=['...'] อ่านเฉพาะ column เดียว ประหยัด memory มาก
            for batch in pf.iter_batches(batch_size=CHUNK_SIZE, columns=['tpep_pickup_datetime']):
                s = pd.to_datetime(batch.column('tpep_pickup_datetime').to_pandas(), errors='coerce')
                unique_dates.update(s.dt.date.dropna().unique())
            del pf
        except Exception as e:
            logger.warning(f"  -> [WARN] Pass 1 อ่าน {fp} ไม่สำเร็จ: {e}")

    # สร้าง dim_date rows จาก unique dates ที่รวบรวมได้
    rows = []
    for d in sorted(unique_dates):
        dt = pd.Timestamp(d)
        rows.append({
            'date_key':    int(dt.strftime('%Y%m%d')),  # surrogate key แบบ YYYYMMDD
            'full_date':   d,
            'year':        dt.year,
            'month':       dt.month,
            'day':         dt.day,
            'day_of_week': dt.dayofweek,                # 0=จันทร์, 6=อาทิตย์
            'day_name':    dt.day_name(),
            'month_name':  dt.month_name(),
            'quarter':     dt.quarter,
            'is_weekend':  dt.dayofweek >= 5,
        })
    return pd.DataFrame(rows)


def _build_fact_trips(silver_files: list, fact_path: str, date_map: dict,
                      vendor_map: dict, payment_map: dict, rate_map: dict) -> int:
    """
    Pass 2 — อ่าน silver ทีละ batch และเขียน fact_trips ผ่าน ParquetWriter (streaming)
    ไม่โหลดข้อมูลทั้งหมดเข้า RAM พร้อมกัน

    FK mapping ใช้ dict.map() แทน merge เพราะเร็วกว่ามากสำหรับ lookup ง่ายๆ
    ค่าที่หา FK ไม่เจอ (เช่น VendorID ที่ไม่รู้จัก) จะถูก fillna(0) → Unknown
    """
    fact_writer = None
    trip_id_counter = 0  # surrogate key ที่เพิ่มต่อเนื่องข้ามทุก silver file
    total_rows = 0

    try:
        for fp in silver_files:
            try:
                pf = pq.ParquetFile(fp)
                schema_names = set(pf.schema_arrow.names)  # columns ที่มีในไฟล์นี้
                file_rows = 0

                for batch in pf.iter_batches(batch_size=CHUNK_SIZE):
                    chunk = batch.to_pandas()
                    chunk['tpep_pickup_datetime'] = pd.to_datetime(chunk['tpep_pickup_datetime'], errors='coerce')
                    chunk['tpep_dropoff_datetime'] = pd.to_datetime(chunk['tpep_dropoff_datetime'], errors='coerce')
                    n = len(chunk)

                    # --- สร้าง fact DataFrame ทีละ batch ---
                    fact = pd.DataFrame()
                    fact['trip_id']  = range(trip_id_counter, trip_id_counter + n)
                    fact['date_key'] = chunk['tpep_pickup_datetime'].dt.date.map(date_map).fillna(0).astype(int)
                    fact['time_key'] = chunk['tpep_pickup_datetime'].dt.hour.fillna(0).astype(int)

                    # FK columns — graceful fallback ถ้า column ไม่มีในไฟล์
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

                    # Measure columns — ค่าที่ขาดหายให้เป็น 0.0 แทน NaN
                    for measure in _MEASURE_COLS:
                        fact[measure] = pd.to_numeric(chunk[measure], errors='coerce').fillna(0.0) if measure in schema_names else 0.0

                    # คำนวณ trip_duration จาก dropoff - pickup (หน่วย: นาที)
                    fact['trip_duration_minutes'] = (
                        (chunk['tpep_dropoff_datetime'] - chunk['tpep_pickup_datetime'])
                        .dt.total_seconds().div(60).clip(lower=0).fillna(0.0).round(2)
                    )

                    # เขียน batch ลง ParquetWriter (streaming — ไม่สะสม memory)
                    table = pa.Table.from_pandas(fact, preserve_index=False)
                    if fact_writer is None:
                        fact_writer = pq.ParquetWriter(fact_path, table.schema, compression='snappy')
                    fact_writer.write_table(table.cast(fact_writer.schema))

                    trip_id_counter += n
                    file_rows += n
                    total_rows += n
                    del chunk, fact, table

                logger.info(f"  -> {os.path.basename(fp)} | rows={file_rows:,}")

            except Exception as e:
                logger.warning(f"  -> [WARN] Pass 2 อ่าน {fp} ไม่สำเร็จ: {e}")

    finally:
        if fact_writer:
            fact_writer.close()

    return total_rows


def create_star_schema(**context):
    """
    สร้าง Star Schema (Gold layer) จาก silver Parquet files
    ใช้ 2-pass streaming เพื่อหลีกเลี่ยง OOM บน dataset ขนาดใหญ่
    """
    STAR_SCHEMA_DIR = os.path.join(GOLD_DIR, 'star_schema')
    os.makedirs(STAR_SCHEMA_DIR, exist_ok=True)

    silver_files = context['task_instance'].xcom_pull(
        key='silver_paths', task_ids='save_clean_data'
    )

    if not silver_files:
        logger.info("[STAR SCHEMA] ไม่พบไฟล์ใน Silver -- ข้ามการสร้าง Gold Layer")
        return {"status": "skipped", "reason": "silver_empty"}

    logger.info(f"[STAR SCHEMA] พบไฟล์ Silver {len(silver_files)} ไฟล์")

    # --- Pass 1: สร้าง dim_date ---
    logger.info("[STAR SCHEMA] Pass 1: สร้าง dim_date...")
    dim_date = _build_dim_date(silver_files)

    if dim_date.empty:
        logger.info("[STAR SCHEMA] ไม่มีข้อมูล datetime -- ข้าม")
        return {"status": "skipped", "reason": "no_readable_files"}

    logger.info(f"  -> dim_date: {len(dim_date):,} unique dates")

    # บันทึก dimension tables ทั้งหมด (เล็กพอโหลดทั้งหมดใน memory)
    dim_tables = {
        'dim_date':         dim_date,
        'dim_time':         _DIM_TIME,
        'dim_vendor':       _DIM_VENDOR,
        'dim_payment_type': _DIM_PAYMENT_TYPE,
        'dim_rate_code':    _DIM_RATE_CODE,
    }
    row_counts = {}
    for name, tbl in dim_tables.items():
        out = os.path.join(STAR_SCHEMA_DIR, f'{name}.parquet')
        tbl.to_parquet(out, compression='snappy', index=False)
        row_counts[name] = len(tbl)
        logger.info(f"  [GOLD] {name}.parquet | rows={len(tbl):,}")

    # สร้าง FK lookup maps (dict lookup เร็วกว่า DataFrame merge)
    date_map    = dict(zip(dim_date['full_date'],                   dim_date['date_key']))
    vendor_map  = dict(zip(_DIM_VENDOR['vendor_id'],                _DIM_VENDOR['vendor_key']))
    payment_map = dict(zip(_DIM_PAYMENT_TYPE['payment_type_code'],  _DIM_PAYMENT_TYPE['payment_key']))
    rate_map    = dict(zip(_DIM_RATE_CODE['rate_code_id'],          _DIM_RATE_CODE['rate_code_key']))

    # --- Pass 2: สร้าง fact_trips ---
    logger.info("[STAR SCHEMA] Pass 2: สร้าง fact_trips แบบ streaming...")
    fact_path = os.path.join(STAR_SCHEMA_DIR, 'fact_trips.parquet')
    total_fact_rows = _build_fact_trips(
        silver_files, fact_path, date_map, vendor_map, payment_map, rate_map
    )

    row_counts['fact_trips'] = total_fact_rows
    logger.info(f"  [GOLD] fact_trips.parquet | rows={total_fact_rows:,}")
    logger.info(f"[STAR SCHEMA] บันทึก {len(row_counts)} ตารางสำเร็จ -> {STAR_SCHEMA_DIR}")

    result = {"status": "success", "tables": row_counts, "output_dir": STAR_SCHEMA_DIR}
    context['task_instance'].xcom_push(key='star_schema_result', value=result)
    return result
