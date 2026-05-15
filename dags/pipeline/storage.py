import os
import shutil
import logging

import pyarrow.parquet as pq

from pipeline.config import CLEAN_DATA_PATH, QUARANTINE_DATA_PATH, TMP_QUARANTINE

logger = logging.getLogger(__name__)


def save_clean_data(**context):
    """
    ย้าย clean Parquet จาก TMP_CLEAN → silver layer
    และรวม path ของ silver ทั้งหมด (เดิม + ใหม่) ให้ Gold layer ใช้ rebuild
    """
    os.makedirs(CLEAN_DATA_PATH, exist_ok=True)

    clean_temp_paths = context['task_instance'].xcom_pull(
        key='clean_temp_paths',
        task_ids='validate_and_split'
    ) or []

    # รวม silver ที่มีอยู่แล้วเสมอ ไม่ว่าจะมีไฟล์ใหม่หรือไม่
    # เพราะ Gold layer ต้อง rebuild จากทุกไฟล์ ไม่ใช่แค่ไฟล์ใหม่
    existing_silver = sorted([
        os.path.join(CLEAN_DATA_PATH, f)
        for f in os.listdir(CLEAN_DATA_PATH)
        if f.endswith('.parquet')
    ]) if os.path.exists(CLEAN_DATA_PATH) else []

    # --- กรณีไม่มีไฟล์ใหม่: ส่ง silver เดิมไปให้ Gold layer ---
    if not clean_temp_paths:
        logger.info(f"No new files to save. Passing {len(existing_silver)} existing silver files to Gold layer.")
        context['task_instance'].xcom_push(key='silver_paths', value=existing_silver)
        context['task_instance'].xcom_push(key='clean_data_saved', value={'file': 'N/A', 'records': 0})
        return "No new clean data"

    try:
        # ย้ายจาก /tmp/clean/<stem>.parquet → silver/<stem>_silver.parquet
        saved_files = []
        for src in clean_temp_paths:
            stem = os.path.basename(src).replace('.parquet', '')
            dest = os.path.join(CLEAN_DATA_PATH, f'{stem}_silver.parquet')
            shutil.move(src, dest)
            saved_files.append(dest)

        clean_records = context['task_instance'].xcom_pull(
            key='clean_data_rows', task_ids='validate_and_split'
        ) or 0
        logger.info(f"✓ Saved {clean_records:,} clean records across {len(saved_files)} new files to {CLEAN_DATA_PATH}")

        # push ทั้งหมด: silver เดิม + ใหม่ เพื่อให้ Gold layer rebuild จากทุกไฟล์
        all_silver = sorted(set(existing_silver) | set(saved_files))
        logger.info(f"  Total silver files for Gold layer: {len(all_silver)}")
        context['task_instance'].xcom_push(key='silver_paths', value=all_silver)
        context['task_instance'].xcom_push(
            key='clean_data_saved',
            value={'file': CLEAN_DATA_PATH, 'records': clean_records}
        )
        return f"Saved {clean_records:,} clean records"

    except Exception as e:
        logger.error(f"✗ Error saving clean data: {str(e)}")
        raise


def save_quarantine_data(**context):
    """
    ย้าย quarantine Parquet จาก TMP_QUARANTINE/<error_type>/ → QUARANTINE_DATA_PATH/<error_type>/
    validate_and_split เขียนแยก folder ไว้แล้ว task นี้แค่ย้ายไฟล์ — ไม่โหลด Parquet ซ้ำ
    """
    tmp_quarantine = TMP_QUARANTINE

    # --- กรณีไม่มี quarantine data ---
    if not os.path.exists(tmp_quarantine) or not os.listdir(tmp_quarantine):
        logger.info("No quarantine data to save")
        context['task_instance'].xcom_push(key='quarantine_data_saved', value={'file': 'N/A', 'records': 0})
        return "No quarantine data"

    try:
        total_saved = 0

        # วน loop ตาม error folder เช่น invalid_fare, invalid_distance, ...
        for folder_name in os.listdir(tmp_quarantine):
            src_dir = os.path.join(tmp_quarantine, folder_name)
            if not os.path.isdir(src_dir):
                continue

            dest_dir = os.path.join(QUARANTINE_DATA_PATH, folder_name)
            os.makedirs(dest_dir, exist_ok=True)

            for filename in os.listdir(src_dir):
                src_file  = os.path.join(src_dir, filename)
                dest_file = os.path.join(dest_dir, filename.replace('.parquet', '_quarantine.parquet'))
                # อ่านแค่ metadata (row count) ไม่โหลดข้อมูลจริง — เร็วมาก
                row_count = pq.read_metadata(src_file).num_rows
                shutil.move(src_file, dest_file)
                logger.info(f"  -> {folder_name}: {row_count:,} records → {dest_file}")
                total_saved += row_count

        # ลบ tmp folder ทิ้ง หลังย้ายครบแล้ว
        shutil.rmtree(tmp_quarantine)
        logger.info(f"Saved {total_saved:,} quarantine records to {QUARANTINE_DATA_PATH}/<error_type>/")

        context['task_instance'].xcom_push(
            key='quarantine_data_saved',
            value={'file': QUARANTINE_DATA_PATH, 'records': total_saved}
        )
        return f"Saved {total_saved:,} quarantine records"

    except Exception as e:
        logger.error(f"✗ Error saving quarantine data: {str(e)}")
        raise
