import logging

from pipeline.config import DATA_QUALITY_THRESHOLD

logger = logging.getLogger(__name__)


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

    skipped_files = context['task_instance'].xcom_pull(
        key='skipped_files', task_ids='get_csv_files'
    ) or []
    if skipped_files:
        logger.info(f"Skipped (silver already exists): {len(skipped_files)} files")
        for f in skipped_files:
            logger.info(f"  ↷ {f}")

    logger.info("")
    logger.info("File-by-File Details (new files only):")
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
            f"⚠️_ ALERT: Data Quality Below Threshold!\n"
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
