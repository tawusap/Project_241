CHUNK_SIZE = 100_000          # rows per CSV chunk (memory vs. speed trade-off)
DATA_QUALITY_THRESHOLD = 80   # % clean records required before raising an alert

RAW_DATA_PATH        = '/opt/airflow/archive/raw'
CLEAN_DATA_PATH      = '/opt/airflow/processed_data/silver'
QUARANTINE_DATA_PATH = '/opt/airflow/processed_data/quarantine'
GOLD_DIR             = '/opt/airflow/processed_data/gold'

TMP_CLEAN      = '/tmp/clean'
TMP_QUARANTINE = '/tmp/quarantine'

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
