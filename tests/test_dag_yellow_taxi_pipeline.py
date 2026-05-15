# Tests have been split into focused modules:
#
#   tests/test_validation.py  — get_csv_files, validate_and_split
#   tests/test_storage.py     — save_clean_data, save_quarantine_data
#   tests/test_reporting.py   — generate_report
#
# Run all:
#   python -m pytest tests/test_validation.py tests/test_storage.py tests/test_reporting.py -v
