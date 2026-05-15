import os
import shutil
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd

from helpers import setup_airflow_mocks, _make_context, _sample_df

setup_airflow_mocks()

import importlib
_dag_module = importlib.import_module('dag_yellow_taxi_pipeline')
get_csv_files = _dag_module.get_csv_files

from pipeline.validation import validate_and_split  # noqa: E402


# ---------------------------------------------------------------------------
# get_csv_files
# ---------------------------------------------------------------------------
class TestGetCsvFiles(unittest.TestCase):

    def test_empty_directory(self):
        ctx, store = _make_context()
        with patch('dag_yellow_taxi_pipeline.os.listdir', return_value=[]), \
             patch('dag_yellow_taxi_pipeline.os.path.exists', return_value=True):
            get_csv_files(**ctx)
        self.assertEqual(store['total_files'], 0)
        self.assertEqual(store['csv_files'], [])

    def test_finds_csv_files(self):
        ctx, store = _make_context()
        fake_files = ['yellow_tripdata_2019-01.csv', 'yellow_tripdata_2019-02.csv', 'other.txt']

        def fake_exists(path):
            # RAW_DATA_PATH exists; silver files do not yet exist
            return not path.endswith('_silver.parquet')

        with patch('dag_yellow_taxi_pipeline.os.path.exists', side_effect=fake_exists), \
             patch('dag_yellow_taxi_pipeline.os.listdir', return_value=fake_files):
            get_csv_files(**ctx)

        self.assertEqual(store['total_files'], 2)
        self.assertEqual(store['csv_files'], ['yellow_tripdata_2019-01.csv', 'yellow_tripdata_2019-02.csv'])


# ---------------------------------------------------------------------------
# validate_and_split
# ---------------------------------------------------------------------------
class TestValidateAndSplit(unittest.TestCase):

    def setUp(self):
        self.tmpdir   = tempfile.mkdtemp()
        self.tmp_clean = os.path.join(self.tmpdir, 'clean')
        self.tmp_quar  = os.path.join(self.tmpdir, 'quarantine')
        os.makedirs(self.tmp_clean)
        os.makedirs(self.tmp_quar)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_no_csv_files(self):
        ctx, store = _make_context({'csv_files': []})
        validate_and_split(**ctx)
        r = store['validation_results']
        self.assertEqual(r['files_processed'], 0)
        self.assertEqual(r['total_records'], 0)

    def test_clean_and_quarantine_split(self):
        df = _sample_df(n_clean=5, n_bad=3)

        def mock_read_csv(*args, **kwargs):
            if kwargs.get('nrows') == 0:
                return df.copy()       # header schema check
            return iter([df.copy()])   # chunked iterator

        ctx, store = _make_context({'csv_files': ['yellow_tripdata_2019-01.csv']})

        with patch('pipeline.validation.pd.read_csv', side_effect=mock_read_csv), \
             patch('pipeline.validation.TMP_CLEAN',      self.tmp_clean), \
             patch('pipeline.validation.TMP_QUARANTINE', self.tmp_quar):
            validate_and_split(**ctx)

        r = store['validation_results']
        self.assertEqual(r['files_processed'], 1)
        self.assertEqual(r['total_records'], 8)
        self.assertEqual(r['clean_records'], 5)
        self.assertEqual(r['quarantine_records'], 3)

    def test_missing_required_column_skips_file(self):
        df = _sample_df(n_clean=3, n_bad=0).drop(columns=['fare_amount'])

        ctx, store = _make_context({'csv_files': ['yellow_tripdata_2019-01.csv']})
        with patch('pipeline.validation.pd.read_csv', return_value=df.copy()), \
             patch('pipeline.validation.TMP_CLEAN',      self.tmp_clean), \
             patch('pipeline.validation.TMP_QUARANTINE', self.tmp_quar):
            validate_and_split(**ctx)

        r = store['validation_results']
        self.assertEqual(r['files_processed'], 0)
        self.assertIn('error', r['details'][0])


if __name__ == '__main__':
    unittest.main(verbosity=2)
