"""
Unit tests for dag_yellow_taxi_pipeline.py
Tests run against the pure Python functions, no Airflow runtime required.
"""
import os
import sys
import shutil
import tempfile
import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd

# Make the dags folder importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'dags'))

# Airflow is not installed in the local venv; stub the minimum needed
import types

airflow_mod = types.ModuleType('airflow')
airflow_dag = types.ModuleType('airflow.models')
airflow_dag_cls = types.ModuleType('airflow')

dag_stub = MagicMock()
python_op_stub = MagicMock()

sys.modules.setdefault('airflow', MagicMock())
sys.modules.setdefault('airflow.models', MagicMock())
sys.modules.setdefault('airflow.providers', MagicMock())
sys.modules.setdefault('airflow.providers.standard', MagicMock())
sys.modules.setdefault('airflow.providers.standard.operators', MagicMock())
sys.modules.setdefault('airflow.providers.standard.operators.python', MagicMock())

# Patch DAG and PythonOperator so the module-level code doesn't fail
import airflow  # noqa: E402 (already mocked above)
airflow.DAG = MagicMock(return_value=MagicMock())

import importlib
dag_module = importlib.import_module('dag_yellow_taxi_pipeline')

get_csv_files       = dag_module.get_csv_files
validate_and_split  = dag_module.validate_and_split
save_clean_data     = dag_module.save_clean_data
save_quarantine_data = dag_module.save_quarantine_data
generate_report     = dag_module.generate_report
create_gold_summary = dag_module.create_gold_summary


def _make_context(xcom_store=None):
    """Return a fake Airflow context with an in-memory XCom store."""
    store = xcom_store if xcom_store is not None else {}

    ti = MagicMock()

    def xcom_push(key, value):
        store[key] = value

    def xcom_pull(key, task_ids=None):
        return store.get(key)

    ti.xcom_push.side_effect = xcom_push
    ti.xcom_pull.side_effect = xcom_pull
    return {'task_instance': ti}, store


def _sample_df(n_clean=5, n_bad=2):
    """Build a tiny DataFrame with some valid and some invalid rows."""
    base = datetime(2019, 1, 1, 10, 0, 0)
    rows = []
    for i in range(n_clean):
        rows.append({
            'tpep_pickup_datetime':  str(base),
            'tpep_dropoff_datetime': str(datetime(2019, 1, 1, 10, 30, 0)),
            'fare_amount':    10.0 + i,
            'trip_distance':   1.5 + i,
            'passenger_count': 1,
            'total_amount':   12.0 + i,
        })
    for i in range(n_bad):
        rows.append({
            'tpep_pickup_datetime':  str(base),
            'tpep_dropoff_datetime': str(datetime(2019, 1, 1, 10, 30, 0)),
            'fare_amount':    -1.0,   # invalid
            'trip_distance':   1.5,
            'passenger_count': 1,
            'total_amount':    0.0,
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# get_csv_files
# ---------------------------------------------------------------------------
class TestGetCsvFiles(unittest.TestCase):

    def test_empty_directory(self):
        ctx, store = _make_context()
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(dag_module, 'get_csv_files.__globals__', {}, create=True):
                # patch the archive_path inside the function
                original = dag_module.get_csv_files
                def patched(**context):
                    # temporarily redirect archive_path
                    import dag_yellow_taxi_pipeline as m
                    old = None
                    with patch('dag_yellow_taxi_pipeline.os.listdir', return_value=[]):
                        with patch('dag_yellow_taxi_pipeline.os.path.exists', return_value=True):
                            return original(**context)
                result = patched(**ctx)
        self.assertEqual(store['total_files'], 0)
        self.assertEqual(store['csv_files'], [])

    def test_finds_csv_files(self):
        ctx, store = _make_context()
        fake_files = ['yellow_tripdata_2019-01.csv', 'yellow_tripdata_2019-02.csv', 'other.txt']
        with patch('dag_yellow_taxi_pipeline.os.path.exists', return_value=True), \
             patch('dag_yellow_taxi_pipeline.os.listdir', return_value=fake_files):
            get_csv_files(**ctx)
        self.assertEqual(store['total_files'], 2)
        self.assertEqual(store['csv_files'], sorted([f for f in fake_files if f.startswith('yellow_tripdata') and f.endswith('.csv')]))


# ---------------------------------------------------------------------------
# validate_and_split
# ---------------------------------------------------------------------------
class TestValidateAndSplit(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.clean_tmp = os.path.join(self.tmpdir, 'clean')
        self.quar_tmp  = os.path.join(self.tmpdir, 'quarantine')
        os.makedirs(self.clean_tmp, exist_ok=True)
        os.makedirs(self.quar_tmp,  exist_ok=True)

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
        csv_path = os.path.join(self.tmpdir, 'yellow_tripdata_2019-01.csv')
        df.to_csv(csv_path, index=False)

        ctx, store = _make_context({'csv_files': ['yellow_tripdata_2019-01.csv']})

        with patch('dag_yellow_taxi_pipeline.os.makedirs'), \
             patch('builtins.open', unittest.mock.mock_open()), \
             patch('dag_yellow_taxi_pipeline.pd.read_csv', return_value=df.copy()), \
             patch('pandas.DataFrame.to_parquet'):
            # patch the temp paths to avoid actual disk writes
            orig_to_parquet = pd.DataFrame.to_parquet

            def fake_to_parquet(self_df, path, **kw):
                # write a real parquet for later assertions
                orig_to_parquet(self_df, path, **kw)

            with patch.object(pd.DataFrame, 'to_parquet', fake_to_parquet):
                with patch('dag_yellow_taxi_pipeline.os.makedirs'):
                    # redirect /tmp paths to our tmpdir
                    with patch('dag_yellow_taxi_pipeline.os.path.join',
                               side_effect=lambda *a: os.path.join(*a)):
                        pass  # just run it normally below

            # Run with real paths patched
            with patch('dag_yellow_taxi_pipeline.os.makedirs'):
                import dag_yellow_taxi_pipeline as m
                orig_makedirs = os.makedirs
                real_clean = '/tmp/clean'
                real_quar  = '/tmp/quarantine'
                validate_and_split(**ctx)

        r = store['validation_results']
        self.assertEqual(r['files_processed'], 1)
        self.assertEqual(r['total_records'], 8)
        self.assertEqual(r['clean_records'], 5)
        self.assertEqual(r['quarantine_records'], 3)

    def test_missing_required_column_skips_file(self):
        df = _sample_df(n_clean=3, n_bad=0)
        df = df.drop(columns=['fare_amount'])

        ctx, store = _make_context({'csv_files': ['yellow_tripdata_2019-01.csv']})
        with patch('dag_yellow_taxi_pipeline.pd.read_csv', return_value=df.copy()), \
             patch('dag_yellow_taxi_pipeline.os.makedirs'):
            validate_and_split(**ctx)

        r = store['validation_results']
        self.assertEqual(r['files_processed'], 0)
        self.assertIn('error', r['details'][0])


# ---------------------------------------------------------------------------
# generate_report  (the buggy functions we fixed)
# ---------------------------------------------------------------------------
class TestGenerateReport(unittest.TestCase):

    def _run_report(self, total, clean, quarantine):
        validation_results = {
            'files_processed': 1 if total > 0 else 0,
            'total_records': total,
            'clean_records': clean,
            'quarantine_records': quarantine,
            'details': [],
        }
        store = {
            'validation_results': validation_results,
            'clean_data_saved':   {'records': clean,      'file': '/silver'},
            'quarantine_data_saved': {'records': quarantine, 'file': '/quarantine'},
        }
        ctx, store = _make_context(store)
        generate_report(**ctx)
        return store

    def test_zero_records_no_false_quality_pass(self):
        """When 0 records, should NOT log '0.00% >= 80%' as a pass."""
        import io, logging
        log_capture = io.StringIO()
        handler = logging.StreamHandler(log_capture)
        logging.getLogger().addHandler(handler)

        self._run_report(total=0, clean=0, quarantine=0)

        logging.getLogger().removeHandler(handler)
        output = log_capture.getvalue()
        self.assertNotIn('0.00% >= 80%', output)

    def test_quarantine_percentage_not_100_when_zero_records(self):
        """Quarantine % must not show 100.00% when there are 0 total records."""
        import io, logging
        log_capture = io.StringIO()
        handler = logging.StreamHandler(log_capture)
        logging.getLogger().addHandler(handler)

        self._run_report(total=0, clean=0, quarantine=0)

        logging.getLogger().removeHandler(handler)
        output = log_capture.getvalue()
        self.assertNotIn('100.00%', output)

    def test_alert_triggered_when_quality_below_threshold(self):
        store = self._run_report(total=100, clean=50, quarantine=50)
        self.assertTrue(store.get('alert_triggered'))

    def test_no_alert_when_quality_above_threshold(self):
        store = self._run_report(total=100, clean=90, quarantine=10)
        self.assertFalse(store.get('alert_triggered'))

    def test_quarantine_percentage_correct(self):
        """With 100 total, 30 quarantine → quarantine % should be 30.0."""
        import io, logging
        log_capture = io.StringIO()
        handler = logging.StreamHandler(log_capture)
        logging.getLogger().addHandler(handler)

        self._run_report(total=100, clean=70, quarantine=30)

        logging.getLogger().removeHandler(handler)
        output = log_capture.getvalue()
        self.assertIn('30.00%', output)


# ---------------------------------------------------------------------------
# save_clean_data / save_quarantine_data  — temp file cleanup
# ---------------------------------------------------------------------------
class TestSaveDataCleanup(unittest.TestCase):

    def test_clean_temp_files_removed_after_save(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            silver_dir = os.path.join(tmpdir, 'silver')
            src_file   = os.path.join(tmpdir, 'yellow_tripdata_2019-01.parquet')

            # Create a real parquet temp file
            pd.DataFrame({'a': [1, 2]}).to_parquet(src_file, index=False)
            self.assertTrue(os.path.exists(src_file))

            store = {'clean_temp_paths': [src_file], 'clean_data_rows': 2}
            ctx, store = _make_context(store)

            with patch.object(dag_module, 'CLEAN_DATA_PATH', silver_dir):
                save_clean_data(**ctx)

            # Temp file must be deleted
            self.assertFalse(os.path.exists(src_file), "Temp clean file was not removed")
            # Silver file must exist
            silver_files = os.listdir(silver_dir)
            self.assertEqual(len(silver_files), 1)

    def test_quarantine_temp_files_removed_after_save(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            quar_dir = os.path.join(tmpdir, 'quarantine')
            src_file = os.path.join(tmpdir, 'yellow_tripdata_2019-01.parquet')

            pd.DataFrame({
                'source_file':  ['f.csv', 'f.csv'],
                'error_reason': ['Fare amount <= 0 or NaN', 'Fare amount <= 0 or NaN'],
                'fare_amount':  [-1.0, -2.0],
            }).to_parquet(src_file, index=False)
            self.assertTrue(os.path.exists(src_file))

            store = {'quarantine_temp_paths': [src_file], 'quarantine_data_rows': 2}
            ctx, store = _make_context(store)

            with patch.object(dag_module, 'QUARANTINE_DATA_PATH', quar_dir):
                save_quarantine_data(**ctx)

            self.assertFalse(os.path.exists(src_file), "Temp quarantine file was not removed")
            # Files are now saved inside error-type subfolders
            saved = [f for _, _, fs in os.walk(quar_dir) for f in fs]
            self.assertEqual(len(saved), 1)


if __name__ == '__main__':
    unittest.main(verbosity=2)
