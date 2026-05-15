import os
import shutil
import tempfile
import unittest

import pandas as pd

from helpers import setup_airflow_mocks, _make_context

setup_airflow_mocks()

from unittest.mock import patch
from pipeline.storage import save_clean_data, save_quarantine_data  # noqa: E402


class TestSaveDataCleanup(unittest.TestCase):

    def test_clean_temp_files_removed_after_save(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            silver_dir = os.path.join(tmpdir, 'silver')
            src_file   = os.path.join(tmpdir, 'yellow_tripdata_2019-01.parquet')

            pd.DataFrame({'a': [1, 2]}).to_parquet(src_file, index=False)
            self.assertTrue(os.path.exists(src_file))

            store = {'clean_temp_paths': [src_file], 'clean_data_rows': 2}
            ctx, store = _make_context(store)

            with patch('pipeline.storage.CLEAN_DATA_PATH', silver_dir):
                save_clean_data(**ctx)

            self.assertFalse(os.path.exists(src_file), "Temp clean file was not removed")
            self.assertEqual(len(os.listdir(silver_dir)), 1)

    def test_quarantine_temp_files_removed_after_save(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            quar_dest  = os.path.join(tmpdir, 'quarantine_dest')

            # สร้าง folder structure ที่ save_quarantine_data คาดหวัง
            tmp_quar   = os.path.join(tmpdir, 'tmp_quarantine')
            folder_dir = os.path.join(tmp_quar, 'invalid_fare')
            os.makedirs(folder_dir)

            src_file = os.path.join(folder_dir, 'yellow_tripdata_2019-01.parquet')
            pd.DataFrame({
                'source_file':  ['f.csv', 'f.csv'],
                'error_reason': ['Fare amount <= 0 or NaN', 'Fare amount <= 0 or NaN'],
                'fare_amount':  [-1.0, -2.0],
            }).to_parquet(src_file, index=False)
            self.assertTrue(os.path.exists(src_file))

            ctx, store = _make_context({})

            with patch('pipeline.storage.TMP_QUARANTINE', tmp_quar), \
                 patch('pipeline.storage.QUARANTINE_DATA_PATH', quar_dest):
                save_quarantine_data(**ctx)

            self.assertFalse(os.path.exists(src_file), "Temp quarantine file was not removed")
            saved = [f for _, _, fs in os.walk(quar_dest) for f in fs]
            self.assertEqual(len(saved), 1)


if __name__ == '__main__':
    unittest.main(verbosity=2)
