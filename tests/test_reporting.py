import io
import logging
import unittest

from helpers import setup_airflow_mocks, _make_context

setup_airflow_mocks()

from pipeline.reporting import generate_report  # noqa: E402


class TestGenerateReport(unittest.TestCase):

    def _run_report(self, total, clean, quarantine):
        store = {
            'validation_results': {
                'files_processed': 1 if total > 0 else 0,
                'total_records': total,
                'clean_records': clean,
                'quarantine_records': quarantine,
                'details': [],
            },
            'clean_data_saved':      {'records': clean,      'file': '/silver'},
            'quarantine_data_saved': {'records': quarantine, 'file': '/quarantine'},
        }
        ctx, store = _make_context(store)
        generate_report(**ctx)
        return store

    def _capture_report(self, total, clean, quarantine):
        """Run report and return captured log output."""
        log_capture = io.StringIO()
        handler = logging.StreamHandler(log_capture)
        root = logging.getLogger()
        original_level = root.level
        root.setLevel(logging.DEBUG)
        root.addHandler(handler)
        try:
            self._run_report(total, clean, quarantine)
        finally:
            root.removeHandler(handler)
            root.setLevel(original_level)
        return log_capture.getvalue()

    def test_zero_records_no_false_quality_pass(self):
        """When 0 records, should NOT log '0.00% >= 80%' as a pass."""
        output = self._capture_report(total=0, clean=0, quarantine=0)
        self.assertNotIn('0.00% >= 80%', output)

    def test_quarantine_percentage_not_100_when_zero_records(self):
        """Quarantine % must not show 100.00% when there are 0 total records."""
        output = self._capture_report(total=0, clean=0, quarantine=0)
        self.assertNotIn('100.00%', output)

    def test_alert_triggered_when_quality_below_threshold(self):
        store = self._run_report(total=100, clean=50, quarantine=50)
        self.assertTrue(store.get('alert_triggered'))

    def test_no_alert_when_quality_above_threshold(self):
        store = self._run_report(total=100, clean=90, quarantine=10)
        self.assertFalse(store.get('alert_triggered'))

    def test_quarantine_percentage_correct(self):
        """With 100 total, 30 quarantine → quarantine % should be 30.00%."""
        output = self._capture_report(total=100, clean=70, quarantine=30)
        self.assertIn('30.00%', output)


if __name__ == '__main__':
    unittest.main(verbosity=2)
