"""Shared test utilities — imported by all test_*.py files."""
import os
import sys
from datetime import datetime
from unittest.mock import MagicMock

import pandas as pd

# Make dags/ and dags/pipeline/ importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'dags'))


def setup_airflow_mocks():
    """Stub Airflow so dag_yellow_taxi_pipeline can be imported without Airflow installed."""
    for mod in [
        'airflow', 'airflow.models',
        'airflow.providers', 'airflow.providers.standard',
        'airflow.providers.standard.operators',
        'airflow.providers.standard.operators.python',
    ]:
        sys.modules.setdefault(mod, MagicMock())
    import airflow
    airflow.DAG = MagicMock(return_value=MagicMock())


def _make_context(xcom_store=None):
    """Return a fake Airflow context with an in-memory XCom store."""
    store = xcom_store if xcom_store is not None else {}
    ti = MagicMock()
    ti.xcom_push.side_effect = lambda key, value: store.update({key: value})
    ti.xcom_pull.side_effect = lambda key, task_ids=None: store.get(key)
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
            'fare_amount':    -1.0,
            'trip_distance':   1.5,
            'passenger_count': 1,
            'total_amount':    0.0,
        })
    return pd.DataFrame(rows)
