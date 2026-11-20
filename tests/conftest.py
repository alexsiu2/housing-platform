import importlib

import pytest


@pytest.fixture(autouse=True)
def no_retry_sleep(monkeypatch):
    """Make FRED retry backoff instant so tests never actually sleep.

    Skipped where the client's dependencies aren't installed: CI's Spark job
    installs only pyspark + pytest and runs tests/test_hmda_lar.py.
    """
    try:
        fred_client = importlib.import_module("ingestion.fred_client")
    except ImportError:
        return
    monkeypatch.setattr(fred_client, "BACKOFF_SECONDS", 0)
