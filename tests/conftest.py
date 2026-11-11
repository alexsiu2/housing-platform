import pytest


@pytest.fixture(autouse=True)
def no_retry_sleep(monkeypatch):
    """Make FRED retry backoff instant so tests never actually sleep."""
    monkeypatch.setattr("ingestion.fred_client.BACKOFF_SECONDS", 0)
