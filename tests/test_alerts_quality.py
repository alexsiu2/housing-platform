from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd
import pytest
from deltalake import write_deltalake

from ingestion import alerts, quality_report

NOW = datetime(2026, 11, 12, 8, 0, tzinfo=timezone.utc)


def test_alert_without_webhook_only_logs(monkeypatch, caplog):
    monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)
    with patch.object(alerts.requests, "post") as post:
        assert alerts.send_alert("boom") is False
    post.assert_not_called()
    assert "ALERT: boom" in caplog.text


def test_alert_posts_slack_format(monkeypatch):
    monkeypatch.setenv("ALERT_WEBHOOK_URL", "https://hooks.example.test/x")
    with patch.object(alerts.requests, "post") as post:
        assert alerts.send_alert("boom") is True
    assert post.call_args.kwargs["json"] == {"text": "boom"}


def test_alert_webhook_failure_never_raises(monkeypatch):
    monkeypatch.setenv("ALERT_WEBHOOK_URL", "https://hooks.example.test/x")
    with patch.object(
        alerts.requests, "post", side_effect=alerts.requests.ConnectionError("down")
    ):
        assert alerts.send_alert("boom") is False


def test_task_failure_alert_names_dag_task_and_error(monkeypatch):
    monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)
    ti = SimpleNamespace(dag_id="ingest_fred", task_id="extract")
    with patch.object(alerts, "send_alert") as send:
        alerts.task_failure_alert(
            {"ti": ti, "run_id": "r1", "exception": ValueError("bad")}
        )
    assert send.call_args.args[0] == (
        "[rpof] ingest_fred.extract failed in r1: ValueError: bad"
    )


@pytest.fixture
def lake(tmp_path, monkeypatch):
    monkeypatch.setattr(
        quality_report, "lake_uri", lambda layer, t: str(tmp_path / layer / t)
    )
    return tmp_path


def test_freshness_fresh_overdue_and_missing(lake):
    write_deltalake(str(lake / "bronze" / "fred_series_raw"), pd.DataFrame({"a": [1]}))
    fresh = quality_report.table_freshness(
        "fred_series_raw", now=datetime.now(timezone.utc), storage_options={}
    )
    assert fresh["status"] == "fresh" and fresh["age_days"] < 1

    later = datetime.now(timezone.utc) + timedelta(days=15)  # cadence 7d, 2x = 14d
    assert (
        quality_report.table_freshness(
            "fred_series_raw", now=later, storage_options={}
        )["status"]
        == "overdue"
    )
    missing = quality_report.table_freshness("zori_raw", now=NOW, storage_options={})
    assert missing["status"] == "missing"


def test_problems_lists_only_unhealthy():
    report = [
        {"table": "a", "status": "fresh"},
        {"table": "b", "status": "missing", "cadence_days": 31},
        {"table": "c", "status": "overdue", "age_days": 40.0, "cadence_days": 7},
    ]
    assert quality_report.problems(report) == [
        "b is missing",
        "c is overdue (40.0d old, cadence 7d)",
    ]
