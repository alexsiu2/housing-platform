import importlib.util
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pytest

_PATH = Path(__file__).resolve().parents[1] / "scripts" / "backfill_fred.py"
_spec = importlib.util.spec_from_file_location("backfill_fred", _PATH)
backfill = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(backfill)


def _metrics(rows_per_series):
    return {
        "validation": {"evaluated": 9},
        "rows_in": sum(rows_per_series.values()),
        "rows_inserted": sum(rows_per_series.values()),
        "rows_updated": 0,
        "min_date": "2020-01-01",
        "max_date": "2024-12-01",
        "rows_per_series": rows_per_series,
    }


@pytest.mark.parametrize(
    "today, expected",
    [(date(2026, 11, 10), date(2021, 11, 10)), (date(2024, 2, 29), date(2019, 2, 28))],
)
def test_years_before(today, expected):
    assert backfill.years_before(today, 5) == expected


def test_backfill_runs_full_pipeline_and_succeeds(capsys):
    with (
        patch.object(backfill, "extract_to_raw", return_value=["k1", "k2"]) as ex,
        patch.object(
            backfill, "load_raw_to_bronze", return_value=_metrics({"A": 3, "B": 4})
        ) as load,
    ):
        m = backfill.run_backfill("2020-01-01", "2024-12-31", ["A", "B"])

    ex.assert_called_once_with("2020-01-01", "2024-12-31", ["A", "B"])
    assert load.call_args.args[0] == ["k1", "k2"]
    assert load.call_args.kwargs["run_id"].startswith("backfill_")
    assert m["missing_series"] == []
    assert "All 2 series present" in capsys.readouterr().out


def test_main_exits_nonzero_when_a_series_is_missing():
    with (
        patch.object(backfill, "extract_to_raw", return_value=["k1"]),
        patch.object(
            backfill, "load_raw_to_bronze", return_value=_metrics({"UNRATE": 5})
        ),
    ):
        code = backfill.main(
            [
                "--start",
                "2020-01-01",
                "--end",
                "2021-01-01",
                "--series",
                "UNRATE",
                "HOUST",
            ]
        )
    assert code == 1


def test_main_rejects_unknown_series():
    with pytest.raises(SystemExit):
        backfill.main(["--series", "NOT_A_SERIES"])
