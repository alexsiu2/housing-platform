import pandas as pd
import pytest
from deltalake import DeltaTable

from ingestion.bronze_loader import merge_to_delta


def _rows(*values):
    return pd.DataFrame(
        {
            "series_id": [v[0] for v in values],
            "date": pd.to_datetime([v[1] for v in values]),
            "value": [v[2] for v in values],
        }
    )


def _read(uri):
    return (
        DeltaTable(uri)
        .to_pandas()
        .sort_values(["series_id", "date"])
        .reset_index(drop=True)
    )


KEYS = ["series_id", "date"]


def test_creates_table_on_first_write(tmp_path):
    uri = str(tmp_path / "t")
    m = merge_to_delta(_rows(("A", "2024-01-01", 1.0)), uri, KEYS)

    assert m["created_table"] is True
    assert m["rows_inserted"] == 1
    assert len(_read(uri)) == 1


def test_rerun_is_idempotent(tmp_path):
    uri = str(tmp_path / "t")
    df = _rows(("A", "2024-01-01", 1.0), ("A", "2024-01-02", 2.0))
    merge_to_delta(df, uri, KEYS)
    m = merge_to_delta(df, uri, KEYS)

    assert m["rows_inserted"] == 0
    assert m["rows_updated"] == 2
    assert len(_read(uri)) == 2


def test_updates_existing_and_inserts_new(tmp_path):
    uri = str(tmp_path / "t")
    merge_to_delta(_rows(("A", "2024-01-01", 1.0)), uri, KEYS)
    m = merge_to_delta(
        _rows(("A", "2024-01-01", 9.0), ("B", "2024-01-01", 5.0)), uri, KEYS
    )

    out = _read(uri)
    assert m["rows_inserted"] == 1
    assert m["rows_updated"] == 1
    assert out["value"].tolist() == [9.0, 5.0]


def test_duplicate_keys_in_input_last_wins(tmp_path):
    uri = str(tmp_path / "t")
    merge_to_delta(_rows(("A", "2024-01-01", 1.0), ("A", "2024-01-01", 2.0)), uri, KEYS)

    out = _read(uri)
    assert len(out) == 1
    assert out["value"].iloc[0] == 2.0


def test_adds_run_id_per_load(tmp_path):
    uri = str(tmp_path / "t")
    merge_to_delta(_rows(("A", "2024-01-01", 1.0)), uri, KEYS, run_id="run1")
    merge_to_delta(_rows(("B", "2024-01-01", 1.0)), uri, KEYS, run_id="run2")

    out = _read(uri)
    assert out["run_id"].tolist() == ["run1", "run2"]


def test_partitioned_table(tmp_path):
    uri = str(tmp_path / "t")
    merge_to_delta(
        _rows(("A", "2024-01-01", 1.0), ("B", "2024-01-01", 2.0)),
        uri,
        KEYS,
        partition_by=["series_id"],
    )

    assert DeltaTable(uri).metadata().partition_columns == ["series_id"]


def test_missing_key_column_raises(tmp_path):
    with pytest.raises(KeyError):
        merge_to_delta(_rows(("A", "2024-01-01", 1.0)), str(tmp_path / "t"), ["nope"])


def test_null_key_raises(tmp_path):
    df = _rows(("A", "2024-01-01", 1.0))
    df.loc[0, "series_id"] = None
    with pytest.raises(ValueError):
        merge_to_delta(df, str(tmp_path / "t"), KEYS)


def test_empty_keys_raises(tmp_path):
    with pytest.raises(ValueError):
        merge_to_delta(_rows(("A", "2024-01-01", 1.0)), str(tmp_path / "t"), [])
