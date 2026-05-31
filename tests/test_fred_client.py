import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from requests.exceptions import HTTPError

# ── Fake API response ──────────────────────────────────────────────────────────
# This is the JSON structure FRED actually returns. We use it as our mock
# instead of making real network calls. Note the "." on Jan 16 — that's
# FRED's way of saying data is missing for that observation.

FAKE_FRED_RESPONSE = {
    "observations": [
        {"date": "2020-01-02", "value": "3.72"},
        {"date": "2020-01-09", "value": "3.64"},
        {"date": "2020-01-16", "value": "."},  # missing — should be filtered out
        {"date": "2020-01-23", "value": "3.60"},
    ]
}


def make_mock_response(json_data: dict, status_code: int = 200) -> MagicMock:
    """
    Helper that builds a fake requests.Response object.
    .json() returns json_data, .raise_for_status() does nothing (success).
    """
    mock = MagicMock()
    mock.status_code = status_code
    mock.json.return_value = json_data
    mock.raise_for_status.return_value = None  # no exception = success
    return mock


# ── fetch_series tests ─────────────────────────────────────────────────────────


class TestFetchSeries:

    @patch("ingestion.fred_client.requests.get")
    @patch.dict("os.environ", {"FRED_API_KEY": "fake-key-123"})
    def test_returns_dataframe_with_correct_columns(self, mock_get):
        """Happy path: valid response returns DataFrame
        with exactly the right columns."""
        from ingestion.fred_client import fetch_series

        mock_get.return_value = make_mock_response(FAKE_FRED_RESPONSE)

        df = fetch_series("MORTGAGE30US", "2020-01-01", "2020-01-31")

        assert isinstance(df, pd.DataFrame)
        assert list(df.columns) == [
            "date",
            "series_id",
            "series_name",
            "value",
            "ingested_at",
        ]

    @patch("ingestion.fred_client.requests.get")
    @patch.dict("os.environ", {"FRED_API_KEY": "fake-key-123"})
    def test_filters_out_missing_dot_values(self, mock_get):
        """
        FRED uses "." for missing observations. These must be dropped.
        Our fake response has 4 rows but 1 is "." — so we expect 3 rows back.
        """
        from ingestion.fred_client import fetch_series

        mock_get.return_value = make_mock_response(FAKE_FRED_RESPONSE)

        df = fetch_series("MORTGAGE30US", "2020-01-01", "2020-01-31")

        assert len(df) == 3
        assert "." not in df["value"].values

    @patch("ingestion.fred_client.requests.get")
    @patch.dict("os.environ", {"FRED_API_KEY": "fake-key-123"})
    def test_correct_types(self, mock_get):
        """
        date must be datetime64, value must be float.
        FRED returns both as strings — our code casts them.
        """
        from ingestion.fred_client import fetch_series

        mock_get.return_value = make_mock_response(FAKE_FRED_RESPONSE)

        df = fetch_series("MORTGAGE30US", "2020-01-01", "2020-01-31")

        assert pd.api.types.is_datetime64_any_dtype(df["date"])
        assert pd.api.types.is_float_dtype(df["value"])

    @patch("ingestion.fred_client.requests.get")
    @patch.dict("os.environ", {"FRED_API_KEY": "fake-key-123"})
    def test_series_metadata_columns_are_correct(self, mock_get):
        """
        series_id should be the ID we passed in.
        series_name should be the human-readable name from FRED_SERIES dict.
        """
        from ingestion.fred_client import fetch_series

        mock_get.return_value = make_mock_response(FAKE_FRED_RESPONSE)

        df = fetch_series("MORTGAGE30US", "2020-01-01", "2020-01-31")

        assert (df["series_id"] == "MORTGAGE30US").all()
        assert (df["series_name"] == "30-Year Fixed Mortgage Rate").all()

    @patch("ingestion.fred_client.requests.get")
    @patch.dict("os.environ", {"FRED_API_KEY": "fake-key-123"})
    def test_unknown_series_id_uses_id_as_name(self, mock_get):
        """
        If a series_id isn't in our FRED_SERIES dict, series_name should
        fall back to the series_id string itself — not crash.
        """
        from ingestion.fred_client import fetch_series

        mock_get.return_value = make_mock_response(FAKE_FRED_RESPONSE)

        df = fetch_series("SOME_UNKNOWN_SERIES", "2020-01-01", "2020-01-31")

        assert (df["series_name"] == "SOME_UNKNOWN_SERIES").all()

    @patch.dict("os.environ", {}, clear=True)
    def test_raises_if_api_key_missing(self):
        """
        If FRED_API_KEY is not set, we should get a clear ValueError
        before any network call is made.
        """
        from ingestion.fred_client import fetch_series

        with pytest.raises(ValueError, match="FRED_API_KEY not found"):
            fetch_series("MORTGAGE30US", "2020-01-01", "2020-01-31")

    @patch("ingestion.fred_client.requests.get")
    @patch.dict("os.environ", {"FRED_API_KEY": "fake-key-123"})
    def test_raises_if_observations_empty(self, mock_get):
        """
        FRED returns an empty observations list when the series has no data
        in the requested date range. We should raise a clear ValueError.
        """
        from ingestion.fred_client import fetch_series

        mock_get.return_value = make_mock_response({"observations": []})

        with pytest.raises(ValueError, match="No observations returned"):
            fetch_series("MORTGAGE30US", "2020-01-01", "2020-01-31")

    @patch("ingestion.fred_client.requests.get")
    @patch.dict("os.environ", {"FRED_API_KEY": "fake-key-123"})
    def test_raises_on_http_error(self, mock_get):
        """
        If FRED returns a 500 or 404, requests.raise_for_status() throws.
        Our code should let that exception propagate — not silently swallow it.
        """
        from ingestion.fred_client import fetch_series

        mock_response = make_mock_response({}, status_code=500)
        mock_response.raise_for_status.side_effect = HTTPError("500 Server Error")
        mock_get.return_value = mock_response

        with pytest.raises(HTTPError):
            fetch_series("MORTGAGE30US", "2020-01-01", "2020-01-31")

    @patch("ingestion.fred_client.requests.get")
    @patch.dict("os.environ", {"FRED_API_KEY": "fake-key-123"})
    def test_passes_correct_params_to_api(self, mock_get):
        """
        Verify that we're actually sending the right query parameters to FRED.
        If this breaks, our API calls would silently return wrong data.
        """
        from ingestion.fred_client import fetch_series

        mock_get.return_value = make_mock_response(FAKE_FRED_RESPONSE)

        fetch_series("MORTGAGE30US", "2020-01-01", "2020-12-31")

        # requests.get is called as get(url, params=params) so check kwargs
        call_params = mock_get.call_args.kwargs.get(
            "params",
            mock_get.call_args.args[1] if len(mock_get.call_args.args) > 1 else {},
        )

        assert call_params["series_id"] == "MORTGAGE30US"
        assert call_params["observation_start"] == "2020-01-01"
        assert call_params["observation_end"] == "2020-12-31"
        assert call_params["file_type"] == "json"


# ── fetch_all_series tests ─────────────────────────────────────────────────────


class TestFetchAllSeries:

    @patch("ingestion.fred_client.fetch_series")
    def test_returns_combined_dataframe_for_all_series(self, mock_fetch):
        """
        fetch_all_series calls fetch_series once per series in FRED_SERIES.
        The result should be all DataFrames stacked vertically.
        """
        from ingestion.fred_client import fetch_all_series, FRED_SERIES

        # Each call to fetch_series returns a 3-row DataFrame
        def fake_fetch(series_id, start, end):
            return pd.DataFrame(
                {
                    "date": pd.to_datetime(["2020-01-01", "2020-02-01", "2020-03-01"]),
                    "series_id": series_id,
                    "series_name": series_id,
                    "value": [1.0, 2.0, 3.0],
                    "ingested_at": pd.Timestamp.now(tz="UTC"),
                }
            )

        mock_fetch.side_effect = fake_fetch

        df = fetch_all_series("2020-01-01", "2020-12-31")

        # 5 series × 3 rows each = 15 total rows
        assert len(df) == len(FRED_SERIES) * 3
        assert set(df["series_id"].unique()) == set(FRED_SERIES.keys())

    @patch("ingestion.fred_client.fetch_series")
    def test_continues_if_one_series_fails(self, mock_fetch):
        """
        If one series fails (e.g. API blip), fetch_all_series should
        log the error and continue fetching the remaining series.
        This is critical for pipeline reliability.
        """
        from ingestion.fred_client import fetch_all_series, FRED_SERIES

        series_ids = list(FRED_SERIES.keys())

        def fake_fetch(series_id, start, end):
            # Make the second series fail
            if series_id == series_ids[1]:
                raise RuntimeError("Simulated API failure")
            return pd.DataFrame(
                {
                    "date": pd.to_datetime(["2020-01-01"]),
                    "series_id": series_id,
                    "series_name": series_id,
                    "value": [1.0],
                    "ingested_at": pd.Timestamp.now(tz="UTC"),
                }
            )

        mock_fetch.side_effect = fake_fetch

        df = fetch_all_series("2020-01-01", "2020-12-31")

        # 4 series succeeded, 1 failed — we get 4 rows
        assert len(df) == 4
        assert series_ids[1] not in df["series_id"].values

    @patch("ingestion.fred_client.fetch_series")
    def test_raises_if_all_series_fail(self, mock_fetch):
        """
        If every single series fails, we should raise a RuntimeError
        rather than returning an empty DataFrame silently.
        """
        from ingestion.fred_client import fetch_all_series

        mock_fetch.side_effect = RuntimeError("Everything is broken")

        with pytest.raises(RuntimeError, match="Failed to fetch any FRED series"):
            fetch_all_series("2020-01-01", "2020-12-31")
