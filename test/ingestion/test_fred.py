import pytest
import pandas as pd
import requests
from unittest.mock import MagicMock
from src.ingestion.fred import Series, FredSeriesFetcher


def test_fred_success():
    fake_client = MagicMock()
    fake_client.get_series.return_value = pd.Series([1.2, 1.4], index=pd.to_datetime(["2024-01-01", "2024-02-01"]))

    fetcher = FredSeriesFetcher(fake_client)
    series = Series(series_id="test_id", date_col="date", value_col="value")

    result = fetcher.fetch(series)

    assert len(result) == 2
    assert list(result.columns)== ["date", "value"]
    fake_client.get_series.assert_called_once_with("test_id")


def test_fred_network_error_returns_none():
    fake_client = MagicMock()
    fake_client.get_series.side_effect = requests.exceptions.RequestException("API down")

    fetcher = FredSeriesFetcher(fake_client)
    series = Series(series_id="test_id", date_col="date", value_col="value")
    result = fetcher.fetch(series)

    assert result is None


def test_fred_fetch_malformed_response_returns_none():
    fake_client = MagicMock()
    # Simulate a response with the wrong number of columns
    fake_client.get_series.return_value = pd.DataFrame({"a": [1], "b": [2], "c": [3]})

    fetcher = FredSeriesFetcher(fake_client)
    series = Series(series_id="test_id", date_col="date", value_col="value")

    result = fetcher.fetch(series)

    assert result is None


def test_fred_unexpected_error():
    fake_client = MagicMock()
    fake_client.get_series.side_effect = TypeError("There's a bug in the code")

    fetcher = FredSeriesFetcher(fake_client)
    series = Series(series_id="test_id", date_col="date", value_col="value")


    with pytest.raises(TypeError):
        fetcher.fetch(series)
        