import pytest
import pandas as pd
import requests
from unittest.mock import MagicMock, patch
from src.ingestion.uk_macro import UkMacroFetcher, NomisUnemploymentFetcher, OnsCpiFetcher


def test_fetch_returns_none_on_network_error():
    mock_session = MagicMock()
    mock_session.get.side_effect = requests.exceptions.ConnectionError("boom")

    fetcher = OnsCpiFetcher(session=mock_session, url="http://fake-url")
    result = fetcher.fetch()

    assert result is None


def test_ons_fetch_returns_dataframe_on_success():
    fake_response = MagicMock()
    fake_response.text = (
        'junk header text","Important notes",'
        '"1989 JAN","5.2"\n'
        '"1990 JAN","7.0"'
    )
    fake_response.raise_for_status.return_value = None

    mock_session = MagicMock()
    mock_session.get.return_value = fake_response

    fetcher = OnsCpiFetcher(session=mock_session, url="http://fake-url")
    result = fetcher.fetch()

    assert result is not None
    assert list(result.columns) == ["period", "rate"]
    assert result.iloc[0]["rate"] == 5.2
    assert isinstance(result, pd.DataFrame)


def test_ons_fetch_returns_none_on_malformed_response():
    fake_response = MagicMock()
    fake_response.text = "this text has no marker in it at all"
    fake_response.raise_for_status.return_value = None

    mock_session = MagicMock()
    mock_session.get.return_value = fake_response

    fetcher = OnsCpiFetcher(session=mock_session, url="http://fake-url")
    result = fetcher.fetch()

    assert result is None


def test_ons_fetch_unexpected_exception_propagates():
    fake_response = MagicMock()
    fake_response.text = None
    fake_response.raise_for_status.return_value = None

    mock_session = MagicMock()
    mock_session.get.return_value = fake_response

    fetcher = OnsCpiFetcher(session=mock_session, url="http://fake-url")

    with pytest.raises(AttributeError):
        fetcher.fetch()


def test_ons_cpi_parse_missing_marker_raises_indexerror():
    fake_response = MagicMock()
    fake_response.text = "this text has no marker in it at all"

    fetcher = OnsCpiFetcher(session=MagicMock(), url="http://fake-url")

    with pytest.raises(IndexError):
        fetcher.parse(fake_response)


def test_nomis_fetch_returns_none_on_network_error():
    mock_session = MagicMock()
    mock_session.get.side_effect = requests.exceptions.ConnectionError("boom")

    fetcher = NomisUnemploymentFetcher(session=mock_session, url="http://fake-url")
    result = fetcher.fetch()

    assert result is None


def test_nomis_parse_success():
    expected_df = pd.DataFrame([{
        "date": "Apr 2021-Jun 2021",
        "geography": "United Kingdom",
        "sex": "Total",
        "Economic Activity": "All persons - aged 16 and over",
        "Value Type": "Level",
        "measures": "number of people",
        "value": 53841502.0,
    }])

    fake_dataset = MagicMock()
    fake_dataset.write.return_value = expected_df

    fake_response = MagicMock()
    fake_response.text = "irrelevant, since Dataset.read is mocked"

    with patch("src.ingestion.uk_macro.pyjstat.Dataset.read", return_value=fake_dataset):
        fetcher = NomisUnemploymentFetcher(session=MagicMock(), url="http://fake-url")
        result = fetcher.parse(fake_response)

    assert list(result.columns) == [
        "date", "geography", "sex", "Economic Activity",
        "Value Type", "measures", "value"
    ]
    assert result.iloc[0]["value"] == 53841502.0


def test_nomis_unemployment_parse_malformed_raises():
    with patch("src.ingestion.uk_macro.pyjstat.Dataset.read", side_effect=ValueError("unexpected response")):
        fetcher = NomisUnemploymentFetcher(session=MagicMock(), url="http://fake-url")

        with pytest.raises(ValueError):
            fetcher.parse(MagicMock())
