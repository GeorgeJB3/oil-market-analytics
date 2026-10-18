import pytest
import pandas as pd
import yfinance as yf
from unittest.mock import MagicMock
from src.ingestion.index import IndexSeries, YfinanceFetcher


def test_yfinance_success():
    data = {
        'Date': ['2021-09-20 00:00:00-04:00', '2021-09-21 00:00:00-04:00'],
        'Open': [406.744360, 408.287567],
        'High': [408.315662, 409.578289],
        'Low': [401.113821, 405.051428],
        'Close': [405.958710, 405.575195],
        'Volume': [166445500, 92526100],
        'Dividends': [0.0, 0.0],
        'Stock Splits': [0.0, 0.0],
        'Capital Gains': [0.0, 0.0]
    }
    
    fake_df = pd.DataFrame(data)
    fake_df = fake_df.set_index("Date")
    
    fake_ticker_object = MagicMock()
    fake_ticker_object.history.return_value = fake_df

    fake_client = MagicMock()
    fake_client.Ticker.return_value = fake_ticker_object

    fetcher = YfinanceFetcher(fake_client)
    series = IndexSeries(ticker="SPY", name="S&P500")

    result = fetcher.fetch(series)

    assert len(result) == 2
    assert list(result.columns)== ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock Splits', 'Capital Gains']


def test_yfinance_returns_None_if_empty():
    data = {}

    fake_df = pd.DataFrame(data)

    fake_ticker_object = MagicMock()
    fake_ticker_object.history.return_value = fake_df

    fake_client = MagicMock()
    fake_client.Ticker.return_value = fake_ticker_object

    fetcher = YfinanceFetcher(fake_client)
    series = IndexSeries(ticker="SPY", name="S&P500")

    result = fetcher.fetch(series)

    assert result is None