import pandas as pd
import logging
from dataclasses import dataclass


logger = logging.getLogger(__name__)


@dataclass
class IndexSeries:
    """
    Describes a single yfinance ticker to fetch.

    Attributes:
        ticker (str): The yfinance ticker symbol (e.g. "SPY", "^FTSE").
        name (str): A short label for the series, used in logging.
    """
    ticker: str
    name: str


class YfinanceFetcher:
    """
    Fetches historical price data from Yahoo Finance via yfinance.

    The yfinance module is injected via `client` rather than imported
    directly, to allow mocking in tests.
    """

    def __init__(self, client):
        """
        Args:
            client: The yfinance module.
        """
        self.client = client

    
    def fetch(self, series: IndexSeries) -> pd.DataFrame:
        """
        Fetch historical price data for a single index series.

        Args:
            series (IndexSeries): The ticker and label to fetch.

        Returns:
            pandas.DataFrame or None: Historical OHLCV data if successful,
            otherwise None if no data was returned or an unexpected error
            occurred.
        """

        ticker = self.client.Ticker(series.ticker)
        df = ticker.history(period="5y", interval="1d", auto_adjust=True)

        if df.empty:
            logger.error(f"No data returned for {series.name} ({series.ticker})")
            return None
        
        df = df.reset_index()
        return df
