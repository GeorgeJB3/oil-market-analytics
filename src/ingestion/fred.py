import pandas as pd
import requests
import logging

from dataclasses import dataclass
from typing import Optional
from fredapi import Fred


logger = logging.getLogger(__name__)


@dataclass
class Series:
    """
    A class to represent a series of data.

    Attributes:
        series_id (str): The ID of the series.
        date_col (str): The name of the date column.
        value_col (str): The name of the value column.
    """
    series_id: str
    date_col: str
    value_col: str

class FredSeriesFetcher:
    """
    A class to fetch series data from the FRED API.

    Attributes:
        client (Fred): FRED API client used to retrieve series data.
    """

    def __init__(self, client: Fred):
        self.client = client

    def fetch(self, series: Series) -> Optional[pd.DataFrame]:
        """
        Fetch data from the FRED API based on the series details.
        """
        try:
            data = self.client.get_series(series.series_id) 
            data = data.reset_index()
            data.columns = [series.date_col, series.value_col]
            return data
        
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error fetching {series.series_id}: {e}")
            return None
        except (KeyError, ValueError) as e:
            logger.error(f"Unexpected response shape for {series.series_id}: {e}")
            return None
