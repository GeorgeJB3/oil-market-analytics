import requests
import pandas as pd
from pyjstat import pyjstat
import logging
from abc import ABC, abstractmethod


logger = logging.getLogger(__name__)


class UkMacroFetcher(ABC):
    """
    Abstract base class for fetching UK macroeconomic data over HTTP.

    Implements the shared fetch workflow (HTTP request, error handling)
    via the template method `fetch()`, while delegating source-specific
    response parsing to subclasses via the abstract `parse()` method.
    Each subclass encapsulates the parsing logic for a single UK data
    source (e.g. Nomis, ONS), which differ significantly in response
    format and cannot be handled generically.

    Attributes:
        session (requests.Session): HTTP session used to make requests,
            injected for testability (allows mocking in unit tests).
        url (str): The endpoint URL this fetcher requests data from.
    """
    def __init__(self, session: requests.Session, url: str):
        """
        Initialise the fetcher with an HTTP session and target URL.

        Args:
            session (requests.Session): Session used to perform the GET
                request. Injected rather than hardcoded to allow mocking
                in tests.
            url (str): The API endpoint to fetch data from.
        """
        self.session = session
        self.url = url

    
    def fetch(self):
        """
        Fetch and parse UK macroeconomic data from the configured URL.

        Performs the HTTP GET request and delegates response parsing to
        the subclass-specific `parse()` implementation. Network errors
        (e.g. connection failures, bad status codes) and response-shape
        errors (e.g. missing keys, malformed values) are caught, logged,
        and result in `None` being returned. Any other exception is
        treated as an unexpected bug and propagates uncaught.

        Returns:
            pandas.DataFrame or None: Parsed data if the request and
            parsing both succeed, otherwise None.
        """
        try:
            response = self.session.get(self.url)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error fetching UK macro data: {e}")
            return None
        
        try:
            return self.parse(response)
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error fetching data: {e}")
            return None
        except (KeyError, ValueError, IndexError) as e:
            logger.error(f"Unexpected response shape: {e}")
            return None

    
    @abstractmethod
    def parse(self, response: requests.Response) -> pd.DataFrame:
        """
        Parse a raw HTTP response into a DataFrame.

        Must be implemented by subclasses, since each UK data source
        returns a differently structured response and requires its own
        parsing logic.

        Args:
            response (requests.Response): The raw HTTP response returned
            by the source API.

        Returns:
            pandas.DataFrame: Parsed data extracted from the response.
        """
        ...


class NomisUnemploymentFetcher(UkMacroFetcher):
    """Fetcher for UK unemployment data from the Nomis API."""    
    def parse(self, response):
        """
        Parse a Nomis JSON-stat response into a DataFrame.

        Nomis returns data in JSON-stat format, a compact multi-
        dimensional structure that must be decoded via pyjstat before
        it can be represented as a flat DataFrame.

        Args:
            response (requests.Response): Raw response from the Nomis
                API containing JSON-stat formatted unemployment data.

        Returns:
            pandas.DataFrame: Unemployment data with one row per
            dimension combination (e.g. date, geography, sex, measure).
        """
        dataset = pyjstat.Dataset.read(response.text)
        df = dataset.write('dataframe')
        return df

        
class OnsCpiFetcher(UkMacroFetcher):
    """Fetcher for UK CPI (inflation rate) data from the ONS API."""

    def parse(self, response):
        """
        Parse an ONS CPI response into a DataFrame.

        The ONS response is not standard JSON but a CSV-like text block
        embedded after a fixed "Important notes" marker. Each line is
        manually split into a period/rate pair.

        Args:
            response (requests.Response): Raw response from the ONS API
                containing CPI data as delimited text.

        Returns:
            pandas.DataFrame: CPI data with `period` and `rate` columns.
        """
        data = response.text
        raw_data = data.split('Important notes",')[1]
        lines = raw_data.strip().split('\n')
        rows = []
        for line in lines:
            period, rate = line.split(',')
            rows.append({"period": period.split('"')[1].strip(), "rate": float(rate.split('"')[1].strip())})

        df = pd.DataFrame(rows)
        return df
