import yfinance as yf

def fetch_SP500_data(spark):
    """
    Fetch historical S&P 500 index data from Yahoo Finance.

    Uses the SPY ticker to retrieve daily adjusted prices for the
    past 5 years. The data is returned as a Pandas DataFrame with
    the date reset as a column.

    Args:
        spark (SparkSession): Included for consistency with pipeline
            functions, though not directly used here.

    Returns:
        pandas.DataFrame: Historical S&P 500 data with date, open,
            high, low, close, and volume fields.
    """
    spyticker = yf.Ticker("SPY")
    spy_df = spyticker.history(period="5y", interval="1d", auto_adjust=True)
    spy_df = spy_df.reset_index()
    return spy_df


def fetch_ftse100_data(spark):
    """
    Fetch historical FTSE 100 index data from Yahoo Finance.

    Uses the ^FTSE ticker to retrieve daily adjusted prices for the
    past 5 years. The data is returned as a Pandas DataFrame with
    the date reset as a column.

    Args:
        spark (SparkSession): Included for consistency with pipeline
            functions, though not directly used here.

    Returns:
        pandas.DataFrame: Historical FTSE 100 data with date, open,
            high, low, close, and volume fields.
    """
    ftse_ticker = yf.Ticker("^FTSE")
    ftse_df = ftse_ticker.history(period="5y", interval="1d", auto_adjust=True)
    ftse_df = ftse_df.reset_index()
    return ftse_df


def fetch_dollar_index_data(spark):
    """
    Fetch historical Dollar Index (DXY) data from Yahoo Finance.

    Uses the dx-y.nyb ticker to retrieve daily adjusted prices for
    the past 5 years. The data is returned as a Pandas DataFrame
    with the date reset as a column.

    Args:
        spark (SparkSession): Included for consistency with pipeline
            functions, though not directly used here.

    Returns:
        pandas.DataFrame: Historical Dollar Index data with date,
            open, high, low, close, and volume fields.
    """
    dollar_index = yf.Ticker("dx-y.nyb")
    dollar_index_df = dollar_index.history(period="5y", interval="1d", auto_adjust=True)
    dollar_index_df = dollar_index_df.reset_index()
    return dollar_index_df
    