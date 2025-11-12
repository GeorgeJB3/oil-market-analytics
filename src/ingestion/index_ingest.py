import yfinance as yf

def fetch_SP500_data(spark):
    '''fetch S&P 500 data from yahoo finance API'''
    spyticker = yf.Ticker("SPY")
    spy_df = spyticker.history(period="5y", interval="1d", auto_adjust=True)
    spy_df = spy_df.reset_index()
    return spy_df


def fetch_ftse100_data(spark):
    '''fetch FTSE 100 data from yahoo finance API'''
    ftse_ticker = yf.Ticker("^FTSE")
    ftse_df = ftse_ticker.history(period="5y", interval="1d", auto_adjust=True)
    ftse_df = ftse_df.reset_index()
    return ftse_df


def fetch_dollar_index_data(spark):
    '''fetch dollar index data from yahoo finance API'''
    dollar_index = yf.Ticker("dx-y.nyb")
    dollar_index_df = dollar_index.history(period="5y", interval="1d", auto_adjust=True)
    dollar_index_df = dollar_index_df.reset_index()
    return dollar_index_df
    