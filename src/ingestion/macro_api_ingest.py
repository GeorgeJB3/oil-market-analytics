import json
import requests

from pyjstat import pyjstat
from fredapi import Fred
from pyspark.sql.functions import col

from load_config import load_config

config = load_config()
UK_UNEMPLOYMENT_URL = config["uk_employment"]["url"]
UK_CPI_URL = config["uk_cpi"]["url"]
FED_KEY = config["fred_api"]["key"]
fred = Fred(api_key=FED_KEY)

def fetch_unemployment_data_uk():
    '''fetch unemployment data from nomis API'''
    response = requests.get(UK_UNEMPLOYMENT_URL)

    # Parse JSON-stat into pyjstat dataset
    dataset = pyjstat.Dataset.read(response.text)
    unemployment_df = dataset.write('dataframe')

    return unemployment_df


def fetch_cpi_data_uk():
    '''fetch inflation rates from ONS API monthly / quarterly / yearly'''
    response = requests.get(UK_CPI_URL)
    data = response.text
    print(data.split('Important notes",')[1])
    raw_data = data.split('Important notes",')[1]

    lines = raw_data.strip().split('\n')
    rows = []
    for line in lines:
        period, rate = line.split(',')
        rows.append({"period": period.split('"')[1].strip(), "rate": float(rate.split('"')[1].strip())})

    full_cpi_df = spark.createDataFrame(rows)

    return full_cpi_df


def fetch_fed_gdp():
    '''fetch gdp data from FRED API'''
    gdp = fred.get_series('GDP')
    gdp = gdp.reset_index()
    gdp.columns = ['date','value']
    gdp = spark.createDataFrame(gdp)
    return gdp


def fetch_fed_cpi():
    '''fetch cpi data from FRED API'''
    cpi = fred.get_series('CPIAUCSL') 
    cpi = cpi.reset_index()
    cpi.columns = ['date','value']
    cpi = spark.createDataFrame(cpi)
    return cpi


def fetch_fed_interest_rate():
    '''fetch interest rate data from FRED API'''
    fed_rate = fred.get_series('FEDFUNDS')
    fed_rate = fed_rate.reset_index()
    fed_rate.columns = ['date','value']
    fed_rate = spark.createDataFrame(fed_rate)
    return fed_rate


def fetch_fed_unemployment():
    '''fetch unemployment rate data from FRED API'''
    unrate = fred.get_series('UNRATE')
    unrate = unrate.reset_index()
    unrate.columns = ['date','value']
    unrate = spark.createDataFrame(unrate)
    return unrate
