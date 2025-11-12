import json
import requests

from pyjstat import pyjstat
from fredapi import Fred
from pyspark.sql.functions import col

from src.ingestion.load_config import load_config

config = load_config()
UK_UNEMPLOYMENT_URL = config["uk_employment"]["url"]
UK_CPI_URL = config["uk_cpi"]["url"]
FED_KEY = config["fred_api"]["key"]
fred = Fred(api_key=FED_KEY)


def fetch_unemployment_data_uk(spark):
    '''fetch unemployment data from nomis API'''
    try:
        response = requests.get(UK_UNEMPLOYMENT_URL)
        response.raise_for_status()
    except requests.exceptions.HTTPError as hte:
        print(f"HTTP error occurred: {hte}")
        return None
    except requests.exceptions.ConnectionError as ce:
        print(f"Connection error while fetching UK unemployment data: {ce}")
        return None
    except requests.exceptions.RequestException as err:
        print(f"Unexpected error: {err}")
        return None
        
    try:
        # Parse JSON-stat into pyjstat dataset
        dataset = pyjstat.Dataset.read(response.text)
        unemployment_df = dataset.write('dataframe')
        return unemployment_df
    except Exception as e:
        print(f"Error occurred while parsing UK unemployment data: {e}")
        return None

    
def fetch_cpi_data_uk(spark):
    '''fetch inflation rates from ONS API monthly / quarterly / yearly'''
    try:
        response = requests.get(UK_CPI_URL)
        response.raise_for_status()
    except requests.exceptions.HTTPError as hte:
        print(f"HTTP error occurred: {hte}")
        return None
    except requests.exceptions.ConnectionError as ce:
        print(f"Connection error while fetching UK CPI data. {ce}")
        return None
    except requests.exceptions.RequestException as err:
        print(f"Unexpected error: {err}")
        return None

    try:
        data = response.text
        raw_data = data.split('Important notes",')[1]
        lines = raw_data.strip().split('\n')
        rows = []
        for line in lines:
            period, rate = line.split(',')
            rows.append({"period": period.split('"')[1].strip(), "rate": float(rate.split('"')[1].strip())})

        full_cpi_df = spark.createDataFrame(rows)

        return full_cpi_df
    except Exception as e:
        print(f"Error parsing UK CPI data: {e}")
        return None


def fetch_fed_gdp(spark):
    '''fetch gdp data from FRED API'''
    try:
        gdp = fred.get_series('GDP')
        gdp = gdp.reset_index()
        gdp.columns = ['date','value']
        gdp = spark.createDataFrame(gdp)
        return gdp
    except Exception as e:
        print(f"Error fetching or processing FED GDP data: {e}")
        return None


def fetch_fed_cpi(spark):
    '''fetch cpi data from FRED API'''
    try:
        cpi = fred.get_series('CPIAUCSL') 
        cpi = cpi.reset_index()
        cpi.columns = ['date','value']
        cpi = spark.createDataFrame(cpi)
        return cpi
    except Exception as e:
        print(f"Error fetching or processing FED CPI data: {e}")
        return None


def fetch_fed_interest_rate(spark):
    '''fetch interest rate data from FRED API'''
    try:
        fed_rate = fred.get_series('FEDFUNDS')
        fed_rate = fed_rate.reset_index()
        fed_rate.columns = ['date','value']
        fed_rate = spark.createDataFrame(fed_rate)
        return fed_rate
    except Exception as e:
        print(f"Error fetching or processing FED Interest rate data: {e}")
        return None


def fetch_fed_unemployment(spark):
    '''fetch unemployment rate data from FRED API'''
    try:
        unrate = fred.get_series('UNRATE')
        unrate = unrate.reset_index()
        unrate.columns = ['date','value']
        unrate = spark.createDataFrame(unrate)
        return unrate
    except Exception as e:
        print(f"Error fetching or processing FED unemployment data: {e}")
        return None
