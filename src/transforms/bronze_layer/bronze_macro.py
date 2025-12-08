import pandas as pd
import os
import glob

from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.utils import AnalysisException
from src.ingestion.macro_api_ingest import fetch_unemployment_data_uk, fetch_cpi_data_uk, fetch_fed_gdp, fetch_fed_cpi, fetch_fed_interest_rate, fetch_fed_unemployment


SCHEMA_NAME = "oil_analytics"

########### TABLE NAMES ###########
UK_UNEM = "bronze_uk_unemployment"
UK_CPI = "bronze_uk_cpi"
FED_UNEM = "bronze_fed_unemployment"
FED_CPI = "bronze_fed_cpi"
FED_GDP = "bronze_fed_gdp"
FED_INTEREST = "bronze_fed_interest_rate"


def bronze_uk_unemployment(spark):
    """
    Fetches UK unemployment data and writes it to the Bronze layer as a Delta table.

    Steps:
        1. Fetch raw unemployment data from nomis API.
        2. Create a Spark DataFrame.
        3. Clean column names (replace spaces with underscores).
        4. Save as a Delta table in overwrite mode.

    Args:
        spark (SparkSession): The active Spark session.
    """

    data = fetch_unemployment_data_uk(spark)
    df = spark.createDataFrame(data)
    bronze_df = df.toDF(*[c.replace(" ", "_") for c in df.columns])
    bronze_df = bronze_df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_system", lit("nomisAPI"))
    try: 
        bronze_df.write.mode("overwrite").saveAsTable(f"{SCHEMA_NAME}.{UK_UNEM}")
        print(f"Created bronze table {SCHEMA_NAME}.{UK_UNEM}")
    except AnalysisException as ae:
        print(f"Analysis error when saving bronze table: {ae}")
    except Exception as e:
        print(f"Unexpected error saving bronze table {SCHEMA_NAME}.{UK_UNEM}: {e}")


def bronze_uk_cpi(spark):
    """
    Fetches UK consumer price index data and writes it to the Bronze layer as a Delta table.

    Steps:
        1. Fetch raw cpi data from API.
        2. Create a Spark DataFrame.
        3. Clean column names (replace spaces with underscores).
        4. Save as a Delta table in overwrite mode.

    Args:
        spark (SparkSession): The active Spark session.
    """
    df = fetch_cpi_data_uk(spark)
    bronze_df = df.toDF(*[c.replace(" ", "_") for c in df.columns])
    bronze_df = bronze_df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_system", lit("onsAPI"))
    try:
        bronze_df.write.mode("overwrite").saveAsTable(f"{SCHEMA_NAME}.{UK_CPI}")
        print(f"Created bronze table {SCHEMA_NAME}.{UK_CPI}")
    except AnalysisException as ae:
        print(f"Analysis error when saving bronze table: {ae}")
    except Exception as e:
        print(f"Unexpected error saving bronze table {SCHEMA_NAME}.{UK_CPI}: {e}")


def bronze_fed_unemployment(spark):
    """
    Fetches US unemployment data and writes it to the Bronze layer as a Delta table.

    Steps:
        1. Fetch raw unemployment data from API.
        2. Create a Spark DataFrame.
        3. Clean column names (replace spaces with underscores).
        4. Save as a Delta table in overwrite mode.

    Args:
        spark (SparkSession): The active Spark session.
    """
    df = fetch_fed_unemployment(spark)
    bronze_df = df.toDF(*[c.replace(" ", "_") for c in df.columns])
    bronze_df = bronze_df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_system", lit("FredAPI"))
    try:
        bronze_df.write.mode("overwrite").saveAsTable(f"{SCHEMA_NAME}.{FED_UNEM}")
        print(f"Created bronze table {SCHEMA_NAME}.{FED_UNEM}")
    except AnalysisException as ae:
        print(f"Analysis error when saving bronze table: {ae}")
    except Exception as e:
        print(f"Unexpected error saving bronze table {SCHEMA_NAME}.{FED_UNEM}: {e}")


def bronze_fed_cpi(spark):
    """
    Fetches US consumer price index data and writes it to the Bronze layer as a Delta table.

    Steps:
        1. Fetch raw cpi data from API.
        2. Create a Spark DataFrame.
        3. Clean column names (replace spaces with underscores).
        4. Save as a Delta table in overwrite mode.

    Args:
        spark (SparkSession): The active Spark session.
    """
    df = fetch_fed_cpi(spark)
    bronze_df = df.toDF(*[c.replace(" ", "_") for c in df.columns])
    bronze_df = bronze_df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_system", lit("FredAPI"))
    try:
        bronze_df.write.mode("overwrite").saveAsTable(f"{SCHEMA_NAME}.{FED_CPI}")
        print(f"Created bronze table {SCHEMA_NAME}.{FED_CPI}")
    except AnalysisException as ae:
        print(f"Analysis error when saving bronze table: {ae}")
    except Exception as e:
        print(f"Unexpected error saving bronze table {SCHEMA_NAME}.{FED_CPI}: {e}")


def bronze_fed_gdp(spark):
    """
    Fetches US gdp data and writes it to the Bronze layer as a Delta table.

    Steps:
        1. Fetch raw gdp data from API.
        2. Create a Spark DataFrame.
        3. Clean column names (replace spaces with underscores).
        4. Save as a Delta table in overwrite mode.

    Args:
        spark (SparkSession): The active Spark session.
    """
    df = fetch_fed_gdp(spark)
    bronze_df = df.toDF(*[c.replace(" ", "_") for c in df.columns])
    bronze_df = bronze_df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_system", lit("FredAPI"))
    try:
        bronze_df.write.mode("overwrite").saveAsTable(f"{SCHEMA_NAME}.{FED_GDP}")
        print(f"Created bronze table {SCHEMA_NAME}.{FED_GDP}")
    except AnalysisException as ae:
        print(f"Analysis error when saving bronze table: {ae}")
    except Exception as e:
        print(f"Unexpected error saving bronze table {SCHEMA_NAME}.{FED_GDP}: {e}")


def bronze_fed_interest_rate(spark):
    """
    Fetches US interest rate data and writes it to the Bronze layer as a Delta table.

    Steps:
        1. Fetch raw interest rate data from API.
        2. Create a Spark DataFrame.
        3. Clean column names (replace spaces with underscores).
        4. Save as a Delta table in overwrite mode.

    Args:
        spark (SparkSession): The active Spark session.
    """
    df = fetch_fed_interest_rate(spark)
    bronze_df = df.toDF(*[c.replace(" ", "_") for c in df.columns])
    bronze_df = bronze_df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_system", lit("FredAPI"))
    try:
        bronze_df.write.mode("overwrite").saveAsTable(f"{SCHEMA_NAME}.{FED_INTEREST}")
        print(f"Created bronze table {SCHEMA_NAME}.{FED_INTEREST}")
    except AnalysisException as ae:
        print(f"Analysis error when saving bronze table: {ae}")
    except Exception as e:
        print(f"Unexpected error saving bronze table {SCHEMA_NAME}.{FED_INTEREST}: {e}")


def generate_bronze_macro_tables(spark):
    """
    Build all bronze macroeconomic tables.

    Runs the individual ingestion functions for UK and US unemployment,
    CPI, GDP, and interest rate data, so each source table is pulled,
    cleaned, and saved into its bronze version.

    Args:
        spark (SparkSession): Active Spark session used to execute
            the ingestion and persistence steps.
    """
    bronze_uk_unemployment(spark)
    bronze_uk_cpi(spark)
    bronze_fed_unemployment(spark)
    bronze_fed_cpi(spark)
    bronze_fed_gdp(spark)
    bronze_fed_interest_rate(spark)
