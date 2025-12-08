import logging

from pyspark.sql.functions import lit, current_timestamp
from pyspark.sql.utils import AnalysisException
from src.ingestion.index_ingest import fetch_SP500_data, fetch_ftse100_data, fetch_dollar_index_data

SCHEMA_NAME = "oil_analytics"
SP_TABLE_NAME = "bronze_sp500"
FTSE_TABLE_NAME = "bronze_ftse100"
DXY_TABLE_NAME = "bronze_dollar_index"

logger = logging.getLogger(__name__)


def create_bronze_sp500(spark): 
    """  
    Create the bronze table for S&P 500 index data.

    Fetches raw S&P 500 data via the ingestion function, converts it
    into a Spark DataFrame, standardises column names, and adds
    ingestion metadata. The processed dataset is stored in
    `oil_analytics.bronze_sp500`.

    Args:
        spark (SparkSession): Active Spark session used to transform
            and persist the dataset.
    """
    
    spy_df = fetch_SP500_data(spark)
    spark_spy_df = spark.createDataFrame(spy_df)

    bronze_spy_df = spark_spy_df.toDF(*[c.replace(" ", "_") for c in spark_spy_df.columns])
    bronze_spy_df = bronze_spy_df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_system", lit("yfinance"))

    try: 
        bronze_spy_df.write.mode("append").saveAsTable(f"{SCHEMA_NAME}.{SP_TABLE_NAME}")
        print(f"Created bronze table {SCHEMA_NAME}.{SP_TABLE_NAME}")
    except AnalysisException as ae:
        print(f"Analysis error when saving bronze table: {ae}")
    except Exception as e:
        print(f"Unexpected error saving bronze table {SCHEMA_NAME}.{SP_TABLE_NAME}: {e}")


def create_bronze_ftse100(spark):
    """   
    Create the bronze table for FTSE 100 index data.

    Retrieves FTSE 100 data, converts it into a Spark DataFrame,
    normalises column names, and appends ingestion metadata. The
    resulting dataset is saved to `oil_analytics.bronze_ftse100`.

    Args:
        spark (SparkSession): Active Spark session used to transform
            and persist the dataset.    
    """
    ftse_df = fetch_ftse100_data(spark)

    spark_ftse_df = spark.createDataFrame(ftse_df)
    bronze_ftse_df = spark_ftse_df.toDF(*[c.replace(" ", "_") for c in spark_ftse_df.columns])
    bronze_ftse_df = bronze_ftse_df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_system", lit("yfinance"))

    try:
        bronze_ftse_df.write.mode("append").saveAsTable(f"{SCHEMA_NAME}.{FTSE_TABLE_NAME}")
        print(f"Created bronze table {SCHEMA_NAME}.{FTSE_TABLE_NAME}")
    except AnalysisException as ae:
        print(f"Analysis error when saving bronze table: {ae}")
    except Exception as e:
        print(f"Unexpected error saving bronze table {SCHEMA_NAME}.{FTSE_TABLE_NAME}: {e}")


def create_bronze_dxy(spark):
    """  
    Create the bronze table for Dollar Index (DXY) data.

    Pulls Dollar Index data, converts it into a Spark DataFrame,
    standardises column names, and adds ingestion metadata. The
    curated dataset is written to `oil_analytics.bronze_dollar_index`.

    Args:
        spark (SparkSession): Active Spark session used to transform
            and persist the dataset.
    """
    dxy_df = fetch_dollar_index_data(spark)
    spark_dxy_df = spark.createDataFrame(dxy_df)
    bronze_dxy_df = spark_dxy_df.toDF(*[c.replace(" ", "_") for c in spark_dxy_df.columns])
    bronze_dxy_df = bronze_dxy_df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_system", lit("yfinance"))

    try: 
        bronze_dxy_df.write.mode("append").saveAsTable(f"{SCHEMA_NAME}.{DXY_TABLE_NAME}")
        print(f"Created bronze table {SCHEMA_NAME}.{DXY_TABLE_NAME}")
    except AnalysisException as ae:
        print(f"Analysis error when saving bronze table: {ae}")
    except Exception as e:
        print(f"Unexpected error saving bronze table {SCHEMA_NAME}.{DXY_TABLE_NAME}: {e}")


def generate_bronze_index_tables(spark):
    """
    Build all bronze index tables in one step.

    Runs the individual functions for S&P 500, FTSE 100, and Dollar
    Index so each dataset is ingested, cleaned, and saved into its
    bronze-level table.

    Args:
        spark (SparkSession): Active Spark session used to execute
            the ingestion and persistence steps.
    """
    create_bronze_sp500(spark)
    create_bronze_ftse100(spark)
    create_bronze_dxy(spark)
    