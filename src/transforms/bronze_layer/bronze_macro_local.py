import pandas as pd
import os
import glob

from pyspark.sql.functions import current_timestamp, lit

SCHEMA_NAME = "oil_analytics"

########### TABLE NAMES ###########
UK_GDP = "bronze_uk_gdp"
UK_INTEREST = "bronze_uk_interest_rate"


def bronze_uk_gdp(spark):
    """
    Loads and stores UK Bank of England gdp data into the bronze layer.

    Reads all xlsx files from `data/bank_of_england/gdp`, combines them
    into a single Pandas DataFrame, casts all values to strings to preserve raw
    structure, converts to a Spark DataFrame, and writes it to the bronze Delta table.

    Args:
        spark (SparkSession): Active Spark session used for writing the data.
    """
    base_path = os.path.join(os.path.dirname(__file__), "../../../data/bank_of_england/gdp")
    excel_files = glob.glob(os.path.join(base_path, "*"))

    all_dfs = []
    for file in excel_files:
        df = pd.read_excel(file, engine="openpyxl")
        all_dfs.append(df)
        
    combined_df = pd.concat(all_dfs, ignore_index=True)
    combined_df = combined_df.astype(str)
    
    try:
        sdf = spark.createDataFrame(combined_df)
        bronze_df = sdf.toDF(*[c.replace(": ", "_") for c in sdf.columns])
        bronze_df = bronze_df \
            .withColumn("ingestion_timestamp", current_timestamp()) \
                .withColumn("source_system", lit("xlsx_file"))
        bronze_df.write.mode("overwrite").saveAsTable(f"{SCHEMA_NAME}.{UK_GDP}")
        print(f"Created bronze table {SCHEMA_NAME}.{UK_GDP}")
    except FileNotFoundError as ae:
        print(f"File not found for bronze table: {ae}")
    except Exception as e:
        print(f"Unexpected error saving bronze table {SCHEMA_NAME}.{UK_GDP}: {e}")


def bronze_uk_interest_rate(spark):
    """
    Loads and stores UK Bank of England interest rate data into the bronze layer.

    Reads all xls files from `data/bank_of_england/bank_rate`, combines them
    into a single Pandas DataFrame, casts all values to strings to preserve raw
    structure, converts to a Spark DataFrame, and writes it to the bronze Delta table.

    Args:
        spark (SparkSession): Active Spark session used for writing the data.
    """
    base_path = os.path.join(os.path.dirname(__file__), "../../../data/bank_of_england/bank_rate")
    excel_files = glob.glob(os.path.join(base_path, "*"))

    all_dfs = []
    for file in excel_files:
        df = pd.read_excel(file, engine="xlrd")
        all_dfs.append(df)
        
    combined_df = pd.concat(all_dfs, ignore_index=True)
    combined_df = combined_df.astype(str)

    cols = [c.strip().lower().replace(" ", "_").replace(":", "_") for c in combined_df.columns]

    counts = 0
    new_cols = []
    for c in cols:
        new_cols.append(f"{counts}_{c}")
        counts += 1

    combined_df.columns = new_cols

    try:
        bronze_df = spark.createDataFrame(combined_df)
        bronze_df = bronze_df \
            .withColumn("ingestion_timestamp", current_timestamp()) \
                .withColumn("source_system", lit("xls_file"))
        bronze_df.write.mode("overwrite").saveAsTable(f"{SCHEMA_NAME}.{UK_INTEREST}")
        print(f"Created bronze table {SCHEMA_NAME}.{UK_INTEREST}")
    except FileNotFoundError as ae:
        print(f"File not found for bronze table: {ae}")
    except Exception as e:
        print(f"Unexpected error saving bronze table {SCHEMA_NAME}.{UK_INTEREST}: {e}")


def generate_bronze_macro_local_tables(spark):
    """
    Generates bronze macro tables.
    """
    bronze_uk_gdp(spark)
    bronze_uk_interest_rate(spark)
