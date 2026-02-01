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
    Loads and stores UK GDP data, from world bank group into the bronze layer.
    
    Reads the csv file from `data/uk_gdp` into a pandas dataframe. Filters for only United Kingdom data.
    Unpivots the dataframe sp we have each year and gdp rate as a separate row.
    Converts to a Spark DataFrame, and writes it to the bronze Delta table.

    Args:
        spark (SparkSession): Active Spark session used for writing the data.
    """
    uk_gdp_file = os.path.abspath("../data/uk_gdp/API_NY.GDP.MKTP.KD.ZG_DS2_en_csv_v2_301919.csv")
    gdp_df = pd.read_csv(uk_gdp_file, header=0)
    gdp_df = gdp_df[gdp_df["Country Name"] == "United Kingdom"]

    unpivot_df = gdp_df.melt(
        id_vars = ["Country Name", "Country Code", "Indicator Name", "Indicator Code"],
        var_name = "year",
        value_name = "uk_gdp_rate_yoy"
    )
    sdf = spark.createDataFrame(unpivot_df)
    bronze_df = sdf \
        .withColumnRenamed("Country Name", "country_name") \
        .withColumnRenamed("Country Code", "country_code") \
        .withColumnRenamed("Indicator Name", "indicator_name") \
        .withColumnRenamed("Indicator Code", "indicator_code") \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_system", lit("world_bank_group_dload"))
    
    try: 
        bronze_df.write.mode("overwrite").saveAsTable(f"{SCHEMA_NAME}.{UK_GDP}")
        print(f"Created bronze table {SCHEMA_NAME}.{UK_GDP}")
    except FileNotFoundError as ae:
        print(f"File not found for bronze table: {ae}")
    except Exception as e:
        print(f"Unexpected error saving bronze table {SCHEMA_NAME}.{UK_GDP}: {e}")


def bronze_uk_interest_rate(spark):
    """
    Loads and stores UK Bank of England interest rate data into the bronze layer.

    Reads csv file from `data/uk_interest_rate` into a pandas dataframe.
    Converts to a Spark DataFrame, and writes it to the bronze Delta table.

    Args:
        spark (SparkSession): Active Spark session used for writing the data.
    """
    uk_ir_file = os.path.abspath("../data/uk_interest_rate/Bank Rate history and data  Bank of England Database.csv")
    ir_df = pd.read_csv(uk_ir_file, header=0)
    ir_df = ir_df.rename(columns={"Date Changed": "date_changed"})

    try:
        bronze_df = spark.createDataFrame(ir_df)
        bronze_df = bronze_df \
            .withColumn("ingestion_timestamp", current_timestamp()) \
                .withColumn("source_system", lit("bank_of_england_dload"))
        bronze_df.write.mode("overwrite").saveAsTable(f"{SCHEMA_NAME}.{UK_INTEREST}")
        print(f"Created bronze table {SCHEMA_NAME}.{UK_INTEREST}")
    except FileNotFoundError as ae:
        print(f"File not found for bronze table: {ae}")
    except Exception as e:
        print(f"Unexpected error saving bronze table {SCHEMA_NAME}.{UK_INTEREST}: {e}")


def generate_bronze_macro_local_tables(spark):
    """
    Build bronze macroeconomic tables for UK data.

    Runs the ingestion functions for UK GDP and UK interest rate,
    pulling raw data from the data files stored locally, cleaning it, and
    saving each dataset into its bronze table.

    Args:
        spark (SparkSession): Active Spark session used to execute
            the ingestion and persistence steps.
    """
    bronze_uk_gdp(spark)
    bronze_uk_interest_rate(spark)
