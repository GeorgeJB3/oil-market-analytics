from pyspark.sql.functions import *
from pyspark.sql.utils import AnalysisException
from pyspark.sql.window import Window


SCHEMA_NAME = "oil_analytics"

##### table names #####
silver_uk_unemployment = "silver_uk_unemployment"
silver_fed_unemployment = "silver_fed_unemployment"
silver_uk_cpi = "silver_uk_cpi"
silver_fed_cpi = "silver_fed_cpi"
silver_uk_gdp = "silver_uk_gdp"
silver_fed_gdp = "silver_fed_gdp"
silver_uk_interest_rate = "silver_uk_interest_rate"
silver_fed_interest_rate = "silver_fed_interest_rate"


def create_silver_uk_unemployment(spark):
    """ 
    Transform and load UK unemployment data into the silver layer.

    Reads the bronze-level UK unemployment table, filters for total unemployed aged 16 and over, 
    aggregates values to compute the unemployment rate, and pulls the start and end dates from period
    ranges. The curated dataset is enriched with source metadata and saved as the 
    silver-level table `oil_analytics.silver_uk_unemployment`.

    Args:
        spark (SparkSession): Active Spark session.
    """
    uk_unemployment_df = spark.table("oil_analytics.bronze_uk_unemployment")
    
    uk_unemployment_df = uk_unemployment_df \
        .filter((col("Economic_Activity") == "Total unemployed - aged 16 and over") & (col("Value_Type") == "Level") & (col("measures") \
            .like("Unemployment rates use employment plus%")))

    uk_unemployment_df = uk_unemployment_df \
        .groupBy("date") \
            .agg(avg("value").alias("uk_unemployment_rate"))

    silver_uk_unem_df = (
        uk_unemployment_df
        .withColumn("from_date", to_date(trim(split(col("date"), "-").getItem(0)), "MMM yyyy")) \
        .withColumn("to_date", to_date(trim(split(col("date"), "-").getItem(1)), "MMM yyyy"))   
        .withColumn("source_system", lit("nomisAPI"))
        .select("from_date", "to_date", "uk_unemployment_rate", "source_system").orderBy(desc("from_date"))    
    )

    try:
        silver_uk_unem_df.write.mode("overwrite").saveAsTable(f"{SCHEMA_NAME}.{silver_uk_unemployment}")
        print(f"Created silver table {SCHEMA_NAME}.{silver_uk_unemployment}")
    except AnalysisException as ae:
        print(f"Analysis error when saving silver table {SCHEMA_NAME}.{silver_uk_unemployment}: {ae}")
    except Exception as e:
        print(f"Unexpected error saving silver table {SCHEMA_NAME}.{silver_uk_unemployment}: {e}")


def create_silver_fed_unemployment(spark):
    """
    Transform and load US unemployment data into the silver layer.

    Reads the bronze-level US unemployment table, standises the data, removes ingestion
    metadata, and orders records by descending date. The curated dataset is saved as the 
    silver-level table `oil_analytics.silver_fed_unemployment`.
    
    Args:
        spark (SparkSession): Active Spark session.
    """
    fed_unemployment_df = spark.table("oil_analytics.bronze_fed_unemployment")

    silver_fed_unem_df = (   fed_unemployment_df
    .withColumn("date", to_date(col("date"))) \
    .withColumnRenamed("fed_unemployment_rate", "us_unemployment_rate") \
    .drop("ingestion_timestamp") \
    .orderBy(desc("date"))
    )
    try:
        silver_fed_unem_df.write.mode("overwrite").saveAsTable(f"{SCHEMA_NAME}.{silver_fed_unemployment}")
        print(f"Created silver table {SCHEMA_NAME}.{silver_fed_unemployment}")
    except AnalysisException as ae:
        print(f"Analysis error when saving silver table {SCHEMA_NAME}.{silver_fed_unemployment}: {ae}")
    except Exception as e:
        print(f"Unexpected error saving silver table {SCHEMA_NAME}.{silver_fed_unemployment}: {e}")


def create_silver_uk_cpi(spark):
    """
    Transform and load UK Consumer Price Index (CPI) data into the silver layer.

    Reads the bronze-level UK CPI table, standardises the data and selects relevant 
    attributes. The curated dataset is ordered by date and saved as the 
    silver-level table `oil_analytics.silver_uk_cpi`.
    
    Args:
        spark (SparkSession): Active Spark session.
    """
    uk_cpi_df = spark.table("oil_analytics.bronze_uk_cpi")
    uk_cpi_df = uk_cpi_df.filter(col("period").rlike(r"\d{4} (JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)"))

    silver_uk_cpi_df = (
        uk_cpi_df
        .withColumn("date", to_date(col("period"), "yyyy MMM"))
        .withColumnRenamed("rate", "uk_cpi_rate")
        .select("date", "uk_cpi_rate", "source_system")
        .orderBy(desc("date"))
        )
    try:
        silver_uk_cpi_df.write.mode("overwrite").saveAsTable(f"{SCHEMA_NAME}.{silver_uk_cpi}")
        print(f"Created silver table {SCHEMA_NAME}.{silver_uk_cpi}")
    except AnalysisException as ae:
        print(f"Analysis error when saving silver table {SCHEMA_NAME}.{silver_uk_cpi}: {ae}")
    except Exception as e:
        print(f"Unexpected error saving silver table {SCHEMA_NAME}.{silver_uk_cpi}: {e}")


def create_silver_fed_cpi(spark):
    """
    Transform and load US Consumer Price Index (CPI) data for the silver layer.

    Reads the bronze US CPI table, converts dates, renames the value column, 
    drops ingestion metadata, and sorts by date. Saves the result to 
    `oil_analytics.silver_fed_cpi`.
    
    Args:
        spark (SparkSession): Active Spark session.
    """
    window_spec = Window.orderBy("date")

    fed_cpi_df = spark.table("oil_analytics.bronze_fed_cpi")

    silver_fed_cpi_df = (
        fed_cpi_df
        .withColumn("date", to_date("date"))
        .withColumn("us_cpi_rate", (col("fed_cpi_rate") / lag("fed_cpi_rate", 12).over(window_spec) - 1) * 100)
        .drop("ingestion_timestamp", "fed_cpi_rate")
        .orderBy(desc("date"))
        )
    try:
        silver_fed_cpi_df.write.mode("overwrite").saveAsTable(f"{SCHEMA_NAME}.{silver_fed_cpi}")
        print(f"Created silver table {SCHEMA_NAME}.{silver_fed_cpi}")
    except AnalysisException as ae:
        print(f"Analysis error when saving silver table {SCHEMA_NAME}.{silver_fed_cpi}: {ae}")
    except Exception as e:
        print(f"Unexpected error saving silver table {SCHEMA_NAME}.{silver_fed_cpi}: {e}")


def create_silver_uk_gdp(spark):
    """
    Transform and load UK Gross Domestic Product (GDP) growth data for the silver layer.

    Reads the bronze UK GDP table, formats growth rates to two decimals, 
    filters valid years, and saves the result to `oil_analytics.silver_uk_gdp`.
    
    Args:
        spark (SparkSession): Active Spark session.
    """
    uk_gdp_df = spark.table("oil_analytics.bronze_uk_gdp")

    uk_gdp_df = uk_gdp_df.select("year", "uk_gdp_rate_yoy", "source_system") \
            .orderBy(desc("year"))

    silver_uk_gdp_df = uk_gdp_df.filter(col("year").rlike(r"\d{4}"))

    try:
        silver_uk_gdp_df.write.mode("overwrite").saveAsTable(f"{SCHEMA_NAME}.{silver_uk_gdp}")
        print(f"Created silver table {SCHEMA_NAME}.{silver_uk_gdp}")
    except AnalysisException as ae:
        print(f"Analysis error when saving silver table {SCHEMA_NAME}.{silver_uk_gdp}: {ae}")
    except Exception as e:
        print(f"Unexpected error saving silver table {SCHEMA_NAME}.{silver_uk_gdp}: {e}")


def create_silver_fed_gdp(spark):
    """
    Transform and load US Gross Domestic Product (GDP) data for the silver layer.

    Reads the bronze US GDP table, converts dates, renames the value column to 
    GDP in billions of USD, drops ingestion metadata, and sorts by date. 
    Saves the result to `oil_analytics.silver_fed_gdp`.
    
    Args:
        spark (SparkSession): Active Spark session.
    """
    fed_gdp_df = spark.table("oil_analytics.bronze_fed_gdp")

    silver_fed_gdp_df = (
        fed_gdp_df
        .withColumn("date", to_date("date"))
        .withColumnRenamed("fed_gdp_rate_yoy", "us_gdp_rate_yoy")
        .drop("ingestion_timestamp")
        .orderBy(desc("date"))
        )
    try:
        silver_fed_gdp_df.write.mode("overwrite").saveAsTable(f"{SCHEMA_NAME}.{silver_fed_gdp}")
        print(f"Created silver table {SCHEMA_NAME}.{silver_fed_gdp}")
    except AnalysisException as ae:
        print(f"Analysis error when saving silver table {SCHEMA_NAME}.{silver_fed_gdp}: {ae}")
    except Exception as e:
        print(f"Unexpected error saving silver table {SCHEMA_NAME}.{silver_fed_gdp}: {e}")


def create_silver_uk_interest_rate(spark):
    """
    Transform and load UK interest rate data for the silver layer.

    Reads the bronze UK interest rate table, converts dates, formats 
    rates to two decimals, and adds source info. Saves the result to
    `oil_analytics.silver_uk_interest_rate`.
    
    Args:
        spark (SparkSession): Active Spark session.
    """
    uk_interest_rate_df = spark.table("oil_analytics.bronze_uk_interest_rate")

    silver_uk_interest_rate_df = uk_interest_rate_df.select(
        to_date(col("date_changed"), "dd MMM yy").alias("date"),
        col("Rate").alias("uk_interest_rate"),
        col("source_system")
    )
    try:
        silver_uk_interest_rate_df.write.mode("overwrite").saveAsTable(f"{SCHEMA_NAME}.{silver_uk_interest_rate}")
        print(f"Created silver table {SCHEMA_NAME}.{silver_uk_interest_rate}")
    except AnalysisException as ae:
        print(f"Analysis error when saving silver table {SCHEMA_NAME}.{silver_uk_interest_rate}: {ae}")
    except Exception as e:
        print(f"Unexpected error saving silver table {SCHEMA_NAME}.{silver_uk_interest_rate}: {e}")


def create_silver_fed_interest_rate(spark):
    """
    Transform and load US interest rate data for the silver layer.

    Reads the bronze US interest rate table, converts dates, renames the 
    value column, drops ingestion metadata, and sorts by date.
    Saves the result to `oil_analytics.silver_fed_interest_rate`.
    
    Args:
        spark (SparkSession): Active Spark session.
    """
    fed_interest_rate_df = spark.table("oil_analytics.bronze_fed_interest_rate")

    silver_fed_interest_rate_df = (
        fed_interest_rate_df
        .withColumn("date", to_date("date"))
        .withColumnRenamed("fed_interest_rate", "us_interest_rate")
        .drop("ingestion_timestamp")
        .orderBy(desc("date"))
        )
    try:
        silver_fed_interest_rate_df.write.mode("overwrite").saveAsTable(f"{SCHEMA_NAME}.{silver_fed_interest_rate}")
        print(f"Created silver table {SCHEMA_NAME}.{silver_fed_interest_rate}")
    except AnalysisException as ae:
        print(f"Analysis error when saving silver table {SCHEMA_NAME}.{silver_fed_interest_rate}: {ae}")
    except Exception as e:
        print(f"Unexpected error saving silver table {SCHEMA_NAME}.{silver_fed_interest_rate}: {e}")


def generate_silver_macro_tables(spark):
    """
    Wrapper function to build silver macro table.
    
    Invokes the individual functions for unemployment, CPI, GDP, and
    interest rates (UK and US).

    Args:
        spark (SparkSession): Active Spark session.
    """
    create_silver_uk_unemployment(spark)
    create_silver_fed_unemployment(spark)
    create_silver_uk_cpi(spark)
    create_silver_fed_cpi(spark)
    create_silver_uk_gdp(spark)
    create_silver_fed_gdp(spark)
    create_silver_uk_interest_rate(spark)
    create_silver_fed_interest_rate(spark)
