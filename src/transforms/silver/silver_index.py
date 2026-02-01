from pyspark.sql.functions import col, to_date, desc, format_number, row_number
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException


def create_silver_index_tables(spark, schema="oil_analytics", tables=["bronze_sp500","bronze_ftse100","bronze_dollar_index"]):
    """
    Transform and load financial index data into the silver layer.

    This function iterates over the bronze-level index tables (S&P 500, FTSE 100, Dollar Index) 
    within the specified schema, applies standardised transformations to normalise values
    and selects relevant attributes. The curated datasets are ordered by date and persisted as 
    silver-level tables for downstream analysis.

    Args:
        spark (SparkSession): Active Spark session.
        schema: Source schema containing bronze tables. Default = oil_analytics
        tables: List of bronze table names. Default = ["bronze_sp500","bronze_ftse100","bronze_dollar_index"]
    """

    for table in tables:

        index = table.split("_")[1]
        silver_table_name = f"silver_{index}"
        if silver_table_name == "silver_dollar":
            silver_table_name = "silver_dollar_index"

        df = spark.table(f"{schema}.{table}")

        clean_df = df \
            .withColumn("Date", to_date(col("Date"))) \
            .withColumn("Open", format_number(col("Open"), 2)) \
            .withColumn("High", format_number(col("High"), 2)) \
            .withColumn("Low", format_number(col("Low"), 2)) \
            .withColumn("Close", format_number(col("Close"), 2))
        
        silver_df = clean_df.select("Date", "Open", "High", "Low", "Close", "Volume", "source_system")
        
        window_spec = Window.partitionBy("Date").orderBy(desc("Close")) 
        
        silver_df_clean = (
            silver_df
            .withColumn("row_num", row_number().over(window_spec))
            .filter(col("row_num") == 1)
            .drop("row_num")
        )

        silver_df_clean = silver_df_clean.orderBy(desc("Date"))

        try:
            silver_df_clean.write.mode("overwrite").saveAsTable(f"{schema}.{silver_table_name}")
            print(f"Created silver table {schema}.{silver_table_name}")
        except AnalysisException as ae:
            print(f"Analysis error when saving silver table {schema}.{silver_table_name}: {ae}")
        except Exception as e:
            print(f"Unexpected error saving silver table {schema}.{silver_table_name}: {e}")


def generate_silver_index_tables(spark):
    """
    Wrapper function to build silver energy price table.
    Invokes `create_silver_index_tables` function.

    Args:
        spark (SparkSession): Active Spark session.
    """
    create_silver_index_tables(spark)
