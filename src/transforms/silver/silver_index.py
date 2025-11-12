from pyspark.sql.functions import col, to_date, desc, format_number

# sp500_df = spark.table("oil_analytics.bronze_sp500")
# ftse100_df = spark.table("oil_analytics.bronze_ftse100")
# dollar_index_df = spark.table("oil_analytics.bronze_dollar_index")


def create_silver_tables(spark, schema="oil_analytics", tables=["bronze_sp500","bronze_ftse100","bronze_dollar_index"]):
    """
    Creates silver index tables.
        schema: oil_analytics
        table: silver_sp500, silver_ftse100, silver_dollar_index

    Args:
        spark: SparkSession
        schema: default = oil_analytics
        tables: default = ["bronze_sp500","bronze_ftse100","bronze_dollar_index"]
    """

    SCHEMA_NAME = "oil_analytics"

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

        silver_df = clean_df.select("Date", "Open", "High", "Low", "Close", "Volume", "source_system").orderBy(desc("Date"))

        try:
            silver_df.write.mode("overwrite").saveAsTable(f"{SCHEMA_NAME}.{silver_table_name}")
            print(f"Created silver table {SCHEMA_NAME}.{silver_table_name}")
        except AnalysisException as ae:
            print(f"Analysis error when saving silver table: {ae}")
        except Exception as e:
            print(f"Unexpected error saving silver table {SCHEMA_NAME}.{silver_table_name}: {e}")


def generate_silver_index_tables(spark):
    create_silver_tables(spark)






    # sp500_df = spark.table(f"{schema}.{table}")

    # clean_sp500_df = sp500_df \
    #     .withColumn("Date", to_date(col("Date"))) \
    #     .withColumn("Open", format_number(col("Open"), 2)) \
    #     .withColumn("High", format_number(col("High"), 2)) \
    #     .withColumn("Low", format_number(col("Low"), 2)) \
    #     .withColumn("Close", format_number(col("Close"), 2))

    # silver_sp500_df = clean_sp500_df.select("Date", "Open", "High", "Low", "Close", "Volume", "source_system").orderBy(desc("Date"))

    # try:
    #     silver_ep_df.write.mode("overwrite").saveAsTable(f"{SCHEMA_NAME}.{SILVER_TABLE_NAME}")
    #     print(f"Created silver table {SCHEMA_NAME}.{SILVER_TABLE_NAME}")
    # except AnalysisException as ae:
    #     print(f"Analysis error when saving silver table: {ae}")
    # except Exception as e:
    #     print(f"Unexpected error saving silver table {SCHEMA_NAME}.{SILVER_TABLE_NAME}: {e}")
