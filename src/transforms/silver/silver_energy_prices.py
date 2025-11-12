from pyspark.sql.functions import regexp_replace, col, to_date, date_format, desc


def create_silver_energy_prices(spark, schema="oil_analytics", table="bronze_energy_prices"):
    """
    Create silver energy prices table.
        schema: oil_analytics
        table: silver_energy_prices

    Args:
        spark: SparkSession
        schema: default = oil_analytics
        table: default = bronze_energy_prices
    """

    SCHEMA_NAME = "oil_analytics"
    SILVER_TABLE_NAME = "silver_energy_prices"

    energy_price_df = spark.table(f"{schema}.{table}")

    clean_ep_df = energy_price_df \
        .withColumn("price", regexp_replace(col("formatted"), "\\$", "" ).cast("double")) \
        .withColumn("date", to_date(col("created_at"))) \
        .withColumn("time", date_format(col("created_at"), "HH:mm:ss"))

    clean_ep_df = clean_ep_df.dropDuplicates(["code", "price", "date", "time"])

    original_count = energy_price_df.count()
    deduped_count = clean_ep_df.count()
    removed_count = original_count - deduped_count
    print(f"Removed {removed_count} duplicate rows")

    silver_ep_df = clean_ep_df.select("code", "price", "type", "date", "time", "source_system").orderBy(desc("date"), desc("time"))

    try:
        silver_ep_df.write.mode("append").saveAsTable(f"{SCHEMA_NAME}.{SILVER_TABLE_NAME}")
        print(f"Created silver table {SCHEMA_NAME}.{SILVER_TABLE_NAME}")
    except AnalysisException as ae:
        print(f"Analysis error when saving silver table: {ae}")
    except Exception as e:
        print(f"Unexpected error saving silver table {SCHEMA_NAME}.{SILVER_TABLE_NAME}: {e}")


def generate_silver_energy_price_table(spark, schema, table):
    create_silver_energy_prices(spark, schema, table)
