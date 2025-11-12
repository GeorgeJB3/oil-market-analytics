import logging

from src.ingestion.oil_api_ingest import fetch_oil_prices
from src.ingestion.kafka_consumer import energy_consumer
from pyspark.sql.functions import col, from_json, current_timestamp, lit
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType


def create_bronze_energy_prices(spark):
    """
    Create bronze energy prices table.
        schema: oil_analytics
        table: bronze_energy_prices
    """
    
    SCHEMA_NAME = "oil_analytics"
    TABLE_NAME = "bronze_energy_prices"

    logger = logging.getLogger(__name__)

    consumed_df = energy_consumer(spark)

    schema = StructType([ 
        StructField("status", StringType(), True), 
        StructField("data", StructType([ 
            StructField("code", StringType(), True), 
            StructField("price", IntegerType(), True), 
            StructField("formatted", StringType(), True), 
            StructField("currency", StringType(), True), 
            StructField("created_at", TimestampType(), True), 
            StructField("type", StringType(), True), ])),
        StructField("ingestion_timestamp", TimestampType(), True),])

    consumed_df = consumed_df.selectExpr("CAST(value AS STRING) as json_str")

    parsed_df = (
    consumed_df 
    .withColumn("data", from_json(col("json_str"), schema)) 
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_system", lit("OilPriceAPI"))
    )

    bronze_energy_df = parsed_df.select(
        col("data.data.code").alias("code"),
        col("data.data.price").alias("price"),
        col("data.data.formatted").alias("formatted"),
        col("data.data.currency").alias("currency"),
        col("data.data.created_at").alias("created_at"),
        col("data.data.type").alias("type"),
        col("data.status").alias("status"),
        col("ingestion_timestamp"),
        col("source_system")
    )

    try:
        bronze_energy_df.write.mode("append").saveAsTable(f"{SCHEMA_NAME}.{TABLE_NAME}")
        print(f"Created bronze table {SCHEMA_NAME}.{TABLE_NAME}")
    except AnalysisException as ae:
        print(f"Analysis error when saving bronze table: {ae}")
    except Exception as e:
        print(f"Unexpected error saving bronze table {SCHEMA_NAME}.{TABLE_NAME}: {e}")


def generate_bronze_energy_price_table(spark):
    """
    Invoke producer to ingest energy price data.
    Then consumer to write to the table.
    """
    fetch_oil_prices(spark, "energy_prices")
    create_bronze_energy_prices(spark)
    