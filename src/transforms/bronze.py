from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

def create_bronze_energy_prices():
    """Create bronze_energy_prices table."""
    
    SCHEMA_NAME = "oil_analytics"
    TABLE_NAME = "bronze_energy_prices"

    consumed_df = energy_consumer()

    schema = StructType([ 
        StructField("status", StringType(), True), 
        StructField("data", StructType([ 
            StructField("code", StringType(), True), 
            StructField("price", StringType(), True), 
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
    )


    bronze_energy_df = parsed_df.select(
        col("data.data.code").alias("code"),
        col("data.data.price").alias("price"),
        col("data.data.formatted").alias("formatted"),
        col("data.data.currency").alias("currency"),
        col("data.data.created_at").alias("created_at"),
        col("data.data.type").alias("type"),
        col("data.status").alias("status"),
        col("ingestion_timestamp")
    )
    # bronze_energy_df.write.mode("append").saveAsTable(f"{SCHEMA_NAME}.{TABLE_NAME}")